package main

import (
	"bfs/blockservice"
	"bfs/client"
	"bfs/config"
	"bfs/nameservice"
	"bfs/ns"
	"bfs/registryservice"
	"bfs/util/size"
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

type Config struct {
	NamespacePath       string
	VolumePaths         ListValue
	AllowInitialization bool
	BindAddress         string

	BlockSize int

	ExtraArgs []string
}

type ListValue []string

func (this *ListValue) Set(value string) error {
	*this = append(*this, value)
	return nil
}

func (this *ListValue) String() string {
	return strings.Join(*this, ",")
}

type BFSServer struct {
	HostConfig            *config.HostConfig
	BlockServiceConfig    *config.BlockServiceConfig
	NameServiceConfig     *config.NameServiceConfig
	RegistryServiceConfig *config.RegistryServiceConfig

	PhysicalVolumes []*blockservice.PhysicalVolume
}

func (this *BFSServer) Run() error {
	this.configure()
	this.start()

	return nil
}

func (this *BFSServer) configure() error {
	hostConfig := &config.HostConfig{}
	nsConfig := &config.NameServiceConfig{}
	rsConfig := &config.RegistryServiceConfig{}
	bsConfig := &config.BlockServiceConfig{}

	var volumePaths ListValue
	var allowAutoInit bool
	var bindAddress string
	var hostLabels ListValue

	serverFlags := flag.NewFlagSet("server", flag.ContinueOnError)
	serverFlags.Var(&volumePaths, "volume", "physical volume directory (repeatable)")
	serverFlags.BoolVar(&allowAutoInit, "auto-init", false, "allow auto-initialization of physical volumes")
	serverFlags.BoolVar(&allowAutoInit, "a", false, "allow auto-initialization of physical volumes")
	serverFlags.StringVar(&bindAddress, "bind", "127.0.0.1:60000", "bind address")
	serverFlags.StringVar(&nsConfig.Path, "ns", "", "namespace directory")
	serverFlags.StringVar(&hostConfig.Id, "id", "", "node id")
	serverFlags.Var(&hostLabels, "label", "host labels")

	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		serverFlags.Var(f.Value, f.Name, f.Usage)
	})

	if err := serverFlags.Parse(os.Args[2:]); err != nil {
		return err
	}

	flag.Parse()

	pvConfigs := make([]*config.PhysicalVolumeConfig, 0, len(volumePaths))

	for _, pathSpec := range volumePaths {
		components := strings.Split(pathSpec, ":")
		var labels []*config.Label

		if len(components) > 1 {
			labelStrs := strings.Split(components[1], ",")
			labels = make([]*config.Label, 0, len(labelStrs))

			for _, labelStr := range labelStrs {
				labelComponents := strings.Split(labelStr, "=")
				labels = append(labels, &config.Label{
					Key:   labelComponents[0],
					Value: labelComponents[1],
				})
			}
		}

		glog.V(1).Infof("Configure volume path %s auto-initialize: %t labels: %v", components[0], allowAutoInit, labels)

		pvConfigs = append(pvConfigs, &config.PhysicalVolumeConfig{
			Path:                components[0],
			AllowAutoInitialize: allowAutoInit,
			Labels:              labels,
		})
	}

	bsConfig.BindAddress = bindAddress
	bsConfig.VolumeConfigs = pvConfigs

	nsConfig.BindAddress = bindAddress
	nsConfig.AdvertiseAddress = bindAddress

	rsConfig.BindAddress = bindAddress
	rsConfig.AdvertiseAddress = bindAddress

	this.BlockServiceConfig = bsConfig
	this.NameServiceConfig = nsConfig
	this.RegistryServiceConfig = rsConfig

	if len(this.NameServiceConfig.Path) == 0 {
		return errors.New("--ns is required")
	}

	if len(this.BlockServiceConfig.VolumeConfigs) == 0 {
		return errors.New("at least one --volume is required")
	}

	parsedLabels := make([]*config.Label, len(hostLabels))
	for i, label := range hostLabels {
		components := strings.Split(label, "=")

		parsedLabels[i] = &config.Label{
			Key:   components[0],
			Value: components[1],
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	hostConfig.NameServiceConfig = nsConfig
	hostConfig.BlockServiceConfig = bsConfig
	hostConfig.RegistryServiceConfig = rsConfig
	hostConfig.Labels = parsedLabels
	hostConfig.Hostname = hostname
	if hostConfig.Id == "" {
		hostConfig.Id = hostname
	}

	this.HostConfig = hostConfig

	return nil
}

func (this *BFSServer) start() error {
	// Assemble the block service
	bsConfig := this.BlockServiceConfig
	pvs := make([]*blockservice.PhysicalVolume, len(bsConfig.VolumeConfigs))

	for i, pvConfig := range bsConfig.VolumeConfigs {
		pvs[i] = blockservice.NewPhysicalVolume(pvConfig.Path)
		pvs[i].Open(pvConfig.AllowAutoInitialize)
		bsConfig.VolumeConfigs[i].Id = pvs[i].ID.String()
	}

	// Assemble the name service
	nsConfig := this.NameServiceConfig
	namespace := ns.New(nsConfig.Path)
	if err := namespace.Open(); err != nil {
		return err
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:        []string{"localhost:2379"},
		DialOptions:      []grpc.DialOption{grpc.WithBlock(), grpc.WithInsecure()},
		AutoSyncInterval: 5 * time.Minute,
	})
	if err != nil {
		return err
	}
	defer etcdClient.Close()

	hostLease, err := etcdClient.Grant(context.Background(), 10)
	if err != nil {
		return err
	}

	keepAliveChan, err := etcdClient.KeepAlive(context.Background(), hostLease.ID)
	if err != nil {
		return err
	}

	go func() {
		glog.V(2).Infof("Keep alive process starting")

		for pulse := range keepAliveChan {
			glog.V(2).Infof("Pulse: lease id: %v ttl: %d", pulse.ID, pulse.TTL)
		}

		glog.V(2).Info("Keep alive process complete")
	}()

	_, err = etcdClient.Put(
		context.Background(),
		filepath.Join(registryservice.DefaultEtcdPrefix, registryservice.EtcdHostsPrefix, this.HostConfig.Id),
		proto.MarshalTextString(this.HostConfig),
		clientv3.WithLease(hostLease.ID),
	)
	if err != nil {
		return err
	}

	// Create the services
	nameService := nameservice.New(namespace)
	blockService := blockservice.New(pvs)
	registryService := registryservice.New(etcdClient)

	// Register services with the RPC server
	server := grpc.NewServer()
	blockservice.RegisterBlockServiceServer(server, blockService)
	nameservice.RegisterNameServiceServer(server, nameService)
	registryservice.RegisterRegistryServiceServer(server, registryService)

	listener, err := net.Listen("tcp", nsConfig.BindAddress)
	if err != nil {
		fmt.Errorf("failed to bind to %s - %v", nsConfig.BindAddress, err)
	}

	// Start signal handler routine
	signalChan := make(chan os.Signal, 8)
	go func() {
		for sig := range signalChan {
			switch sig {
			case os.Interrupt, syscall.SIGTERM:
				glog.Info("Shutting down")
				server.GracefulStop()
			default:
			}
		}
	}()
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(10 * time.Second)
	quitChan := make(chan error)

	go func() {
		err := server.Serve(listener)
		ticker.Stop()
		quitChan <- err
	}()

	_, err = registryService.RegisterHost(context.Background(), &registryservice.RegisterHostRequest{
		HostConfig: this.HostConfig,
	})
	if err != nil {
		return err
	}

	// Start health and status routine
	go func() {
		for t := range ticker.C {
			volumeStats := make([]*registryservice.PhysicalVolumeStatus, 0, len(bsConfig.VolumeConfigs))

			for _, pvConfig := range bsConfig.VolumeConfigs {
				fsStat := syscall.Statfs_t{}
				err := syscall.Statfs(pvConfig.Path, &fsStat)
				if err != nil {
					glog.Errorf("Unable to get filesystem info for %s - %v", pvConfig.Path, err)
					continue
				}

				devicePath := make([]rune, 0, 1024)
				for _, c := range fsStat.Mntfromname {
					devicePath = append(devicePath, rune(c))
				}
				mountPath := make([]rune, 0, 1024)
				for _, c := range fsStat.Mntonname {
					mountPath = append(mountPath, rune(c))
				}

				volumeStat := &registryservice.PhysicalVolumeStatus{
					Path: pvConfig.Path,
					FileSystemStatus: &registryservice.FileSystemStatus{
						MountPath:       string(mountPath),
						DevicePath:      string(devicePath),
						IoSize:          fsStat.Iosize,
						Files:           fsStat.Files,
						FilesFree:       fsStat.Ffree,
						Blocks:          fsStat.Blocks,
						BlockSize:       fsStat.Bsize,
						BlocksAvailable: fsStat.Bavail,
						BlocksFree:      fsStat.Bfree,
					},
				}
				volumeStats = append(volumeStats, volumeStat)
			}

			_, err := registryService.HostStatus(context.Background(), &registryservice.HostStatusRequest{
				Id:          this.HostConfig.Id,
				VolumeStats: volumeStats,
			})

			if err != nil {
				glog.Errorf("Unable to report host status %s - %v", t.String(), err)
			}
		}
	}()
	glog.Infof("Server running on %s - use SIGINT to stop", nsConfig.BindAddress)

	err = <-quitChan
	if err != nil {
		glog.Errorf("RPC server failed - %v", err)
	}

	if err := namespace.Close(); err != nil {
		return err
	}

	for _, pv := range pvs {
		if err := pv.Close(); err != nil {
			return err
		}
	}

	return nil
}

type BFSClient struct {
	Client *client.Client
}

func (this *BFSClient) Run() error {
	var etcdEndpoints string
	var blockSize int

	clientFlags := flag.NewFlagSet("client", flag.ContinueOnError)
	clientFlags.StringVar(&etcdEndpoints, "etcd", "http://localhost:2379", "comma separated list of etcd host:port")
	clientFlags.IntVar(&blockSize, "block-size", 8, "block size for write (in MB)")
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		clientFlags.Var(f.Value, f.Name, f.Usage)
	})

	clientFlags.Parse(os.Args[2:])

	flag.Parse()
	clientArgs := clientFlags.Args()

	cli, err := client.New(strings.Split(etcdEndpoints, ","))
	if err != nil {
		return err
	}

	switch clientArgs[0] {
	case "ls":
		startKey := ""
		endKey := ""

		if len(clientArgs) > 1 {
			startKey = clientArgs[1]
		}
		if len(clientArgs) > 2 {
			endKey = clientArgs[2]
		}

		resultChan := cli.List(startKey, endKey)
		for entry := range resultChan {
			fmt.Printf("%s %d %d\n", entry.Path, entry.Size, len(entry.Blocks))
		}
	case "put":
		if len(clientArgs) != 3 {
			return errors.New("usage: put <source file> <dest file>")
		}

		reader, err := os.Open(clientArgs[1])
		if err != nil {
			return err
		}
		defer reader.Close()

		writer, err := cli.Create(clientArgs[2], blockSize*size.MB)
		if err != nil {
			return err
		}

		writeLen, err := io.Copy(writer, reader)
		if err != nil && err != io.EOF {
			return err
		}

		if err := writer.Close(); err != nil {
			return err
		}

		fmt.Printf("Copied %s -> %s (%d bytes)\n", clientArgs[1], clientArgs[2], writeLen)
	case "get":
		if len(clientArgs) != 3 {
			return errors.New("usage: get <source file> <dest file>")
		}

		reader, err := cli.Open(clientArgs[1])
		if err != nil {
			return err
		}
		defer reader.Close()

		writer, err := os.Create(clientArgs[2])
		if err != nil {
			return err
		}

		writeLen, err := io.Copy(writer, reader)
		if err != nil && err != io.EOF {
			return err
		}

		if err := writer.Close(); err != nil {
			return err
		}

		fmt.Printf("Copied %s -> %s (%d bytes)\n", clientArgs[1], clientArgs[2], writeLen)
	case "stat":
		if len(clientArgs) != 2 {
			return errors.New("usage: stat <file>")
		}

		entry, err := cli.Stat(clientArgs[1])
		if err != nil {
			return err
		}

		fmt.Printf("%s %d (%d blocks, %d replica(s), %d block size)\n",
			entry.Path,
			entry.Size,
			len(entry.Blocks),
			entry.ReplicationLevel,
			entry.BlockSize,
		)

		for i, block := range entry.Blocks {
			fmt.Printf("  %3d: block: %s pv: %s\n", i, block.BlockId, block.PvId)
		}
	case "mv":
		if len(clientArgs) != 3 {
			return errors.New("usage: mv <source file> <dest file>")
		}

		if err := cli.Rename(clientArgs[1], clientArgs[2]); err != nil {
			return err
		}
	case "rm":
		if len(clientArgs) < 2 {
			return errors.New("usage: rm <file> [file...]")
		}

		if err := cli.Remove(clientArgs[1]); err != nil {
			return err
		}
	case "pvs":
		hostConfigs := cli.Hosts()
		for _, hostConfig := range hostConfigs {
			for _, pv := range hostConfig.BlockServiceConfig.VolumeConfigs {
				fmt.Printf("%s %s\n", pv.Id, hostConfig.Hostname)
			}
		}
	case "lvs":
		lvs, err := cli.ListVolumes()
		if err != nil {
			return err
		}

		for _, lvConfig := range lvs {
			fmt.Printf("Logical volume: %s %s\n", lvConfig.Id, strings.Join(lvConfig.PvIds, ", "))
			for _, label := range lvConfig.Labels {
				fmt.Printf("%15s = %s\n", label.Key, label.Value)
			}
		}
	case "lvcreate":
		if len(clientArgs) != 4 {
			return errors.New("usage: lvcreate <id> <pv,pv,pv...> <key1=value1,key2=value2,...>")
		}

		labelPairs := strings.Split(clientArgs[3], ",")
		labels := make([]*config.Label, len(labelPairs))

		for i, labelPair := range labelPairs {
			components := strings.Split(labelPair, "=")
			labels[i] = &config.Label{Key: components[0], Value: components[1]}
		}

		err := cli.CreateLogicalVolume(&config.LogicalVolumeConfig{
			Id:     clientArgs[1],
			PvIds:  strings.Split(clientArgs[2], ","),
			Labels: labels,
		})
		if err != nil {
			return err
		}

		fmt.Printf("Created logical volume: %s\n", clientArgs[1])
	case "lvdestroy":
		if len(clientArgs) != 2 {
			return errors.New("usage: lvdestroy <id>")
		}

		ok, err := cli.DeleteLogicalVolume(clientArgs[1])
		if err != nil {
			return err
		}

		if ok {
			fmt.Printf("Deleted volume %s\n", clientArgs[1])
		} else {
			fmt.Printf("No such volume %s\n", clientArgs[1])
		}
	default:
		return fmt.Errorf("unknown command %s", clientArgs[0])
	}

	glog.V(2).Infof("Client memory footprint: ~%.02fKB", float64(cli.Stats())/1024.0)

	return nil
}

func main() {
	args := os.Args

	var err error

	if len(args) > 2 {
		switch args[1] {
		case "client":
			cli := &BFSClient{}
			err = cli.Run()
		case "server":
			server := &BFSServer{}
			err = server.Run()
		default:
			err = fmt.Errorf("unknown command %s", args[1])
		}
	} else {
		err = fmt.Errorf("usage: %s <client | server> [command options...]", args[0])
	}

	if err != nil {
		glog.Errorf("Error: %s", err.Error())
		os.Exit(1)
	}
}

/*
func runClient(config *Config) {
	conn, err := grpc.Dial(config.BindAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		glog.Errorf("Failed to connect to server %s - %v", config.BindAddress, err)
	}
	defer conn.Close()

	nameClient := nameservice.NewNameServiceClient(conn)
	blockClient := blockservice.NewBlockServiceClient(conn)
	registryClient := registryservice.NewRegistryServiceClient(conn)

	switch config.ExtraArgs[0] {
	case "rm":
		for _, path := range config.ExtraArgs[1:] {
			_, err := nameClient.Delete(context.Background(), &nameservice.DeleteRequest{Path: path})
			if err != nil {
				glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
				return
			}
		}
	case "mv":
		_, err := nameClient.Rename(context.Background(), &nameservice.RenameRequest{
			SourcePath:      config.ExtraArgs[1],
			DestinationPath: config.ExtraArgs[2],
		})
		if err != nil {
			glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
			return
		}
	case "stat":
		resp, err := nameClient.Get(context.Background(), &nameservice.GetRequest{Path: config.ExtraArgs[1]})
		if err != nil {
			glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
			return
		}

		fmt.Printf("%s %d (%d blocks, %d replica(s), %d block size)\n",
			resp.Entry.Path,
			resp.Entry.Size,
			len(resp.Entry.Blocks),
			resp.Entry.ReplicationLevel,
			resp.Entry.BlockSize,
		)

		for i, block := range resp.Entry.Blocks {
			fmt.Printf("  %3d: block: %s pv: %s\n", i, block.BlockId, block.PvId)
		}
	case "put":
		reader, err := os.Open(config.ExtraArgs[1])
		if err != nil {
			glog.Errorf("Unable to open %s - %v", config.ExtraArgs[1], err)
			return
		}
		defer reader.Close()

		// FIXME
		writer, err := file.NewWriter(nameClient, blockClient, nil, config.ExtraArgs[2], config.BlockSize)
		if err != nil {
			glog.Errorf("Unable to open writer %s - %v", config.ExtraArgs[2], err)
			return
		}
		defer writer.Close()

		written, err := io.Copy(writer, reader)
		if err != nil {
			glog.Errorf("Failed to copy %s to %s - %v", config.ExtraArgs[1], config.ExtraArgs[2], err)
			return
		}

		glog.Infof("Copied %d bytes", written)
	case "get":
		reader := file.NewReader(nameClient, blockClient, config.ExtraArgs[1])
		if err := reader.Open(); err != nil {
			glog.Errorf("Failed to open file reader - %v", err)
			return
		}
		defer reader.Close()

		writer, err := os.Create(config.ExtraArgs[2])
		if err != nil {
			glog.Errorf("Unable to open %s for write - %v", config.ExtraArgs[2], err)
			return
		}
		defer writer.Close()

		written, err := io.Copy(writer, reader)
		if err != nil {
			glog.Errorf("Unable to copy file - %v", err)
			return
		}

		glog.Infof("Copied %d bytes", written)
	case "ls":
		stream, err := nameClient.List(context.Background(), &nameservice.ListRequest{
			StartKey: config.ExtraArgs[1],
			EndKey:   config.ExtraArgs[2],
		})
		if err != nil {
			glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
			return
		}
		defer stream.CloseSend()

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			} else if err != nil {
				glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
				return
			}

			for _, entry := range resp.Entries {
				fmt.Printf("%s %d %d %d\n", entry.Path, entry.Size, entry.BlockSize, len(entry.Blocks))
			}
		}
	case "hosts":
		var sel string
		if len(config.ExtraArgs) == 2 {
			sel = config.ExtraArgs[1]
		}

		resp, err := registryClient.Hosts(context.Background(), &registryservice.HostsRequest{Selector: sel})
		if err != nil {
			glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
			return
		}

		for i, entry := range resp.Hosts {
			fmt.Printf("Host %d: %s\n%15s: %s\n%15s: %s (%s ago)\n%15s: %s (%s ago)\n%15s:\n",
				i+1,
				resp.HostConfigs[i].Hostname,
				"id", entry.Id,
				"first seen", time.Unix(0, entry.FirstSeen).String(),
				time.Now().Sub(time.Unix(0, entry.FirstSeen)).Truncate(time.Millisecond).String(),
				"last seen", time.Unix(0, entry.LastSeen).String(),
				time.Now().Sub(time.Unix(0, entry.LastSeen)).Truncate(time.Millisecond).String(),
				"volumes",
			)

			for j, volumeStats := range entry.VolumeStats {
				bytesTotal := size.Bytes(float64(
					resp.Hosts[i].VolumeStats[j].FileSystemStatus.Blocks *
						uint64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.BlockSize),
				))
				bytesFree := size.Bytes(float64(
					resp.Hosts[i].VolumeStats[j].FileSystemStatus.BlocksFree *
						uint64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.BlockSize),
				))

				fmt.Printf(
					"%18d: %s - path: %s fs: %.2fGB of %.02fGB (%.2f%%) free %.03fm of %.03fm files (%.2f%%) free %s at %s\n",
					j+1,
					volumeStats.Id,
					resp.Hosts[i].VolumeStats[j].Path,
					bytesFree.ToGigabytes(),
					bytesTotal.ToGigabytes(),
					100* (float64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.BlocksFree) /
						float64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.Blocks)),
					float64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.FilesFree)/1000000,
					float64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.Files)/1000000,
					100* (float64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.FilesFree) /
						float64(resp.Hosts[i].VolumeStats[j].FileSystemStatus.Files)),
					resp.Hosts[i].VolumeStats[j].FileSystemStatus.DevicePath,
					resp.Hosts[i].VolumeStats[j].FileSystemStatus.MountPath,
				)
			}

			fmt.Printf("%15s:\n", "labels")
			for _, label := range resp.HostConfigs[i].Labels {
				fmt.Printf("%18s: %s\n", label.Key, label.Value)
			}
			fmt.Println()
		}
	default:
		glog.Errorf("Unknown command %s", config.ExtraArgs[0])
	}
}
*/
