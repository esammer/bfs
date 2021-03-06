package main

import (
	"bfs/service/blockservice"
	"bfs/client"
	"bfs/config"
	"bfs/server/blockserver"
	"bfs/server/nameserver"
	"bfs/util/logging"
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
	"sort"
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
	HostConfig         *config.HostConfig
	BlockServiceConfig *config.BlockServiceConfig
	NameServiceConfig  *config.NameServiceConfig

	nameServer  *nameserver.NameServer
	blockServer *blockserver.BlockServer

	PhysicalVolumes []*blockservice.PhysicalVolume
}

func (this *BFSServer) Run() error {
	if err := this.configure(); err != nil {
		return err
	}

	if err := this.start(); err != nil {
		return err
	}

	return nil
}

func (this *BFSServer) configure() error {
	var volumePaths ListValue
	var allowAutoInit bool
	var port int
	var hostLabels ListValue
	var nsPath string
	var hostId string

	serverFlags := flag.NewFlagSet("server", flag.ContinueOnError)
	serverFlags.Var(&volumePaths, "volume", "physical volume directory (repeatable)")
	serverFlags.BoolVar(&allowAutoInit, "auto-init", false, "allow auto-initialization of physical volumes")
	serverFlags.BoolVar(&allowAutoInit, "a", false, "allow auto-initialization of physical volumes")
	serverFlags.IntVar(&port, "port", 60000, "bind port")
	serverFlags.StringVar(&nsPath, "ns", "", "namespace directory")
	serverFlags.StringVar(&hostId, "id", "", "node id")
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
		var labels map[string]string

		if len(components) > 1 {
			labelStrs := strings.Split(components[1], ",")
			labels = make(map[string]string, len(labelStrs))

			for _, labelStr := range labelStrs {
				labelComponents := strings.Split(labelStr, "=")
				labels[labelComponents[0]] = labelComponents[1]
			}
		}

		glog.V(logging.LogLevelDebug).Infof("Configure volume path %s auto-initialize: %t labels: %v", components[0], allowAutoInit, labels)

		pvConfigs = append(pvConfigs, &config.PhysicalVolumeConfig{
			Path:                components[0],
			AllowAutoInitialize: allowAutoInit,
			Labels:              labels,
		})
	}

	parsedLabels := make(map[string]string, len(hostLabels))
	for _, label := range hostLabels {
		components := strings.Split(label, "=")
		parsedLabels[components[0]] = components[1]
	}

	var hostname string

	if hostId == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}

		hostId = hostname
		glog.V(logging.LogLevelTrace).Infof("No host id specified - discovered hostname %s", hostname)
	} else {
		hostname = hostId
		glog.V(logging.LogLevelTrace).Infof("Using host id %s as host identity", hostId)
	}

	bindIPs, err := net.LookupIP(hostname)
	if err != nil {
		return err
	}

	nsConfig := &config.NameServiceConfig{
		Hostname: hostname,
		GroupId:  "ns-shard-1",
		Path:     nsPath,
		Port:     int32(port),
		Nodes: []*config.NameServiceNodeConfig{
			{Id: hostId, Hostname: hostname, BindAddress: bindIPs[0].String(), ClientPort: 7000, PeerPort: 7001},
		},
	}
	bsConfig := &config.BlockServiceConfig{
		Hostname:      hostname,
		Port:          int32(port),
		VolumeConfigs: pvConfigs,
	}
	hostConfig := &config.HostConfig{
		Id:                 hostId,
		Hostname:           hostname,
		Port:               int32(port),
		Labels:             parsedLabels,
		NameServiceConfig:  nsConfig,
		BlockServiceConfig: bsConfig,
	}

	this.HostConfig = hostConfig
	this.NameServiceConfig = nsConfig
	this.BlockServiceConfig = bsConfig

	return nil
}

func (this *BFSServer) start() error {
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

	// Start keep alive process
	go func() {
		glog.V(logging.LogLevelTrace).Infof("Keep alive process starting")

		for pulse := range keepAliveChan {
			glog.V(4).Infof("Pulse: lease id: %v ttl: %d", pulse.ID, pulse.TTL)
		}

		glog.V(logging.LogLevelTrace).Info("Keep alive process complete")
	}()

	rpcServer := grpc.NewServer()

	bindAddress := fmt.Sprintf("%s:%d", this.HostConfig.Hostname, this.HostConfig.Port)
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		return fmt.Errorf("failed to bind to %s - %v", bindAddress, err)
	}

	this.blockServer = blockserver.New(this.BlockServiceConfig, rpcServer)
	if err := this.blockServer.Start(); err != nil {
		return err
	}

	this.nameServer = nameserver.New(this.NameServiceConfig, rpcServer)
	if err := this.nameServer.Start(); err != nil {
		return err
	}

	// Register or update host config
	_, err = etcdClient.Put(
		context.Background(),
		filepath.Join(client.DefaultEtcdPrefix, client.EtcdHostsPrefix, client.EtcdHostsConfigPrefix, this.HostConfig.Id),
		proto.MarshalTextString(this.HostConfig),
	)
	if err != nil {
		glog.Errorf("Unable to set or update host config - %v", err)
		return err
	}

	// Start signal handler routine
	signalChan := make(chan os.Signal, 8)
	go func() {
		for sig := range signalChan {
			switch sig {
			case os.Interrupt, syscall.SIGTERM:
				glog.Info("Shutting down")
				rpcServer.GracefulStop()
			default:
			}
		}
	}()
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	rpcQuitChan := make(chan error)
	ticker := time.NewTicker(10 * time.Second)

	// RPC server process
	go func() {
		err := rpcServer.Serve(listener)
		ticker.Stop()
		rpcQuitChan <- err
	}()

	// Start health and status routine
	go func() {
		for t := range ticker.C {
			volumeStats := make(map[string]*config.PhysicalVolumeStatus, len(this.BlockServiceConfig.VolumeConfigs))

			for _, pvConfig := range this.blockServer.Config.VolumeConfigs {
				fsStat := syscall.Statfs_t{}
				err := syscall.Statfs(pvConfig.Path, &fsStat)
				if err != nil {
					glog.Errorf("Unable to get filesystem info for %s - %v", pvConfig.Path, err)
					continue
				}

				devicePath := make([]rune, 0, 1024)
				for _, c := range fsStat.Mntfromname {
					if c == 0x0 {
						break
					}
					devicePath = append(devicePath, rune(c))
				}
				mountPath := make([]rune, 0, 1024)
				for _, c := range fsStat.Mntonname {
					if c == 0x0 {
						break
					}
					mountPath = append(mountPath, rune(c))
				}

				volumeStat := &config.PhysicalVolumeStatus{
					Id:   pvConfig.Id,
					Path: pvConfig.Path,
					FileSystemStatus: &config.FileSystemStatus{
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
				volumeStats[pvConfig.Id] = volumeStat
			}

			// Register for service.
			_, err = etcdClient.Put(
				context.Background(),
				filepath.Join(client.DefaultEtcdPrefix, client.EtcdHostsPrefix, client.EtcdHostsStatusPrefix, this.HostConfig.Id),
				proto.MarshalTextString(&config.HostStatus{
					Id:           this.HostConfig.Id,
					FirstSeen:    0,
					LastSeen:     t.UnixNano(),
					VolumeStatus: volumeStats,
				}),
				clientv3.WithLease(hostLease.ID),
			)
			if err != nil {
				glog.Errorf("Unable to report host status %s - %v", t.String(), err)
			}
		}
	}()
	glog.Infof("Server running on %s - use SIGINT to stop", bindAddress)

	err = <-rpcQuitChan
	if err != nil {
		glog.Errorf("RPC server failed - %v", err)
	}

	if err := this.nameServer.Stop(); err != nil {
		glog.Errorf("Stopping name server failed - %v", err)
	}

	if err := this.blockServer.Stop(); err != nil {
		glog.Errorf("Stopping block server failed - %v", err)
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

	lsFlags := flag.NewFlagSet("ls", flag.ContinueOnError)
	humanNumbers := lsFlags.Bool("H", false, "use human-friendly numbers")

	rmFlags := flag.NewFlagSet("rm", flag.ContinueOnError)
	rmRecursive := rmFlags.Bool("R", false, "recursively delete files under the given path")

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

		if err := lsFlags.Parse(clientArgs[1:]); err != nil {
			return err
		}

		if len(clientArgs) > 1 {
			startKey = clientArgs[1]
		}
		if len(clientArgs) > 2 {
			endKey = clientArgs[2]
		}

		resultChan := cli.List(startKey, endKey)
		for listEntry := range resultChan {
			if listEntry.Err != nil {
				return listEntry.Err
			}

			entry := listEntry.Entry
			var sizeStr string

			if *humanNumbers {
				sizeStr = size.Bytes(float64(entry.Size)).String()
			} else {
				sizeStr = fmt.Sprint(entry.Size)
			}

			fmt.Printf("%s %s %d %s %s\n",
				entry.Path,
				sizeStr,
				len(entry.Blocks),
				time.Unix(entry.Ctime.Seconds, entry.Ctime.Nanos).UTC().String(),
				time.Unix(entry.Mtime.Seconds, entry.Mtime.Nanos).UTC().String(),
			)
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

		fmt.Printf("%s %d (blocks: %d, replicas: %d, block-size: %d, created: %s, modified: %s)\n",
			entry.Path,
			entry.Size,
			len(entry.Blocks),
			entry.ReplicationLevel,
			entry.BlockSize,
			time.Unix(entry.Ctime.Seconds, entry.Ctime.Nanos).UTC().String(),
			time.Unix(entry.Mtime.Seconds, entry.Mtime.Nanos).UTC().String(),
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
		if err := rmFlags.Parse(clientArgs[1:]); err != nil {
			return err
		}

		clientArgs = rmFlags.Args()
		if len(clientArgs) == 0 {
			return errors.New("usage: rm <file>")
		}

		if deleted, err := cli.Remove(clientArgs[0], *rmRecursive); err != nil {
			return err
		} else {
			fmt.Printf("%d files removed\n", deleted)
		}
	case "pvs":
		hostConfigs := cli.Hosts()
		for _, hostConfig := range hostConfigs {
			for _, pv := range hostConfig.BlockServiceConfig.VolumeConfigs {
				fmt.Printf("%s %s", hostConfig.Hostname, pv.Id)

				if len(pv.Labels) > 0 {
					keys := make([]string, 0, len(pv.Labels))

					for k := range pv.Labels {
						keys = append(keys, k)
					}

					sort.Slice(keys, func(i, j int) bool {
						return strings.Compare(keys[i], keys[j]) == -1
					})

					for i := range keys {
						keys[i] = keys[i] + " = " + pv.Labels[keys[i]]
					}

					fmt.Printf(" [%s]", strings.Join(keys, ", "))
				}

				fmt.Println()
			}
		}
	case "lvs":
		lvs, err := cli.ListVolumes()
		if err != nil {
			return err
		}

		for _, lvConfig := range lvs {
			fmt.Printf("Logical volume: %s %s\n", lvConfig.Id, strings.Join(lvConfig.PvIds, ", "))
			for key, value := range lvConfig.Labels {
				fmt.Printf("%15s = %s\n", key, value)
			}
		}
	case "lvcreate":
		if len(clientArgs) != 4 {
			return errors.New("usage: lvcreate <id> <pv,pv,pv...> <key1=value1,key2=value2,...>")
		}

		labelPairs := strings.Split(clientArgs[3], ",")
		labels := make(map[string]string, len(labelPairs))

		for _, labelPair := range labelPairs {
			components := strings.Split(labelPair, "=")
			labels[components[0]] = components[1]
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
	case "hosts":
		hostConfigs := cli.Hosts()
		hostStatus := cli.HostStatus()

		for i, entry := range hostStatus {
			fmt.Printf("Host %d: %s\n%15s: %s\n%15s: %s (%s ago)\n%15s: %s (%s ago)\n%15s:\n",
				i+1,
				hostConfigs[i].Hostname,
				"id", entry.Id,
				"first seen", time.Unix(0, entry.FirstSeen).String(),
				time.Now().Sub(time.Unix(0, entry.FirstSeen)).Truncate(time.Millisecond).String(),
				"last seen", time.Unix(0, entry.LastSeen).String(),
				time.Now().Sub(time.Unix(0, entry.LastSeen)).Truncate(time.Millisecond).String(),
				"volumes",
			)

			for pvId, volumeStats := range entry.VolumeStatus {
				bytesTotal := size.Bytes(float64(
					volumeStats.FileSystemStatus.Blocks *
						uint64(volumeStats.FileSystemStatus.BlockSize),
				))
				bytesFree := size.Bytes(float64(
					volumeStats.FileSystemStatus.BlocksFree *
						uint64(volumeStats.FileSystemStatus.BlockSize),
				))

				fmt.Printf(
					"%18s: %s - path: %s fs: %.2fGB of %.02fGB (%.2f%%) free %.03fm of %.03fm files (%.2f%%) free %s at %s\n",
					pvId,
					volumeStats.Id,
					volumeStats.Path,
					bytesFree.ToGigabytes(),
					bytesTotal.ToGigabytes(),
					100* (float64(volumeStats.FileSystemStatus.BlocksFree) /
						float64(volumeStats.FileSystemStatus.Blocks)),
					float64(volumeStats.FileSystemStatus.FilesFree)/1000000,
					float64(volumeStats.FileSystemStatus.Files)/1000000,
					100* (float64(volumeStats.FileSystemStatus.FilesFree) /
						float64(volumeStats.FileSystemStatus.Files)),
					volumeStats.FileSystemStatus.DevicePath,
					volumeStats.FileSystemStatus.MountPath,
				)
			}

			fmt.Printf("%15s:\n", "labels")
			for key, value := range hostConfigs[i].Labels {
				fmt.Printf("%18s: %s\n", key, value)
			}
			fmt.Println()
		}
	default:
		return fmt.Errorf("unknown command %s", clientArgs[0])
	}

	glog.V(logging.LogLevelTrace).Infof("Client memory footprint: ~%.02fKB", float64(cli.Stats())/1024.0)

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

	switch err {
	case nil:
		// No error.
	case context.DeadlineExceeded:
		glog.Errorf("Error: Command timed out")
		os.Exit(1)
	default:
		glog.Errorf("Error: %s", err.Error())
		os.Exit(1)
	}
}
