package main

import (
	"bfs/blockservice"
	"bfs/file"
	"bfs/nameservice"
	"bfs/ns"
	"bfs/registryservice"
	"bfs/util/size"
	"context"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
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

func main() {
	config := &Config{}

	clientFlags := flag.NewFlagSet("client", flag.ContinueOnError)
	clientFlags.StringVar(&config.BindAddress, "server", "", "server host:port")
	clientFlags.IntVar(&config.BlockSize, "block-size", 0, "block size for write (in MB)")
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		clientFlags.Var(f.Value, f.Name, f.Usage)
	})

	serverFlags := flag.NewFlagSet("server", flag.ContinueOnError)
	serverFlags.StringVar(&config.NamespacePath, "ns", "", "namespace directory")
	serverFlags.StringVar(&config.BindAddress, "bind", "127.0.0.1:60000", "bind address")
	serverFlags.BoolVar(&config.AllowInitialization, "auto-init", false, "allow auto-initialization of physical volumes")
	serverFlags.BoolVar(&config.AllowInitialization, "a", false, "allow auto-initialization of physical volumes")
	serverFlags.Var(&config.VolumePaths, "volume", "physical volume directory (repeatable)")
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		serverFlags.Var(f.Value, f.Name, f.Usage)
	})

	flag.Parse()

	args := os.Args
	if len(args) > 2 {
		switch args[1] {
		case "client":
			clientFlags.Parse(args[2:])
			config.ExtraArgs = clientFlags.Args()
			runClient(config)
		case "server":
			serverFlags.Parse(args[2:])
			config.ExtraArgs = clientFlags.Args()
			runServer(config)
		}
	}
}

func runClient(config *Config) {
	conn, err := grpc.Dial(config.BindAddress, grpc.WithInsecure())
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

		writer, err := file.NewWriter(nameClient, blockClient, "1", config.ExtraArgs[2], config.BlockSize)
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
		resp, err := registryClient.Hosts(context.Background(), &registryservice.HostsRequest{})
		if err != nil {
			glog.Errorf("%s failed - %v", config.ExtraArgs[0], err)
			return
		}

		for i, entry := range resp.Hosts {
			fmt.Printf("Host %d: %s:%d\n%15s: %s\n%15s: %s (%s ago)\n%15s: %s (%s ago)\n%15s:\n",
				i+1,
				entry.Hostname,
				entry.Port,
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

			fmt.Println()
		}
	default:
		glog.Errorf("Unknown command %s", config.ExtraArgs[0])
	}
}

func runServer(config *Config) {
	if len(config.NamespacePath) == 0 {
		glog.Error("--ns is required")
		os.Exit(1)
	}
	if len(config.VolumePaths) == 0 {
		glog.Error("At least one --volume is required")
		os.Exit(1)
	}

	pvs := make([]*blockservice.PhysicalVolume, len(config.VolumePaths))
	for i, path := range config.VolumePaths {
		pvs[i] = blockservice.NewPhysicalVolume(path)
		pvs[i].Open(config.AllowInitialization)
	}

	namespace := ns.New(config.NamespacePath)
	namespace.Open()

	pvIds := make([]string, len(pvs))
	for i, pv := range pvs {
		pvIds[i] = pv.ID.String()
	}

	if _, err := namespace.Volume("1"); err != nil {
		glog.Infof("Initializing logical volume")

		if err := namespace.AddVolume("1", pvIds); err != nil {
			glog.Errorf("Unable to initialize logical volume - %v", err)
			os.Exit(1)
		}
	}

	nameService := nameservice.New(namespace)
	blockService := blockservice.New(pvs)
	registryService := registryservice.New()

	listener, err := net.Listen("tcp", config.BindAddress)
	if err != nil {
		glog.Errorf("Failed to bind to %s - %v", config.BindAddress, err)
		os.Exit(1)
	}

	server := grpc.NewServer()
	blockservice.RegisterBlockServiceServer(server, blockService)
	nameservice.RegisterNameServiceServer(server, nameService)
	registryservice.RegisterRegistryServiceServer(server, registryService)

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
	rpcQuitChan := make(chan error)

	go func() {
		err := server.Serve(listener)
		ticker.Stop()
		rpcQuitChan <- err
	}()

	go func() {
		hostname, portStr, err := net.SplitHostPort(config.BindAddress)
		if err != nil {
			glog.Errorf("Unable to determine host and port from %s - %v", config.BindAddress, err)
			return
		}

		port, err := strconv.ParseUint(portStr, 10, 32)
		if err != nil {
			glog.Errorf("Unable to parse port from %s - %v", portStr, err)
			return
		}

		for t := range ticker.C {
			volumeStats := make([]*registryservice.PhysicalVolumeStatus, 0, len(config.VolumePaths))

			for _, path := range config.VolumePaths {
				fsStat := syscall.Statfs_t{}
				err := syscall.Statfs(path, &fsStat)
				if err != nil {
					glog.Errorf("Unable to get filesystem info for %s - %v", path, err)
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
					Path: path,
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
				Id:          "1",
				Hostname:    hostname,
				Port:        uint32(port),
				VolumeStats: volumeStats,
			})

			if err != nil {
				glog.Errorf("Unable to report host status %s - %v", t.String(), err)
			}
		}
	}()
	glog.Infof("Server running on %s - use SIGINT to stop", config.BindAddress)

	err = <-rpcQuitChan
	if err != nil {
		glog.Errorf("RPC server failed - %v", err)
	}

	namespace.Close()

	for _, pv := range pvs {
		pv.Close()
	}
}
