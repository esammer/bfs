package main

import (
	"bfs/blockservice"
	"bfs/nameservice"
	"bfs/ns"
	"bfs/volume"
	"context"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Config struct {
	NamespacePath       string
	VolumePaths         ListValue
	AllowInitialization bool
	BindAddress         string

	PutFile   string
	GetFile   string
	StatPath  string
	BlockSize int
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
	clientFlags.StringVar(&config.PutFile, "put", "", "put file")
	clientFlags.StringVar(&config.GetFile, "get", "", "get file")
	clientFlags.StringVar(&config.StatPath, "stat", "", "stat a file")
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
			runClient(config)
		case "server":
			serverFlags.Parse(args[2:])
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

	if len(config.PutFile) != 0 {
		reader, err := os.Open(config.PutFile)
		if err != nil {
			glog.Errorf("Unable to open %s - %v", config.PutFile, err)
			return
		}
		defer reader.Close()

		pvIds, err := blockClient.Volumes(context.Background(), &blockservice.VolumeRequest{})
		if err != nil {
			glog.Errorf("Unable to get volume metadata - %v", err)
			return
		}

		writer := volume.NewWriter(nameClient, blockClient, pvIds.VolumeId, config.PutFile, config.BlockSize, nil)
		defer writer.Close()

		written, err := io.Copy(writer, reader)
		if err != nil {
			glog.Errorf("Failed to copy %s - %v", config.PutFile, err)
			return
		}

		glog.Infof("Copied %d bytes", written)
	} else if len(config.GetFile) != 0 {
		reader := volume.NewReader(nameClient, blockClient, config.GetFile)
		if err := reader.Open(); err != nil {
			glog.Errorf("Failed to open file reader - %v", err)
			return
		}
		defer reader.Close()

		writer, err := os.Create(config.GetFile + ".new")
		if err != nil {
			glog.Errorf("Unable to open %s.new for write - %v", config.GetFile, err)
			return
		}
		defer writer.Close()

		written, err := io.Copy(writer, reader)
		if err != nil {
			glog.Errorf("Unable to copy file - %v", err)
			return
		}

		glog.Infof("Copied %d bytes", written)
	} else if len(config.StatPath) != 0 {
		resp, err := nameClient.Get(context.Background(), &nameservice.GetRequest{Path: config.StatPath})
		if err != nil {
			glog.Errorf("Stat failed - %v", err)
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

	nameService := nameservice.New(namespace)
	blockService := blockservice.New(pvs)

	listener, err := net.Listen("tcp", config.BindAddress)
	if err != nil {
		glog.Errorf("Failed to bind to %s - %v", config.BindAddress, err)
		os.Exit(1)
	}

	server := grpc.NewServer()
	blockservice.RegisterBlockServiceServer(server, blockService)
	nameservice.RegisterNameServiceServer(server, nameService)

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

	rpcQuitChan := make(chan error)

	go func() {
		err := server.Serve(listener)
		rpcQuitChan <- err
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

/*
func main() {
	config := &Config{}

	flag.StringVar(&config.NamespacePath, "ns", "", "namespace directory")
	flag.BoolVar(&config.AllowInitialization, "autoinit", false, "allow auto-initialization of physical volumes")
	flag.Var(&config.VolumePaths, "volume", "physical volume directory (repeatable)")

	flag.Parse()

	if len(config.NamespacePath) == 0 {
		glog.Error("--ns is required")
		os.Exit(1)
	}
	if len(config.VolumePaths) == 0 {
		glog.Error("At least one --volume is required")
		os.Exit(1)
	}

	pvs := make([]*volume.PhysicalVolume, 0, len(config.VolumePaths))
	eventChannel := make(chan interface{}, 1024)

	namespace := ns.New(config.NamespacePath)

	go func() {
		for event := range eventChannel {
			switch e := event.(type) {
			case *block.BlockWriteEvent:
				e.ResponseChannel <- e
			}
		}
	}()

	for _, path := range config.VolumePaths {
		pv := volume.NewPhysicalVolume(path, eventChannel)
		pvs = append(pvs, pv)
	}

	for _, pv := range pvs {
		err := pv.Open(config.AllowInitialization)
		if err != nil {
			glog.Errorf("Failed to open volume %s - %s", pv.RootPath, err)
			os.Exit(1)
		}
	}

	fs := &volume.LocalFileSystem{
		Namespace:    namespace,
		Volumes:      []*volume.LogicalVolume{volume.NewLogicalVolume("/", pvs, eventChannel)},
		EventChannel: eventChannel,
	}

	if err := fs.Open(); err != nil {
		glog.Errorf("Unable to open filesystem - %s", err)
		os.Exit(1)
	}

	zeroBuf := bytes.Repeat([]byte{0}, 1024*1024)

	for i := 1; i <= 10; i++ {
		if writer, err := fs.OpenWrite(fmt.Sprintf("/tmp/%d", i), 256*1024); err != nil {
			glog.Fatalf("Unable to open writer - %v", err)
		} else {
			if _, err := writer.Write(zeroBuf); err != nil {
				glog.Errorf("Unable to write to file - %v", err)
			}

			if err := writer.Close(); err != nil {
				glog.Errorf("Unable to close writer for file - %v", err)
			}
		}
	}

	if err := fs.Close(); err != nil {
		glog.Errorf("Unable to close filesystem - %s", err)
		os.Exit(1)
	}

	for _, pv := range pvs {
		err := pv.Close()
		if err != nil {
			glog.Errorf("Failed to close volume %s - %s", pv.RootPath, err)
			os.Exit(1)
		}
	}

	close(eventChannel)

	glog.Flush()
}
*/
