package blockserver

import (
	"bfs/blockservice"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"net"
)

type BlockServer struct {
	Paths       []string
	BindAddress string
	Options     []grpc.ServerOption

	PhysicalVolumes []*blockservice.PhysicalVolume

	server      *grpc.Server
	rpcDoneChan chan error
}

func (this *BlockServer) Start() error {
	glog.V(1).Infof("Starting block server %s", this.BindAddress)

	this.PhysicalVolumes = make([]*blockservice.PhysicalVolume, len(this.Paths))

	this.rpcDoneChan = make(chan error)

	for i, path := range this.Paths {
		this.PhysicalVolumes[i] = blockservice.NewPhysicalVolume(path)

		if err := this.PhysicalVolumes[i].Open(true); err != nil {
			for j := 0; j < i; j++ {
				this.PhysicalVolumes[j].Close()
			}

			return err
		}
	}

	blockService := blockservice.New(this.PhysicalVolumes)
	listener, err := net.Listen("tcp", this.BindAddress)
	if err != nil {
		return err
	}

	this.server = grpc.NewServer(this.Options...)
	blockservice.RegisterBlockServiceServer(this.server, blockService)

	go func() {
		glog.V(1).Infof("Starting block server RPC")

		err := this.server.Serve(listener)
		if err != nil {
			glog.Errorf("RPC server failed - %v", err)
		}

		glog.V(1).Infof("Stopped block server RPC")
		this.rpcDoneChan <- err
	}()

	glog.V(1).Infof("Started block server %s", this.BindAddress)

	return nil
}

func (this *BlockServer) Stop() error {
	glog.V(1).Infof("Stopping block server %s", this.BindAddress)

	this.server.GracefulStop()
	err := <-this.rpcDoneChan
	if err != nil {
		return err
	}

	for _, pv := range this.PhysicalVolumes {
		if err := pv.Close(); err != nil {
			return err
		}
	}

	glog.V(1).Infof("Stopped block server %s", this.BindAddress)

	return nil
}
