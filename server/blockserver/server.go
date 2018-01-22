package blockserver

import (
	"bfs/blockservice"
	"bfs/config"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type BlockServer struct {
	Config *config.BlockServiceConfig
	server *grpc.Server

	PhysicalVolumes []*blockservice.PhysicalVolume
}

func New(config *config.BlockServiceConfig, server *grpc.Server) *BlockServer {
	return &BlockServer{
		Config: config,
		server: server,
	}
}

func (this *BlockServer) Start() error {
	glog.V(1).Infof("Starting block server %s", this.Config.BindAddress)

	this.PhysicalVolumes = make([]*blockservice.PhysicalVolume, 0, len(this.Config.VolumeConfigs))

	for _, pvConfig := range this.Config.VolumeConfigs {
		pv := blockservice.NewPhysicalVolume(pvConfig.Path)

		if err := pv.Open(pvConfig.AllowAutoInitialize); err != nil {
			return err
		}

		// Hack for populating pv ID. Requiring mutability here sucks.
		pvConfig.Id = pv.ID.String()
		this.PhysicalVolumes = append(this.PhysicalVolumes, pv)
	}

	blockService := blockservice.New(this.PhysicalVolumes)
	blockservice.RegisterBlockServiceServer(this.server, blockService)

	glog.V(1).Infof("Started block server %s", this.Config.BindAddress)

	return nil
}

func (this *BlockServer) Stop() error {
	glog.V(1).Infof("Stopping block server %s", this.Config.BindAddress)

	for _, pv := range this.PhysicalVolumes {
		if err := pv.Close(); err != nil {
			return err
		}
	}

	glog.V(1).Infof("Stopped block server %s", this.Config.BindAddress)

	return nil
}
