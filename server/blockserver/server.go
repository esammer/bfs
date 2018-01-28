package blockserver

import (
	"bfs/service/blockservice"
	"bfs/config"
	"bfs/util/logging"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type BlockServer struct {
	Config      *config.BlockServiceConfig
	server      *grpc.Server
	bindAddress string

	PhysicalVolumes []*blockservice.PhysicalVolume
}

func New(config *config.BlockServiceConfig, server *grpc.Server) *BlockServer {
	return &BlockServer{
		Config:      config,
		server:      server,
		bindAddress: fmt.Sprintf("%s:%d", config.Hostname, config.Port),
	}
}

func (this *BlockServer) Start() error {
	glog.V(logging.LogLevelDebug).Infof("Starting block server %s", this.bindAddress)

	this.PhysicalVolumes = make([]*blockservice.PhysicalVolume, 0, len(this.Config.VolumeConfigs))

	for _, pvConfig := range this.Config.VolumeConfigs {
		pv := blockservice.NewPhysicalVolume(pvConfig.Path)

		if err := pv.Open(pvConfig.AllowAutoInitialize); err != nil {
			return err
		}

		// Hack for populating pv ID. Requiring mutability here sucks.
		pvConfig.Id = pv.ID.String()
		pvConfig.Labels["endpoint"] = this.bindAddress
		pvConfig.Labels["hostname"] = this.Config.Hostname
		this.PhysicalVolumes = append(this.PhysicalVolumes, pv)
	}

	blockService := blockservice.New(this.PhysicalVolumes)
	blockservice.RegisterBlockServiceServer(this.server, blockService)

	glog.V(logging.LogLevelDebug).Infof("Started block server %s", this.bindAddress)

	return nil
}

func (this *BlockServer) Stop() error {
	glog.V(logging.LogLevelDebug).Infof("Stopping block server %s", this.bindAddress)

	for _, pv := range this.PhysicalVolumes {
		if err := pv.Close(); err != nil {
			return err
		}
	}

	glog.V(logging.LogLevelDebug).Infof("Stopped block server %s", this.bindAddress)

	return nil
}
