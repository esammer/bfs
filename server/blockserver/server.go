package blockserver

import (
	"bfs/config"
	"bfs/service/blockservice"
	"bfs/util/fsm"
	"bfs/util/logging"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

const (
	StateInitial = "INITIAL"
	StateRunning = "RUNNING"
	StateStopped = "STOPPED"
	StateError   = "ERROR"
)

var serviceFSM = fsm.New(StateInitial).
	Allow(StateInitial, StateRunning).
	Allow(StateRunning, StateStopped).
	Allow(StateRunning, StateError).
	Allow(StateError, StateStopped).
	Allow(StateError, StateError)

type BlockServer struct {
	Config      *config.BlockServiceConfig
	server      *grpc.Server
	bindAddress string
	fsm         *fsm.FSMInstance

	PhysicalVolumes []*blockservice.PhysicalVolume
}

func New(config *config.BlockServiceConfig, server *grpc.Server) *BlockServer {
	return &BlockServer{
		Config:      config,
		server:      server,
		bindAddress: fmt.Sprintf("%s:%d", config.Hostname, config.Port),
		fsm:         serviceFSM.NewInstance(),
	}
}

func (this *BlockServer) Start() error {
	glog.V(logging.LogLevelDebug).Infof("Starting block server %s", this.bindAddress)

	if err := this.fsm.Is(StateInitial); err != nil {
		return err
	}

	this.PhysicalVolumes = make([]*blockservice.PhysicalVolume, 0, len(this.Config.VolumeConfigs))

	for _, pvConfig := range this.Config.VolumeConfigs {
		pv := blockservice.NewPhysicalVolume(pvConfig.Path)

		if err := pv.Open(pvConfig.AllowAutoInitialize); err != nil {
			return this.fsm.ToWithErr(StateError, err)
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

	return this.fsm.To(StateRunning)
}

func (this *BlockServer) Stop() error {
	glog.V(logging.LogLevelDebug).Infof("Stopping block server %s", this.bindAddress)

	if err := this.fsm.IsOneOf(StateRunning, StateError); err != nil {
		return err
	}

	if this.PhysicalVolumes != nil {
		for _, pv := range this.PhysicalVolumes {
			if err := pv.Close(); err != nil {
				return this.fsm.ToWithErr(StateError, err)
			}
		}
	}

	glog.V(logging.LogLevelDebug).Infof("Stopped block server %s", this.bindAddress)

	return this.fsm.To(StateStopped)
}
