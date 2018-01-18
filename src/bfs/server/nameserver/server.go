package nameserver

import (
	"bfs/config"
	"bfs/nameservice"
	"bfs/ns"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type NameServer struct {
	Config *config.NameServiceConfig
	server *grpc.Server

	namespace   *ns.Namespace
	nameService *nameservice.NameService
}

func New(conf *config.NameServiceConfig, server *grpc.Server) *NameServer {
	return &NameServer{
		Config: conf,
		server: server,
	}
}

func (this *NameServer) Start() error {
	glog.V(1).Infof("Starting name server %s", this.Config.BindAddress)

	this.namespace = ns.New(this.Config.Path)
	if err := this.namespace.Open(); err != nil {
		return err
	}

	this.nameService = &nameservice.NameService{Namespace: this.namespace}
	nameservice.RegisterNameServiceServer(this.server, this.nameService)

	glog.V(1).Infof("Started name server %s", this.Config.BindAddress)

	return nil
}

func (this *NameServer) Stop() error {
	glog.V(1).Infof("Stopping name server %s", this.Config.BindAddress)

	if err := this.namespace.Close(); err != nil {
		return err
	}

	glog.V(1).Infof("Stopped name server %s", this.Config.BindAddress)

	return nil
}
