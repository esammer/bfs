package nameserver

import (
	"bfs/config"
	"bfs/nameservice"
	"bfs/ns/etcd"
	"bfs/util/logging"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

type NameServer struct {
	Config *config.NameServiceConfig

	server *grpc.Server

	namespace   *etcd.EtcdNamespace
	nameService *nameservice.NameService
}

func New(conf *config.NameServiceConfig, server *grpc.Server) *NameServer {
	return &NameServer{
		Config: conf,
		server: server,
	}
}

func (this *NameServer) Start() error {
	glog.V(logging.LogLevelDebug).Infof("Starting name server")

	self := -1
	convertedNodes := make([]*etcd.NsNode, len(this.Config.Nodes))

	for i, node := range this.Config.Nodes {
		if node.Hostname == this.Config.Hostname {
			self = i
		}

		var converted etcd.NsNode
		converted = etcd.NsNode(*node)
		convertedNodes[i] = &converted
	}

	if self == -1 {
		return fmt.Errorf("unable to find hostname %s in configured nodes", this.Config.Hostname)
	}

	ensc := &etcd.Config{
		Path:    this.Config.Path,
		GroupId: this.Config.GroupId,
		Self:    self,
		Nodes:   convertedNodes,
	}

	this.namespace = etcd.New(ensc)
	if this.namespace == nil {
		return errors.New("unable to create namespace")
	}

	if err := this.namespace.Open(); err != nil {
		return err
	}

	this.nameService = &nameservice.NameService{Namespace: this.namespace}
	nameservice.RegisterNameServiceServer(this.server, this.nameService)

	glog.V(logging.LogLevelDebug).Info("Started name server")

	return nil
}

func (this *NameServer) Stop() error {
	glog.V(logging.LogLevelDebug).Info("Stopping name server")

	if err := this.namespace.Close(); err != nil {
		return err
	}

	glog.V(logging.LogLevelDebug).Info("Stopped name server")

	return nil
}
