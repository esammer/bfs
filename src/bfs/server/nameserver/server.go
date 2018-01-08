package nameserver

import (
	"bfs/nameservice"
	"bfs/ns"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"net"
)

type NameServer struct {
	BindAddress string
	Path        string

	server      *grpc.Server
	namespace   *ns.Namespace
	nameService *nameservice.NameService
	rpcDoneChan chan error
}

func (this *NameServer) Start() error {
	this.rpcDoneChan = make(chan error)

	this.namespace = ns.New(this.Path)
	if err := this.namespace.Open(); err != nil {
		return err
	}

	this.nameService = &nameservice.NameService{Namespace: this.namespace}

	this.server = grpc.NewServer()
	nameservice.RegisterNameServiceServer(this.server, this.nameService)

	listener, err := net.Listen("tcp", this.BindAddress)
	if err != nil {
		if err := this.namespace.Close(); err != nil {
			return err
		}

		return err
	}

	go func() {
		if err := this.server.Serve(listener); err != nil {
			glog.Errorf("RPC server failed - %v", err)
		}

		this.rpcDoneChan <- err
	}()

	return nil
}

func (this *NameServer) Stop() error {
	this.server.GracefulStop()

	if err := <-this.rpcDoneChan; err != nil {
		return err
	}

	if err := this.namespace.Close(); err != nil {
		return err
	}

	return nil
}
