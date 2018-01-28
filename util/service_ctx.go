package util

import (
	"bfs/service/blockservice"
	"bfs/service/nameservice"
	"google.golang.org/grpc"
)

type ServiceCtx struct {
	Conn               *grpc.ClientConn
	BlockServiceClient blockservice.BlockServiceClient
	NameServiceClient  nameservice.NameServiceClient
}
