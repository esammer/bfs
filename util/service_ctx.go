package util

import (
	"bfs/blockservice"
	"bfs/nameservice"
	"google.golang.org/grpc"
)

type ServiceCtx struct {
	Conn               *grpc.ClientConn
	BlockServiceClient blockservice.BlockServiceClient
	NameServiceClient  nameservice.NameServiceClient
}
