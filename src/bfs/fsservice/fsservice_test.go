package fsservice

import (
	"bytes"
	"context"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"testing"
)

func TestFsService(t *testing.T) {
	defer glog.Flush()

	listener, err := net.Listen("tcp", "127.0.0.1:8085")
	require.NoError(t, err)

	fsService := &FsService{}

	server := grpc.NewServer()
	RegisterFsServiceServer(server, fsService)
	defer server.GracefulStop()

	go func() {
		err := server.Serve(listener)
		assert.NoError(t, err)

		glog.Info("RPC server complete")
	}()

	conn, err := grpc.Dial("127.0.0.1:8085", grpc.WithInsecure(), grpc.WithBlock())
	defer conn.Close()

	fsServiceClient := NewFsServiceClient(conn)

	createStream, err := fsServiceClient.Create(context.Background())
	require.NoError(t, err)

	err = createStream.Send(&CreateRequest{
		Request: &CreateRequest_Metadata{
			Metadata: &CreateMetadata{
				Path:        "/a.txt",
				Permissions: 0,
			},
		},
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		err := createStream.Send(&CreateRequest{
			Request: &CreateRequest_Packet{
				Packet: &WritePacket{
					Buffer: bytes.Repeat([]byte{0}, i),
				},
			},
		})
		require.NoError(t, err)
	}

	resp, err := createStream.CloseAndRecv()
	require.NoError(t, err)

	glog.Infof("Received response: %v", resp)
}
