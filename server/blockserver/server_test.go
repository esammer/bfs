package blockserver

import (
	"bfs/service/blockservice"
	"bfs/config"
	"bfs/test"
	"bfs/util/size"
	"bytes"
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"net"
	"path/filepath"
	"testing"
)

func TestBlockServer(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	bsc := &config.BlockServiceConfig{
		Hostname: "localhost",
		Port:     8086,
		VolumeConfigs: []*config.PhysicalVolumeConfig{
			{Path: filepath.Join(testDir.Path, "1"), AllowAutoInitialize: true, Labels: map[string]string{}},
			{Path: filepath.Join(testDir.Path, "2"), AllowAutoInitialize: true, Labels: map[string]string{}},
		},
	}

	bindAddress := fmt.Sprintf("%s:%d", "localhost", 8086)

	listener, err := net.Listen("tcp", bindAddress)
	require.NoError(t, err)
	rpcServer := grpc.NewServer()
	defer rpcServer.GracefulStop()

	server := New(bsc, rpcServer)
	require.NoError(t, server.Start())
	defer func() { assert.NoError(t, server.Stop()) }()

	go func() {
		err := rpcServer.Serve(listener)
		assert.NoError(t, err)
	}()

	conn, err := grpc.Dial(bindAddress, grpc.WithInsecure(), grpc.WithBlock())
	require.NoError(t, err)
	defer conn.Close()

	bsClient := blockservice.NewBlockServiceClient(conn)
	writeStream, err := bsClient.Write(context.Background())
	require.NoError(t, err)

	err = writeStream.Send(&blockservice.WriteRequest{
		VolumeId: server.PhysicalVolumes[0].ID.String(),
		Buffer:   bytes.Repeat([]byte{0}, size.MB),
	})
	require.NoError(t, err)

	writeResp, err := writeStream.CloseAndRecv()
	require.NoError(t, err)

	_, err = bsClient.Delete(
		context.Background(),
		&blockservice.ReadRequest{
			VolumeId: server.PhysicalVolumes[0].ID.String(),
			BlockId:  writeResp.BlockId,
		},
	)
	require.NoError(t, err)
}
