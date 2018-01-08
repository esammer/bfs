package blockserver

import (
	"bfs/blockservice"
	"bfs/test"
	"bfs/util/size"
	"bytes"
	"context"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"path/filepath"
	"testing"
)

func TestBlockServer(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	server := &BlockServer{
		BindAddress: "127.0.0.1:8086",
		Paths: []string{
			filepath.Join(testDir.Path, "1"),
		},
	}

	require.NoError(t, server.Start())
	defer func() { assert.NoError(t, server.Stop()) }()

	conn, err := grpc.Dial(server.BindAddress, grpc.WithInsecure(), grpc.WithBlock())
	require.NoError(t, err)
	defer conn.Close()

	bsc := blockservice.NewBlockServiceClient(conn)
	writeStream, err := bsc.Write(context.Background())
	require.NoError(t, err)

	err = writeStream.Send(&blockservice.WriteRequest{
		VolumeId: server.PhysicalVolumes[0].ID.String(),
		ClientId: "",
		Seq:      0,
		Buffer:   bytes.Repeat([]byte{0}, size.MB),
	})
	require.NoError(t, err)

	writeResp, err := writeStream.CloseAndRecv()
	require.NoError(t, err)

	_, err = bsc.Delete(
		context.Background(),
		&blockservice.ReadRequest{
			VolumeId: server.PhysicalVolumes[0].ID.String(),
			BlockId:  writeResp.BlockId,
		},
	)
	require.NoError(t, err)
}
