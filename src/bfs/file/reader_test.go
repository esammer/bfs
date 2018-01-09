package file

import (
	"bfs/blockservice"
	"bfs/nameservice"
	"bfs/server/blockserver"
	"bfs/server/nameserver"
	"bfs/test"
	"bfs/util/size"
	"bytes"
	"context"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"io"
	"path/filepath"
	"testing"
)

func TestLocalFileReader_Read(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())

	require.NoError(t, testDir.Create())
	defer func() {
		glog.Info("Removing temp directory")
		testDir.Destroy()
		glog.Info("Removed temp directory")
	}()

	blockServer := blockserver.BlockServer{
		BindAddress: "127.0.0.1:8083",
		Paths:       []string{filepath.Join(testDir.Path, "block")},
		Options: []grpc.ServerOption{
			grpc.WriteBufferSize(size.MB * 8),
			grpc.ReadBufferSize(size.MB * 8),
			grpc.MaxRecvMsgSize(size.MB * 10),
			grpc.MaxSendMsgSize(size.MB * 10),
		},
	}
	err := blockServer.Start()
	require.NoError(t, err)
	defer func() { assert.NoError(t, blockServer.Stop()) }()

	blockConn, err := grpc.Dial(
		blockServer.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithWriteBufferSize(size.MB*8),
		grpc.WithReadBufferSize(size.MB*8),
		grpc.WithInitialWindowSize(size.MB),
	)
	require.NoError(t, err)
	defer blockConn.Close()

	blockClient := blockservice.NewBlockServiceClient(blockConn)

	nameServer := nameserver.NameServer{
		BindAddress: "127.0.0.1:8084",
		Path:        filepath.Join(testDir.Path, "name"),
	}
	err = nameServer.Start()
	require.NoError(t, err)
	defer func() { assert.NoError(t, nameServer.Stop()) }()

	nameConn, err := grpc.Dial(
		nameServer.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	defer nameConn.Close()

	nameClient := nameservice.NewNameServiceClient(nameConn)

	zeroBuf := bytes.Repeat([]byte{0}, size.MB)

	pvIds := make([]string, len(blockServer.PhysicalVolumes))
	for i, pv := range blockServer.PhysicalVolumes {
		pvIds[i] = pv.ID.String()
	}

	_, err = nameClient.AddVolume(context.Background(), &nameservice.AddVolumeRequest{VolumeId: "1", PvIds: pvIds})

	writer, err := NewWriter(nameClient, blockClient, "1", "/test.txt", size.MB)
	require.NoError(t, err)

	_, err = writer.Write(zeroBuf)
	require.NoError(t, err)
	_, err = writer.Write(zeroBuf)
	require.NoError(t, err)

	require.NoError(t, writer.Close())

	reader := NewReader(nameClient, blockClient, "/test.txt")
	require.NoError(t, reader.Open())

	totalRead := 0
	readBuf := make([]byte, size.MB/2)

	for {
		readLen, err := reader.Read(readBuf)
		totalRead += readLen

		if err == io.EOF {
			break
		} else if err != nil {
			assert.NoError(t, err)
			break
		}

		require.Equal(t, size.MB/2, readLen)
	}

	require.Equal(t, size.MB*2, totalRead)

	require.NoError(t, reader.Close())
}
