package file

import (
	"bfs/blockservice"
	"bfs/config"
	"bfs/lru"
	"bfs/nameservice"
	"bfs/server/blockserver"
	"bfs/server/nameserver"
	"bfs/test"
	"bfs/util"
	"bfs/util/size"
	"bytes"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"io"
	"net"
	"path/filepath"
	"testing"
)

func TestLocalFileReader_Read(t *testing.T) {
	defer glog.Flush()

	testDir := test.New("build", "test", t.Name())

	require.NoError(t, testDir.Create())
	defer func() {
		testDir.Destroy()
	}()

	rpcServer := grpc.NewServer(
		grpc.WriteBufferSize(size.MB*8),
		grpc.ReadBufferSize(size.MB*8),
		grpc.MaxRecvMsgSize(size.MB*10),
		grpc.MaxSendMsgSize(size.MB*10),
	)
	defer rpcServer.GracefulStop()

	blockServer := blockserver.New(
		&config.BlockServiceConfig{
			BindAddress: "localhost:8084",
			VolumeConfigs: []*config.PhysicalVolumeConfig{
				{Path: filepath.Join(testDir.Path, "pv1"), AllowAutoInitialize: true, Labels: map[string]string{}},
				{Path: filepath.Join(testDir.Path, "pv2"), AllowAutoInitialize: true, Labels: map[string]string{}},
			},
		},
		rpcServer,
	)

	require.NoError(t, blockServer.Start())
	defer func() { assert.NoError(t, blockServer.Stop()) }()

	nameServer := nameserver.New(
		&config.NameServiceConfig{
			BindAddress:      "localhost:8084",
			AdvertiseAddress: "localhost:8084",
			Path:             filepath.Join(testDir.Path, "ns"),
		},
		rpcServer,
	)
	require.NoError(t, nameServer.Start())
	defer func() { assert.NoError(t, nameServer.Stop()) }()

	listener, err := net.Listen("tcp", blockServer.Config.BindAddress)
	go func() {
		assert.NoError(t, rpcServer.Serve(listener))
	}()

	blockConn, err := grpc.Dial(
		blockServer.Config.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithWriteBufferSize(size.MB*8),
		grpc.WithReadBufferSize(size.MB*8),
		grpc.WithInitialWindowSize(size.MB),
	)
	require.NoError(t, err)
	defer blockConn.Close()

	blockClient := blockservice.NewBlockServiceClient(blockConn)

	nameConn, err := grpc.Dial(
		nameServer.Config.BindAddress,
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

	placementPolicy := NewLabelAwarePlacementPolicy(
		blockServer.Config.VolumeConfigs,
		"hostname",
		true,
		1,
		1,
		nil,
	)

	clientFactory := lru.NewCache(
		2,
		func(name string) (interface{}, error) {
			conn, err := grpc.Dial(name, grpc.WithBlock(), grpc.WithInsecure())
			if err != nil {
				return nil, err
			}

			return &util.ServiceCtx{
				Conn:               conn,
				BlockServiceClient: blockservice.NewBlockServiceClient(conn),
			}, nil
		},
		func(name string, value interface{}) error {
			if value != nil {
				return value.(*util.ServiceCtx).Conn.Close()
			}

			return nil
		},
	)

	writer, err := NewWriter(nameClient, clientFactory, placementPolicy, "/test.txt", size.MB)
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
