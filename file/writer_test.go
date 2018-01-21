package file

import (
	"bfs/blockservice"
	"bfs/config"
	"bfs/nameservice"
	"bfs/server/blockserver"
	"bfs/server/nameserver"
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
	"os"
	"path/filepath"
	"testing"
)

func TestLocalFileWriter_Write(t *testing.T) {
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
				{Path: filepath.Join(testDir.Path, "pv1"), AllowAutoInitialize: true},
				{Path: filepath.Join(testDir.Path, "pv2"), AllowAutoInitialize: true},
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

	zeroBuf := bytes.Repeat([]byte{0}, size.KB-1)

	placementPolicy := NewLabelAwarePlacementPolicy(
		blockServer.Config.VolumeConfigs,
		"hostname",
		true,
		1,
		1,
		nil,
	)

	//_, err = nameClient.AddVolume(context.Background(), &nameservice.AddVolumeRequest{VolumeId: "1", PvIds: pvIds})

	writer, err := NewWriter(nameClient, blockClient, placementPolicy, "/test.txt", size.MB)
	require.NoError(t, err)

	writeLen, err := writer.Write(zeroBuf)
	require.NoError(t, err)
	require.Equal(t, size.KB-1, writeLen)

	err = writer.Flush()
	require.NoError(t, err)

	require.Equal(t, len(zeroBuf), writeLen)

	err = writer.Close()
	require.NoError(t, err)
}

// Benchmark write speed through the block service.
//
// This benchmark performs writes through the block service at multiple multiple
// file sizes, and with multiple block sizes. Additionally, a baseline benchmark
// that does direct local IO (i.e. os.Create()) is performed for comparison.
//
// File sizes are 1, 10, 100, and 512MB. Block sizes are 1, 8, and 10MB.
// Write() calls are 1MB buffers of byte(0) in all instances.
func BenchmarkLocalFileWriter_Write(b *testing.B) {
	defer glog.Flush()

	testDir := test.New("build", "test", b.Name())
	require.NoError(b, testDir.Create())
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
				{Path: filepath.Join(testDir.Path, "pv1"), AllowAutoInitialize: true},
				{Path: filepath.Join(testDir.Path, "pv2"), AllowAutoInitialize: true},
			},
		},
		rpcServer,
	)

	require.NoError(b, blockServer.Start())
	defer func() { assert.NoError(b, blockServer.Stop()) }()

	nameServer := nameserver.New(
		&config.NameServiceConfig{
			BindAddress:      "localhost:8084",
			AdvertiseAddress: "localhost:8084",
			Path:             filepath.Join(testDir.Path, "ns"),
		},
		rpcServer,
	)
	require.NoError(b, nameServer.Start())
	defer func() { assert.NoError(b, nameServer.Stop()) }()

	listener, err := net.Listen("tcp", blockServer.Config.BindAddress)
	go func() {
		assert.NoError(b, rpcServer.Serve(listener))
	}()

	blockConn, err := grpc.Dial(
		blockServer.Config.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithWriteBufferSize(size.MB*8),
		grpc.WithReadBufferSize(size.MB*8),
		grpc.WithInitialWindowSize(size.MB),
	)
	require.NoError(b, err)
	defer blockConn.Close()

	blockClient := blockservice.NewBlockServiceClient(blockConn)

	nameConn, err := grpc.Dial(
		nameServer.Config.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	require.NoError(b, err)
	defer nameConn.Close()

	nameClient := nameservice.NewNameServiceClient(nameConn)

	placementPolicy := NewLabelAwarePlacementPolicy(
		blockServer.Config.VolumeConfigs,
		"hostname",
		true,
		1,
		1,
		nil,
	)

	zeroBuf := bytes.Repeat([]byte{0}, size.MB)
	fileSizes := []size.Size{
		size.Megabytes(1),
		size.Megabytes(16),
		size.Megabytes(128),
		size.Megabytes(256),
	}

	pvIds := make([]string, len(blockServer.PhysicalVolumes))
	for i, pv := range blockServer.PhysicalVolumes {
		pvIds[i] = pv.ID.String()
	}

	_, err = nameClient.AddVolume(context.Background(), &nameservice.AddVolumeRequest{VolumeId: "1", PvIds: pvIds})
	require.NoError(b, err)

	b.ResetTimer()

	for _, fileSize := range fileSizes {
		b.Run(
			fmt.Sprintf("fileSize=%s", fileSize.String()),
			func(b *testing.B) {
				for fileSizeIter := 0; fileSizeIter < b.N; fileSizeIter++ {
					writeCount := int(fileSize.ToBytes()) / len(zeroBuf)

					b.Run("baseline", func(b *testing.B) {
						for i := 0; i < b.N; i++ {
							f, err := os.Create(filepath.Join(testDir.Path, "control.txt"))
							require.NoError(b, err)

							for j := 0; j < writeCount; j++ {
								_, err := f.Write(zeroBuf)
								require.NoError(b, err)
							}

							err = f.Close()
							require.NoError(b, err)
						}
					})

					for _, blockSize := range []int{size.MB, size.MB * 8, size.MB * 16} {
						s := size.Size(blockSize)

						b.Run(
							fmt.Sprintf("blockSize=%s", s.String()),
							func(b *testing.B) {
								glog.Infof("Test: fileSize: %s blockSize: %s writes: %d",
									fileSize.String(),
									s.String(),
									writeCount,
								)

								for i := 0; i < b.N; i++ {
									writer, err := NewWriter(nameClient, blockClient, placementPolicy, "/test.txt", blockSize)
									require.NoError(b, err)

									for j := 0; j < writeCount; j++ {
										_, err = writer.Write(zeroBuf)
										require.NoError(b, err)
									}

									err = writer.Close()
									require.NoError(b, err)
								}
							},
						)
					}
				}
			},
		)
	}
}
