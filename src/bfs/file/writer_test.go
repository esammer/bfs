package file

import (
	"bfs/blockservice"
	"bfs/nameservice"
	"bfs/server/blockserver"
	"bfs/server/nameserver"
	"bfs/test"
	"bfs/util/size"
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalFileWriter_Write(t *testing.T) {
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

	zeroBuf := bytes.Repeat([]byte{0}, size.KB-1)

	pvIds := make([]string, len(blockServer.PhysicalVolumes))
	for i, pv := range blockServer.PhysicalVolumes {
		pvIds[i] = pv.ID.String()
	}

	writer := NewWriter(nameClient, blockClient, pvIds, "/test.txt", size.MB)

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
		glog.Info("Removing temp directory")
		testDir.Destroy()
		glog.Info("Removed temp directory")
	}()

	server := blockserver.BlockServer{
		BindAddress: "127.0.0.1:8083",
		Paths:       []string{filepath.Join(testDir.Path, "block")},
		Options: []grpc.ServerOption{
			grpc.WriteBufferSize(size.MB * 8),
			grpc.ReadBufferSize(size.MB * 8),
			grpc.MaxRecvMsgSize(size.MB * 10),
			grpc.MaxSendMsgSize(size.MB * 10),
		},
	}
	err := server.Start()
	require.NoError(b, err)
	defer func() { assert.NoError(b, server.Stop()) }()

	conn, err := grpc.Dial(
		server.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithWriteBufferSize(size.MB*8),
		grpc.WithReadBufferSize(size.MB*8),
		grpc.WithInitialWindowSize(size.MB),
	)
	require.NoError(b, err)
	defer conn.Close()

	blockClient := blockservice.NewBlockServiceClient(conn)

	nameServer := nameserver.NameServer{
		BindAddress: "127.0.0.1:8084",
		Path:        filepath.Join(testDir.Path, "name"),
	}
	err = nameServer.Start()
	require.NoError(b, err)
	defer func() { assert.NoError(b, nameServer.Stop()) }()

	nameConn, err := grpc.Dial(
		nameServer.BindAddress,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	require.NoError(b, err)
	defer nameConn.Close()

	nameClient := nameservice.NewNameServiceClient(nameConn)

	zeroBuf := bytes.Repeat([]byte{0}, size.MB)
	fileSizes := []size.Size{
		size.Megabytes(1),
		size.Megabytes(16),
		size.Megabytes(128),
		size.Megabytes(256),
	}

	pvIds := make([]string, len(server.PhysicalVolumes))
	for i, pv := range server.PhysicalVolumes {
		pvIds[i] = pv.ID.String()
	}

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
									writer := NewWriter(nameClient, blockClient, pvIds, "/test.txt", blockSize)

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
