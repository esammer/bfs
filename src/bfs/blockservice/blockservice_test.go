package blockservice

import (
	"bfs/block"
	"bfs/test"
	"bfs/volume"
	"bytes"
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"io"
	"math/rand"
	"net"
	"path/filepath"
	"testing"
)

func TestBlockService_Read(t *testing.T) {
	testDir := test.New("build", "test", t.Name())
	err := testDir.Create()
	require.NoError(t, err)
	defer testDir.Destroy()

	eventChannel := make(chan interface{}, 1024)
	defer close(eventChannel)

	clientCount := 10
	blockCount := 4
	maxConcurrency := 8
	bindAddress := "127.0.0.1:8082"
	blockWriteSize := 1024 * 64 // 64K write chunks
	blockReadSize := 1024 * 32  // 32K read chunks

	type blockVolumePair struct {
		blockId  string
		volumeId string
	}
	blocks := make([]*blockVolumePair, 0, clientCount)

	go func() {
		glog.V(1).Info("Block response service starting")

		for event := range eventChannel {
			switch e := event.(type) {
			case *block.BlockWriteEvent:
				e.ResponseChannel <- e
			}
		}

		glog.V(1).Info("Block response service stopped")
	}()

	pvs := []*volume.PhysicalVolume{
		volume.NewPhysicalVolume(filepath.Join(testDir.Path, "1"), eventChannel),
		volume.NewPhysicalVolume(filepath.Join(testDir.Path, "2"), eventChannel),
	}
	defer pvs[0].Close()
	defer pvs[1].Close()

	for _, pv := range pvs {
		err := pv.Open(true)
		require.NoError(t, err)
	}

	blockService := New(pvs)

	server := grpc.NewServer(grpc.ReadBufferSize(1024*1024), grpc.WriteBufferSize(1024*1024),
		grpc.InitialConnWindowSize(1024*1024), grpc.InitialWindowSize(1024*1024))
	defer server.GracefulStop()
	RegisterBlockServiceServer(server, blockService)

	go func() {
		glog.V(1).Info("RPC server starting")

		listener, err := net.Listen("tcp", bindAddress)
		require.NoError(t, err)
		require.NoError(t, server.Serve(listener))

		glog.V(1).Info("RPC server stopped")
	}()

	conn, err := grpc.Dial(bindAddress, grpc.WithInsecure(), grpc.WithWriteBufferSize(1024*1024),
		grpc.WithReadBufferSize(1024*1024))
	require.NoError(t, err)
	defer conn.Close()

	client := NewBlockServiceClient(conn)

	sem := make(chan int, maxConcurrency)
	defer close(sem)

	completeSem := make(chan int, clientCount)
	defer close(completeSem)

	for i := 0; i < blockCount; i++ {
		writer, err := client.Write(context.Background())
		require.NoError(t, err)

		for j := 0; j < 2; j++ {
			err = writer.Send(&WriteRequest{
				ClientId: fmt.Sprint(i),
				VolumeId: pvs[i%len(pvs)].ID.String(),
				Seq:      uint32(j),
				Buffer:   bytes.Repeat([]byte{0}, blockWriteSize),
			})

			require.NoError(t, err)
		}

		response, err := writer.CloseAndRecv()
		require.NoError(t, err)

		if response != nil {
			glog.V(2).Infof("Received write response: %v", response)

			blocks = append(blocks, &blockVolumePair{response.BlockId, response.VolumeId})
		}

		if err == io.EOF {
			glog.V(2).Info("Received EOF from writer response stream")
			break
		} else if err != nil {
			require.Fail(t, err.Error())
		}
	}

	for i := 0; i < maxConcurrency; i++ {
		sem <- i
	}

	for i := 0; i < clientCount; i++ {
		go func(i int) {
			lockId := <-sem
			glog.V(2).Infof("Client %d acquired lock %d", i, lockId)

			blockSelection := blocks[rand.Intn(len(blocks))]

			iStr := fmt.Sprint(i)

			readStream, err := client.Read(
				context.Background(),
				&ReadRequest{
					VolumeId:  blockSelection.volumeId,
					BlockId:   blockSelection.blockId,
					ClientId:  iStr,
					Position:  0,
					ChunkSize: uint32(blockReadSize),
				},
			)
			require.NoError(t, err)

			for {
				response, err := readStream.Recv()

				if response != nil {
					glog.V(2).Infof("Received response - client: %v, volumeId: %v, blockId: %v, seq: %v, "+
						"status: %v buffer len: %d",
						response.ClientId,
						response.VolumeId,
						response.BlockId,
						response.Seq,
						response.Status,
						len(response.Buffer),
					)
				}

				if err == io.EOF {
					break
				} else if err != nil {
					glog.Errorf("Other read error: %v", err)
					require.NoError(t, err)
				}
			}

			completeSem <- i
			sem <- lockId
		}(i)
	}

	for i := 0; i < clientCount; i++ {
		clientId := <-completeSem
		glog.V(1).Infof("Client %d complete", clientId)
	}

	glog.V(1).Infof("All clients complete")
}

func TestBlockService_Delete(t *testing.T) {
	testDir := test.New("build", "test", t.Name())
	require.NoError(t, testDir.Create())
	defer testDir.Destroy()

	eventChannel := make(chan interface{}, 10)
	defer close(eventChannel)

	go func() {
		for event := range eventChannel {
			switch e := event.(type) {
			case *block.BlockWriteEvent:
				e.ResponseChannel <- e
			}
		}
	}()

	pv := volume.NewPhysicalVolume(testDir.Path, eventChannel)
	require.NoError(t, pv.Open(true))
	defer pv.Close()

	blockService := New([]*volume.PhysicalVolume{pv})

	listener, err := net.Listen("tcp", "127.0.0.1:8083")
	require.NoError(t, err)

	server := grpc.NewServer()
	RegisterBlockServiceServer(server, blockService)
	defer server.GracefulStop()

	go func() {
		err := server.Serve(listener)
		if err != nil {
			glog.Errorf("RPC server failed - %v", err)
		}
	}()

	conn, err := grpc.Dial("127.0.0.1:8083", grpc.WithInsecure())
	require.NoError(t, err)
	defer conn.Close()

	blockClient := NewBlockServiceClient(conn)

	writerStream, err := blockClient.Write(context.Background())
	require.NoError(t, err)

	err = writerStream.Send(&WriteRequest{
		ClientId: "1",
		VolumeId: pv.ID.String(),
		Seq:      0,
		Buffer:   []byte{0},
	})
	require.NoError(t, err)

	response, err := writerStream.CloseAndRecv()
	require.NoError(t, err)

	deleteResp, err := blockClient.Delete(context.Background(), &ReadRequest{
		ClientId:  "1",
		VolumeId:  pv.ID.String(),
		BlockId:   response.BlockId,
		ChunkSize: 1024,
		Position:  0,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
}
