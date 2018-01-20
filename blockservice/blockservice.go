package blockservice

import (
	"bfs/block"
	"bfs/util/size"
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
	"io"
)

const (
	DefaultMaxReadSize = 8 * size.MB
)

type BlockService struct {
	volumeIdx map[string]*PhysicalVolume
}

func New(volumes []*PhysicalVolume) *BlockService {
	volumeIdx := make(map[string]*PhysicalVolume, len(volumes))

	for _, pv := range volumes {
		volumeIdx[pv.ID.String()] = pv
	}

	this := &BlockService{
		volumeIdx: volumeIdx,
	}

	return this
}

func (this *BlockService) Write(stream BlockService_WriteServer) error {
	glog.V(1).Info("Received write request")

	var writer block.BlockWriter
	var blockId string
	var volumeId string

	totalWritten := 0

	for chunkIter := 0; ; chunkIter++ {
		glog.V(2).Infof("Writer iter %d - start", chunkIter)

		request, err := stream.Recv()

		if err == io.EOF {
			glog.V(2).Infof("Writer iter %d - EOF", chunkIter)

			break
		} else if err != nil {
			glog.Errorf("Writer iter %d - Error receiving write request - %v", chunkIter, err)

			return err
		}

		if writer == nil {
			glog.V(2).Infof("Writer iter %d - Start writer", chunkIter)

			volumeId = request.VolumeId
			pv, ok := this.volumeIdx[volumeId]
			if !ok {
				return fmt.Errorf("no such volume id '%s'", volumeId)
			}

			blockUUID := uuid.NewRandom()
			blockId = blockUUID.String()

			var err error
			writer, err = pv.OpenWrite(blockId)
			if err != nil {
				return err
			}
		}

		glog.V(2).Infof(
			"Write iter %d - Received request size: %d",
			chunkIter,
			len(request.Buffer),
		)

		size, err := writer.Write(request.Buffer)
		if err != nil {
			glog.Errorf("Write failed - %v", err)
			return err
		}

		totalWritten += size
	}

	if writer != nil {
		if err := writer.Close(); err != nil {
			return nil
		}
	}

	if err := stream.SendAndClose(&WriteResponse{
		BlockId:  blockId,
		VolumeId: volumeId,
		Size:     uint32(totalWritten),
	}); err != nil {
		return err
	}

	glog.V(2).Infof("Completed write request - wrote %d bytes", totalWritten)

	return nil
}

func (this *BlockService) Read(request *ReadRequest, stream BlockService_ReadServer) error {
	glog.V(1).Infof("Read - volumeId: %s blockId: %s", request.VolumeId, request.BlockId)

	volumeId := request.VolumeId
	pv, ok := this.volumeIdx[volumeId]
	if !ok {
		return fmt.Errorf("no such volume id '%s'", volumeId)
	}

	reader, err := pv.OpenRead(request.BlockId)
	if err != nil {
		return err
	}

	defer reader.Close()

	chunkSize := DefaultMaxReadSize
	if request.ChunkSize < DefaultMaxReadSize {
		chunkSize = int(request.ChunkSize)
	}

	buffer := make([]byte, chunkSize)
	readCalls := 0
	sendCalls := 0
	totalRead := 0

	for i := 0; true; i++ {
		readLen, err := reader.Read(buffer)
		totalRead += readLen
		readCalls++

		if readLen > 0 {
			response := &ReadResponse{
				VolumeId: pv.ID.String(),
				BlockId:  request.BlockId,
				Buffer:   buffer[:readLen],
			}

			if err := stream.Send(response); err != nil {
				return err
			}
			sendCalls++
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	glog.V(1).Infof("Read complete - %s", request.BlockId)

	if glog.V(2) {
		glog.Infof("Performance: %f bytes/read - readCalls: %d sendCalls: %d requestChunkSize: %d actualChunkSize: %d",
			float64(totalRead)/float64(readCalls),
			readCalls,
			sendCalls,
			request.ChunkSize,
			chunkSize,
		)
	}

	return nil
}

func (this *BlockService) Delete(context context.Context, request *ReadRequest) (*DeleteResponse, error) {
	glog.V(1).Infof(
		"Delete request received - volumeId: %s blockId: %s",
		request.VolumeId,
		request.BlockId,
	)

	volumeId := request.VolumeId
	pv, ok := this.volumeIdx[volumeId]
	if !ok {
		return nil, fmt.Errorf("no such volume id '%s'", volumeId)
	}

	err := pv.Delete(request.BlockId)

	if err != nil {
		return nil, err
	}

	response := &DeleteResponse{
		VolumeId: request.VolumeId,
		Status:   Status_SUCCESS,
	}

	glog.V(1).Infof("Delete request complete - %v", response)

	return response, nil
}

func (this *BlockService) Volumes(context context.Context, request *VolumeRequest) (*VolumeResponse, error) {
	glog.V(1).Infof("Volume metadata request")

	volumeIds := make([]string, 0, 8)

	for k := range this.volumeIdx {
		volumeIds = append(volumeIds, k)
	}

	response := &VolumeResponse{
		VolumeIds: volumeIds,
	}

	return response, nil
}
