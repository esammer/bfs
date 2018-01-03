package blockservice

import (
	"bfs/block"
	"bfs/volume"
	"context"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
	"io"
)

type BlockService struct {
	volumes   []*volume.PhysicalVolume
	volumeIdx map[string]int
}

func New(volumes []*volume.PhysicalVolume) *BlockService {
	volumeIdx := make(map[string]int, len(volumes))

	for i, pv := range volumes {
		volumeIdx[pv.ID.String()] = i
	}

	this := &BlockService{
		volumes:   volumes,
		volumeIdx: volumeIdx,
	}

	return this
}

func (this *BlockService) Write(stream BlockService_WriteServer) error {
	glog.V(1).Info("Received write request")

	var writer block.BlockWriter
	var blockId string
	var clientId string
	var volumeId string

	totalWritten := 0

	for chunkIter := 0; ; chunkIter++ {
		glog.V(2).Infof("Writer iter %d - start", chunkIter)

		request, err := stream.Recv()

		if err == io.EOF {
			glog.V(2).Infof("Writer iter %d - EOF for client %s", chunkIter, clientId)

			break
		} else if err != nil {
			glog.Errorf("Writer iter %d - client %s - Error receiving write request - %v", chunkIter, clientId, err)

			return err
		}

		if writer == nil {
			glog.V(2).Infof("Writer iter %d - Start writer", chunkIter)

			volumeId = request.VolumeId
			pv := this.volumes[this.volumeIdx[volumeId]]

			var err error
			blockUUID := uuid.NewRandom()
			blockId = blockUUID.String()
			writer, err = pv.OpenWrite(blockId)

			if err != nil {
				return err
			}

			clientId = request.ClientId
		}

		glog.V(2).Infof(
			"Write iter %d - Received request clientId: %s seqId: %d size: %d",
			chunkIter,
			clientId,
			request.Seq,
			len(request.Buffer),
		)

		size, err := writer.Write(request.Buffer)
		totalWritten += size

		if err != nil {
			glog.Errorf("Write failed - %v", err)
			return err
		}

	}

	if err := writer.Close(); err != nil {
		return nil
	}

	if err := stream.SendAndClose(&WriteResponse{
		ClientId: clientId,
		BlockId:  blockId,
		Seq:      0,
		VolumeId: volumeId,
		Size:     uint32(totalWritten),
	}); err != nil {
		return err
	}

	glog.V(2).Info("Completed write request")

	return nil
}

func (this *BlockService) Read(request *ReadRequest, stream BlockService_ReadServer) error {
	glog.V(1).Infof("Read - volumeId: %s blockId: %s", request.VolumeId, request.BlockId)

	volumeIdx := this.volumeIdx[request.VolumeId]
	pv := this.volumes[volumeIdx]

	reader, err := pv.OpenRead(request.BlockId)
	if err != nil {
		return err
	}

	defer reader.Close()

	buffer := make([]byte, request.ChunkSize)

	for i := 0; true; i++ {
		size, err := reader.Read(buffer)

		if size > 0 {
			response := &ReadResponse{
				ClientId: request.ClientId,
				VolumeId: pv.ID.String(),
				BlockId:  request.BlockId,
				Buffer:   buffer[:size],
				Seq:      uint32(i),
				Status:   Status_SUCCESS,
			}

			if err := stream.Send(response); err != nil {
				return err
			}
		}

		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
	}

	glog.V(1).Infof("Read complete - %s", request.BlockId)

	return nil
}

func (this *BlockService) Delete(context context.Context, request *ReadRequest) (*DeleteResponse, error) {
	glog.V(1).Infof(
		"Delete request received - clientId: %s volumeId: %s blockId: %s",
		request.ClientId,
		request.VolumeId,
		request.BlockId,
	)

	pv := this.volumes[this.volumeIdx[request.VolumeId]]
	err := pv.Delete(request.BlockId)

	if err != nil {
		return nil, err
	}

	response := &DeleteResponse{
		VolumeId: request.VolumeId,
		ClientId: request.ClientId,
		Status:   Status_SUCCESS,
	}

	glog.V(1).Infof("Delete request complete - %v", response)

	return response, nil
}

func (this *BlockService) GetEventStream(request *EventRequest, stream BlockService_GetEventStreamServer) error {
	return nil
}
