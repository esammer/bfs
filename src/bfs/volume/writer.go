package volume

import (
	"bfs/blockservice"
	"bfs/nameservice"
	"bfs/ns"
	"context"
	"github.com/golang/glog"
	"io"
	"math/rand"
	"time"
)

/*
 * LocalFileWriter
 */

type Writer interface {
	io.Writer
	io.Closer
}

type LocalFileWriter struct {
	// Configuration
	nameClient  nameservice.NameServiceClient
	blockClient blockservice.BlockServiceClient
	pvIds       []string
	blockSize   int
	filename    string

	// File state.
	filePos    int
	blockCount int
	blockList  []*nameservice.BlockMetadata

	// Block service state.
	writeStream blockservice.BlockService_WriteClient

	// Current block writer state.
	blockPos        int
	selectedPvId    string
	pvSelectionSeed int
}

type FileWriteEvent struct {
	Time            time.Time
	Volume          string
	Path            string
	Size            int
	Status          ns.FileStatus
	Blocks          []*nameservice.BlockMetadata
	ResponseChannel chan error
}

func NewWriter(nameClient nameservice.NameServiceClient, blockClient blockservice.BlockServiceClient, pvIds []string,
	filename string,
	blockSize int,
	eventChannel chan interface{}) *LocalFileWriter {

	glog.V(2).Infof("Allocate writer for %v with blockSize %d", filename, blockSize)

	return &LocalFileWriter{
		nameClient:      nameClient,
		blockClient:     blockClient,
		pvIds:           pvIds,
		blockSize:       blockSize,
		filename:        filename,
		blockList:       make([]*nameservice.BlockMetadata, 0, 16),
		pvSelectionSeed: rand.Int(),
	}
}

func (this *LocalFileWriter) Write(buffer []byte) (int, error) {
	bufferPos := 0
	bufferRemaining := len(buffer)
	totalWritten := 0

	// While there is more buffer data to write...
	for bufferRemaining > 0 {
		writeLen := 0

		// If we've reached the end of a block, it time to start a new one.
		if this.blockPos == this.blockSize || this.blockCount == 0 {
			if this.blockCount != 0 {
				if err := this.Flush(); err != nil {
					return totalWritten, err
				}
			}

			this.blockCount++

			// Allocate a new block by creating a new write stream.
			if writeStream, err := this.blockClient.Write(context.Background()); err != nil {
				return totalWritten, err
			} else {
				this.writeStream = writeStream
			}

			this.selectedPvId = this.pvIds[(this.pvSelectionSeed+this.blockCount)%len(this.pvIds)]
			this.blockPos = 0

			glog.V(1).Infof("Allocated new block %d on %s - filePos: %d", this.blockCount, this.selectedPvId, this.filePos)
		}

		// Decide how much of the buffer to write.
		if bufferRemaining < (this.blockSize - this.blockPos) {
			writeLen = bufferRemaining
		} else {
			writeLen = this.blockSize - this.blockPos
		}

		// If there's data left to write, write it.
		if writeLen > 0 {
			glog.V(2).Infof("Write %d:%d of %d bytes to %v on %s", bufferPos, bufferPos+writeLen, len(buffer),
				this.filename, this.selectedPvId)

			if err := this.writeStream.Send(&blockservice.WriteRequest{
				VolumeId: this.selectedPvId,
				Buffer:   buffer[bufferPos:bufferPos+writeLen],
			}); err != nil {
				return totalWritten, err
			}

			this.filePos += writeLen
			this.blockPos += writeLen
			bufferPos += writeLen
			bufferRemaining -= writeLen

			totalWritten += writeLen
		}
	}

	return totalWritten, nil
};

// Flushes any remaining data to block storage.
//
// This method effectively closes the underlying block and, as a result,
// should not be called unless absolutely necessary. Most applications
// should rely on the internal invocations of this method by Write() and
// Close(). Improper use of flushes may result in short block writes and
// inefficient block and metadata storage consumption, as well as subsequent
// read performance. If no block is currently open for write, calling this
// method has no effect.
func (this *LocalFileWriter) Flush() error {
	glog.V(1).Infof("Flush writer for %s", this.filename)

	// If a write stream is still open, close the block.
	if this.writeStream != nil {
		response, err := this.writeStream.CloseAndRecv()
		if err != nil {
			return err
		}

		blockId := response.BlockId
		pvId := response.VolumeId

		blockMetadata := &nameservice.BlockMetadata{
			BlockId: blockId,
			PvId:    pvId,
		}

		this.blockList = append(this.blockList, blockMetadata)
		this.writeStream = nil

		glog.V(2).Infof("Received block writer response: %v blockMetadata: %v", response, blockMetadata)
	}

	return nil
}

func (this *LocalFileWriter) Close() error {
	glog.V(1).Infof("Closing writer for file %v.", this.filename)

	if err := this.Flush(); err != nil {
		return err
	}

	_, err := this.nameClient.Add(context.Background(), &nameservice.AddRequest{
		Entry: &nameservice.Entry{
			Path:             this.filename,
			Blocks:           this.blockList,
			Permissions:      0,
			LvId:             "/",
			ReplicationLevel: 1,
			BlockSize:        uint64(this.blockSize),
			Size:             uint64(this.filePos),
		},
	})
	if err != nil {
		return err
	}

	glog.V(1).Infof("Closed writer for %s. Wrote %d bytes to %d blocks", this.filename, this.filePos, this.blockCount)

	return nil
}
