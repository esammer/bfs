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

		// Open a new block.
		if this.blockPos == this.blockSize || this.blockCount == 0 {
			// Explicitly flush so we can properly track bytes written.
			flushedLen, err := this.Flush()
			totalWritten += flushedLen
			if err != nil {
				return totalWritten, err
			}

			if err := this.Close(); err != nil {
				return totalWritten, err
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
				ClientId: "",
				VolumeId: this.selectedPvId,
				Buffer:   buffer[bufferPos:bufferPos+writeLen],
				Seq:      0,
			}); err != nil {
				return totalWritten, err
			}

			this.filePos += writeLen
			this.blockPos += writeLen
			bufferPos += writeLen
			bufferRemaining -= writeLen
		}
	}

	return totalWritten, nil
}

// Flushes any remaining data to block storage.
//
// Upon flush, the size of any data that is actually written is returned along with
// any errors encountered. Explicit flush calls are not required; both Write() and
// Close() will call this method as needed. Since Close() does not return the number
// of bytes written, some applications may wish to explicitly call Flush() to know the
// full file size prior to close. This method effectively closes the underlying block
// and, as a result, should not be called unless absolutely necessary. Improper use of
// flushes may result in short block writes and inefficient block and metadata storage
// consumption, as well as subsequent read performance. If no block is currently open
// for write, calling this method has no effect and it will return a write size of zero.
func (this *LocalFileWriter) Flush() (int, error) {
	glog.V(1).Infof("Flush writer for %s", this.filename)

	totalWritten := 0

	// If a write stream is still open, close the block.
	if this.writeStream != nil {
		response, err := this.writeStream.CloseAndRecv()
		if err != nil {
			return 0, err
		}

		blockId := response.BlockId
		pvId := response.VolumeId
		totalWritten = int(response.Size)

		blockMetadata := &nameservice.BlockMetadata{
			BlockId: blockId,
			PvId:    pvId,
		}

		this.blockList = append(this.blockList, blockMetadata)
		this.writeStream = nil

		glog.V(2).Infof("Received block writer response: %v blockMetadata: %v", response, blockMetadata)
	}

	glog.V(2).Infof("Flushed %d bytes", totalWritten)

	return totalWritten, nil
}

func (this *LocalFileWriter) Close() error {
	glog.V(1).Infof("Closing writer for file %v.", this.filename)

	if _, err := this.Flush(); err != nil {
		return err
	}

	_, err := this.nameClient.Add(context.Background(), &nameservice.AddRequest{
		Entry: &nameservice.Entry{
			Path:        this.filename,
			Blocks:      this.blockList,
			Permissions: 0,
			LvId:        "/",
		},
	})
	if err != nil {
		return err
	}

	glog.V(1).Infof("Closed writer for %s. Wrote %d bytes to %d blocks", this.filename, this.filePos, this.blockCount)

	return nil
}
