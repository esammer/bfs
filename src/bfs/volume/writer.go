package volume

import (
	"bfs/block"
	"fmt"
	"github.com/golang/glog"
)

/*
 * Writer
 */

type Writer struct {
	lv        *LogicalVolume
	blockSize int
	filename  string

	filePos int

	blockWriter block.BlockWriter
	blockPos    int
	blockCount  int
}

func NewWriter(lv *LogicalVolume, filename string, blockSize int) *Writer {
	glog.V(2).Infof("Allocate writer for %v with blockSize %d on %#v", filename, blockSize, lv)

	return &Writer{
		lv:        lv,
		blockSize: blockSize,
		filename:  filename,
	}
}

func (this *Writer) Write(buffer []byte) (int, error) {
	bufferPos := 0
	bufferRemaining := len(buffer)

	for bufferRemaining > 0 {
		writeLen := 0

		if this.blockPos == this.blockSize || this.blockCount == 0 {
			this.blockCount++

			if this.blockWriter != nil {
				if err := this.blockWriter.Close(); err != nil {
					return 0, err
				}
			}

			if blockWriter, err := this.lv.volumes[0].WriterFor(fmt.Sprintf("%d", this.blockCount)); err == nil {
				this.blockWriter = blockWriter
			} else {
				return 0, err
			}

			glog.V(1).Infof("Allocated block %d - old blockPos: %d filePos: %d", this.blockCount, this.blockPos, this.filePos)

			this.blockPos = 0
		}

		if bufferRemaining < (this.blockSize - this.blockPos) {
			writeLen = bufferRemaining
		} else {
			writeLen = this.blockSize - this.blockPos
		}

		if writeLen > 0 {
			glog.V(2).Infof("Write %d:%d of %d bytes to %v", bufferPos, bufferPos+writeLen, len(buffer), this.filename)

			if _, err := this.blockWriter.Write(buffer[bufferPos: bufferPos+writeLen]); err != nil {
				return 0, err
			}

			this.filePos += writeLen
			this.blockPos += writeLen
			bufferPos += writeLen
			bufferRemaining -= writeLen
		}
	}

	return 0, nil
}

func (this *Writer) Close() error {
	glog.V(1).Infof("Closing writer for file %v on %#v", this.filename, this.lv)

	if this.blockWriter != nil {
		return this.blockWriter.Close()
	}

	return nil
}
