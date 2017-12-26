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
	fileSystem *FileSystem
	lv         *LogicalVolume
	blockSize  int
	filename   string

	filePos int

	blockWriter block.BlockWriter
	blockPos    int
	blockCount  int
}

func NewWriter(fs *FileSystem, lv *LogicalVolume, filename string, blockSize int) *Writer {
	glog.V(2).Infof("Allocate writer for %v with blockSize %d on %#v", filename, blockSize, lv)

	return &Writer{
		fileSystem: fs,
		lv:         lv,
		blockSize:  blockSize,
		filename:   filename,
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

			pvIdx := this.blockCount % len(this.lv.volumes)

			if blockWriter, err := this.lv.volumes[pvIdx].WriterFor(fmt.Sprintf("%d", this.blockCount)); err == nil {
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
	glog.V(1).Infof("Closing writer for file %v on volume %v.", this.filename, this.lv.Namespace)

	var err error

	if this.blockWriter != nil {
		err = this.blockWriter.Close()
	}

	glog.V(1).Infof("Closed writer for %s on %s. Wrote %d bytes to %d blocks", this.filename, this.lv.Namespace,
		this.filePos, this.blockCount)

	return err
}
