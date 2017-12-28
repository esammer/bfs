package volume

import (
	"bfs/block"
	"bfs/ns"
	"fmt"
	"github.com/golang/glog"
	"math/rand"
	"os"
	"time"
)

/*
 * Writer
 */

type Writer struct {
	fileSystem *LocalFileSystem
	volume     *LogicalVolume
	blockSize  int
	filename   string

	filePos int

	blockWriter     block.BlockWriter
	blockPos        int
	blockCount      int
	blockList       []*ns.BlockMetadata
	pvSelectionSeed int
}

func NewWriter(fs *LocalFileSystem, volume *LogicalVolume, filename string, blockSize int) *Writer {
	glog.V(2).Infof("Allocate writer for %v with blockSize %d on %#v", filename, blockSize, volume)

	return &Writer{
		fileSystem:      fs,
		volume:          volume,
		blockSize:       blockSize,
		filename:        filename,
		blockList:       make([]*ns.BlockMetadata, 0, 16),
		pvSelectionSeed: rand.Int(),
	}
}

func (this *Writer) Write(buffer []byte) (int, error) {
	bufferPos := 0
	bufferRemaining := len(buffer)

	for bufferRemaining > 0 {
		writeLen := 0

		if this.blockPos == this.blockSize || this.blockCount == 0 {
			now := (time.Now().Unix() * 1000000000) + int64(time.Now().Nanosecond())
			hostname, err := os.Hostname()

			if err != nil {
				return 0, err
			}

			blockId := fmt.Sprintf("%s-%d", hostname, now)
			this.blockCount++

			if this.blockWriter != nil {
				if err := this.blockWriter.Close(); err != nil {
					return 0, err
				}
			}

			pvIdx := (this.blockCount + this.pvSelectionSeed) % len(this.volume.volumes)
			pv := this.volume.volumes[pvIdx]
			blockMetadata := &ns.BlockMetadata{
				Block:  blockId,
				PVID:   pv.ID.String(),
				LVName: this.volume.Namespace,
			}

			this.blockList = append(this.blockList, blockMetadata)

			if blockWriter, err := pv.WriterFor(blockId); err == nil {
				this.blockWriter = blockWriter
			} else {
				return 0, err
			}

			glog.V(1).Infof("Allocated block %s - old blockPos: %d filePos: %d", blockId, this.blockPos, this.filePos)

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
	glog.V(1).Infof("Closing writer for file %v on volume %v.", this.filename, this.volume.Namespace)

	if this.blockWriter != nil {
		if err := this.blockWriter.Close(); err != nil {
			return err
		}
	}

	entry := &ns.Entry{
		VolumeName: this.volume.Namespace,
		Path:       this.filename,
		Blocks:     this.blockList,
		Status:     ns.FileStatus_OK,
	}

	if err := this.fileSystem.Namespace.Add(entry); err != nil {
		return err
	}

	glog.V(1).Infof("Closed writer for %s on %s. Wrote %d bytes to %d blocks", this.filename, this.volume.Namespace,
		this.filePos, this.blockCount)

	return nil
}
