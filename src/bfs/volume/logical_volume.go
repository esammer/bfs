package volume

import (
	"fmt"
	"github.com/golang/glog"

	"bfs/block"
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

func NewFileWriter(lv *LogicalVolume, filename string, blockSize int) *Writer {
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
			writeLen = (this.blockSize - this.blockPos)
		}

		if writeLen > 0 {
			glog.V(2).Infof("Write %d:%d of %d bytes to %v", bufferPos, bufferPos+writeLen, len(buffer), this.filename)

			if _, err := this.blockWriter.Write(buffer[bufferPos : bufferPos+writeLen]); err != nil {
				return 0, err
			}

			this.filePos += writeLen
			this.blockPos += writeLen
			bufferPos += writeLen
			bufferRemaining -= writeLen

			//log.Printf("State bufferPos: %d bufferRemaining: %d blockPos: %d", bufferPos, bufferRemaining, this.blockPos)
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

/*
 * LogicalVolume
 */

type LogicalVolume struct {
	Namespace string
	volumes   []*PhysicalVolume
	state     VolumeState
}

func NewLogicalVolume(namespace string, volumes []*PhysicalVolume) *LogicalVolume {
	glog.V(1).Infof("Allocate logical volume for namespace %v", namespace)

	return &LogicalVolume{
		Namespace: namespace,
		volumes:   volumes,
		state:     VOLUME_INITIAL,
	}
}

func (this *LogicalVolume) Open() error {
	glog.Infof("Open logical volume for namespace %v", this.Namespace)

	if this.state != VOLUME_INITIAL {
		return fmt.Errorf("Attempt to open volume from state %v", this.state)
	}

	this.state = VOLUME_OPEN

	return nil
}

func (this *LogicalVolume) Close() error {
	glog.Infof("Close logical volume for namespace %v", this.Namespace)

	if this.state != VOLUME_OPEN {
		return fmt.Errorf("Attempt to close volume from state %v", this.state)
	}

	this.state = VOLUME_CLOSED

	return nil
}

func (this *LogicalVolume) WriterFor(filename string, blockSize int) (*Writer, error) {
	if this.state != VOLUME_OPEN {
		return nil, fmt.Errorf("Attempt to open writer from volume in state %v", this.state)
	}

	glog.V(1).Infof("Opening writer for %v with block size %v", filename, blockSize)

	return NewFileWriter(this, filename, blockSize), nil
}

func (this *LogicalVolume) ReaderFor(filename string) error {
	if this.state != VOLUME_OPEN {
		return fmt.Errorf("Attempt to open reader from volume in state %v", this.state)
	}

	glog.V(1).Infof("Opening reader for %v", filename)

	return nil
}
