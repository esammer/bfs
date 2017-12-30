package volume

import (
	"fmt"
	"github.com/golang/glog"
)

/*
 * LogicalVolume
 */

type LogicalVolume struct {
	Namespace    string
	volumes      []*PhysicalVolume
	state        VolumeState
	eventChannel chan interface{}
}

func NewLogicalVolume(namespace string, volumes []*PhysicalVolume, eventChannel chan interface{}) *LogicalVolume {
	glog.V(1).Infof("Allocate logical volume for namespace %v", namespace)

	return &LogicalVolume{
		Namespace:    namespace,
		volumes:      volumes,
		state:        VolumeState_Initial,
		eventChannel: eventChannel,
	}
}

func (this *LogicalVolume) Open() error {
	glog.Infof("Open logical volume for namespace %v", this.Namespace)

	if this.state != VolumeState_Initial {
		return fmt.Errorf("Attempt to open volume from state %v", this.state)
	}

	this.state = VolumeState_Open

	return nil
}

func (this *LogicalVolume) Close() error {
	glog.Infof("Close logical volume for namespace %v", this.Namespace)

	if this.state != VolumeState_Open {
		return fmt.Errorf("Attempt to close volume from state %v", this.state)
	}

	this.state = VolumeState_Closed

	return nil
}

func (this *LogicalVolume) WriterFor(fs FileSystem, filename string, blockSize int) (*LocalFileWriter, error) {
	if this.state != VolumeState_Open {
		return nil, fmt.Errorf("Attempt to open writer from volume in state %v", this.state)
	}

	glog.V(1).Infof("Opening writer for %v with block size %v", filename, blockSize)

	return NewWriter(fs, this, filename, blockSize, this.eventChannel), nil
}

func (this *LogicalVolume) ReaderFor(fs *LocalFileSystem, filename string) (*LocalFileReader, error) {
	if this.state != VolumeState_Open {
		return nil, fmt.Errorf("Attempt to open reader from volume in state %v", this.state)
	}

	glog.V(1).Infof("Opening reader for %v", filename)

	return NewReader(fs, this, filename), nil
}
