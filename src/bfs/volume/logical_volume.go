package volume

import (
	"fmt"
	"github.com/golang/glog"

)

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

	return NewWriter(this, filename, blockSize), nil
}

func (this *LogicalVolume) ReaderFor(filename string) error {
	if this.state != VOLUME_OPEN {
		return fmt.Errorf("Attempt to open reader from volume in state %v", this.state)
	}

	glog.V(1).Infof("Opening reader for %v", filename)

	return nil
}
