package volume

import (
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/pborman/uuid"
	"io/ioutil"
	"os"
	"path/filepath"

	"bfs/block"
)

/*
 * PhysicalVolume
 */

type PhysicalVolume struct {
	ID           uuid.UUID
	RootPath     string
	eventChannel chan interface{}

	state VolumeState
}

type VolumeState int

const (
	VolumeState_Initial VolumeState = iota
	VolumeState_Closed
	VolumeState_Open
)

var volumeStateStr = []string{
	"INITIAL",
	"CLOSED",
	"OPEN",
}

func (this *VolumeState) String() string {
	return volumeStateStr[*this]
}

func NewPhysicalVolume(rootPath string, eventChannel chan interface{}) *PhysicalVolume {
	glog.V(1).Infof("Create physical volume at %v", rootPath)

	return &PhysicalVolume{
		RootPath:     rootPath,
		eventChannel: eventChannel,
		state:        VolumeState_Initial,
	}
}

func (this *PhysicalVolume) Open(allowInitialization bool) error {
	glog.Infof("Open physical volume at %v", this.RootPath)

	if this.state != VolumeState_Initial {
		return fmt.Errorf("Can not open volume from state %v", this.state)
	}

	idPath := filepath.Join(this.RootPath, "id")

	if info, err := os.Stat(idPath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("Unable to open volume - %v is not a file", idPath)
		}
	} else if allowInitialization {
		glog.Infof("Volume %v does not exist or is uninitialized - creating it.", this.RootPath)

		if err := os.MkdirAll(this.RootPath, 0700); err == nil {
			glog.Infof("Volume path created at %v", this.RootPath)
		}

		id := uuid.NewRandom()
		if err := ioutil.WriteFile(idPath, id, 0644); err != nil {
			return err
		}

		glog.Infof("Generated volume ID: %v", id.String())
	} else {
		return err
	}

	id, err := ioutil.ReadFile(idPath)
	if err != nil {
		return err
	}

	this.ID = id
	this.state = VolumeState_Open

	glog.Infof("Opened physical volume %s at %s", this.ID, this.RootPath)

	return nil
}

func (this *PhysicalVolume) Close() error {
	glog.Infof("Close physical volume at %v", this.RootPath)

	if this.state == VolumeState_Open {
		this.state = VolumeState_Closed
	} else {
		return errors.New("Can not close unopened volume!")
	}

	return nil
}

func (this *PhysicalVolume) OpenRead(blockId string) (block.BlockReader, error) {
	if this.state != VolumeState_Open {
		return nil, fmt.Errorf("Can not create block reader on volume in state %v", this.state)
	}

	return block.NewReader(this.RootPath, blockId)
}

func (this *PhysicalVolume) OpenWrite(blockId string) (block.BlockWriter, error) {
	if this.state != VolumeState_Open {
		return nil, fmt.Errorf("Can not create block writer on volume in state %v", this.state)
	}

	return block.NewWriter(this.RootPath, blockId, this.eventChannel)
}
