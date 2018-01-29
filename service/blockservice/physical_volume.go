package blockservice

import (
	"bfs/util/fsm"
	"bfs/util/logging"
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

const (
	StateInitial = "INITIAL"
	StateOpen    = "OPEN"
	StateClosed  = "CLOSED"
	StateError   = "ERROR"
)

var volumeFSM = fsm.New(StateInitial).
	Allow(StateInitial, StateOpen).
	Allow(StateInitial, StateError).
	Allow(StateOpen, StateClosed).
	Allow(StateOpen, StateError).
	Allow(StateError, StateClosed)

type PhysicalVolume struct {
	ID       uuid.UUID
	RootPath string

	fsm *fsm.FSMInstance
}

func NewPhysicalVolume(rootPath string) *PhysicalVolume {
	glog.V(logging.LogLevelDebug).Infof("Create physical volume at %v", rootPath)

	return &PhysicalVolume{
		RootPath: rootPath,
		fsm:      volumeFSM.NewInstance(),
	}
}

func (this *PhysicalVolume) Open(allowInitialization bool) error {
	glog.Infof("Open physical volume at %v", this.RootPath)

	if err := this.fsm.Is(StateInitial); err != nil {
		return err
	}

	idPath := filepath.Join(this.RootPath, "id")

	if info, err := os.Stat(idPath); err == nil {
		if !info.Mode().IsRegular() {
			this.fsm.To(StateError)
			return fmt.Errorf("Unable to open volume - %v is not a file", idPath)
		}
	} else if allowInitialization {
		glog.Infof("Volume %v does not exist or is uninitialized - creating it.", this.RootPath)

		if err := os.MkdirAll(this.RootPath, 0700); err == nil {
			glog.Infof("Volume path created at %v", this.RootPath)
		}

		id := uuid.NewRandom()
		if err := ioutil.WriteFile(idPath, id, 0644); err != nil {
			this.fsm.To(StateError)
			return err
		}

		glog.Infof("Generated volume ID: %v", id.String())
	} else {
		this.fsm.To(StateError)
		return err
	}

	id, err := ioutil.ReadFile(idPath)
	if err != nil {
		this.fsm.To(StateError)
		return err
	}

	this.ID = id

	glog.Infof("Opened physical volume %s at %s", this.ID, this.RootPath)

	return this.fsm.To(StateOpen)
}

func (this *PhysicalVolume) Close() error {
	glog.Infof("Close physical volume at %v", this.RootPath)

	if err := this.fsm.IsOneOf(StateOpen, StateError); err != nil {
		return err
	}

	return this.fsm.To(StateClosed)
}

func (this *PhysicalVolume) OpenRead(blockId string) (block.BlockReader, error) {
	if err := this.fsm.Is(StateOpen); err != nil {
		return nil, err
	}

	return block.NewReader(this.RootPath, blockId)
}

func (this *PhysicalVolume) OpenWrite(blockId string) (block.BlockWriter, error) {
	if err := this.fsm.Is(StateOpen); err != nil {
		return nil, err
	}

	return block.NewWriter(this.RootPath, blockId)
}

func (this *PhysicalVolume) Delete(blockId string) error {
	if err := this.fsm.Is(StateOpen); err != nil {
		return err
	}

	path := filepath.Join(this.RootPath, blockId)

	return os.Remove(path)
}
