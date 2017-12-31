package volume

import (
	"bfs/ns"
	"fmt"
	"github.com/golang/glog"
	"strings"
)

type FileSystem interface {
	Open() error
	Close() error

	OpenRead(path string) (Reader, error)
	OpenWrite(path string, blockSize int) (Writer, error)
}

type LocalFileSystem struct {
	Namespace    *ns.Namespace
	Volumes      []*LogicalVolume
	EventChannel chan interface{}
}

func (this *LocalFileSystem) Open() error {
	glog.Info("Opening filesystem")

	if err := this.Namespace.Open(); err != nil {
		return err
	}

	for _, lv := range this.Volumes {
		if err := lv.Open(); err != nil {
			return err
		}
	}

	glog.Info("Opened filesystem")

	return nil
}

func (this *LocalFileSystem) Close() error {
	glog.Info("Closing filesystem")

	if err := this.Namespace.Close(); err != nil {
		return err
	}

	for _, lv := range this.Volumes {
		if err := lv.Close(); err != nil {
			return err
		}
	}

	glog.Info("Closed filesystem")

	return nil
}

func (this *LocalFileSystem) OpenWrite(path string, blockSize int) (Writer, error) {
	glog.V(1).Infof("Opening %v for write", path)

	selectedLv, err := this.selectLogicalVolume(path)
	if err != nil {
		return nil, err
	}

	return selectedLv.OpenWrite(this, path, blockSize)
}

func (this *LocalFileSystem) OpenRead(path string) (Reader, error) {
	glog.V(1).Infof("Opening %v for read", path)

	selectedLv, err := this.selectLogicalVolume(path)
	if err != nil {
		return nil, err
	}

	return selectedLv.OpenRead(this, path)
}

func (this *LocalFileSystem) selectLogicalVolume(path string) (*LogicalVolume, error) {
	for _, lv := range this.Volumes {
		if strings.HasPrefix(path, lv.Namespace) {
			glog.V(2).Infof("Select volume %v for file %v", lv.Namespace, path)
			return lv, nil
		}
	}

	return nil, fmt.Errorf("unable to find volume for %s", path)
}
