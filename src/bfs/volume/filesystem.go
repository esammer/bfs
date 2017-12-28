package volume

import (
	"bfs/ns"
	"fmt"
	"github.com/golang/glog"
	"strings"
)

type FileSystem struct {
	Namespace *ns.Namespace
	Volumes   []*LogicalVolume
}

func (this *FileSystem) Open() error {
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

func (this *FileSystem) Close() error {
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

func (this *FileSystem) OpenWrite(path string, blockSize int) (*Writer, error) {
	glog.V(1).Infof("Opening %v for write", path)

	selectedLv, err := this.selectLogicalVolume(path)
	if err != nil {
		return nil, err
	}

	return selectedLv.WriterFor(this, path, blockSize)
}

func (this *FileSystem) OpenRead(path string) (*Reader, error) {
	glog.V(1).Infof("Opening %v for read", path)

	selectedLv, err := this.selectLogicalVolume(path)
	if err != nil {
		return nil, err
	}

	return selectedLv.ReaderFor(this, path)
}

func (this *FileSystem) selectLogicalVolume(path string) (*LogicalVolume, error) {
	for _, lv := range this.Volumes {
		if strings.HasPrefix(path, lv.Namespace) {
			glog.V(2).Infof("Select volume %v for file %v", lv.Namespace, path)
			return lv, nil
		}
	}

	return nil, fmt.Errorf("unable to find volume for %s", path)
}