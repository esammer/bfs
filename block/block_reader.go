package block

import (
	"github.com/golang/glog"
	"io"
	"os"
	"path/filepath"
)

/*
 * BlockReader
 */

type BlockReader interface {
	io.Reader
	io.Closer
}

/*
 * LocalBlockReader
 */

type LocalBlockReader struct {
	BlockId string
	Reader  *os.File
}

func NewReader(rootPath string, blockId string) (*LocalBlockReader, error) {
	path := filepath.Join(rootPath, blockId)

	glog.V(1).Infof("Open block %v @ %v for read", blockId, path)

	if reader, err := os.Open(path); err == nil {
		return &LocalBlockReader{
			BlockId: blockId,
			Reader:  reader,
		}, nil
	} else {
		return nil, err
	}
}

func (this *LocalBlockReader) Read(buffer []byte) (int, error) {
	glog.V(2).Infof("Reading up to %v bytes from block %v", len(buffer), this.BlockId)

	return this.Reader.Read(buffer)
}

func (this *LocalBlockReader) Close() error {
	glog.V(1).Infof("Closing block reader for block %v", this.BlockId)

	if err := this.Reader.Close(); err == nil {
		glog.V(1).Infof("Block reader closed")
		return nil
	} else {
		return err
	}
}
