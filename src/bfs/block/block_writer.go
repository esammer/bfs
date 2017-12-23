package block

import (
	"fmt"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

/*
 * BlockWriter
 */

type BlockWriter interface {
	io.Writer
	io.Closer
}

/*
 * LocalBlockWriter
 */

type LocalBlockWriter struct {
	BlockId  string
	RootPath string
	Writer   *os.File
}

func NewWriter(rootPath string, blockId string) (*LocalBlockWriter, error) {
	path := filepath.Join(rootPath, blockId)

	glog.V(2).Infof("Open block %v @ %v for write", blockId, path)

	if writer, err := ioutil.TempFile(rootPath, fmt.Sprintf(".%s-", blockId)); err == nil {
		return &LocalBlockWriter{
			BlockId:  blockId,
			RootPath: rootPath,
			Writer:   writer,
		}, nil
	} else {
		return nil, err
	}
}

func (this *LocalBlockWriter) WriteString(text string) (int, error) {
	glog.V(2).Infof("Write string %s to block %v", text, this.BlockId)

	return io.WriteString(this.Writer, text)
}

func (this *LocalBlockWriter) Write(buffer []byte) (int, error) {
	glog.V(2).Infof("Write %d bytes to block %v", len(buffer), this.BlockId)

	return this.Writer.Write(buffer)
}

func (this *LocalBlockWriter) Close() error {
	if err := this.Writer.Close(); err == nil {
		path := filepath.Join(this.RootPath, this.BlockId)

		glog.V(1).Infof("Commiting block %v - move %v -> %v", this.BlockId, this.Writer.Name(), path)

		if err := os.Rename(this.Writer.Name(), path); err == nil {
			glog.V(2).Infof("Block %v committed", this.BlockId)
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}
