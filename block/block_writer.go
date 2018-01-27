package block

import (
	"bfs/util/logging"
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
	Size     int
	writer   *os.File
}

func NewWriter(rootPath string, blockId string) (*LocalBlockWriter, error) {
	path := filepath.Join(rootPath, blockId)

	glog.V(logging.LogLevelTrace).Infof("Open block %v @ %v for write", blockId, path)

	if writer, err := ioutil.TempFile(rootPath, fmt.Sprintf(".%s-", blockId)); err == nil {
		return &LocalBlockWriter{
			BlockId:  blockId,
			RootPath: rootPath,
			writer:   writer,
		}, nil
	} else {
		return nil, err
	}
}

func (this *LocalBlockWriter) WriteString(text string) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Write string %s to block %v", text, this.BlockId)

	size, err := io.WriteString(this.writer, text)
	this.Size += size

	return size, err
}

func (this *LocalBlockWriter) Write(buffer []byte) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Write %d bytes to block %v", len(buffer), this.BlockId)

	size, err := this.writer.Write(buffer)
	this.Size += size

	return size, err
}

func (this *LocalBlockWriter) Close() error {
	if err := this.writer.Close(); err == nil {
		path := filepath.Join(this.RootPath, this.BlockId)

		glog.V(logging.LogLevelDebug).Infof("Committing block %v - move %v -> %v", this.BlockId, this.writer.Name(), path)

		if err := os.Rename(this.writer.Name(), path); err == nil {
			glog.V(logging.LogLevelTrace).Infof("Block %v committed", this.BlockId)

			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}
