package block

import (
	"bfs/util/fsm"
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

	fsm *fsm.FSMInstance
}

func NewWriter(rootPath string, blockId string) (*LocalBlockWriter, error) {
	path := filepath.Join(rootPath, blockId)

	glog.V(logging.LogLevelTrace).Infof("Open block %v @ %v for write", blockId, path)

	fsmInst := readerWriterFSM.NewInstance()

	if writer, err := ioutil.TempFile(rootPath, fmt.Sprintf(".%s-", blockId)); err == nil {
		if err := fsmInst.To(StateOpen); err != nil {
			return nil, err
		}

		return &LocalBlockWriter{
			BlockId:  blockId,
			RootPath: rootPath,
			writer:   writer,
			fsm:      fsmInst,
		}, nil
	} else {
		return nil, fsmInst.ToWithErr(StateError, err)
	}
}

func (this *LocalBlockWriter) WriteString(text string) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Write string %s to block %v", text, this.BlockId)

	if err := this.fsm.Is(StateOpen); err != nil {
		return 0, err
	}

	size, err := io.WriteString(this.writer, text)
	if err != nil {
		return size, this.fsm.ToWithErr(StateError, err)
	}

	this.Size += size

	return size, nil
}

func (this *LocalBlockWriter) Write(buffer []byte) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Write %d bytes to block %v", len(buffer), this.BlockId)

	if err := this.fsm.Is(StateOpen); err != nil {
		return 0, err
	}

	size, err := this.writer.Write(buffer)
	if err != nil {
		return size, this.fsm.ToWithErr(StateError, err)
	}

	this.Size += size

	return size, nil
}

func (this *LocalBlockWriter) Close() error {
	if err := this.fsm.IsOneOf(StateOpen, StateError); err != nil {
		return err
	}

	if err := this.writer.Close(); err != nil {
		return this.fsm.ToWithErr(StateError, err)
	}

	path := filepath.Join(this.RootPath, this.BlockId)

	glog.V(logging.LogLevelDebug).Infof("Committing block %v - move %v -> %v", this.BlockId, this.writer.Name(), path)

	if err := os.Rename(this.writer.Name(), path); err != nil {
		return this.fsm.ToWithErr(StateError, err)
	}

	glog.V(logging.LogLevelTrace).Infof("Block %v committed", this.BlockId)

	return nil
}
