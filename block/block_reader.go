package block

import (
	"bfs/util/fsm"
	"bfs/util/logging"
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

	fsm *fsm.FSMInstance
}

func NewReader(rootPath string, blockId string) (*LocalBlockReader, error) {
	path := filepath.Join(rootPath, blockId)

	glog.V(logging.LogLevelDebug).Infof("Open block %v @ %v for read", blockId, path)

	fsmInst := readerWriterFSM.NewInstance()

	if reader, err := os.Open(path); err == nil {
		if err := fsmInst.To(StateOpen); err != nil {
			return nil, err
		}

		return &LocalBlockReader{
			BlockId: blockId,
			Reader:  reader,
			fsm:     fsmInst,
		}, nil
	} else {
		return nil, fsmInst.ToWithErr(StateError, err)
	}
}

func (this *LocalBlockReader) Read(buffer []byte) (int, error) {
	glog.V(logging.LogLevelTrace).Infof("Reading up to %v bytes from block %v", len(buffer), this.BlockId)

	if err := this.fsm.Is(StateOpen); err != nil {
		return 0, err
	}

	readLen, err := this.Reader.Read(buffer)
	if err != nil {
		return 0, this.fsm.ToWithErr(StateError, err)
	}

	return readLen, err
}

func (this *LocalBlockReader) Close() error {
	glog.V(logging.LogLevelDebug).Infof("Closing block reader for block %v", this.BlockId)

	if err := this.fsm.IsOneOf(StateOpen, StateError); err != nil {
		return err
	}

	if err := this.Reader.Close(); err != nil {
		return this.fsm.ToWithErr(StateError, err)
	}

	glog.V(logging.LogLevelDebug).Infof("Block reader closed")

	return this.fsm.To(StateClosed)
}
