package block

import (
	"fmt"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
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
	BlockId      string
	RootPath     string
	Size         int
	writer       *os.File
	eventChannel chan interface{}
}

type BlockWriteEvent struct {
	Time            time.Time
	BlockId         string
	Size            int
	ResponseChannel chan interface{}
}

func NewWriter(rootPath string, blockId string, eventChannel chan interface{}) (*LocalBlockWriter, error) {
	path := filepath.Join(rootPath, blockId)

	glog.V(2).Infof("Open block %v @ %v for write", blockId, path)

	if writer, err := ioutil.TempFile(rootPath, fmt.Sprintf(".%s-", blockId)); err == nil {
		return &LocalBlockWriter{
			BlockId:      blockId,
			RootPath:     rootPath,
			writer:       writer,
			eventChannel: eventChannel,
		}, nil
	} else {
		return nil, err
	}
}

func (this *LocalBlockWriter) WriteString(text string) (int, error) {
	glog.V(2).Infof("Write string %s to block %v", text, this.BlockId)

	size, err := io.WriteString(this.writer, text)
	this.Size += size

	return size, err
}

func (this *LocalBlockWriter) Write(buffer []byte) (int, error) {
	glog.V(2).Infof("Write %d bytes to block %v", len(buffer), this.BlockId)

	size, err := this.writer.Write(buffer)
	this.Size += size

	return size, err
}

func (this *LocalBlockWriter) Close() error {
	if err := this.writer.Close(); err == nil {
		path := filepath.Join(this.RootPath, this.BlockId)

		glog.V(1).Infof("Committing block %v - move %v -> %v", this.BlockId, this.writer.Name(), path)

		if err := os.Rename(this.writer.Name(), path); err == nil {
			glog.V(2).Infof("Block %v committed", this.BlockId)

			responseChannel := make(chan interface{})

			this.eventChannel <- &BlockWriteEvent{
				Time:            time.Now(),
				BlockId:         this.BlockId,
				Size:            this.Size,
				ResponseChannel: responseChannel,
			}

			response := <-responseChannel
			close(responseChannel)

			glog.V(1).Infof("Received response: %v", response)

			if val, ok := response.(*BlockWriteEvent); ok {
				if val.BlockId != this.BlockId {
					return fmt.Errorf("received ack for wrong block: %s", this.BlockId)
				}
			} else {
				return fmt.Errorf("received a non-block write event on ack channel: %s", response)
			}
			return nil
		} else {
			return err
		}
	} else {
		return err
	}
}
