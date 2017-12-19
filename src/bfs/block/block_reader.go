package block

import (
	"io"
	"log"
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

	log.Printf("Open block %v @ %v for read", blockId, path)

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
	log.Printf("Reading up to %v bytes from block %v", len(buffer), this.BlockId)

	return this.Reader.Read(buffer)
}

func (this *LocalBlockReader) Close() error {
	log.Printf("Closing block reader for block %v", this.BlockId)

	if err := this.Reader.Close(); err == nil {
		log.Printf("Block reader closed")
		return nil
	} else {
		return err
	}
}
