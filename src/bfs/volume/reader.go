package volume

import (
	"bfs/block"
	"github.com/golang/glog"
	"io"
)

type Reader struct {
	fileSystem *FileSystem
	volume     *LogicalVolume
	filename   string

	blockReader *block.BlockReader
	blockIdx    int
}

func NewReader(fileSystem *FileSystem, volume *LogicalVolume, path string) *Reader {
	return &Reader{
		fileSystem: fileSystem,
		volume:     volume,
		filename:   path,
	}
}

func (this *Reader) Open() error {
	glog.Infof("Opening reader for %s", this.filename)

	entry, err := this.fileSystem.Namespace.Get(this.filename)
	if err != nil {
		return err
	}

	glog.Infof("Entry: %v", entry)
	this.blockIdx = 0

	return nil
}

func (this *Reader) Read(buffer []byte) (int, error) {
	glog.Infof("Read up to %d", cap(buffer))

	return 0, io.EOF
}

func (this *Reader) Close() error {
	glog.Infof("Closing reader for %s", this.filename)

	return nil
}
