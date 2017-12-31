package volume

import (
	"bfs/block"
	"bfs/ns"
	"github.com/golang/glog"
	"io"
)

type Reader interface {
	io.Reader
	io.Closer

	Open() error
}

type LocalFileReader struct {
	// Configuration
	fileSystem *LocalFileSystem
	volume     *LogicalVolume
	filename   string

	// Reader state
	entry    *ns.Entry
	blockIdx int

	// Current block state
	blockReader block.BlockReader
}

func NewReader(fileSystem *LocalFileSystem, volume *LogicalVolume, path string) *LocalFileReader {
	return &LocalFileReader{
		fileSystem: fileSystem,
		volume:     volume,
		filename:   path,
	}
}

func (this *LocalFileReader) Open() error {
	glog.V(1).Infof("Opening reader for %s", this.filename)

	entry, err := this.fileSystem.Namespace.Get(this.filename)
	if err != nil {
		return err
	}

	// Set up reader state.
	glog.V(2).Infof("Entry: %v", entry)
	this.entry = entry

	// Set up initial block state.
	this.blockIdx = 0

	return nil
}

func (this *LocalFileReader) Read(buffer []byte) (int, error) {
	glog.V(2).Infof("Read up to %d bytes", len(buffer))

	totalRead := 0

	for totalRead < len(buffer) {
		if this.blockReader == nil {
			glog.V(2).Infof("Opening new reader for block %v", this.entry.Blocks[this.blockIdx])

			for _, pv := range this.volume.volumes {
				if pv.ID.String() == this.entry.Blocks[this.blockIdx].PVID {
					glog.V(2).Infof("Read from pv %s", pv.ID.String())
					reader, err := pv.OpenRead(this.entry.Blocks[this.blockIdx].Block)
					if err != nil {
						return 0, err
					}

					this.blockReader = reader
					break
				}
			}
		}

		amountRead, err := this.blockReader.Read(buffer[totalRead:])
		totalRead += amountRead

		glog.V(2).Infof("Read %d bytes from block, %d total, %d allowed, %v err", amountRead, totalRead, len(buffer), err)

		if err != nil {
			if err != io.EOF {
				return totalRead, err
			}

			glog.V(2).Info("Hit end of block")
			this.blockIdx++
			this.blockReader = nil

			if this.blockIdx >= len(this.entry.Blocks) {
				glog.V(2).Info("No blocks left to read - EOF")
				return totalRead, io.EOF
			}
		}
	}

	return totalRead, nil
}

func (this *LocalFileReader) Close() error {
	glog.V(1).Infof("Closing reader for %s", this.filename)

	if this.blockReader != nil {
		return this.blockReader.Close()
	}

	return nil
}
