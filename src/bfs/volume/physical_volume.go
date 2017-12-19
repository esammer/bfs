package volume

import (
	"errors"
	"fmt"
	"log"
	"os"

	"bfs/block"
)

/*
 * PhysicalVolume
 */

type PhysicalVolume struct {
	RootPath string

	state VolumeState
}

type VolumeState int

const (
	VOLUME_INITIAL VolumeState = iota
	VOLUME_CLOSED
	VOLUME_OPEN
)

func NewPhysicalVolume(rootPath string) *PhysicalVolume {
	log.Printf("Create physical volume at %v", rootPath)

	return &PhysicalVolume{
		RootPath: rootPath,
		state:    VOLUME_INITIAL,
	}
}

func (this *PhysicalVolume) Open(allowInitialization bool) error {
	log.Printf("Open physical volume at %v", this.RootPath)

	if this.state == VOLUME_INITIAL {
		if info, err := os.Stat(this.RootPath); err == nil {
			if info.IsDir() {
				this.state = VOLUME_OPEN
			} else {
				return fmt.Errorf("Unable to open volume - %v is not a directory", this.RootPath)
			}
		} else {
			if allowInitialization {
				log.Printf("Volume path %v does not exist - creating it.", this.RootPath)

				// NB: This overrides err and simply returns it no matter what. See comment below.
				if err = os.MkdirAll(this.RootPath, 0700); err == nil {
					log.Printf("Volme path created at %v", this.RootPath)
					this.state = VOLUME_OPEN
				}
			}

			/*
			 * In the case allowInitialization is false, err is always the error encountered
			 * while trying to open the volume. Otherwise, err will contain any error encountered
			 * while making all directories in the volume path or nil if the operation was
			 * successful. The usual caveats about `mkdir -p`-style operations atomicity apply.
			 */
			return err
		}
	} else {
		return fmt.Errorf("Can not open volume from state %v", this.state)
	}

	return nil
}

func (this *PhysicalVolume) Close() error {
	log.Printf("Close physical volume at %v", this.RootPath)

	if this.state == VOLUME_OPEN {
		this.state = VOLUME_CLOSED
	} else {
		return errors.New("Can not close unopened volume!")
	}

	return nil
}

func (this *PhysicalVolume) ReaderFor(blockId string) (block.BlockReader, error) {
	return block.NewReader(this.RootPath, blockId)
}

func (this *PhysicalVolume) WriterFor(blockId string) (block.BlockWriter, error) {
	return block.NewWriter(this.RootPath, blockId)
}
