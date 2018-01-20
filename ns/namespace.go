package ns

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"io"
)

type FileStatus uint8

const (
	FileStatus_Unknown           FileStatus = iota
	FileStatus_UnderConstruction
	FileStatus_OK
	FileStatus_PendingDelete
)

var fileStatusStr = []string{
	"UNKNOWN",
	"UNDER_CONSTRUCTION",
	"OK",
	"PENDING_DELETE",
}

func (this *FileStatus) String() string {
	return fileStatusStr[*this]
}

type Entry struct {
	VolumeName       string
	Path             string
	Blocks           []*BlockMetadata
	Permissions      uint8
	Status           FileStatus
	BlockSize        uint64
	Size             uint64
	ReplicationLevel uint32
}

type BlockMetadata struct {
	Block  string
	LVName string
	PVID   string
}

type status int

const (
	namespaceStatus_INITIAL status = iota
	namespaceStatus_OPEN
	namespaceStatus_CLOSED
)

var namespaceStatusStr = []string{
	"INITIAL",
	"OPEN",
	"CLOSED",
}

func (this *status) String() string {
	return namespaceStatusStr[*this]
}

type Namespace struct {
	path string
	db   *leveldb.DB

	state status
}

type Error struct {
	error
	Path string
}

func (this *Error) Error() string {
	return ErrNoSuchEntry.Error() + " - " + this.Path
}

var ErrNoSuchEntry = errors.New("no such entry")

// Default LevelDB read and write options.
var defaultReadOpts = &opt.ReadOptions{}
var defaultWriteOpts = &opt.WriteOptions{Sync: true}

const (
	// The initial size of the result buffer for List() operations. The result
	// buffer holds pointers (*Entry) so the cost of over-allocating should be
	// small.
	listAllocSize = 1024

	dbPrefix_Entry           = byte(1)
	dbPrefix_GlobalMetadata  = byte(2)
	dbPrefix_VolumeMetadata  = byte(3)
	dbPrefix_BlockAssignment = byte(4)
)

func New(path string) *Namespace {
	return &Namespace{
		path:  path,
		state: namespaceStatus_INITIAL,
	}
}

func (this *Namespace) Open() error {
	glog.V(1).Infof("Opening namespace at %v", this.path)

	if this.state != namespaceStatus_INITIAL {
		return fmt.Errorf("unable to open namespace from state %v", this.state)
	}

	options := &opt.Options{
		ErrorIfMissing: false,
	}

	if db, err := leveldb.OpenFile(this.path, options); err != nil {
		return err
	} else {
		this.db = db
	}

	key := keyFor(dbPrefix_GlobalMetadata, "blockId")

	if ok, err := this.db.Has(key, defaultReadOpts); ok {
		glog.V(1).Info("Last blockId exists")
	} else if err != nil {
		return err
	} else {
		if err := this.db.Put(key, []byte{byte(0)}, defaultWriteOpts); err != nil {
			glog.Errorf("Failed to set initial blockId for the namespace - %v", err)
			return err
		} else {
			glog.V(1).Info("Initialized blockId for the namespace")
		}
	}

	this.state = namespaceStatus_OPEN

	return nil
}

func (this *Namespace) Add(entry *Entry) error {
	glog.V(1).Infof("Adding entry %#v", entry)

	if this.state != namespaceStatus_OPEN {
		return fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	value, err := json.Marshal(entry)

	if err != nil {
		return err
	}

	key := keyFor(dbPrefix_Entry, entry.Path)

	glog.V(2).Infof("Serialized to entry: %v", string(value))

	if err = this.db.Put(key, value, defaultWriteOpts); err != nil {
		return err
	}

	for _, blockMetadata := range entry.Blocks {
		key := keyFor(dbPrefix_BlockAssignment, blockMetadata.Block)

		value, err := json.Marshal(blockMetadata)
		if err != nil {
			return err
		}

		glog.V(2).Infof("Serialized block %v", string(value))
		if err := this.db.Put(key, value, defaultWriteOpts); err != nil {
			return err
		}
	}

	return nil
}

func (this *Namespace) Get(path string) (*Entry, error) {
	glog.V(1).Infof("Getting entry %v", path)

	if this.state != namespaceStatus_OPEN {
		return nil, fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	key := keyFor(dbPrefix_Entry, path)

	value, err := this.db.Get(key, defaultReadOpts)

	if err == leveldb.ErrNotFound {
		return nil, &Error{Path: path, error: ErrNoSuchEntry}
	} else if err != nil {
		return nil, err
	}

	var entry Entry

	if err := json.Unmarshal(value, &entry); err != nil {
		return nil, err
	}

	return &entry, nil
}

func (this *Namespace) List(from string, to string, visitor func(*Entry, error) (bool, error)) error {
	glog.V(1).Infof("Listing entries from %v to %v", from, to)

	if this.state != namespaceStatus_OPEN {
		return fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	var startKey []byte
	if from != "" {
		startKey = keyFor(dbPrefix_Entry, from)
	}

	var endKey []byte
	if to != "" {
		endKey = keyFor(dbPrefix_Entry, to)
	}

	r := &util.Range{
		Start: startKey,
		Limit: endKey,
	}

	iter := this.db.NewIterator(r, defaultReadOpts)
	defer iter.Release()

	for iter.Next() {
		if iter.Key()[0] != dbPrefix_Entry {
			continue
		}

		var entry Entry

		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			return err
		}

		glog.V(2).Infof("Entry: %#v", entry)

		if ok, err := visitor(&entry, nil); err != nil {
			return err
		} else if !ok {
			break
		}
	}

	visitor(nil, io.EOF)

	return nil
}

func (this *Namespace) Delete(path string) error {
	glog.V(1).Infof("Deleting entry %s", path)

	if this.state != namespaceStatus_OPEN {
		return fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	return this.db.Delete(keyFor(dbPrefix_Entry, path), defaultWriteOpts)
}

func (this *Namespace) Rename(from string, to string) (bool, error) {
	glog.V(1).Infof("Renaming entry %s to %s", from, to)

	if this.state != namespaceStatus_OPEN {
		return false, fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	entry, err := this.Get(from)
	if err != nil {
		return false, err
	}

	entry.Path = to

	value, err := json.Marshal(entry)
	if err != nil {
		return false, err
	}

	batch := leveldb.Batch{}
	batch.Put(keyFor(dbPrefix_Entry, to), value)
	batch.Delete(keyFor(dbPrefix_Entry, from))

	return true, this.db.Write(&batch, defaultWriteOpts)
}

func (this *Namespace) AddVolume(volumeId string, pvIds []string) error {
	if this.state != namespaceStatus_OPEN {
		return fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	value, err := json.Marshal(pvIds)
	if err != nil {
		return err
	}

	return this.db.Put(keyFor(dbPrefix_VolumeMetadata, volumeId), value, defaultWriteOpts)
}

func (this *Namespace) Volume(volumeId string) ([]string, error) {
	if this.state != namespaceStatus_OPEN {
		return nil, fmt.Errorf("unable to perform operation in state %v", this.state)
	}

	value, err := this.db.Get(keyFor(dbPrefix_VolumeMetadata, volumeId), defaultReadOpts)
	if err != nil {
		return nil, err
	}

	pvIds := make([]string, 0, 128)
	err = json.Unmarshal(value, &pvIds)
	if err != nil {
		return nil, err
	}

	return pvIds, nil
}

func (this *Namespace) Close() error {
	glog.V(1).Infof("Closing namespace at %v", this.path)

	if this.state != namespaceStatus_OPEN {
		return fmt.Errorf("unable to close namespace from state %v", this.state)
	}

	err := this.db.Close()

	this.state = namespaceStatus_CLOSED

	return err
}

func keyFor(table byte, key string) []byte {
	return bytes.Join(
		[][]byte{
			{table},
			[]byte(key),
		},
		nil,
	)
}
