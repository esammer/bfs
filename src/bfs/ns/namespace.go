package ns

import (
	"bytes"
	"encoding/json"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type FileStatus uint8

const (
	FileStatus_Unknown           FileStatus = iota
	FileStatus_UnderConstruction
	FileStatus_OK
	FileStatus_PendingDelete
)

var fileStatusStr = map[FileStatus]string{
	FileStatus_Unknown:           "UNKNOWN",
	FileStatus_UnderConstruction: "UNDER_CONSTRUCTION",
	FileStatus_OK:                "OK",
	FileStatus_PendingDelete:     "PENDING_DELETE",
}

func (this FileStatus) String() string {
	return fileStatusStr[this]
}

type Entry struct {
	Path        string
	Blocks      []string
	Permissions uint8
	Status      FileStatus
}

type Namespace struct {
	path string
	db   *leveldb.DB
}

// Default LevelDB read and write options.
var defaultReadOpts = &opt.ReadOptions{}
var defaultWriteOpts = &opt.WriteOptions{Sync: true}

const (
	// The initial size of the result buffer for List() operations. The result
	// buffer holds pointers (*Entry) so the cost of over-allocating should be
	// small.
	LIST_ALLOC_SIZE = 1024

	dbPrefix_Entry           = byte(1)
	dbPrefix_GlobalMetadata  = byte(2)
	dbPrefix_VolumeMetadata  = byte(3)
	dbPrefix_BlockAssignment = byte(4)
)

func New(path string) *Namespace {
	return &Namespace{
		path: path,
	}
}

func (this *Namespace) Open() error {
	glog.V(1).Infof("Opening namespace at %v", this.path)

	options := &opt.Options{
		ErrorIfMissing: false,
	}

	if db, err := leveldb.OpenFile(this.path, options); err != nil {
		return err
	} else {
		this.db = db
	}

	key := bytes.Join(
		[][]byte{
			{dbPrefix_GlobalMetadata},
			[]byte("blockId"),
		},
		nil,
	)

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

	return nil
}

func (this *Namespace) Add(path string, blockIds []string) error {
	glog.V(1).Infof("Adding entry %v blockIds: %v", path, blockIds)

	entry := &Entry{
		Path:        path,
		Blocks:      blockIds,
		Permissions: 0,
		Status:      FileStatus_Unknown,
	}

	if value, err := json.Marshal(entry); err != nil {
		return err
	} else {
		key := bytes.Join(
			[][]byte{
				{dbPrefix_Entry},
				[]byte(path),
			},
			nil,
		)
		return this.db.Put(key, value, defaultWriteOpts)
	}
}

func (this *Namespace) Get(path string) (*Entry, error) {
	glog.V(1).Infof("Getting entry %v", path)

	key := bytes.Join(
		[][]byte{
			{dbPrefix_Entry},
			[]byte(path),
		},
		nil,
	)

	if value, err := this.db.Get(key, defaultReadOpts); err != nil {
		return nil, err
	} else {
		var entry Entry

		if err := json.Unmarshal(value, &entry); err != nil {
			return nil, err
		}

		return &entry, nil
	}
}

func (this *Namespace) List(from string, to string) ([]*Entry, error) {
	glog.V(1).Infof("Listing entries from %v to %v", from, to)

	startKey := bytes.Join(
		[][]byte{
			{dbPrefix_Entry},
			[]byte(from),
		},
		nil,
	)
	endKey := bytes.Join(
		[][]byte{
			{dbPrefix_Entry},
			[]byte(to),
		},
		nil,
	)

	r := &util.Range{
		Start: startKey,
		Limit: endKey,
	}

	iter := this.db.NewIterator(r, defaultReadOpts)
	defer iter.Release()

	entries := make([]*Entry, 0, LIST_ALLOC_SIZE)

	for iter.Next() {
		var entry Entry

		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			return nil, err
		}

		glog.V(2).Infof("Entry: %#v", entry)

		entries = append(entries, &entry)
	}

	return entries, nil
}

func (this *Namespace) Close() error {
	glog.V(1).Infof("Closing namespace at %v", this.path)

	return this.db.Close()
}
