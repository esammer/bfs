package ns

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type Entry struct {
	Path   string
	Blocks []string
}

type Namespace struct {
	dbPath string
	db     *leveldb.DB
}

func New(dbPath string) *Namespace {
	return &Namespace{
		dbPath: dbPath,
	}
}

func (this *Namespace) Open() error {
	options := &opt.Options{
		ErrorIfMissing: false,
	}

	if db, err := leveldb.OpenFile(this.dbPath, options); err != nil {
		return err
	} else {
		this.db = db
	}

	if ok, err := this.db.Has([]byte("global/blockId"), &opt.ReadOptions{}); ok {
		glog.V(1).Info("Last blockId exists")
	} else if err != nil {
		return err
	} else {
		if err := this.db.Put([]byte("global/blockId"), []byte{byte(0)}, &opt.WriteOptions{}); err != nil {
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

	if value, err := json.Marshal(blockIds); err != nil {
		return err
	} else {
		return this.db.Put([]byte(path), value, &opt.WriteOptions{Sync: true})
	}
}

func (this *Namespace) Get(path string) (*Entry, error) {
	glog.V(1).Infof("Getting entry %v", path)

	if value, err := this.db.Get([]byte(path), &opt.ReadOptions{}); err != nil {
		return nil, err
	} else {
		var blockIds []string

		if err := json.Unmarshal(value, &blockIds); err != nil {
			return nil, err
		}

		return &Entry{
			Path:   path,
			Blocks: blockIds,
		}, nil
	}
}

func (this *Namespace) List(from string, to string) ([]*Entry, error) {
	glog.V(1).Infof("Listing entries from %v to %v", from, to)

	r := &util.Range{
		Start: []byte(from),
		Limit: []byte(to),
	}

	iter := this.db.NewIterator(r, &opt.ReadOptions{})
	defer iter.Release()

	entries := make([]*Entry, 0)

	for iter.Next() {
		var blockIds []string

		if err := json.Unmarshal(iter.Value(), &blockIds); err != nil {
			return nil, err
		}

		entry := &Entry{
			Path:   string(iter.Key()),
			Blocks: blockIds,
		}

		glog.V(2).Infof("Entry: %#v", entry)

		entries = append(entries, entry)
	}

	return entries, nil
}

func (this *Namespace) Close() error {
	glog.V(1).Infof("Closing namespace at %v", this.dbPath)

	return this.db.Close()
}
