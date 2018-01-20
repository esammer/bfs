package lru

import (
	"container/list"
	"sync"
)

type LRUCache struct {
	Size int

	entryIndex map[string]*list.Element
	entries    *list.List
	mutex      *sync.Mutex

	createFunc  CreateFunc
	destroyFunc DestroyFunc
}

type CreateFunc func(name string) (interface{}, error)
type DestroyFunc func(name string, value interface{}) error

type Entry struct {
	Name  string
	Value interface{}
}

var (
	DefaultCreateFunc  CreateFunc  = func(name string) (interface{}, error) { return nil, nil }
	DefaultDestroyFunc DestroyFunc = nil
)

func NewCache(size int, createFunc CreateFunc, destroyFunc DestroyFunc) *LRUCache {
	pool := &LRUCache{
		Size:        size,
		entryIndex:  make(map[string]*list.Element, size),
		entries:     list.New(),
		mutex:       &sync.Mutex{},
		createFunc:  createFunc,
		destroyFunc: destroyFunc,
	}

	return pool
}

func (this *LRUCache) Get(name string) (interface{}, error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	elem, ok := this.entryIndex[name]

	if !ok {
		if err := this.checkEviction(); err != nil {
			return nil, err
		}

		val, err := this.createFunc(name)
		if err != nil {
			return nil, err
		}

		this.entryIndex[name] = this.entries.PushFront(&Entry{Name: name, Value: val})

		return val, err
	}

	this.entries.MoveToFront(elem)

	return elem.Value.(*Entry).Value, nil
}

func (this *LRUCache) checkEviction() error {
	// This purposefully evicts 1 element early (i.e. >= rather than >) because we want
	// the LRU to never exceed Size.
	for i := 0; this.entries.Len() >= this.Size; i++ {
		oldestElem := this.entries.Back()
		this.entries.Remove(oldestElem)

		delete(this.entryIndex, oldestElem.Value.(*Entry).Name)

		if this.destroyFunc != nil {
			entry := oldestElem.Value.(*Entry)
			if err := this.destroyFunc(entry.Name, entry.Value); err != nil {
				return err
			}
		}
	}

	return nil
}

func (this *LRUCache) Entries() []*Entry {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	entries := make([]*Entry, this.entries.Len())
	i := 0

	for elem := this.entries.Front(); elem != nil; elem = elem.Next() {
		entries[i] = elem.Value.(*Entry)
		i++
	}

	return entries
}

func (this *LRUCache) Purge() error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.destroyFunc != nil {
		for elem := this.entries.Front(); elem != nil; elem = elem.Next() {
			entry := elem.Value.(*Entry)
			if err := this.destroyFunc(entry.Name, entry.Value); err != nil {
				return err
			}
		}
	}

	this.entryIndex = make(map[string]*list.Element, 0)
	this.entries.Init()

	return nil
}

func (this *LRUCache) Len() int {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	return this.entries.Len()
}
