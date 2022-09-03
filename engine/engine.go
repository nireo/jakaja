package engine

import (
	"fmt"
	"sync"

	"github.com/nireo/jakaja/entry"
	"github.com/syndtr/goleveldb/leveldb"
)

type Engine struct {
	db              *leveldb.DB
	mu              sync.Mutex
	keylocks        map[string]struct{}
	storages        []string
	replicaCount    int
	substorageCount int
}

func (e *Engine) LockKey(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.keylocks[key]; ok {
		return fmt.Errorf("key already locked")
	}
	e.keylocks[key] = struct{}{}
	return nil
}

func (e *Engine) RemoveLock(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.keylocks, key)
}

func (e *Engine) Put(key []byte, ent entry.Entry) error {
	return e.db.Put(key, ent.ToBytes(), nil)
}

func (e *Engine) Get(key []byte) entry.Entry {
	b, err := e.db.Get(key, nil)
	en := entry.Entry{Storages: []string{}, Status: entry.HardDeleted, Hash: ""}

	if err == leveldb.ErrNotFound {
		return en
	}

	en = entry.EntryFromBytes(b)
	return en
}

func NewEngine() *Engine {
	return &Engine{}
}
