package engine

import (
	"fmt"
	"sync"

	"github.com/nireo/jakaja/entry"
	"github.com/syndtr/goleveldb/leveldb"
)

type Engine struct {
	DB              *leveldb.DB
	mu              sync.Mutex
	Keylocks        map[string]struct{}
	Storages        []string
	ReplicaCount    int
	SubstorageCount int
}

func (e *Engine) LockKey(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.Keylocks[key]; ok {
		return fmt.Errorf("key already locked")
	}
	e.Keylocks[key] = struct{}{}
	return nil
}

func (e *Engine) RemoveLock(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.Keylocks, key)
}

func (e *Engine) Put(key []byte, ent entry.Entry) error {
	return e.DB.Put(key, ent.ToBytes(), nil)
}

func (e *Engine) Get(key []byte) entry.Entry {
	b, err := e.DB.Get(key, nil)
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
