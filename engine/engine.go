package engine

import (
	"fmt"
	"sync"
)

type Engine struct {
	mu sync.Mutex
	keylocks map[string]struct{}
	storages []string
	replicaCount int
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

func (e *Engine) RemoveLock(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.keylocks, key)
}
