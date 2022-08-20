package master

import (
	"fmt"
	"strings"
	"sync"
)

type FileNamespace struct {
	root *Entity
}

// recursive file representation
type Entity struct {
	dir        bool
	items      map[string]*Entity
	chunkCount int64
	len        int64

	sync.RWMutex
}

func NewFileNamespace() *FileNamespace {
	return &FileNamespace{
		root: &Entity{
			dir:   true,
			items: make(map[string]*Entity),
		},
	}
}

func (fn *FileNamespace) RLockAbove(path string, lockBelow bool) (
	*Entity, []string, error,
) {
	parents := strings.Split(path, "/")
	currEntity := fn.root

	if len(parents) == 0 {
		return currEntity, parents, nil
	}

	currEntity.RLock()
	for idx, name := range parents {
		child, ok := currEntity.items[name]
		if !ok {
			return currEntity, parents, fmt.Errorf("path was not found: %s", path)
		}

		if idx == (len(parents)-1) && lockBelow {
			currEntity = child
		} else {
			currEntity.RLock()
		}
	}

	return currEntity, parents, nil
}

func (fn *FileNamespace) RUnlockAbove(parents []string) {
	currEntity := fn.root

	if len(parents) != 0 {
		currEntity.Unlock()

		for _, name := range parents[:len(parents)-1] {
			child, ok := currEntity.items[name]
			if !ok {
				panic("??? lock")
			}

			currEntity = child
			currEntity.RUnlock()
		}
	}
}

func getPathAndName(path string) (string, string) {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[:i], path[i+1:]
		}
	}
	return "", ""
}

func (fn *FileNamespace) Create(p string) error {
	path, name := getPathAndName(p)

	currEntity, parents, err := fn.RLockAbove(path, true)
	defer fn.RUnlockAbove(parents)
	if err != nil {
		return err
	}

	currEntity.Lock()
	defer currEntity.Unlock()

	if _, ok := currEntity.items[name]; ok {
		return fmt.Errorf("path already exists %s", path)
	}

	currEntity.items[name] = new(Entity)
	return nil
}

func (fn *FileNamespace) Delete(p string) error {
	currEntity, parents, err := fn.RLockAbove(p, false)
	defer fn.RUnlockAbove(parents)
	if err != nil {
		return err
	}
	currEntity.Lock()
	defer currEntity.Unlock()

	delete(currEntity.items, parents[len(parents)-1])
	return nil
}
