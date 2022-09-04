package engine

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nireo/jakaja/entry"
)

type breq struct {
	key         []byte
	storages    []string
	keyStorages []string
}

func (e *Engine) balance(r breq) bool {
	keyHash := entry.HashKey(r.key)

	// filter available volumes
	storages := make([]string, 0)
	for _, s := range r.storages {
		addr := fmt.Sprintf("http://%s%s", s, keyHash)
		ok, err := httpheader(addr, 1*time.Minute)
		if err != nil {
			return false
		}

		if ok {
			storages = append(storages, s)
		}
	}

	if len(storages) == 0 {
		return false
	}

	if !shouldBalance(storages, r.keyStorages) {
		return true
	}

	var err error = nil
	var ss []byte
	for _, s := range storages {
		addr := fmt.Sprintf("http://%s%s", s, keyHash)

		ss, err = httpget(addr)
		if err == nil {
			break
		}
	}

	if err != nil {
		return false
	}

	balanceErr := false
	for _, s := range r.keyStorages {
		shouldWrite := true
		for _, s2 := range storages {
			if s == s2 {
				shouldWrite = false
				break
			}
		}

		if shouldWrite {
			addr := fmt.Sprintf("http://%s%s", s, keyHash)
			if err := httpput(addr, bytes.NewReader(ss), int64(len(ss))); err != nil {
				log.Printf("error balancing put: %s\n", err)
				balanceErr = true
			}
		}
	}

	if balanceErr {
		return false
	}

	if err := e.Put(r.key, entry.Entry{
		Storages: r.keyStorages,
		Status:   entry.Exists,
		Hash:     "",
	}); err != nil {
		log.Printf("failed putting into database when balancing: %s\n", err)
	}

	delErr := false
	for _, s := range storages {
		shouldDelete := true
		for _, s2 := range r.keyStorages {
			if s == s2 {
				shouldDelete = false
				break
			}
		}

		if shouldDelete {
			addr := fmt.Sprintf("http://%s%s", s, keyHash)
			if err := httpdel(addr); err != nil {
				log.Printf("balance del error: %s\n", err)
				delErr = true
			}
		}
	}

	if delErr {
		return false
	}

	return true
}

func (e *Engine) Balance() {
	var wg sync.WaitGroup
	requests := make(chan breq, 20000)
	for i := 0; i < 16; i++ {
		go func() {
			for r := range requests {
				e.balance(r)
				wg.Done()
			}
		}()
	}

	it := e.DB.NewIterator(nil, nil)
	defer it.Release()

	for it.Next() {
		key := make([]byte, len(it.Key()))
		copy(key, it.Key())
		ent := entry.EntryFromBytes(it.Value())

		keyStorages := entry.KeyToStorage(key, e.Storages, e.ReplicaCount, e.SubstorageCount)
		wg.Add(1)

		requests <- breq{
			key:         key,
			storages:    ent.Storages,
			keyStorages: keyStorages,
		}
	}
	close(requests)
	wg.Wait()
}
