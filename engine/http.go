package engine

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/nireo/jakaja/entry"
)

func shouldBalance(entryStorages, keyStorages []string) bool {
	if len(entryStorages) != len(keyStorages) {
		return true
	}

	for i := 0; i < len(entryStorages); i++ {
		if keyStorages[i] != entryStorages[i] {
			return true
		}
	}

	return false
}

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := []byte(r.URL.Path)

	if r.Method == http.MethodGet || r.Method == http.MethodPut ||
		r.Method == http.MethodDelete {
		if err := e.LockKey(r.URL.Path); err != nil {
			w.WriteHeader(http.StatusConflict)
			return
		}
		defer e.RemoveLock(r.URL.Path)
	}

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		ent := e.Get(key)
		var addr string

		if len(ent.Hash) != 0 {
			w.Header().Set("Content-Md5", ent.Hash)
		}

		if ent.Status == entry.SoftDeleted || ent.Status == entry.HardDeleted {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusNotFound)
			return
		}

		keyStorages := entry.KeyToStorage(key, e.storages, e.replicaCount, e.substorageCount)
		if shouldBalance(ent.Storages, keyStorages) {
			w.Header().Set("Balanced", "n")
		} else {
			w.Header().Set("Balanced", "y")
		}
		w.Header().Set("Storages", strings.Join(ent.Storages, ","))

		ok := false
		for _, ridx := range rand.Perm(len(ent.Storages)) {
			addr = fmt.Sprintf("http://%s%s", ent.Storages[ridx], entry.HashKey(key))
			found, _ := httpheader(addr, 1*time.Second)
			if found {
				ok = true
				break
			}
		}

		if !ok {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Location", addr)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusMovedPermanently)
	}
}
