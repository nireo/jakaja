package engine

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"log"
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

// WriteToStorage handles writing the key-value pair into storage volumes. It
// returns the resulting http status code.
func (e *Engine) WriteToStorage(key []byte, value io.Reader, clen int64) int {
	keyStorages := entry.KeyToStorage(key, e.storages, e.replicaCount, e.substorageCount)

	if err := e.Put(key, entry.Entry{
		Storages: keyStorages,
		Status:   entry.SoftDeleted,
		Hash:     "",
	}); err != nil {
		return http.StatusInternalServerError
	}

	var buf bytes.Buffer
	body := io.TeeReader(value, &buf)

	for i := 0; i < len(keyStorages); i++ {
		if i != 0 {
			body = bytes.NewReader(buf.Bytes())
		}

		addr := fmt.Sprintf("http://%s%s", keyStorages[i], entry.HashKey(key))
		if httpput(addr, body, clen) != nil {
			log.Printf("replica %d write failed: %s\n", i, addr)
			return http.StatusInternalServerError
		}
	}

	hash := fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
	if err := e.Put(key, entry.Entry{
		Storages: e.storages,
		Status:   entry.Exists,
		Hash:     hash,
	}); err != nil {
		return http.StatusInternalServerError
	}

	return http.StatusCreated
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
	case http.MethodPost:
		status := e.WriteToStorage(key, r.Body, r.ContentLength)
		w.WriteHeader(status)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
