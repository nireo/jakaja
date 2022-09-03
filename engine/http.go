package engine

// http.go implements the http server interface and thus also handles all of the
// http endpoints. The methods are:
// - POST: Create entry
// - GET: Find entry
// - DELETE: Delete Entry
//
// Address format is http://localhost:$PORT/$KEYNAME. Having KEYNAME as path makes
// parsing easier and helps getting information out of the address. Other information
// is not passed in the URL, rather using HTTP headers.

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
	keyStorages := entry.KeyToStorage(key, e.Storages, e.ReplicaCount, e.SubstorageCount)

	// write entry into the leveldb
	if err := e.Put(key, entry.Entry{
		Storages: keyStorages,
		Status:   entry.SoftDeleted,
		Hash:     "",
	}); err != nil {
		return http.StatusInternalServerError
	}

	var buf bytes.Buffer
	body := io.TeeReader(value, &buf)
	// loop over all storages
	for i := 0; i < len(keyStorages); i++ {
		if i != 0 {
			body = bytes.NewReader(buf.Bytes())
		}

		// send body to all storage servers
		addr := fmt.Sprintf("http://%s%s", keyStorages[i], entry.HashKey(key))
		if httpput(addr, body, clen) != nil {
			log.Printf("replica %d write failed: %s\n", i, addr)
			return http.StatusInternalServerError
		}
	}

	// md5 checksum
	hash := fmt.Sprintf("%x", md5.Sum(buf.Bytes()))
	if err := e.Put(key, entry.Entry{
		Storages: e.Storages,
		Status:   entry.Exists,
		Hash:     hash,
	}); err != nil {
		return http.StatusInternalServerError
	}

	return http.StatusCreated
}

// Delete removes a given key and returns a http response status.
func (e *Engine) DeleteHandler(key []byte) int {
	ent := e.Get(key)
	if ent.Status == entry.HardDeleted {
		return http.StatusNotFound
	}

	if err := e.Put(key, entry.Entry{
		Storages: ent.Storages,
		Status:   entry.SoftDeleted,
		Hash:     ent.Hash,
	}); err != nil {
		return http.StatusInternalServerError
	}

	failed := false
	for _, sto := range ent.Storages {
		addr := fmt.Sprintf("http://%s%s", sto, entry.HashKey(key))
		if httpdel(addr) != nil {
			failed = true
		}
	}

	if failed {
		return http.StatusInternalServerError
	}

	// can hard delete
	e.DB.Delete(key, nil)

	return http.StatusNoContent
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

		keyStorages := entry.KeyToStorage(key, e.Storages, e.ReplicaCount, e.SubstorageCount)
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
	case http.MethodDelete:
		status := e.DeleteHandler(key)
		w.WriteHeader(status)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
