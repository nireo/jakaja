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
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nireo/jakaja/entry"
)

// shouldBalance checks that entryStorages and keyStorages should be the same.
// if not then we need to balance the keys again.
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

	buf, err := io.ReadAll(value)
	if err != nil {
		return http.StatusInternalServerError
	}

	var wg sync.WaitGroup

	// create a channel for errors so that we can concurrently handle them.
	errs := make(chan error, len(keyStorages))

	// Start a thread for each key storage that transports the file.
	for i := 0; i < len(keyStorages); i++ {
		wg.Add(1)

		// start a thread that writes a given io.Reader body to a storage server using a HTTP Put request.
		go func(storage string, body io.Reader) {
			defer wg.Done()
			addr := fmt.Sprintf("http://%s%s", storage, entry.HashKey(key))
			if err := httpput(addr, body, clen); err != nil {
				errs <- err
			}
		}(keyStorages[i], bytes.NewReader(buf))
	}

	// make sure that every write is done.
	wg.Wait()
	close(errs) // close the error channel

	// if a single entry is in errors, the whole write process hasn't been successful.
	if len(errs) != 0 {
		return 500
	}

	// md5 checksum
	hash := fmt.Sprintf("%x", md5.Sum(buf))
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
	ent.Status = entry.SoftDeleted

	if err := e.Put(key, ent); err != nil {
		return http.StatusInternalServerError
	}

	failed := false

	// delete the entry from all of the replica servers
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

	// ensure that no other actions are being done on that key.
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

		// set md5 checksum header if exists
		if len(ent.Hash) != 0 {
			w.Header().Set("Content-Md5", ent.Hash)
		}

		// cannot get value that has been softly or hardly deleted.
		if ent.Status == entry.SoftDeleted || ent.Status == entry.HardDeleted {
			w.Header().Set("Content-Length", "0")
			w.WriteHeader(http.StatusNotFound)
			return
		}

		keyStorages := entry.KeyToStorage(key, e.Storages, e.ReplicaCount, e.SubstorageCount)

		// set useful extra info in header
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

		// redirect the request to the storage server.
		w.Header().Set("Location", addr)
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusMovedPermanently)
	case http.MethodPut:
		status := e.WriteToStorage(key, r.Body, r.ContentLength)
		w.WriteHeader(status)
	case http.MethodDelete:
		status := e.DeleteHandler(key)
		w.WriteHeader(status)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
