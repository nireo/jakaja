package engine

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/nireo/jakaja/entry"
	"github.com/syndtr/goleveldb/leveldb"
)

type rreq struct {
	storage string
	addr    string
}

type rfile struct {
	name string
	ty   string
}

func addrFiles(addr string) []rfile {
	var files []rfile

	b, err := httpget(addr)
	if err != nil {
		return files
	}

	json.Unmarshal(b, &files)
	return files
}

func (e *Engine) buildFile(storage, name string) error {
	k, err := base64.StdEncoding.DecodeString(name)
	if err != nil {
		return err
	}
	skey := string(k)

	keyStorages := entry.KeyToStorage(k, e.Storages, e.ReplicaCount, e.SubstorageCount)

	if err := e.LockKey(skey); err != nil {
		return err
	}
	defer e.RemoveLock(skey)

	b, err := e.DB.Get(k, nil)
	var ent entry.Entry

	if err == leveldb.ErrNotFound {
		ent = entry.Entry{Storages: []string{storage}, Status: entry.Exists, Hash: ""}
	} else {
		ent = entry.EntryFromBytes(b)
		ent.Storages = append(ent.Storages, storage)
	}

	matching := make([]string, 0)
	for _, s1 := range keyStorages {
		for _, s2 := range ent.Storages {
			if s1 == s2 {
				matching = append(matching, s1)
			}
		}
	}

	for _, s1 := range ent.Storages {
		doInsertion := true
		for _, s2 := range keyStorages {
			if s1 == s2 {
				doInsertion = false
				break
			}
		}

		if doInsertion {
			matching = append(matching, s1)
		}
	}

	if err := e.Put(k, entry.Entry{
		Storages: matching,
		Status:   entry.Exists,
		Hash:     "",
	}); err != nil {
		return err
	}

	return nil
}

func valid(f rfile) bool {
	if len(f.name) != 2 || f.ty != "directory" {
		return false
	}

	decoded, err := hex.DecodeString(f.name)
	if err != nil {
		return false
	}

	if len(decoded) != 1 {
		return false
	}

	return true
}

func (e *Engine) Build() {
	it := e.DB.NewIterator(nil, nil)
	for it.Next() {
		e.DB.Delete(it.Key(), nil)
	}

	// waitgroup to ensure that everything has been done.
	var wg sync.WaitGroup
	requests := make(chan rreq, 20000)

	// spawn 128 goroutines to execute rebuilding
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range requests {
				files := addrFiles(req.addr)
				for _, f := range files {
					e.buildFile(req.storage, f.name)
				}
			}
		}()
	}

	parse := func(sto string) {
		for _, i := range addrFiles(fmt.Sprintf("http://%s/", sto)) {
			if valid(i) {
				for _, j := range addrFiles(fmt.Sprintf("http://%s/%s/", sto, i.name)) {
					if valid(j) {
						addr := fmt.Sprintf("http://%s/%s/%s/", sto, i.name, j.name)
						requests <- rreq{sto, addr}
					}
				}
			}
		}
	}

	for _, storage := range e.Storages {
		hasSubstorage := false

		for _, f := range addrFiles(fmt.Sprintf("http://%s/", storage)) {
			if len(f.name) == 4 && strings.HasPrefix(f.name, "sv") && f.ty == "directory" {
				parse(fmt.Sprintf("%s/%s", storage, f.name))
				hasSubstorage = true
			}
		}

		if !hasSubstorage {
			parse(storage)
		}
	}

	close(requests)
	wg.Wait()
}
