package entry

import (
	"strings"
)

type DeletionStatus int

const (
	Exists DeletionStatus = iota
	SoftDeleted
	HardDeleted
)

type Entry struct {
	Storages []string
	Status   DeletionStatus
	Hash     string
}

// EntryFromBytes creates a entry struct from a given byte array. It first converts
// the bytes into a strings on which analysis is easier.
func EntryFromBytes(b []byte) Entry {
	// TODO: only use bytes such that encoding is faster and more robust.
	var e Entry
	s := string(b)
	e.Status = Exists

	if strings.HasPrefix(s, "DELETE") {
		e.Status = SoftDeleted
		s = s[7:]
	}

	if strings.HasPrefix(s, "HASH") {
		e.Hash = s[4:36]
		s = s[36:]
	}
	e.Storages = strings.Split(s, ",")

	return e
}

func (e *Entry) ToBytes() []byte {
	// TODO: probably optimize with string builder
	prefixStr := ""
	if e.Status == HardDeleted {
		panic("cannot put hard delete")
	}

	if e.Status == SoftDeleted {
		prefixStr = "DELETE"
	}

	if len(e.Hash) == 32 {
		prefixStr += "HASH" + e.Hash
	}
	return []byte(prefixStr + strings.Join(e.Storages, ","))
}
