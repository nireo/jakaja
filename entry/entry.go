package entry

import (
	"strings"
)

// TODO: Make entry serialization and deserialization faster somehow. Since
// manipulating strings (joining|splitting) is not that fast and makes a lot of
// not necessary allocations.

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
	var e Entry
	s := string(b)
	e.Status = Exists
	e.Hash = ""

	if strings.HasPrefix(s, "DELETE") {
		e.Status = SoftDeleted
		s = s[6:]
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
