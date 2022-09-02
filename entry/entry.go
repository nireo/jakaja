package entry

import "strings"

type DeletionStatus int

const (
	Exists DeletionStatus = iota
	SoftDeleted
	HardDeleted
)

type Entry struct {
	hash    string
	volumes []string
	status  DeletionStatus
}

// EntryFromBytes creates a entry struct from a given byte array. It first converts
// the bytes into a strings on which analysis is easier.
func EntryFromBytes(b []byte) Entry {
	// TODO: only use bytes such that encoding is faster and more robust.
	var e Entry
	s := string(b)
	e.status = Exists

	if strings.HasPrefix(s, "DELETE") {
		e.status = SoftDeleted
		s = s[7:]
	}

	if strings.HasPrefix(s, "HASH") {
		e.hash = s[4:36]
		s = s[36:]
	}
	e.volumes = strings.Split(s, ",")

	return e
}

func (e *Entry) ToBytes() []byte {
	// TODO: probably optimize with string builder
	prefixStr := ""
	if e.status == HardDeleted {
		panic("cannot put hard delete")
	}

	if e.status == SoftDeleted {
		prefixStr = "DELETE"
	}

	if len(e.hash) == 32 {
		prefixStr += "HASH" + e.hash
	}
	return []byte(prefixStr + strings.Join(e.volumes, ","))
}
