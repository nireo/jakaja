package entry

import (
	"bytes"
	"encoding/binary"
	"math"
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

func (e *Entry) SerializeExperimental() []byte {
	if e.Status == HardDeleted {
		panic("cannot serialize hard delete")
	}
	b := bytes.NewBuffer(nil)
	if e.Status == SoftDeleted {
		a := make([]byte, 2)
		binary.LittleEndian.PutUint16(a, math.MaxUint16)
		b.Write(a)
	}

	if len(e.Hash) == 32 {
		a := make([]byte, 2)
		binary.LittleEndian.PutUint16(a, math.MaxUint16-1)
		b.Write(a)
		b.Write([]byte(e.Hash))
	}

	for idx := range e.Storages {
		b.Write([]byte(e.Storages[idx]))
		if idx != len(e.Storages)-1 {
			b.Write([]byte{','})
		}
	}
	return b.Bytes()
}

func DeserializeExperimental(b []byte) Entry {
	var e Entry
	e.Status = Exists
	e.Storages = []string{}

	// read prefixes properly first
	pref := binary.LittleEndian.Uint16(b[0:2])
	if pref == math.MaxUint16 {
		// we have deletion
		e.Status = SoftDeleted
		pref = binary.LittleEndian.Uint16(b[2:4])
	}

	if pref == (math.MaxUint16 - 1) {
		// we have a hash
		e.Hash = string(b[4:36])
		b = b[36:]
	}

	temp := make([]byte, 48)
	pos := 0
	// parse storages
	for idx := range b {
		if b[idx] == byte(',') {
			e.Storages = append(e.Storages, string(temp[:pos]))
			pos = 0
		}

		temp[pos] = b[idx]
		pos++

		if idx == len(b)-1 {
			e.Storages = append(e.Storages, string(temp[:pos]))
			pos = 0
		}
	}

	return e
}
