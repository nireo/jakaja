package entry_test

import (
	"crypto/md5"
	"fmt"
	"reflect"
	"testing"

	"github.com/nireo/jakaja/entry"
)

func Test_entrySerialization(t *testing.T) {
	hash := fmt.Sprintf("%x", md5.Sum([]byte("testhash")))

	entries := []entry.Entry{
		{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.Exists, Hash: hash},
		{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.SoftDeleted, Hash: ""},
		{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.SoftDeleted, Hash: hash},
		{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.Exists, Hash: ""},
	}

	for idx, ent := range entries {
		byteArray := ent.ToBytes()

		if !reflect.DeepEqual(entry.EntryFromBytes(byteArray), ent) {
			fmt.Println(entry.EntryFromBytes(byteArray), ent)
			t.Fatalf("failed to serialize/unserialize entry %d", idx)
		}
	}
}

func Benchmark_entrySerializationString(b *testing.B) {
	b.ReportAllocs()
	entry := entry.Entry{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.Exists, Hash: ""}

	for i := 0; i < b.N; i++ {
		entry.ToBytes()
	}
}

func Benchmark_entryDeserializationString(b *testing.B) {
	b.ReportAllocs()
	ent := entry.Entry{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.Exists, Hash: ""}
	by := ent.ToBytes()

	for i := 0; i < b.N; i++ {
		entry.EntryFromBytes(by)
	}
}

func Benchmark_entrySerializationBytes(b *testing.B) {
	b.ReportAllocs()
	entry := entry.Entry{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.Exists, Hash: ""}

	for i := 0; i < b.N; i++ {
		entry.SerializeExperimental()
	}
}

func Benchmark_entryDeserializationByte(b *testing.B) {
	b.ReportAllocs()
	ent := entry.Entry{Storages: []string{"localhost:1", "localhost:2", "localhost:3"}, Status: entry.Exists, Hash: ""}
	by := ent.ToBytes()

	for i := 0; i < b.N; i++ {
		entry.DeserializeExperimental(by)
	}
}
