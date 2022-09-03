package entry

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"sort"
)

type volSort struct {
	score   []byte
	storage string
}

type byScore []volSort

func (s byScore) Len() int      { return len(s) }
func (s byScore) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byScore) Less(i, j int) bool {
	return bytes.Compare(s[i].score, s[j].score) == 1
}

func HashKey(key []byte) string {
	md5key := md5.Sum(key)
	b64key := base64.StdEncoding.EncodeToString(key)

	return fmt.Sprintf("/%02x/%02x/%s", md5key[0], md5key[1], b64key)
}

// KeyToStorage converts a key and a storages list into a list of available
// storages for a given key.
func KeyToStorage(key []byte, storages []string, count, sv int) []string {
	vSort := make([]volSort, len(storages))

	for idx, v := range storages {
		hash := md5.New()
		hash.Write(key)
		hash.Write([]byte(v))

		score := hash.Sum(nil)
		vSort[idx] = volSort{score, v}
	}

	sort.Stable(byScore(vSort))

	rstorages := make([]string, count)
	for i := 0; i < count; i++ {
		s := vSort[i]
		var vol string
		if sv == 1 {
			vol = s.storage
		} else {
			svhash := uint(s.score[12])<<24 + uint(s.score[13])<<16 +
				uint(s.score[14])<<8 + uint(s.score[15])
			vol = fmt.Sprintf("%s/sv%02X", s.storage, svhash%uint(sv))
		}

		rstorages[i] = vol
	}

	return rstorages
}
