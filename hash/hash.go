package hash

import (
	"fmt"
	"hash"
	"strings"
)

var hashTypes map[string]hash.Hash

func init() {
	hashTypes = make(map[string]hash.Hash)
}

func RegisterHash(engine string, hash hash.Hash) {
	hashTypes[engine] = hash
}

func New(htype string) (hash.Hash, error) {
	hash, ok := hashTypes[htype]
	if !ok {
		return nil, fmt.Errorf("unknown hash type %s. only %s supported", htype, strings.Join(HashTypes(), ","))
	}

	return hash, nil
}

func HashTypes() []string {
	var htypes []string
	for htype, _ := range hashTypes {
		htypes = append(htypes, htype)
	}
	return htypes
}
