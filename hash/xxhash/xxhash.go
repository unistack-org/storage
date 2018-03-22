package xxhash

import (
	"github.com/OneOfOne/xxhash"

	"github.com/sdstack/storage/hash"
)

func init() {
	hash.RegisterHash("xxhash", &xxhash.XXHash64{})
}
