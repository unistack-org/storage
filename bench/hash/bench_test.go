package main

import (
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"hash/crc32"
	"testing"

	creachadairCity "bitbucket.org/creachadair/cityhash"
	jpathyCity "bitbucket.org/jpathy/dmc/cityhash"
	"github.com/OneOfOne/xxhash"
	dgryskiSpooky "github.com/dgryski/go-spooky"
	huichenMurmur "github.com/huichen/murmur"
	farmhash "github.com/leemcloughlin/gofarmhash"
	"github.com/minio/blake2b-simd"
	"github.com/minio/highwayhash"
	sha256simd "github.com/minio/sha256-simd"
	reuseeMurmur "github.com/reusee/mmh3"
	hashlandSpooky "github.com/tildeleb/hashland/spooky"
	zhangMurmur "github.com/zhangxinngang/murmur"
)

var result interface{}

func mkinput(n int) []byte {
	rv := make([]byte, n)
	rand.Read(rv)
	return rv
}

func benchmarkHash(num int, fn func(i int)) {
	for n := 0; n < num; n++ {
		fn(n)
	}
}

const benchSize = 32 << 20

func BenchmarkHighwayhash64(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		panic(err)
	}
	h, err := highwayhash.New64(key)
	if err != nil {
		panic(err)
	}

	benchmarkHash(b.N, func(n int) {
		_ = h.Sum(input)
	})
}

func BenchmarkFarmHashHash32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = farmhash.Hash32(input)
	})
}
func BenchmarkFarmHashHash64(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = farmhash.Hash64(input)
	})
}
func BenchmarkHuichenMurmur(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = huichenMurmur.Murmur3(input)
	})
}
func BenchmarkReuseeMurmur(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = reuseeMurmur.Sum32(input)
	})
}
func BenchmarkZhangMurmur(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = zhangMurmur.Murmur3(input)
	})
}
func BenchmarkDgryskiSpooky32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = dgryskiSpooky.Hash32(input)
	})
}
func BenchmarkDgryskiSpooky64(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = dgryskiSpooky.Hash64(input)
	})
}
func BenchmarkHashlandSpooky32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = hashlandSpooky.Hash32(input, 0)
	})
}
func BenchmarkHashlandSpooky64(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = hashlandSpooky.Hash64(input, 0)
	})
}
func BenchmarkHashlandSpooky128(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_, _ = hashlandSpooky.Hash128(input, 0)
	})
}
func BenchmarkJPathyCity32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = jpathyCity.Hash32(input)
	})
}
func BenchmarkCreachadairCity32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = creachadairCity.Hash32(input)
	})
}
func BenchmarkCreachadairCity64(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = creachadairCity.Hash64(input)
	})
}
func BenchmarkCreachadairCity128(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_, _ = creachadairCity.Hash128(input)
	})
}

func BenchmarkXxHash32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = xxhash.Checksum32(input)
	})
}

func BenchmarkXxHash64(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = xxhash.Checksum64(input)
	})
}

func BenchmarkBlake2bSIMD256(b *testing.B) {
	input := mkinput(benchSize)
	h := blake2b.New256()
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		h.Reset()
		_, _ = h.Write(input)
		h.Sum(nil)
	})
}

func BenchmarkSHA256(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = sha256.Sum256(input)
	})
}
func BenchmarkSHA1(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = sha1.Sum(input)
	})
}
func BenchmarkCRC32(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	h := crc32.NewIEEE()
	benchmarkHash(b.N, func(n int) {
		_ = h.Sum(input)
	})
}
func BenchmarkSHA256SIMBD(b *testing.B) {
	input := mkinput(benchSize)
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	benchmarkHash(b.N, func(n int) {
		_ = sha256simd.Sum256(input)
	})
}
func main() {

}
