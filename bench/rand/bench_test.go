package main

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fastrand"
)

var BenchSink uint32

func BenchmarkMathRandIntn(b *testing.B) {
	rand.Seed(time.Now().Unix())
	b.RunParallel(func(pb *testing.PB) {
		s := uint32(0)
		for pb.Next() {
			s += uint32(rand.Intn(1e6))
		}
		atomic.AddUint32(&BenchSink, s)
	})
}

func BenchmarkRNGUint32nWithLock(b *testing.B) {
	var r fastrand.RNG
	var rMu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		s := uint32(0)
		for pb.Next() {
			rMu.Lock()
			s += r.Uint32n(1e6)
			rMu.Unlock()
		}
		atomic.AddUint32(&BenchSink, s)
	})
}

func main() {

}
