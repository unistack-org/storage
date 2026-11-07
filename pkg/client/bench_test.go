package client

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// benchVolumeN spins up a cluster with n OSDs and a volume with n
// replicas (quorum = all: no stragglers, no payload copies).
func benchVolumeN(b testing.TB, n int) (*Volume, context.Context) {
	b.Helper()
	c := newCluster(b, n)
	c.cl.SetRefreshInterval(50 * time.Millisecond)
	c.publishMap(1)
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	const size = 64 << 20
	if _, err := c.cl.CreateVolume(ctx, "bench", size, n, 0, 64, "data"); err != nil {
		b.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "bench")
	if err != nil {
		b.Fatalf("OpenVolume: %v", err)
	}
	return v, ctx
}

// benchVolume spins up a 3-OSD cluster with a small volume.
func benchVolume(b testing.TB) (*Volume, context.Context) {
	b.Helper()
	return benchVolumeN(b, 3)
}

// BenchmarkVolumeWrite4K measures the full client write path:
// placement lookup + fan-out to 3 replicas + quorum commit, over
// loopback TCP with in-process OSDs.
func BenchmarkVolumeWrite4K(b *testing.B) {
	v, ctx := benchVolume(b)
	p := make([]byte, 4096)
	p[0] = 0x5A
	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := uint64(i&255) * 4096 // spread over 1 MiB of objects
		if _, err := v.WriteAt(ctx, p, off); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// BenchmarkVolumeWrite4KSingle measures the single-replica write path
// (the single-node MVP configuration): one synchronous replica write,
// no fan-out goroutines, channels, detached contexts, or payload
// copies.
func BenchmarkVolumeWrite4KSingle(b *testing.B) {
	v, ctx := benchVolumeN(b, 1)
	p := make([]byte, 4096)
	p[0] = 0x5A
	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := uint64(i&255) * 4096
		if _, err := v.WriteAt(ctx, p, off); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// BenchmarkVolumeRead4K measures a single-replica read path.
func BenchmarkVolumeRead4K(b *testing.B) {
	v, ctx := benchVolume(b)
	p := make([]byte, 4096)
	if _, err := v.WriteAt(ctx, p, 0); err != nil {
		b.Fatalf("seed write: %v", err)
	}
	got := make([]byte, 4096)
	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := v.ReadAt(ctx, got, 0); err != nil {
			b.Fatalf("read: %v", err)
		}
	}
}

// TestClientZeroAllocGate guards the volume data path against allocation
// regressions (a recompile storm in Refresh, per-replica channels in
// quorumWrite, etc). The path is not required to be zero-alloc: the
// in-process OSD servers, the store's file cache, and the sdsnet server
// contribute a small fixed cost. The budget (4 KiB, 40 allocs per
// write+read pair) sits well below the pre-optimisation ~10 KiB/220
// and catches any regression that puts the crushmap lexer or per-replica
// fan-out channels back on the hot path.
func TestClientZeroAllocGate(t *testing.T) {
	v, ctx := benchVolume(t)
	const ops = 2000
	p := make([]byte, 4096)
	// Warm the caches (pool connections, store file entries).
	for i := 0; i < 50; i++ {
		if _, err := v.WriteAt(ctx, p, uint64(i%32)*4096); err != nil {
			t.Fatalf("warm write: %v", err)
		}
		if _, err := v.ReadAt(ctx, p, uint64(i%32)*4096); err != nil {
			t.Fatalf("warm read: %v", err)
		}
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < ops; i++ {
		off := uint64(i&31) * 4096
		if _, err := v.WriteAt(ctx, p, off); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := v.ReadAt(ctx, p, off); err != nil {
			t.Fatalf("read: %v", err)
		}
	}
	runtime.ReadMemStats(&after)
	perPair := after.TotalAlloc - before.TotalAlloc
	pairs := uint64(ops)
	// Budget per 4K write+read pair: one payload copy per write (the
	// straggler payload-ownership copy in quorumWrite, len(p) = 4096 B)
	// plus bookkeeping. Read must stay allocation-free.
	if perPair > 6144*pairs {
		t.Fatalf("data path allocating %d B/op (%d allocs/op): regression",
			perPair/pairs, (after.Mallocs-before.Mallocs)/pairs)
	}
	if allocs := (after.Mallocs - before.Mallocs) / pairs; allocs > 40 {
		t.Fatalf("data path allocating %d allocs/op: regression", allocs)
	}
}
