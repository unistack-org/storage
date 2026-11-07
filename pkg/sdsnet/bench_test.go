package sdsnet

import (
	"context"
	"io"
	"net"
	"runtime"
	"testing"
	"time"
)

// zeroBackend is an allocation-free in-kernel-style Backend for gates:
// reads return zeroes, writes discard. It prices the wire protocol +
// per-connection buffers, not storage.
type zeroBackend struct{ size uint64 }

func (z *zeroBackend) Size() uint64 { return z.size }
func (z *zeroBackend) ReadAt(_ context.Context, oid, off uint64, p []byte) (int, error) {
	if off >= z.size {
		return 0, io.EOF
	}
	return len(p), nil
}
func (z *zeroBackend) WriteAt(_ context.Context, oid, off uint64, p []byte) error {
	if off >= z.size {
		return io.EOF
	}
	return nil
}
func (z *zeroBackend) WriteZeroes(context.Context, uint64, uint64, uint64, bool) error {
	return nil
}
func (z *zeroBackend) Sync(context.Context) error { return nil }

func benchServer(tb testing.TB) (string, *Server) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := &Server{Backend: &zeroBackend{size: 1 << 30}, Logf: func(string, ...any) {}}
	go srv.ServeListener(ln)
	tb.Cleanup(func() { srv.Close() })
	return ln.Addr().String(), srv
}

func benchmarkOp(b *testing.B, size int, write bool) {
	addr, _ := benchServer(b)
	cc, err := Dial(addr, "vol0", 5*time.Second)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer cc.Close()
	ctx := context.Background()

	p := make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		if write {
			err = cc.Write(ctx, 1, 0, p, false)
		} else {
			_, err = cc.Read(ctx, 1, 0, p)
		}
		if err != nil {
			b.Fatalf("op: %v", err)
		}
	}
}

func BenchmarkSDSNetRead4K(b *testing.B)  { benchmarkOp(b, 4096, false) }
func BenchmarkSDSNetRead64K(b *testing.B) { benchmarkOp(b, 64*1024, false) }
func BenchmarkSDSNetWrite4K(b *testing.B) { benchmarkOp(b, 4096, true) }
func BenchmarkSDSNetWrite64K(b *testing.B) {
	benchmarkOp(b, 64*1024, true)
}

// TestSDSNetZeroAllocGate is the internal-protocol twin of
// TestNBDZeroAllocGate: a warm Conn round-trip (client + server hot
// paths together, same process) must not allocate per request. Budget:
// 256 B/request — the per-request make() regressions we removed cost
// >= payload bytes/request (4096+), the measured baseline is 0.
func TestSDSNetZeroAllocGate(t *testing.T) {
	addr, _ := benchServer(t)
	cc, err := Dial(addr, "vol0", 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cc.Close()
	ctx := context.Background()

	p := make([]byte, 4096)
	const n = 4000
	for i := 0; i < 500; i++ { // warm: buffers grown, conn settled
		if _, err := cc.Read(ctx, 1, 0, p); err != nil {
			t.Fatalf("warm read: %v", err)
		}
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < n; i++ {
		if _, err := cc.Read(ctx, 1, 0, p); err != nil {
			t.Fatalf("read: %v", err)
		}
		if err := cc.Write(ctx, 1, 0, p, false); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.ReadMemStats(&after)
		if after.Mallocs-before.Mallocs >= uint64(2*n) || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	allocs := after.Mallocs - before.Mallocs
	bytes := after.TotalAlloc - before.TotalAlloc
	ops := uint64(2 * n) // read + write per iteration
	if bytes > ops*256 {
		t.Fatalf("warm round-trip allocated %d B for %d ops (%.0f B/op): hot path not alloc-free",
			bytes, ops, float64(bytes)/float64(ops))
	}
	t.Logf("gate: %d round-trips -> %d allocs / %d B (%.2f allocs/op, %.1f B/op)",
		n, allocs, bytes, float64(allocs)/float64(ops), float64(bytes)/float64(ops))
}
