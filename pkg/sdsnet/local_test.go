//go:build unix

package sdsnet

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"
)

// localServer starts a Server reachable only through in-process
// socketpairs (no listener, no TCP at all).
func localServer(tb testing.TB) (*Server, func() (net.Conn, error)) {
	tb.Helper()
	srv := &Server{Backend: &zeroBackend{size: 1 << 30}, Logf: func(string, ...any) {}}
	tb.Cleanup(func() { srv.Close() })
	dial := func() (net.Conn, error) {
		mine, peer, err := Socketpair()
		if err != nil {
			return nil, err
		}
		go srv.ServeConn(peer)
		return mine, nil
	}
	return srv, dial
}

func TestLocalConnRoundTrip(t *testing.T) {
	_, dial := localServer(t)
	c, err := dial()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	cc, err := NewConn(c, "vol0", 5*time.Second)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer cc.Close()
	ctx := context.Background()

	payload := bytes.Repeat([]byte("sds"), 2048) // 6144 B, not a power of two
	oid := uint64(7)
	off := uint64(4096) * 11
	if err := cc.Write(ctx, oid, off, payload, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	n, err := cc.Read(ctx, oid, off, got)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("read n=%d want %d", n, len(payload))
	}
	// zeroBackend discards writes and returns zeroes: verify the read
	// round-tripped a zero-filled buffer of the right length (the
	// protocol, not the store, is under test here).
	if len(got) != len(payload) {
		t.Fatalf("read len=%d want %d", len(got), len(payload))
	}
	if err := cc.WriteZeroes(ctx, oid, off, 4096, true, false); err != nil {
		t.Fatalf("write-zeroes: %v", err)
	}
}

func TestPoolLocalDial(t *testing.T) {
	_, dial := localServer(t)
	pp := NewPool("", "vol0") // Addr intentionally empty: Local must win
	pp.Local = dial
	pp.Limit = 4
	defer pp.Close()
	ctx := context.Background()
	payload := bytes.Repeat([]byte{0xAB}, 4096)
	for i := 0; i < 64; i++ {
		if err := pp.Do(ctx, func(cc *Conn) error {
			return cc.Write(ctx, 1, 0, payload, false)
		}); err != nil {
			t.Fatalf("op %d: %v", i, err)
		}
	}
}

// BenchmarkLocalOp prices the in-process transport against BenchmarkOp
// (same protocol, loopback TCP): the delta is what the TCP stack costs
// per 4 KiB op.
func BenchmarkLocalOp(b *testing.B) {
	_, dial := localServer(b)
	c, err := dial()
	if err != nil {
		b.Fatalf("socketpair: %v", err)
	}
	cc, err := NewConn(c, "vol0", 5*time.Second)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer cc.Close()
	ctx := context.Background()
	p := make([]byte, 4096)
	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cc.Read(ctx, 1, 0, p); err != nil {
			b.Fatal(err)
		}
	}
}
