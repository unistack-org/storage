package sdsnet

import (
	"context"
	"errors"
	"testing"
	"time"
)

// statFSBackend is a memBackend that also answers OpStatFS.
type statFSBackend struct {
	*memBackend
	usage, free, total uint64
}

func (b *statFSBackend) StatFS(_ context.Context) (uint64, uint64, uint64, error) {
	return b.usage, b.free, b.total, nil
}

func serveTest(t *testing.T, backend Backend) *Server {
	t.Helper()
	s := &Server{Backend: backend}
	go func() { _ = s.Serve("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not start")
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestStatFSRoundTrip checks the OpStatFS wire path: the 24-byte
// usage/free/total payload, and StatusNotSupp for a backend without the
// optional interface (the old-server degradation an agent must handle).
func TestStatFSRoundTrip(t *testing.T) {
	ctx := context.Background()

	s := serveTest(t, &statFSBackend{memBackend: newMemBackend(1 << 20), usage: 1234, free: 5678, total: 99999})
	cc, err := Dial(s.Addr(), "vol", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	usage, free, total, err := cc.StatDisk(ctx)
	if err != nil {
		t.Fatalf("StatDisk: %v", err)
	}
	if usage != 1234 || free != 5678 || total != 99999 {
		t.Fatalf("StatDisk = %d %d %d", usage, free, total)
	}
	// The payload round-trip must leave the stream in sync: a following
	// write/read on the same Conn works.
	if err := cc.Write(ctx, 1, 0, []byte("abc"), false); err != nil {
		t.Fatalf("write after StatDisk: %v", err)
	}
	p := make([]byte, 3)
	if n, err := cc.Read(ctx, 1, 0, p); err != nil || n != 3 || string(p) != "abc" {
		t.Fatalf("read after StatDisk: %d %v", n, err)
	}

	// Backend without the interface: NotSupp, connection stays usable.
	s2 := serveTest(t, newMemBackend(1 << 20))
	cc2, err := Dial(s2.Addr(), "vol", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc2.Close()
	var e *Error
	if _, _, _, err := cc2.StatDisk(ctx); !errors.As(err, &e) || e.Status != StatusNotSupp {
		t.Fatalf("want *Error NotSupp, got %v", err)
	}
	if err := cc2.Ping(ctx); err != nil {
		t.Fatalf("conn unusable after NotSupp: %v", err)
	}
}
