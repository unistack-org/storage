package sdsnet

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// syncCountingBackend counts Sync() calls so a test can see whether the
// server honored the FUA flag on a write.
type syncCountingBackend struct {
	memBackend
	syncs atomic.Int64
}

func (b *syncCountingBackend) Sync(ctx context.Context) error {
	b.syncs.Add(1)
	return b.memBackend.Sync(ctx)
}

func TestFUAHonoredOnWrite(t *testing.T) {
	be := &syncCountingBackend{memBackend: *newMemBackend(1 << 20)}
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	// Plain write: no server-side Sync.
	if err := cc.Write(ctx, 1, 0, []byte("plain"), false); err != nil {
		t.Fatal(err)
	}
	if got := be.syncs.Load(); got != 0 {
		t.Fatalf("plain write caused %d syncs, want 0", got)
	}

	// FUA write: the server must fsync (Sync) before acking.
	if err := cc.Write(ctx, 1, 0, []byte("fua!!"), true); err != nil {
		t.Fatal(err)
	}
	if got := be.syncs.Load(); got != 1 {
		t.Fatalf("FUA write caused %d syncs, want 1", got)
	}

	// FUA zeroes write, same contract.
	if err := cc.WriteZeroes(ctx, 1, 4096, 4096, true, true); err != nil {
		t.Fatal(err)
	}
	if got := be.syncs.Load(); got != 2 {
		t.Fatalf("FUA zeroes caused %d total syncs, want 2", got)
	}

	// Explicit OpSync still works.
	if err := cc.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if got := be.syncs.Load(); got != 3 {
		t.Fatalf("OpSync caused %d total syncs, want 3", got)
	}

	// The FUA write is readable (the ack implies durability).
	buf := make([]byte, 5)
	if _, err := cc.Read(ctx, 1, 0, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "fua!!" {
		t.Fatalf("read after FUA write = %q", buf)
	}
}
