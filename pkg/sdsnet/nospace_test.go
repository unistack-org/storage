package sdsnet

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

// noSpaceBackend answers every write with the raw errno the store raises
// when the filesystem is full; reads succeed (a full disk must stay
// readable — the guest needs its existing data).
type noSpaceBackend struct {
	memBackend
}

func (b *noSpaceBackend) WriteAt(_ context.Context, oid, off uint64, p []byte) error {
	return syscall.ENOSPC
}

func (b *noSpaceBackend) WriteZeroes(_ context.Context, oid, off, n uint64, sparse bool) error {
	return syscall.ENOSPC
}

func TestNoSpaceStatusOverWire(t *testing.T) {
	be := &noSpaceBackend{memBackend: *newMemBackend(1 << 20)}
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	err = cc.Write(ctx, 1, 0, []byte("data"), false)
	e, ok := AsError(err)
	if !ok {
		t.Fatalf("write on full backend: got %v, want protocol error", err)
	}
	if e.Status != StatusNoSpace {
		t.Fatalf("status = %s, want StatusNoSpace", e.Status)
	}
	if e.Status.String() != "no space" {
		t.Fatalf("Status.String() = %q", e.Status.String())
	}

	// A zeroes write must map the same way.
	err = cc.WriteZeroes(ctx, 1, 0, 4096, true, false)
	if e, ok := AsError(err); !ok || e.Status != StatusNoSpace {
		t.Fatalf("write zeroes: got %v", err)
	}

	// Reads keep working: ENOSPC is not a device failure. The mem
	// backend zero-fills thin regions, so this must come back StatusOK.
	buf := make([]byte, 512)
	if _, err := cc.Read(ctx, 1, 0, buf); err != nil {
		t.Fatalf("read on full backend: %v", err)
	}
}

func TestStatusFromErrQuotafull(t *testing.T) {
	if got := statusFromErr(syscall.EDQUOT); got != StatusNoSpace {
		t.Fatalf("EDQUOT mapped to %s, want StatusNoSpace", got)
	}
	wrapped := errors.New("x: " + syscall.ENOSPC.Error())
	_ = wrapped // string-only wrapping must NOT map (documents the contract)
	if got := statusFromErr(wrapped); got == StatusNoSpace {
		t.Fatal("string-wrapped ENOSPC mapped as NoSpace: must use %w")
	}
}
