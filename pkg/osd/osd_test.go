package osd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/store"
)

func TestObjectBackendWriteZeroesSparse(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	st, err := store.New(dir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	be := &objectBackend{st: st}
	ctx := context.Background()

	const oid = uint64(0x4242)
	data := bytes.Repeat([]byte("S"), 1<<20)
	if err := be.WriteAt(ctx, oid, 0, data); err != nil {
		t.Fatal(err)
	}
	if err := be.WriteZeroes(ctx, oid, 0, uint64(len(data)), true); err != nil {
		t.Fatalf("sparse zeroes: %v", err)
	}
	got := make([]byte, len(data))
	n, err := be.ReadAt(ctx, oid, 0, got)
	if err != nil || n != len(data) {
		t.Fatalf("read after zeroes: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, make([]byte, len(data))) {
		t.Fatal("range not zeroed")
	}
}

// TestObjectBackendPunchReclaimsBlocks is the guest-TRIM payoff test: a
// sparse zeroing must return the blocks to the filesystem, not just
// overwrite them with zeroes (the old behavior ate bandwidth and freed
// nothing).
func TestObjectBackendPunchReclaimsBlocks(t *testing.T) {
	if !store.PunchSupported {
		t.Skip("no hole punch on this platform")
	}
	dir := filepath.Join(t.TempDir(), "objects")
	st, err := store.New(dir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	be := &objectBackend{st: st}
	ctx := context.Background()

	const oid = uint64(0x99)
	chunk := bytes.Repeat([]byte("P"), 1<<20)
	for off := uint64(0); off < object.Size; off += uint64(len(chunk)) {
		if err := be.WriteAt(ctx, oid, off, chunk); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "0000000000000099")
	alloc := func() int64 {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok {
			return st.Blocks * 512
		}
		return fi.Size()
	}
	before := alloc()
	if before < int64(object.Size)/2 {
		t.Skipf("fs allocates only %d bytes for a filled %d-byte object", before, object.Size)
	}
	if err := be.WriteZeroes(ctx, oid, 0, uint64(object.Size), true); err != nil {
		t.Fatal(err)
	}
	after := alloc()
	// A zero-fill would leave allocation unchanged (the object is a
	// full-size file); a punch must drop it to (nearly) nothing.
	if after > before-int64(object.Size)/2 {
		t.Fatalf("sparse zeroes freed %d bytes of %d: no punch happened", before-after, before)
	}
}
