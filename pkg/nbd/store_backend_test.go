package nbd

import (
	"bytes"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/store"
)

func testStoreServer(t *testing.T, size uint64, opts ServerOptions) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "objects")
	b, err := NewStoreBackend(dir, size, store.Options{})
	if err != nil {
		t.Fatalf("store backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	srv := NewServer([]*Export{{Name: "vol0", Backend: b}}, opts)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) {
			// server closed
		}
	}()
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String()
}

// TestStoreBackendNBD exercises the phase-1 data path over the real NBD
// wire: a store-backed export, writes that cross 4 MiB object
// boundaries, reads back, flush.
func TestStoreBackendNBD(t *testing.T) {
	const size = 9 << 20 // 9 MiB: spans 3 objects (0,1,2)
	addr := testStoreServer(t, size, ServerOptions{})
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	if cl.size != size {
		t.Fatalf("size = %d, want %d", cl.size, size)
	}

	// Write a 1 MiB pattern straddling the object 0/1 boundary.
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	off := uint64(4<<20 - 1<<19) // 3.5 MiB
	if e := cl.write(t, off, payload); e != ErrOK {
		t.Fatalf("crossing write: %d", e)
	}
	data, e := cl.read(t, off, uint32(len(payload)))
	if e != ErrOK {
		t.Fatalf("crossing read: %d", e)
	}
	if !bytes.Equal(data, payload) {
		t.Fatal("crossing read mismatch")
	}

	// Single 4 MiB request: full object 1.
	block := make([]byte, 4<<20)
	for i := range block {
		block[i] = byte(i)
	}
	if e := cl.write(t, 4<<20, block); e != ErrOK {
		t.Fatalf("object write: %d", e)
	}
	data, e = cl.read(t, 4<<20, 4<<20)
	if e != ErrOK || !bytes.Equal(data, block) {
		t.Fatalf("object read: %d match=%v", e, bytes.Equal(data, block))
	}

	// Tail clip near volume end.
	tail := make([]byte, 64<<10)
	for i := range tail {
		tail[i] = 0x5a
	}
	if e := cl.write(t, size-uint64(len(tail)), tail); e != ErrOK {
		t.Fatalf("tail write: %d", e)
	}
	data, e = cl.read(t, size-uint64(len(tail)), uint32(len(tail)))
	if e != ErrOK || !bytes.Equal(data, tail) {
		t.Fatalf("tail read: %d match=%v", e, bytes.Equal(data, tail))
	}

	if e := cl.flush(t); e != ErrOK {
		t.Fatalf("flush: %d", e)
	}
	cl.disconnect(t)
}

// TestStoreBackendDirect checks the Backend API directly: boundary
// crossing, zeroing, persistence across reopen.
func TestStoreBackendDirect(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	b, err := NewStoreBackend(dir, 10<<20, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	p := bytes.Repeat([]byte("q"), 1<<20)
	// Write across object 1/2 boundary.
	if n, err := b.WriteAt(p, int64(2<<20-1024)); err != nil || n != len(p) {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	got := make([]byte, len(p))
	if n, err := b.ReadAt(got, int64(2<<20-1024)); err != nil || n != len(p) || !bytes.Equal(got, p) {
		t.Fatalf("ReadAt = %d, %v match=%v", n, err, bytes.Equal(got, p))
	}
	// WriteZeroes with punchHole=true zeroes the range on every platform
	// (punched interior + zero-filled edges) — data reads back zero.
	if err := b.WriteZeroes(int64(2<<20-1024), 1<<20, true); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	if _, err := b.ReadAt(got, int64(2<<20-1024)); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(got, []byte("q")) {
		t.Fatal("WriteZeroes left data")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen: the zeroed object persists.
	b2, err := NewStoreBackend(dir, 10<<20, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if _, err := b2.ReadAt(got, int64(2<<20-1024)); err != nil {
		t.Fatal(err)
	}
	for _, c := range got {
		if c != 0 {
			t.Fatal("data after reopen")
		}
	}
}

// TestStoreBackendHoleRead: a never-written object is a hole and must
// read back as zeroes (QEMU probes the volume with reads from offset 0
// before anything is written).
func TestStoreBackendHoleRead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	b, err := NewStoreBackend(dir, 10<<20, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	got := make([]byte, 64<<10)
	for _, off := range []int64{0, int64(5 << 20), int64(9<<20) - 4096} {
		n, err := b.ReadAt(got, off)
		if err != nil {
			t.Fatalf("ReadAt at hole off=%d: %v", off, err)
		}
		if n != len(got) {
			t.Fatalf("ReadAt at hole off=%d = %d, want %d", off, n, len(got))
		}
		for _, c := range got {
			if c != 0 {
				t.Fatal("hole read not zero")
			}
		}
	}
}

func TestStoreBackendZeroSize(t *testing.T) {
	if _, err := NewStoreBackend(filepath.Join(t.TempDir(), "o"), 0, store.Options{}); err == nil {
		t.Fatal("zero-size volume must fail")
	}
}
