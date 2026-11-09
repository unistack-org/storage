package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// allocatedBytes reports the blocks the file at path actually occupies on
// disk (st_blocks * 512), the only honest measure of what a punch freed.
func allocatedBytes(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fileAllocated(fi)
}

func TestWriteZeroesZeroFill(t *testing.T) {
	s := newTestStore(t, Options{})
	const oid = object.OID(0x55)

	payload := bytes.Repeat([]byte("Z"), 200_000)
	if _, err := s.WriteAt(oid, payload, 8192); err != nil {
		t.Fatal(err)
	}
	// sparse=false must store literal zeroes and keep the data readable.
	if err := s.WriteZeroes(oid, 8192, 200_000, false); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload)+8192)
	if _, err := s.ReadAt(oid, got, 0); err != nil {
		t.Fatal(err)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("byte %d = %d, want 0", i, b)
		}
	}
}

func TestWriteZeroesBounds(t *testing.T) {
	s := newTestStore(t, Options{})
	const oid = object.OID(0x56)
	if _, err := s.WriteAt(oid, []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteZeroes(oid, uint64(object.Size)-10, 20, true); err != ErrObjectTooSmall {
		t.Fatalf("past-end zeroes: got %v", err)
	}
	if err := s.WriteZeroes(oid, 0, 0, true); err != nil {
		t.Fatalf("zero-length zeroes: got %v", err)
	}
}

func TestPunchFreesBlocks(t *testing.T) {
	if !punchSupported {
		t.Skip("no hole punch on this platform")
	}
	s := newTestStore(t, Options{})
	const oid = object.OID(0x77)

	// Fill the whole object with real data, so every block is allocated.
	const fill = object.Size
	buf := bytes.Repeat([]byte("D"), 1<<20)
	for off := int64(0); off < int64(fill); off += int64(len(buf)) {
		if _, err := s.WriteAt(oid, buf, off); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(s.dir, "0000000000000077")
	before := allocatedBytes(t, path)
	if before < int64(fill)/2 {
		t.Skipf("filesystem does not seem to allocate written bytes (%d)", before)
	}

	// Punch 2 MiB out of the middle.
	if err := s.WriteZeroes(oid, 1<<20, 2<<20, true); err != nil {
		t.Fatal(err)
	}
	after := allocatedBytes(t, path)
	if after > before-int64(1<<20) {
		t.Fatalf("punch freed too little: allocated %d -> %d", before, after)
	}

	// The punched region reads as zeroes; neighbours survive.
	got := make([]byte, 2<<20)
	if _, err := s.ReadAt(oid, got, 1<<20); err != nil {
		t.Fatal(err)
	}
	for i, b := range got {
		if b != 0 {
			t.Fatalf("punched byte %d = %d, want 0", i, b)
		}
	}
	if _, err := s.ReadAt(oid, got[:1<<20], 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:1<<20], buf) {
		t.Fatal("data before the hole changed")
	}
	if _, err := s.ReadAt(oid, got[:1<<20], 3<<20); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:1<<20], buf) {
		t.Fatal("data after the hole changed")
	}

	// Rewriting into a hole works and re-allocates.
	if _, err := s.WriteAt(oid, buf, 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadAt(oid, got[:1<<20], 1<<20); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:1<<20], buf) {
		t.Fatal("rewrite into hole failed")
	}
}

func TestPunchUnalignedEdges(t *testing.T) {
	if !punchSupported {
		t.Skip("no hole punch on this platform")
	}
	s := newTestStore(t, Options{})
	const oid = object.OID(0x78)
	buf := bytes.Repeat([]byte("E"), 128*1024)
	if _, err := s.WriteAt(oid, buf, 0); err != nil {
		t.Fatal(err)
	}
	// 512-byte-aligned request: head/tail are sub-block and must be
	// zero-filled, the interior punched; the whole range must read zero.
	if err := s.WriteZeroes(oid, 512, 100_000, true); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 128*1024)
	if _, err := s.ReadAt(oid, got, 0); err != nil {
		t.Fatal(err)
	}
	for i, b := range got {
		want := byte('E')
		if i >= 512 && i < 512+100_000 {
			want = 0
		}
		if b != want {
			t.Fatalf("byte %d = %d, want %d", i, b, want)
		}
	}
}

func TestAlignRange(t *testing.T) {
	tests := []struct {
		off, n     uint64
		head, tail uint64
		ok         bool
	}{
		{0, 4096, 0, 4096, true},
		// 512..4608 contains only one aligned byte-range boundary: the
		// aligned interior [4096,4096) is empty.
		{512, 4096, 0, 0, false},
		{0, 512, 0, 0, false},
		// 512..100512: interior is [4096, 98304), i.e. 3584..97792
		// relative to off.
		{512, 100_000, 3584, 97792, true},
		{4096, 8192, 0, 8192, true},
	}
	for _, tc := range tests {
		h, tl, ok := alignRange(tc.off, tc.n, punchAlign)
		if h != tc.head || tl != tc.tail || ok != tc.ok {
			t.Errorf("alignRange(%d,%d) = (%d,%d,%v), want (%d,%d,%v)",
				tc.off, tc.n, h, tl, ok, tc.head, tc.tail, tc.ok)
		}
	}
}
