package store

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// With a registered-buffer arena configured, every ring I/O under a
// slot size moves through an arena slot (READ_FIXED/WRITE_FIXED on
// Linux); data must stay byte-exact, and buffers over the slot size
// must take the plain path.
func TestStoreRingArena(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	for _, tc := range []struct {
		name  string
		slots int
		size  int
	}{
		{"arena-16x4k", 16, 4096},
		{"plain", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := New(dir, Options{
				CommitMode:     CommitSync,
				AsyncRing:      32,
				RingArenaSlots: tc.slots,
				RingArenaSlot:  tc.size,
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer st.Close()
			if tc.slots > 0 && st.arena == nil {
				t.Fatal("arena requested but not initialised")
			}
			oid := object.OID(1)
			blk := make([]byte, 4096)
			for i := range blk {
				blk[i] = byte(i)
			}
			if n, err := st.WriteAt(oid, blk, 0); n != 4096 || err != nil {
				t.Fatalf("WriteAt: %v %v", n, err)
			}
			got := make([]byte, 4096)
			if n, err := st.ReadAt(oid, got, 0); n != 4096 || err != nil {
				t.Fatalf("ReadAt: %v %v", n, err)
			}
			if !bytes.Equal(got, blk) {
				t.Fatal("data mismatch through arena")
			}
			// Shorter read (partial slot): the completion must be
			// clamped to the caller's length.
			small := make([]byte, 100)
			if n, err := st.ReadAt(oid, small, 500); n != 100 || err != nil {
				t.Fatalf("short ReadAt: %v %v", n, err)
			}
			if !bytes.Equal(small, blk[500:600]) {
				t.Fatal("short read mismatch")
			}
		})
	}
}

// Concurrent writers share the slot pool: with fewer slots than
// goroutines, acquire must block and release, never corrupt data.
func TestStoreRingArenaContention(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "st")
	st, err := New(dir, Options{CommitMode: CommitSync, AsyncRing: 32, RingArenaSlots: 4, RingArenaSlot: 4096})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer st.Close()
	const n = 16
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			oid := object.OID(10 + i)
			blk := make([]byte, 4096)
			for j := range blk {
				blk[j] = byte(i*7 + j)
			}
			if _, err := st.WriteAt(oid, blk, 0); err != nil {
				errs <- err
				return
			}
			got := make([]byte, 4096)
			if _, err := st.ReadAt(oid, got, 0); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, blk) {
				errs <- fmt.Errorf("obj %d mismatch", i)
				return
			}
			errs <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
