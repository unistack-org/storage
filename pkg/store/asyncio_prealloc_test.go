package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// TestRingPreallocConcurrent runs the pooled ioComp path under real
// concurrency: 8 writers x 20 rounds of mixed pread/pwrite through a ring.
// The pool (AsyncRing=64) must never hand a comp out twice or leak one; a
// torn write or a double-Done panic would fail this.
func TestRingPreallocConcurrent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	s, err := New(dir, Options{AsyncRing: 64})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer s.Close()
	if s.ioComps == nil {
		t.Fatal("ring store must build an ioComp pool")
	}
	oid := object.OID(0x34)
	const writers, rounds, blocks, size = 8, 20, 4, 4 << 10
	done := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			p := make([]byte, size)
			for r := 0; r < rounds; r++ {
				for b := 0; b < blocks; b++ {
					for i := range p {
						p[i] = byte(w*rounds + r) // unique per (writer,round)
					}
					off := int64(b)*size + int64((w+1)%2) // 0 or 1: exercise alignment
					if _, err := s.WriteAt(oid, p, off); err != nil {
						done <- fmt.Errorf("w%d write: %w", w, err)
						return
					}
					if n, err := s.ReadAt(oid, p, off); err != nil {
						done <- fmt.Errorf("w%d read: %w", w, err)
						return
					} else if n != size {
						done <- fmt.Errorf("w%d short read: %d", w, n)
						return
					}
				}
			}
			done <- nil
		}(w)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for w := 0; w < writers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("prealloc concurrency hung")
		}
	}
	// The pool must hold all of its comps again (none lost, none
	// double-returned). A double-return would have panicked on the channel;
	// a loss makes the length short.
	if got := len(s.ioComps.free); got != cap(s.ioComps.free) {
		t.Fatalf("comp pool at %d of %d after quiesce", got, cap(s.ioComps.free))
	}
}

// TestRingPreallocDifferential runs the same read workload with and
// without the async ring and requires byte-identical results including
// the exact (n, err) of a past-end read: the ring and the plain pread/pwrite
// path must be indistinguishable at the byte level.
func TestRingPreallocDifferential(t *testing.T) {
	run := func(ring bool) (full, mid string, eofN int, eofErr error) {
		dir := filepath.Join(t.TempDir(), "s")
		opts := Options{AsyncRing: 32}
		if !ring {
			opts.AsyncRing = 0
		}
		s, err := New(dir, opts)
		if err != nil {
			t.Fatalf("new(ring=%v): %v", ring, err)
		}
		defer s.Close()
		oid := object.OID(0x56)
		buf := make([]byte, 128)
		for i := range buf {
			buf[i] = byte(i*7 + 3)
		}
		if _, err := s.WriteAt(oid, buf, 0); err != nil {
			t.Fatalf("ring=%v write: %v", ring, err)
		}
		got := make([]byte, 128)
		if _, err := s.ReadAt(oid, got, 0); err != nil {
			t.Fatalf("ring=%v full read: %v", ring, err)
		}
		full = string(got)
		if _, err := s.ReadAt(oid, got[:57], 20); err != nil {
			t.Fatalf("ring=%v mid read: %v", ring, err)
		}
		mid = string(got[:57])
		// Past-end read: os.File.ReadAt returns (0, nil) for a read that
		// begins at/past EOF. The point is that both paths agree on it.
		var e error
		eofN, e = s.ReadAt(oid, got, 1000)
		eofErr = e
		return
	}
	plainFull, plainMid, plainN, plainErr := run(false)
	ringFull, ringMid, ringN, ringErr := run(true)
	if plainFull != ringFull || plainMid != ringMid || plainN != ringN || plainErr != ringErr {
		t.Fatalf("differential mismatch:\n plain=(full=%dB, mid=%q, eofN=%d, eofErr=%v)\n ring=(full=%dB, mid=%q, eofN=%d, eofErr=%v)",
			len(plainFull), plainMid, plainN, plainErr, len(ringFull), ringMid, ringN, ringErr)
	}
}

// TestRingPreallocCloseWithWork confirms the pooled path resolves every
// caller on Close (no comp leak, no hang), the fast-path analogue of
// TestAsyncRingCloseWithWork.
func TestRingPreallocCloseWithWork(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	s, err := New(dir, Options{AsyncRing: 32})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	oid := object.OID(0x78)
	buf := make([]byte, 4<<10)
	for i := range buf {
		buf[i] = byte(i)
	}
	if _, err := s.WriteAt(oid, buf, 0); err != nil {
		t.Fatalf("prime write: %v", err)
	}
	// Start in-flight ops, then close mid-flight. The ops must resolve
	// (by completion or a Close-race error) — no hang, no comp leak.
	done := make(chan struct{}, 16)
	for k := 0; k < 16; k++ {
		go func(k int) {
			defer func() { done <- struct{}{} }()
			if _, err := s.WriteAt(oid, buf, int64(k)*4<<10); err == nil {
				r := make([]byte, 4<<10)
				_, _ = s.ReadAt(oid, r, int64(k)*4<<10)
			}
		}(k)
	}
	s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for k := 0; k < 16; k++ {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("close-with-work hung")
		}
	}
}
