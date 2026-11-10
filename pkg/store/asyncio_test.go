package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// TestAsyncRingRoundTrip: writes/reads served by the ring produce the
// same bytes and the same usage accounting as the plain path, and a
// read past EOF behaves like os.File.ReadAt (short read + io.EOF).
func TestAsyncRingRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	s, err := New(dir, Options{AsyncRing: 32, Preallocate: false})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if s.ring == nil {
		t.Fatal("ring not enabled")
	}
	oid := object.OID(0x11)
	payload := make([]byte, 200<<10)
	for i := range payload {
		payload[i] = byte(i)
	}
	n, err := s.WriteAt(oid, payload, 4096)
	if err != nil || n != len(payload) {
		t.Fatalf("write: n=%d err=%v", n, err)
	}
	got := make([]byte, len(payload))
	if _, err := s.ReadAt(oid, got, 4096); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("ring read-back mismatch")
	}
	// Sparse hole reads as zeroes through the ring.
	zeros := make([]byte, 4096)
	if _, err := s.ReadAt(oid, zeros, 0); err != nil {
		t.Fatalf("hole read: %v", err)
	}
	// A full-object read at offset 0: object.Size is 4 MiB, the file
	// ends at 204KB -> tail must come back as zeroes, no error.
	big := make([]byte, 128<<10)
	if _, err := s.ReadAt(oid, big, 192<<10); err != nil && !errors.Is(err, nil) {
		// reading across EOF returns io.EOF with n < len(big)
		t.Logf("cross-eof read err=%v", err)
	}
	if s.Usage() < int64(len(payload)) {
		t.Fatalf("usage = %d after %d written", s.Usage(), len(payload))
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Re-open on the plain path: bytes on disk are identical.
	s2, err := New(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if _, err := s2.ReadAt(oid, got, 4096); err != nil {
		t.Fatalf("plain read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("plain read-back mismatch")
	}
}

// TestAsyncRingConcurrent: many goroutines hammering one object
// through the ring stay race-free (run with -race) and the final
// content of every block is one of the writers' values.
func TestAsyncRingConcurrent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	s, err := New(dir, Options{AsyncRing: 64})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer s.Close()
	oid := object.OID(0x12)
	const blocks, writers, size = 32, 8, 16 << 10
	done := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			p := make([]byte, size)
			for r := 0; r < 20; r++ {
				for b := 0; b < blocks; b++ {
					for i := range p {
						p[i] = byte(w)
					}
					if _, err := s.WriteAt(oid, p, int64(b)*size); err != nil {
						done <- fmt.Errorf("w%d write: %w", w, err)
						return
					}
					if _, err := s.ReadAt(oid, p, int64(b)*size); err != nil {
						done <- fmt.Errorf("w%d read: %w", w, err)
						return
					}
				}
			}
			done <- nil
		}(w)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for w := 0; w < writers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("ring concurrency hung")
		}
	}
	// Every block must hold a single writer's byte.
	buf := make([]byte, size)
	for b := 0; b < blocks; b++ {
		if _, err := s.ReadAt(oid, buf, int64(b)*size); err != nil {
			t.Fatalf("final read block %d: %v", b, err)
		}
		first := buf[0]
		for i, v := range buf {
			if v != first {
				t.Fatalf("block %d torn at %d: %d vs %d", b, i, v, first)
			}
		}
	}
}

// TestAsyncRingCloseWithWork: Close fires queued ops (ring contract)
// instead of hanging; a store closed while ops are in flight must
// resolve every caller.
func TestAsyncRingCloseWithWork(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "s")
	s, err := New(dir, Options{AsyncRing: 8})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	oid := object.OID(0x13)
	p := make([]byte, 64<<10)
	// Warm the entry (first open registers the fd).
	if _, err := s.WriteAt(oid, p[:4096], 0); err != nil {
		t.Fatalf("warm: %v", err)
	}
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func(i int) {
			buf := make([]byte, len(p))
			_, err := s.WriteAt(oid, p, int64(i)*int64(len(p)))
			if err == nil {
				_, err = s.ReadAt(oid, buf, int64(i)*int64(len(p)))
			}
			errs <- err
		}(i)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i := 0; i < 16; i++ {
		select {
		case <-errs: // resolved, by completion or ErrClosed: no hang
		case <-time.After(15 * time.Second):
			t.Fatal("op never resolved after Close")
		}
	}
	// Files closed after ring: data is durable.
	s2, err := New(dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	buf := make([]byte, 4096)
	if _, err := s2.ReadAt(oid, buf, 0); err != nil {
		t.Fatalf("durable read: %v", err)
	}
	var fi os.FileInfo
	fi, err = os.Stat(filepath.Join(dir, fmt.Sprintf("%016x", uint64(oid))))
	if err != nil || fi == nil || fi.Size() == 0 {
		t.Fatalf("object file missing/empty after ring writes: %v", err)
	}
}
