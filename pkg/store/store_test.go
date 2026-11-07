package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

func newTestStore(t *testing.T, opts Options) *Store {
	t.Helper()
	s, err := New(filepath.Join(t.TempDir(), "objects"), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestRoundtripAndBounds(t *testing.T) {
	s := newTestStore(t, Options{})
	const oid = object.OID(0x1234)

	data := bytes.Repeat([]byte("abcd"), 1024)
	n, err := s.WriteAt(oid, data, 4096)
	if err != nil || n != len(data) {
		t.Fatalf("WriteAt = %d, %v", n, err)
	}
	got := make([]byte, len(data))
	n, err = s.ReadAt(oid, got, 4096)
	if err != nil || n != len(data) || !bytes.Equal(got, data) {
		t.Fatalf("ReadAt = %d, %v, equal=%v", n, err, bytes.Equal(got, data))
	}

	// Bounds: object is exactly object.Size, no crossing.
	if _, err := s.WriteAt(oid, make([]byte, 4), int64(object.Size)-2); err != ErrObjectTooSmall {
		t.Fatalf("crossing write: got %v", err)
	}
	if _, err := s.ReadAt(oid, make([]byte, 4), int64(object.Size)-2); err != ErrObjectTooSmall {
		t.Fatalf("crossing read: got %v", err)
	}
}

func TestMissingObject(t *testing.T) {
	s := newTestStore(t, Options{})
	if _, err := s.ReadAt(object.OID(99), make([]byte, 4), 0); err != ErrNoSuchObject {
		t.Fatalf("ReadAt missing: got %v", err)
	}
}

func TestUsageRemoveLoad(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "objects")
	s, err := New(dir, Options{Preallocate: false})
	if err != nil {
		t.Fatal(err)
	}
	// Every created object file is (sparse-)grown to the full object
	// size by design, so Usage counts 4 MiB per object.
	payload := bytes.Repeat([]byte("x"), 8192)
	for i := 0; i < 3; i++ {
		if _, err := s.WriteAt(object.OID(10+i), payload, 0); err != nil {
			t.Fatal(err)
		}
	}
	want := int64(3 * object.Size)
	if got := s.Usage(); got != want {
		t.Fatalf("Usage = %d, want %d", got, want)
	}
	if err := s.Remove(object.OID(11)); err != nil {
		t.Fatal(err)
	}
	want -= int64(object.Size)
	if got := s.Usage(); got != want {
		t.Fatalf("Usage after Remove = %d, want %d", got, want)
	}
	if _, err := s.ReadAt(object.OID(11), make([]byte, 4), 0); err != ErrNoSuchObject {
		t.Fatalf("ReadAt removed: got %v", err)
	}
	// Remove of a non-existent object is not an error.
	if err := s.Remove(object.OID(777)); err != nil {
		t.Fatalf("Remove missing: %v", err)
	}
	s.Close()

	// Fresh store over the same dir: Load accounts pre-existing files.
	s2, err := New(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if err := s2.Load(); err != nil {
		t.Fatal(err)
	}
	if got := s2.Usage(); got != want {
		t.Fatalf("Usage after Load = %d, want %d", got, want)
	}
	got := make([]byte, len(payload))
	if _, err := s2.ReadAt(object.OID(10), got, 0); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("ReadAt after reload: %v, equal=%v", err, bytes.Equal(got, payload))
	}
}

func TestPreallocateFullSize(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a full 4 MiB object")
	}
	s := newTestStore(t, Options{Preallocate: true})
	const oid = object.OID(7)
	if _, err := s.WriteAt(oid, []byte("hi"), 0); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(s.dir, "0000000000000007")
	st, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != int64(object.Size) {
		t.Fatalf("preallocated size = %d, want %d", st.Size(), object.Size)
	}
	if got := s.Usage(); got != int64(object.Size) {
		t.Fatalf("Usage = %d, want %d", got, object.Size)
	}
}

func TestConcurrentWrites(t *testing.T) {
	s := newTestStore(t, Options{})
	const oid = object.OID(42)
	const writers = 8
	const chunk = 512
	done := make(chan struct{})
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer func() { done <- struct{}{} }()
			payload := bytes.Repeat([]byte{byte('a' + w)}, chunk)
			for i := 0; i < 50; i++ {
				off := int64(w*chunk + i*writers*chunk)
				if _, err := s.WriteAt(oid, payload, off); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	for w := 0; w < writers; w++ {
		<-done
	}
	// Verify the last write of each writer's last iteration region.
	buf := make([]byte, chunk)
	for w := 0; w < writers; w++ {
		off := int64(w*chunk + 49*writers*chunk)
		if _, err := s.ReadAt(oid, buf, off); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, bytes.Repeat([]byte{byte('a' + w)}, chunk)) {
			t.Fatalf("writer %d: corrupted region", w)
		}
	}
}
