package store

import (
	"errors"
	"syscall"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// TestNoSpaceSentinel: the store's ENOSPC sentinel must classify both as
// itself and as the raw errno — the OSD server keys statusFromErr off the
// errno, and callers (client, NBD) branch with errors.Is on the sentinel.
func TestNoSpaceSentinel(t *testing.T) {
	if !errors.Is(ErrNoSpace, syscall.ENOSPC) {
		t.Fatal("ErrNoSpace must wrap syscall.ENOSPC")
	}
	if !isNoSpace(ErrNoSpace) {
		t.Fatal("isNoSpace(ErrNoSpace) must be true")
	}
	if !isNoSpace(syscall.ENOSPC) || !isNoSpace(syscall.EDQUOT) {
		t.Fatal("raw ENOSPC/EDQUOT must classify as no-space")
	}
	if isNoSpace(syscall.EIO) || isNoSpace(nil) {
		t.Fatal("EIO/nil must not classify as no-space")
	}
}

// TestMinFreeGuard: with MinFreeBytes above what the filesystem has free,
// creating a NEW object must fail with ErrNoSpace before the write, while
// reads and writes of already-open objects keep working (a full disk must
// not lock a VM out of its own data).
func TestMinFreeGuard(t *testing.T) {
	dir := t.TempDir()
	d, err := storeDirStat(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	avail, ok := fileAvail(d)
	if !ok {
		t.Skip("statfs unavailable")
	}
	const oid = object.OID(9001)

	s, err := New(dir, Options{MinFreeBytes: avail + (1 << 30)})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.WriteAt(oid, []byte("x"), 0); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("new-object write under guard: got %v, want ErrNoSpace", err)
	}

	// An object created before the guard applies stays readable and
	// writable: create it with an unguarded store, reopen guarded.
	s2, err := New(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.WriteAt(oid, []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := New(dir, Options{MinFreeBytes: avail + (1 << 30)})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	if _, err := s3.WriteAt(oid, []byte("!"), 5); err != nil {
		t.Fatalf("write to existing object under guard: %v", err)
	}
	b := make([]byte, 6)
	if _, err := s3.ReadAt(oid, b, 0); err != nil {
		t.Fatalf("read under guard: %v", err)
	}
	if string(b) != "hello!" {
		t.Fatalf("content = %q", b)
	}
}
