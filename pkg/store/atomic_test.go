package store

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

func TestAtomicWritePossibleGate(t *testing.T) {
	s := &Store{opts: Options{AtomicWrites: true, AtomicSize: 4096}}
	s.atomicMax.Store(262144)
	s.atomicUnit.Store(4096)

	ok := func(off int64, n int) bool { return s.atomicWritePossible(off, n) }
	cases := []struct {
		off  int64
		n    int
		want bool
	}{
		{0, 4096, true},
		{4096, 4096, true},
		{8192, 4096, true},
		{0, 262144, false},        // above AtomicSize (the 4096 default caps it)
		{0, 512, false},           // below the atomic unit
		{512, 4096, false},        // offset not unit-aligned
		{4096, 4096 + 512, false}, // length not a unit multiple
	}
	for _, c := range cases {
		if got := ok(c.off, c.n); got != c.want {
			t.Errorf("off=%d n=%d: possible=%v, want %v", c.off, c.n, got, c.want)
		}
	}

	// The option being off disables everything.
	s.opts.AtomicWrites = false
	if ok(0, 4096) {
		t.Error("AtomicWrites=false must disable the atomic path")
	}
	s.opts.AtomicWrites = true

	// Raising AtomicSize lets a device-sized write through.
	s.opts.AtomicSize = 262144
	if !ok(8192, 262144) {
		t.Error("AtomicSize=262144 must allow a 256K unit-aligned write")
	}
	if ok(8192, 262144+4096) {
		t.Error("above the device maximum must stay off")
	}
	s.opts.AtomicSize = 0
	if !ok(0, 4096) {
		t.Error("AtomicSize=0 must fall back to the 4096 default")
	}
	s.opts.AtomicSize = 4096

	// A device without an atomic unit (the common case) disables it.
	s.atomicMax.Store(0)
	if ok(0, 4096) {
		t.Error("atomic_max=0 must disable the atomic path")
	}
	s.atomicMax.Store(262144)

	// Once the kernel refuses (EINVAL latch), the path stays off.
	s.atomicFailed.Store(true)
	if ok(0, 4096) {
		t.Error("atomicFailed must disable the atomic path")
	}
}

func TestAtomicWritesInertWithoutGeometry(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, Options{AtomicWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.atomicWritePossible(0, 4096) {
		t.Fatal("the temp dir (no sysfs atomic unit on the dev stand) must leave the path inert")
	}
	// Writes must still work through the normal path.
	data := bytes.Repeat([]byte("x"), 8192)
	if _, err := s.WriteAt(object.OID(1), data, 0); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8192)
	if _, err := s.ReadAt(object.OID(1), got, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("read-back mismatch")
	}
}

func TestProbeDiskTempDir(t *testing.T) {
	dir := t.TempDir()
	info, err := ProbeDisk(dir)
	if info.Path != dir {
		t.Fatalf("Path = %q, want %q", info.Path, dir)
	}
	// A tmpfs/overlay temp dir (containers, macOS) has no block
	// device: the probe must say so cleanly, never panic. On a real
	// Linux filesystem the device must resolve.
	if err != nil {
		if !strings.Contains(err.Error(), "virtual filesystem") {
			t.Fatalf("unexpected probe error: %v", err)
		}
	}
	// A path that does not exist must produce a clean error, not a panic.
	if _, err := ProbeDisk(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("expected an error for a missing path")
	}
}
