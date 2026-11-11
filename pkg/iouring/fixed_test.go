//go:build linux

package iouring

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// requireRegister skips the test when the kernel refuses
// io_uring_register outright: virtualised kernels (the apple-virt
// Fedora podman machine, for one) answer EFAULT/EBADF for every
// pointer-carrying registration op, and the production code degrades
// to plain opcodes in exactly this situation — the plain path is
// already covered by the roundtrip tests.
func requireRegister(t *testing.T, ring Ring) {
	t.Helper()
	probe := make([]byte, 4096)
	_, err := ring.RegisterBuffer(probe)
	if err == nil {
		ring.UnregisterBuffer(1)
		return
	}
	for _, e := range []error{syscall.EFAULT, syscall.EBADF, syscall.EINVAL, syscall.EPERM} {
		if errors.Is(err, e) {
			t.Skipf("io_uring_register unsupported here: %v", err)
		}
	}
	t.Fatalf("RegisterBuffer probe: %v", err)
}

// TestRingFixedFilesBuffers drives the real fixed-file + fixed-buffer
// path (IOSQE_FIXED_FILE + READ_FIXED/WRITE_FIXED) and verifies the
// bytes on disk through the plain path.
func TestRingFixedFilesBuffers(t *testing.T) {
	ring := newTestRing(t, 32)
	defer ring.Close()
	requireRegister(t, ring)
	u := ring.(*uring)

	path := filepath.Join(t.TempDir(), "obj")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fd := int32(f.Fd())
	ring.RegisterFile(fd, f)
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}

	arena := make([]byte, 64*1024)
	idx, err := ring.RegisterBuffer(arena)
	if err != nil {
		t.Fatalf("RegisterBuffer: %v", err)
	}
	if idx != 1 {
		t.Fatalf("first buffer index = %d, want 1", idx)
	}

	const chunk = 4096
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		// Fill the arena slice with a pattern, write it fixed.
		seg := arena[i*chunk : (i+1)*chunk]
		for j := range seg {
			seg[j] = byte(i*7 + j)
		}
		wg.Add(1)
		if err := ring.Submit([]Op{{
			Fd: fd, Buf: seg, Off: int64(i) * chunk, Write: true, BufIndex: idx,
			Done: func(n int, err error) {
				if n != chunk || err != nil {
					t.Errorf("fixed write: n=%d err=%v", n, err)
				}
				wg.Done()
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()

	// Read it back fixed into a clean arena region and compare.
	readArena := make([]byte, 16*chunk)
	ridx, err := ring.RegisterBuffer(readArena)
	if err != nil {
		t.Fatalf("RegisterBuffer(read): %v", err)
	}
	if ridx != 2 {
		t.Fatalf("second buffer index = %d, want 2", ridx)
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		seg := readArena[i*chunk : (i+1)*chunk]
		if err := ring.Submit([]Op{{
			Fd: fd, Buf: seg, Off: int64(i) * chunk, BufIndex: ridx,
			Done: func(n int, err error) {
				if n != chunk || err != nil {
					t.Errorf("fixed read: n=%d err=%v", n, err)
				}
				wg.Done()
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	for i := 0; i < 16; i++ {
		want := arena[i*chunk : (i+1)*chunk]
		got := readArena[i*chunk : (i+1)*chunk]
		if !bytes.Equal(got, want) {
			t.Fatalf("segment %d mismatch", i)
		}
	}

	// The fixed path must be the one that ran (kernel accepted the
	// registration): fixedOK degrades only on a refused registration.
	if !u.fixedOK.Load() {
		t.Fatal("fixed path degraded on a supported kernel")
	}
	if u.fileSlots[fd] == 0 {
		t.Fatal("fd missing from fixed-file table")
	}

	// Unregister drops the slot; a later op on the fd uses plain I/O.
	ring.UnregisterBuffer(ridx)
	ring.UnregisterFile(fd)
	if u.fileSlots[fd] != 0 {
		t.Fatal("fd still registered after UnregisterFile")
	}
	got := make([]byte, chunk)
	done := make(chan struct{})
	if err := ring.Submit([]Op{{
		Fd: fd, Buf: got, Off: 3 * chunk,
		Done: func(n int, err error) {
			if n != chunk || err != nil || !bytes.Equal(got, arena[3*chunk:4*chunk]) {
				t.Errorf("plain read after unregister: n=%d err=%v", n, err)
			}
			close(done)
		},
	}}); err != nil {
		t.Fatal(err)
	}
	<-done

	// Verify on-disk bytes with the plain file API.
	disk := make([]byte, 16*chunk)
	if n, err := f.ReadAt(disk, 0); n != len(disk) || err != nil {
		t.Fatalf("ReadAt: %d %v", n, err)
	}
	if !bytes.Equal(disk, arena[:16*chunk]) {
		t.Fatal("disk contents mismatch")
	}
}

// TestRingFixedMixedFiles drives several registered and plain fds at
// once: the per-op decision (fixed vs plain) must be independent. On
// kernels that refuse io_uring_register (fixedOK degrades) every op
// silently runs plain, so this doubles as the degradation test.
func TestRingFixedMixedFiles(t *testing.T) {
	ring := newTestRing(t, 32)
	defer ring.Close()
	dir := t.TempDir()

	files := make([]*os.File, 4)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for k := 0; k < 4; k++ {
		f, err := os.Create(filepath.Join(dir, string(rune('a'+k))))
		if err != nil {
			t.Fatal(err)
		}
		files[k] = f
		defer f.Close()
		fd := int32(f.Fd())
		if k%2 == 0 {
			ring.RegisterFile(fd, f) // fixed
		} // else: plain
		if err := f.Truncate(64 * 1024); err != nil {
			t.Fatal(err)
		}
		buf := bytes.Repeat([]byte{byte('a' + k)}, 4096)
		wg.Add(1)
		if err := ring.Submit([]Op{{
			Fd: fd, Buf: buf, Off: 4096, Write: true,
			Done: func(n int, err error) {
				mu.Lock()
				if firstErr == nil && (n != 4096 || err != nil) {
					firstErr = err
				}
				mu.Unlock()
				wg.Done()
			},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	for k := 0; k < 4; k++ {
		f, err := os.Open(filepath.Join(dir, string(rune('a'+k))))
		if err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 4096)
		if n, err := f.ReadAt(got, 4096); n != 4096 || err != nil {
			t.Fatalf("file %d: %d %v", k, n, err)
		}
		f.Close()
		if !bytes.Equal(got, bytes.Repeat([]byte{byte('a' + k)}, 4096)) {
			t.Fatalf("file %d content mismatch", k)
		}
	}
}
