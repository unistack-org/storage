package iouring

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestRingRoundtrip exercises whichever implementation the platform
// provides: io_uring on Linux, the worker pool elsewhere.
func TestRingRoundtrip(t *testing.T) {
	ring, err := New(16)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "obj")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fd := int32(f.Fd())
	ring.RegisterFile(fd, f)

	payload := bytes.Repeat([]byte("ring"), 4096) // 16 KiB
	if err := f.Truncate(1 << 20); err != nil {
		t.Fatal(err)
	}

	// Write at offset 8192.
	var wg sync.WaitGroup
	wg.Add(1)
	wbuf := append([]byte(nil), payload...)
	err = ring.Submit([]Op{{
		Fd: fd, Buf: wbuf, Off: 8192, Write: true,
		Done: func(n int, err error) {
			if n != len(payload) || err != nil {
				t.Errorf("write done: n=%d err=%v", n, err)
			}
			wg.Done()
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	// Read it back.
	got := make([]byte, len(payload))
	wg.Add(1)
	err = ring.Submit([]Op{{
		Fd: fd, Buf: got, Off: 8192,
		Done: func(n int, err error) {
			if n != len(payload) || err != nil {
				t.Errorf("read done: n=%d err=%v", n, err)
			}
			wg.Done()
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if !bytes.Equal(got, payload) {
		t.Fatal("read data mismatch")
	}

	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRingConcurrent(t *testing.T) {
	ring, err := New(64)
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	path := filepath.Join(t.TempDir(), "many")
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

	const ops = 256
	var wg sync.WaitGroup
	wg.Add(ops)
	var mu sync.Mutex
	var firstErr error
	for i := 0; i < ops; i++ {
		buf := bytes.Repeat([]byte{byte(i)}, 512)
		if err := ring.Submit([]Op{{
			Fd: fd, Buf: buf, Off: int64(i) * 512, Write: true,
			Done: func(n int, err error) {
				if err == nil && n != 512 {
					err = errors.New("short write")
				}
				mu.Lock()
				if firstErr == nil && err != nil {
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
	// Spot-check contents through the ring.
	for _, idx := range []int{0, 7, 128, ops - 1} {
		got := make([]byte, 512)
		done := make(chan struct{})
		if err := ring.Submit([]Op{{
			Fd: fd, Buf: got, Off: int64(idx) * 512,
			Done: func(n int, err error) {
				if err != nil || n != 512 || !bytes.Equal(got, bytes.Repeat([]byte{byte(idx)}, 512)) {
					mu.Lock()
					if firstErr == nil {
						firstErr = errors.New("mismatch")
					}
					mu.Unlock()
				}
				close(done)
			},
		}}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("read at %d timed out", idx)
		}
	}
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

func TestRingClosePending(t *testing.T) {
	ring, err := New(8)
	if err != nil {
		t.Fatal(err)
	}
	// Submit to a closed ring fires Done with ErrClosed.
	if err := ring.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	err = ring.Submit([]Op{{Buf: make([]byte, 4), Done: func(_ int, err error) { done <- err }}})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit after Close = %v, want ErrClosed", err)
	}
	select {
	case e := <-done:
		if !errors.Is(e, ErrClosed) {
			t.Fatalf("callback err = %v, want ErrClosed", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback not fired after Close")
	}
}
