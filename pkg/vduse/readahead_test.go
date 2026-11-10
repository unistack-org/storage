package vduse

import (
	"bytes"
	"testing"
)

// A sequential run of small reads triggers exactly one prefetch, at
// the second request of the run, and the following requests are cache
// hits served zero-copy.
func TestReadAheadSequentialRun(t *testing.T) {
	const size = 64 << 20
	ra := newReadAhead(true, ReadAheadOptions{WindowBytes: 256 << 10, MinGoodSize: 16 << 10})

	// Fill fake backing: byte at offset i = i%251, so slice contents
	// prove the hit came from the right place.
	back := make([]byte, 512<<10)
	for i := range back {
		back[i] = byte(i % 251)
	}

	off := int64(0)
	const n = 4096
	// Request 1: opens the run, no prefetch yet (a lone read is not a
	// sequential stream).
	p, tok, hs, hit := ra.planRead(off, n, size)
	if p != 0 || hit {
		t.Fatalf("first read: prefetch=%d hit=%v, want 0/false", p, hit)
	}
	off += n

	// Request 2: the run is proven → prefetch the full window.
	p, tok, hs, hit = ra.planRead(off, n, size)
	if hit {
		t.Fatal("second read must not be a hit before the prefetch is kept")
	}
	if p != 256<<10 {
		t.Fatalf("second read prefetch = %d, want %d", p, 256<<10)
	}
	// Caller fetches [off, off+p), serves the first n, keeps the tail.
	buf := back[off : off+int64(p)]
	ra.keep(off+int64(n), buf[n:], tok)
	served := buf[:n]
	if !bytes.Equal(served, back[off:off+n]) {
		t.Fatal("served bytes wrong")
	}
	off += n

	// Requests 3..: all hits inside the window, contents exact.
	hits := 0
	for off+int64(n) <= int64(len(back)) && hits < 60 {
		p, tok, hs, hit = ra.planRead(off, n, size)
		if !hit {
			t.Fatalf("read at %d: expected hit (window live), got prefetch=%d", off, p)
		}
		if !bytes.Equal(hs, back[off:off+int64(n)]) {
			t.Fatalf("read at %d: hit slice contents wrong", off)
		}
		if p != 0 {
			t.Fatalf("hit must not request a prefetch (=%d)", p)
		}
		hits++
		off += n
	}
	if hits < 58 {
		t.Fatalf("only %d hits; window should cover ~62 requests", hits)
	}

	// Jump past the prefetched window: a read beyond it must miss,
	// and the continuous run re-arms a prefetch at the window
	// boundary (the request that leaves the cache is the new run
	// start).
	past := int64(600 << 10)
	p, _, _, hit = ra.planRead(past, n, size)
	if hit {
		t.Fatal("read past window must not hit")
	}
	p, _, _, _ = ra.planRead(past+n, n, size)
	if p <= n {
		t.Fatalf("resumed run should prefetch, got %d", p)
	}
	// A continuous stream leaving the window re-prefetches at the
	// boundary: refill the window at 8192, walk it to its end, and
	// the next in-order request must prefetch again.
	ra.invalidate()
	ra.planRead(0, n, size)
	p, tok, _, _ = ra.planRead(int64(n), n, size)
	ra.keep(int64(2*n), make([]byte, p-n), tok)
	off = int64(2 * n)
	for end := off + int64(p-n); off < end; off += n {
		if _, _, _, hit = ra.planRead(off, n, size); !hit {
			t.Fatalf("stream at %d inside window must hit", off)
		}
	}
	if p, _, _, _ = ra.planRead(off, n, size); p <= n {
		t.Fatalf("run at window boundary must re-prefetch, got %d", p)
	}
}

// Writes, discards and zeroes drop the window: the next read of the
// same range goes to the backend, not to stale cache.
func TestReadAheadWriteInvalidates(t *testing.T) {
	const size = 64 << 20
	ra := newReadAhead(true, ReadAheadOptions{})
	// Open a run and fill the window.
	ra.planRead(0, 4096, size)
	p, tok, _, _ := ra.planRead(4096, 4096, size)
	if p == 0 {
		t.Fatal("expected prefetch")
	}
	ra.keep(8192, make([]byte, p-4096), tok)
	if _, _, _, hit := ra.planRead(8192, 4096, size); !hit {
		t.Fatal("setup: expected hit")
	}
	ra.invalidate()
	if _, _, _, hit := ra.planRead(8192, 4096, size); hit {
		t.Fatal("read after invalidate must miss")
	}
	// Sequence state also reset: the post-invalidate read at 8192
	// opens a NEW run (prefetch=0), and only the next in-order read
	// proves it and prefetches — the pre-write run was broken.
	p, _, _, _ = ra.planRead(12288, 4096, size)
	if want := 256 << 10; p != want {
		t.Fatalf("new run after invalidate should prefetch %d, got %d", want, p)
	}
}

// The window never crosses the device size.
func TestReadAheadBoundedByDeviceSize(t *testing.T) {
	ra := newReadAhead(true, ReadAheadOptions{WindowBytes: 256 << 10})
	// Device of 12 KiB: run of 4 KiB reads.
	ra.planRead(0, 4096, 12<<10)
	p, _, _, _ := ra.planRead(4096, 4096, 12<<10)
	if want := 8 << 10; p != want {
		t.Fatalf("prefetch = %d, want clamped %d", p, want)
	}
	// A read at the very end of the device prefetches nothing.
	ra.invalidate()
	ra.planRead(0, 4096, 12<<10)
	p, _, _, _ = ra.planRead(8192, 4096, 12<<10)
	if p != 0 {
		t.Fatalf("prefetch at device end = %d, want 0", p)
	}
}

// Large requests bypass read-ahead entirely (they already stream).
func TestReadAheadLargeReadsPassthrough(t *testing.T) {
	ra := newReadAhead(true, ReadAheadOptions{MinGoodSize: 16 << 10})
	off := int64(0)
	for i := 0; i < 8; i++ {
		p, _, _, hit := ra.planRead(off, 64<<10, 64<<20)
		if p != 0 || hit {
			t.Fatalf("large read %d: prefetch=%d hit=%v, want passthrough", i, p, hit)
		}
		off += 64 << 10
	}
}

// Random (non-sequential) reads never prefetch.
func TestReadAheadRandomPassthrough(t *testing.T) {
	ra := newReadAhead(true, ReadAheadOptions{})
	seq := []int64{1 << 20, 9 << 20, 3 << 20, 45 << 20, 7 << 20, 30 << 20, 2 << 20, 60 << 20}
	for i, off := range seq {
		p, _, _, hit := ra.planRead(off, 4096, 64<<20)
		if p != 0 || hit {
			t.Fatalf("random read %d: prefetch=%d hit=%v, want passthrough", i, p, hit)
		}
	}
}

// Disabled read-ahead (nil tracker) is inert on every entry point.
func TestReadAheadDisabledNil(t *testing.T) {
	var ra *readAhead
	p, _, hs, hit := ra.planRead(0, 4096, 64<<20)
	if p != 0 || hs != nil || hit {
		t.Fatal("nil tracker must pass through")
	}
	ra.invalidate()
	ra.keep(0, []byte{1}, 1)
}

// A window that is live suppresses re-prefetch while it still covers
// the stream ahead; re-prefetch happens only after it is consumed.
func TestReadAheadNoReprefetchWhileLive(t *testing.T) {
	ra := newReadAhead(true, ReadAheadOptions{WindowBytes: 64 << 10})
	const n = 4096
	ra.planRead(0, n, 64<<20)
	p, tok, _, _ := ra.planRead(n, n, 64<<20)
	if p != 64<<10 {
		t.Fatalf("prefetch %d", p)
	}
	ra.keep(int64(2*n), make([]byte, p-n), tok)
	off := int64(2 * n)
	for i := 0; off < 60<<10; i++ {
		p, _, _, hit := ra.planRead(off, n, 64<<20)
		if !hit {
			// Leaving the window must be a plain miss, not a prefetch
			// (the run continues, window re-arms at run restart only).
			if p != 0 {
				t.Fatalf("mid-stream at %d: unexpected prefetch %d", off, p)
			}
		}
		off += n
	}
}
