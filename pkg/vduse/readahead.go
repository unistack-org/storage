package vduse

// Read-ahead (ideas §3.8). The guest reads sequentially in large
// strides (page cache readahead, dd, mkfs, sequential scans); a
// sequential run of N requests of size S each costs N backend round
// trips of S bytes. The read-ahead detector below notices the run and
// widens the FIRST request of the run to a bigger window: the guest
// pays one S-sized round trip, the device fetches the window (and
// caches it), and the following N-1 requests are served from cache.
// Vitastor measured 2-3x from exactly this on their file/VDUSE path.
//
// The design is deliberately small:
//
//   - One tracker per virtqueue (sequential streams are per-queue:
//     the guest's block layer steers one stream to one queue).
//   - The cache is attached to the request that triggered the
//     prefetch and consumed by requests that fall inside it; the
//     window is bounded (WindowBytes), which is the memory bound
//     ideas §3.8 asks for.
//   - Writes, discards and zeroes invalidate the cache: the device
//     must never serve a guest-visible write out of a stale prefetch.
//
// Portable (pure logic over byte slices) so it is unit-testable on
// dev hosts; the Linux runtime wires it into the IN/OUT path.

// ReadAheadOptions configures the detector. Zero values select the
// defaults tuned for a 4 KiB-granular cluster backend.
type ReadAheadOptions struct {
	// WindowBytes is the prefetch window per hit (default 256 KiB).
	// It bounds the cache, so it also bounds the memory a streaming
	// guest can pin per queue.
	WindowBytes int
	// MinGoodSize: requests up to this size (default 16 KiB) get
	// read-ahead — small sequential reads are the target. Larger
	// requests already stream and go straight to the backend.
	MinGoodSize int
}

func (o *ReadAheadOptions) window() int {
	if o.WindowBytes >= 4096 {
		return o.WindowBytes
	}
	return 256 << 10
}

func (o *ReadAheadOptions) minGoodSize() int {
	if o.MinGoodSize > 0 {
		return o.MinGoodSize
	}
	return 16 << 10
}

// readAhead is the per-queue sequential detector + single-window
// cache. Not concurrency-safe: one per queue (queueState owns it, the
// worker pool for a queue shares it under its own serialisation — see
// planRead callers).
type readAhead struct {
	opt        ReadAheadOptions
	seqOff     int64 // expected offset of the next request in the run
	seqSize    int   // size of requests in the current run
	hits       int   // consecutive in-run requests observed
	prefetched int64 // end offset of the prefetched window (cache covers up to here)
	cacheOff   int64
	cache      []byte // cached bytes [cacheOff, cacheOff+len(cache))
	enabled    bool
}

func newReadAhead(enabled bool, opt ReadAheadOptions) *readAhead {
	if !enabled {
		return nil
	}
	return &readAhead{opt: opt, enabled: true, seqOff: -1}
}

// planRead inspects a read of n bytes at off (before it goes to the
// backend). Returns:
//
//   - hitSlice/hit: the request lies wholly inside the cached window;
//     serve it from hitSlice (valid until the next invalidate/keep).
//   - prefetch: fetch [off, off+prefetch) from the backend (bounded
//     to the device size). The caller serves the first n bytes to the
//     guest and keeps prefetch-n bytes (starting at off+n) via keep.
//   - otherwise: plain passthrough (the caller reads n bytes).
//
// A read outside a sequential run resets the tracker; a window is
// prefetched once per run start, not per request.
func (ra *readAhead) planRead(off int64, n int, size int64) (prefetch int, hitSlice []byte, hit bool) {
	if ra == nil || !ra.enabled || n <= 0 {
		return 0, nil, false
	}
	// Cache hit: the request lies wholly inside the cached window.
	if len(ra.cache) > 0 && off >= ra.cacheOff && off+int64(n) <= ra.cacheOff+int64(len(ra.cache)) {
		ra.consume(off, n)
		return 0, ra.cache[off-ra.cacheOff : off-ra.cacheOff+int64(n)], true
	}
	// Sequential run tracking: a request that begins exactly where
	// the previous one ended, at the same size.
	if ra.seqSize == n && off == ra.seqOff && ra.seqOff >= 0 {
		ra.hits++
	} else {
		ra.hits = 0
		ra.seqSize = n
	}
	ra.seqOff = off + int64(n)
	if off >= ra.prefetched {
		ra.dropCache() // window expired behind us
	}
	// Prefetch only for small sequential reads, at the START of a
	// run, and only when no cache window is live (a live window
	// already covers the stream ahead).
	if ra.hits == 1 && n <= ra.opt.minGoodSize() && len(ra.cache) == 0 {
		win := int64(ra.opt.window())
		want := off + win
		if want > size {
			want = size
		}
		if p := int(want - off); p > n {
			ra.prefetched = want
			return p, nil, false
		}
	}
	return 0, nil, false
}

// consume advances the run pointer after a cache hit; the cached
// window stays (hits inside it are free).
func (ra *readAhead) consume(off int64, n int) {
	if end := off + int64(n); end > ra.seqOff {
		ra.seqOff = end
	}
	ra.hits++
}

// keep stores the prefetched tail the caller fetched (bytes starting
// at keepOff).
func (ra *readAhead) keep(keepOff int64, b []byte) {
	if ra == nil || !ra.enabled {
		return
	}
	ra.cacheOff = keepOff
	ra.cache = b
}

// dropCache invalidates the cached window.
func (ra *readAhead) dropCache() {
	ra.cache = nil
	ra.cacheOff = 0
	ra.prefetched = 0
}

// invalidate drops cache AND sequence state: any mutation (write,
// discard, write-zeroes) makes a prefetched window suspect, and the
// sequence detector cannot trust ordering across a write.
func (ra *readAhead) invalidate() {
	if ra == nil {
		return
	}
	ra.dropCache()
	ra.seqOff = -1
	ra.seqSize = 0
	ra.hits = 0
}
