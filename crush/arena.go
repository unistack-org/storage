package crush

import "unsafe"

// Arena is a bump allocator over raw memory blocks obtained outside the Go
// heap (mmap on unix; GC-managed blocks as a portable fallback). Motivation:
// the CRUSH hot path must not feed the GC. Go slices of int32 carry no
// pointers, so they are never *scanned*, but every make() is a
// GC-accounted heap allocation: it grows the heap, triggers cycles and
// contributes to STW/pause work. Arena memory is invisible to the GC
// entirely: allocation is a pointer bump, "free" is Reset/Free of a whole
// arena, and per-op allocations are zero.
//
// Arenas are NOT safe for concurrent use. The pattern in this package:
// one Arena per worker goroutine (Work), allocated at startup, Reset between
// operations at most.
//
// (GOEXPERIMENT=arenas offers the same idea upstream, but it is
// experimental, not concurrency-safe and tied to a Go build flag; we keep a
// stable implementation with identical economics. A *Arena parameter on
// public constructors lets callers pool arenas across Work instances.)
type Arena struct {
	blocksize int
	first     *arenaBlock
	cur       *arenaBlock
	total     int // bytes handed out since creation (watermark stats)
	count     int // live blocks
}

type arenaBlock struct {
	data []byte // raw memory; data pointer is not GC-managed on unix
	off  int
	next *arenaBlock
}

// arenaAlign is the alignment guaranteed by Alloc: enough for any scalar the
// engine stores (int32/uint32/int64 pointers).
const arenaAlign = 8

// NewArena returns an arena that mints blocks of blocksize bytes (rounded up
// to the OS page size). On allocation failure it panics like make() would.
func NewArena(blocksize int) *Arena {
	if blocksize < 2*arenaPageSize {
		blocksize = 2 * arenaPageSize
	}
	return &Arena{blocksize: blocksize}
}

func (a *Arena) mkmind(min int) *arenaBlock {
	size := a.blocksize
	if size < min {
		size = min
	}
	size = (size + arenaPageSize - 1) &^ (arenaPageSize - 1)
	return &arenaBlock{data: arenaMmap(size)}
}

// Alloc bumps off n bytes (8-byte aligned). The returned memory is
// uninitialized and owned by the arena: callers must not retain it past
// Reset/Free.
func (a *Arena) Alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	off := 0
	if a.cur != nil {
		off = (a.cur.off + arenaAlign - 1) &^ (arenaAlign - 1)
	}
	if a.cur == nil || off+n > len(a.cur.data) {
		b := a.mkmind(arenaAlign + n)
		a.count++
		b.next = a.cur
		a.first, a.cur = b, b
		off = 0
	}
	a.cur.off = off + n
	a.total += n
	return a.cur.data[off : off+n]
}

// AllocInt32 is Alloc for a []int32 of length n.
func (a *Arena) AllocInt32(n int) []int32 {
	if n <= 0 {
		return nil
	}
	b := a.Alloc(n * 4)
	// The arena block keeps the backing memory alive for the slice header's
	// lifetime; elements are scalar, so the GC never needs to see inside.
	return unsafe.Slice((*int32)(unsafe.Pointer(unsafe.SliceData(b))), n)
}

// Reset rewinds the bump pointer: every allocation from the arena becomes
// invalid and the memory is immediately reusable (blocks stay mapped). The
// oldest block is kept mapped to avoid remap churn in the steady state.
func (a *Arena) Reset() {
	b := a.first
	for b != nil && b.next != nil {
		b = b.next
	}
	if b == nil {
		return
	}
	for x := a.first; x != b; {
		nx := x.next
		arenaMunmap(x.data)
		a.count--
		x = nx
	}
	a.first, a.cur = b, b
	b.off = 0
}

// Free releases every mapped block. The arena is empty and reusable after.
func (a *Arena) Free() {
	for b := a.first; b != nil; {
		nx := b.next
		arenaMunmap(b.data)
		b = nx
	}
	a.first, a.cur, a.count = nil, nil, 0
}

// Stats reports bytes currently mapped and the lifetime allocation
// watermark; useful in benchmarks and for tuning blocksize.
func (a *Arena) Stats() (mapped, total int) {
	m := 0
	for b := a.first; b != nil; b = b.next {
		m += len(b.data)
	}
	return m, a.total
}
