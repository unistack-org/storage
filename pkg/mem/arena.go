// Package mem provides the arenas the I/O path allocates its buffers
// from: big, long-lived, page-class-backed regions whose pages the
// kernel touches once and keeps. Registered io_uring buffers and the
// net send/recv pools live here, so the hot path never pays page
// faults or per-I/O page pinning.
package mem

// Arena is a fixed-size byte region handed out in equal slots. The
// backing memory is allocated once at New: on Linux with hugepages
// requested it is an anonymous mmap advised with MADV_HUGEPAGE, so a
// multi-megabyte arena is a handful of TLB entries instead of
// thousands; everywhere else it is ordinary zeroed memory. Slots are
// carved from Data, so a slot is a plain []byte usable with any API
// (including the kernel, which only sees addresses inside Data).
type Arena struct {
	Data []byte
	Huge bool // the backing pages are (advised to be) huge
	size int  // slot size in bytes
	n    int  // slot count
}

// NewArena builds an arena of n slots of size bytes each (size is
// rounded up to the page size). hugepages is a hint honoured on Linux
// only; a kernel that refuses the advice (THP disabled) silently keeps
// the plain pages, so Huge reports what was requested and the caller
// must not depend on it.
func NewArena(n, size int, hugepages bool) (*Arena, error) {
	return newArena(n, size, hugepages)
}

// Slot hands out the slot with the given index (0-based); the caller
// owns the slot's bookkeeping (a channel pool, a per-worker index).
// It panics on an out-of-range index: slot indexes come from a pool of
// exactly n tokens.
func (a *Arena) Slot(i int) []byte {
	return a.Data[i*a.size : (i+1)*a.size]
}

// N is the slot count.
func (a *Arena) N() int { return a.n }

// Size is the per-slot byte size (page-rounded).
func (a *Arena) Size() int { return a.size }

// Close releases the arena's backing memory. In-flight users of Slot
// slices after Close is a use-after-free on Linux (the pages are
// munmapped), so the owner must drain its I/O first — mirrors
// iouring.UnregisterBuffer ordering rules.
func (a *Arena) Close() error {
	if a == nil || a.Data == nil {
		return nil
	}
	b := a.Data
	a.Data = nil
	return closeArena(b)
}
