package vduse

import (
	"errors"
	"sort"
	"sync"
)

// IOTLB maps guest IOVA ranges to host virtual addresses.
//
// The VDUSE kernel module hands out one mmap-able fd per IOVA region
// (VDUSE_IOTLB_GET_FD); the runtime mmaps it and registers the resulting
// base address here. QEMU-type VMs expose guest RAM as one region at
// IOVA 0, so lookups are O(log n) over a tiny slice, guarded by an
// RWMutex: readers (I/O path) never block each other, and region
// updates (VM start/teardown) are rare.
type IOTLB struct {
	mu sync.RWMutex
	rs []Region // sorted by Start, non-overlapping
}

// Region is one mapped IOVA range. Mem is the host mapping of IOVA
// Start..Last (from syscall.Mmap); keeping the slice itself (rather
// than a bare uintptr) is what makes the unsafe.Pointer conversions
// vet-clean and keeps the mapping referenced.
type Region struct {
	Start uint64
	Last  uint64 // inclusive
	Mem   []byte
}

// ErrNotMapped is returned when a range is not (fully) inside any region.
var ErrNotMapped = errors.New("vduse: iova range not mapped")

// Add registers a region, replacing (and returning for removal) any
// region overlapping the same range. The caller must munmap the
// returned regions' Mem.
func (t *IOTLB) Add(r Region) []Region {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := t.rs[:0]
	var dropped []Region
	for _, x := range t.rs {
		if x.Start > r.Last || x.Last < r.Start {
			out = append(out, x)
		} else {
			dropped = append(dropped, x)
		}
	}
	t.rs = append(out, r)
	sort.Slice(t.rs, func(i, j int) bool { return t.rs[i].Start < t.rs[j].Start })
	return dropped
}

// RemoveRange removes and returns all regions fully covered by [start,last].
// The caller must munmap the returned regions' Mem.
func (t *IOTLB) RemoveRange(start, last uint64) []Region {
	t.mu.Lock()
	defer t.mu.Unlock()
	var removed, keep []Region
	for _, x := range t.rs {
		if x.Start >= start && x.Last <= last {
			removed = append(removed, x)
		} else {
			keep = append(keep, x)
		}
	}
	t.rs = keep
	return removed
}

// Lookup returns a host slice view of [addr, addr+length). The range
// must lie entirely within one region.
func (t *IOTLB) Lookup(addr, length uint64) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// binary search: last region with Start <= addr
	i := sort.Search(len(t.rs), func(i int) bool { return t.rs[i].Start > addr }) - 1
	if i < 0 {
		return nil, ErrNotMapped
	}
	r := t.rs[i]
	off := addr - r.Start
	if length == 0 || off+length > uint64(len(r.Mem)) {
		return nil, ErrNotMapped
	}
	return r.Mem[off : off+length], nil
}

// Snapshot returns a copy of the current regions (for teardown).
func (t *IOTLB) Snapshot() []Region {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return append([]Region(nil), t.rs...)
}

// Exact returns the region mapped exactly at [start,last], if any.
func (t *IOTLB) Exact(start, last uint64) (Region, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, r := range t.rs {
		if r.Start == start && r.Last == last {
			return r, true
		}
	}
	return Region{}, false
}

// Replace drops every region not keyed by a Start present in keep and
// returns the dropped regions; the caller munmaps their Mem. Regions in
// keep stay registered untouched.
func (t *IOTLB) Replace(keep map[uint64]Region) []Region {
	t.mu.Lock()
	defer t.mu.Unlock()
	var removed, held []Region
	for _, x := range t.rs {
		if _, ok := keep[x.Start]; ok {
			held = append(held, x)
		} else {
			removed = append(removed, x)
		}
	}
	t.rs = held
	return removed
}

// Evict removes the region containing addr, if any, and returns it; the
// caller must munmap its Mem once no in-flight request can touch it.
// Used after completing a request: the driver DMA-unmaps request buffers
// as soon as it sees the used entry and recycles the IOVA, so keeping
// the mapping would risk writing into detached pages.
func (t *IOTLB) Evict(addr uint64) (Region, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	i := sort.Search(len(t.rs), func(i int) bool { return t.rs[i].Start > addr }) - 1
	if i < 0 {
		return Region{}, false
	}
	r := t.rs[i]
	if addr > r.Last {
		return Region{}, false
	}
	t.rs = append(t.rs[:i], t.rs[i+1:]...)
	return r, true
}

// iovaMem adapts an IOTLB to the GuestMem interface used by the ring
// logic. pull (optional) is called on a mapping miss to (re)fetch the
// range from the kernel, then the lookup is retried once: the
// virtio-vDPA driver never notifies the daemon of new mappings (no
// UPDATE_IOTLB in the DMA-ops path), so misses are how fresh per-request
// IOVA mappings are discovered.
type iovaMem struct {
	t    *IOTLB
	pull func(iova, length uint64) error
}

// NewGuestMem returns a GuestMem backed by the IOTLB mapping.
func NewGuestMem(t *IOTLB) GuestMem { return &iovaMem{t: t} }

// NewGuestMemWithPull is NewGuestMem plus a pull-on-miss hook.
func NewGuestMemWithPull(t *IOTLB, pull func(iova, length uint64) error) SliceGuestMem {
	return &iovaMem{t: t, pull: pull}
}

func (m *iovaMem) lookup(iova uint64, length uint64) ([]byte, error) {
	b, err := m.t.Lookup(iova, length)
	if err != nil && m.pull != nil {
		if perr := m.pull(iova, length); perr == nil {
			b, err = m.t.Lookup(iova, length)
		}
	}
	return b, err
}

func (m *iovaMem) Read(dst []byte, iova uint64) error {
	src, err := m.lookup(iova, uint64(len(dst)))
	if err != nil {
		return err
	}
	copy(dst, src)
	return nil
}

func (m *iovaMem) Write(src []byte, iova uint64) error {
	dst, err := m.lookup(iova, uint64(len(src)))
	if err != nil {
		return err
	}
	copy(dst, src)
	return nil
}

// Slice returns a zero-copy host slice for [iova, iova+len). The hot I/O
// path uses it to hand guest buffers straight to the backend with no
// copying — this is what keeps the datapath at vhost speed.
type SliceGuestMem interface {
	GuestMem
	Slice(iova uint64, length int) ([]byte, error)
}

func (m *iovaMem) Slice(iova uint64, length int) ([]byte, error) {
	return m.lookup(iova, uint64(length))
}

var _ SliceGuestMem = (*iovaMem)(nil)
