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

// Add registers a region, replacing any region overlapping the same range.
func (t *IOTLB) Add(r Region) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := t.rs[:0]
	for _, x := range t.rs {
		if x.Start > r.Last || x.Last < r.Start {
			out = append(out, x)
		}
		// an overlapping old region is dropped (replaced)
	}
	t.rs = append(out, r)
	sort.Slice(t.rs, func(i, j int) bool { return t.rs[i].Start < t.rs[j].Start })
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

// iovaMem adapts an IOTLB to the GuestMem interface used by the ring
// logic.
type iovaMem struct {
	t *IOTLB
}

// NewGuestMem returns a GuestMem backed by the IOTLB mapping.
func NewGuestMem(t *IOTLB) GuestMem { return &iovaMem{t: t} }

func (m *iovaMem) Read(dst []byte, iova uint64) error {
	src, err := m.t.Lookup(iova, uint64(len(dst)))
	if err != nil {
		return err
	}
	copy(dst, src)
	return nil
}

func (m *iovaMem) Write(src []byte, iova uint64) error {
	dst, err := m.t.Lookup(iova, uint64(len(src)))
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
	return m.t.Lookup(iova, uint64(length))
}

var _ SliceGuestMem = (*iovaMem)(nil)
