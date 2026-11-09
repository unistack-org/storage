package vduse

import (
	"errors"
	"sort"
	"sync"
	"syscall"
)

// IOTLB maps guest IOVA ranges to host virtual addresses.
//
// The VDUSE kernel module hands out one mmap-able fd per IOVA region
// (VDUSE_IOTLB_GET_FD); the runtime mmaps it and registers the resulting
// base address here. QEMU-type VMs expose guest RAM as one region at
// IOVA 0, so lookups are O(log n) over a tiny slice, guarded by an
// RWMutex: readers (I/O path) never block each other, and region
// updates (VM start/teardown) are rare.
//
// The table OWNS its mappings: every mutator (Add/RemoveRange/Replace/
// Evict) unmaps the regions it removes, and regions pinned by in-flight
// work (Acquire) are parked as zombies instead, munmapped by the last
// Release. Callers never munmap region memory themselves — with a
// parallel data path an immediate munmap would pull pages out from
// under a worker mid-DMA (SIGBUS).
//
// Regions are reference counted. A worker that must touch a guest
// buffer across a backend round-trip Acquires a RegionRef for it; an
// Evict arriving in the meantime only unlinks, so a concurrent
// in-flight DMA (or a Slice view handed to the backend) can never land
// on an unmapped page.
type IOTLB struct {
	mu      sync.RWMutex
	rs      []*region // sorted by Start, non-overlapping
	zombies []*region // unlinked while pinned; dropped at last release
	closed  bool      // ReleaseAll ran: refuse new pins (mappings gone)
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

// region is the table's internal view of a mapping: the Region value
// plus its pin count. refs is guarded by IOTLB.mu. A region with
// refs>0 is never munmapped on unlink: it becomes a zombie, released
// by the final Release.
type region struct {
	Region
	refs  int
	alive bool
}

// RegionRef is a pin on the region covering a range the caller reads
// and writes until Release. A nil *RegionRef is a valid "unpinned"
// reference: Slice fails, Release is a no-op.
type RegionRef struct {
	t *IOTLB
	r *region
}

// ErrNotMapped is returned when a range is not (fully) inside any region.
var ErrNotMapped = errors.New("vduse: iova range not mapped")

// Add registers a region, replacing any region overlapping the same
// range: the table unmaps every region it removes (pinned ones are
// parked, see the type comment).
func (t *IOTLB) Add(r Region) {
	t.mu.Lock()
	var dead []*region
	out := t.rs[:0]
	for _, x := range t.rs {
		if x.Start > r.Last || x.Last < r.Start {
			out = append(out, x)
		} else {
			dead = t.drop(x, dead)
		}
	}
	t.rs = append(out, &region{Region: r, alive: true})
	sort.Slice(t.rs, func(i, j int) bool { return t.rs[i].Start < t.rs[j].Start })
	t.mu.Unlock()
	unmapAll(dead)
}

// RemoveRange unmaps and removes all regions fully covered by
// [start,last] (pinned ones are parked, not unmapped).
func (t *IOTLB) RemoveRange(start, last uint64) {
	t.mu.Lock()
	var dead []*region
	var keep []*region
	for _, x := range t.rs {
		if x.Start >= start && x.Last <= last {
			dead = t.drop(x, dead)
		} else {
			keep = append(keep, x)
		}
	}
	t.rs = keep
	t.mu.Unlock()
	unmapAll(dead)
}

// Lookup returns a host slice view of [addr, addr+length). The range
// must lie entirely within one region.
//
// The view is NOT pinned: it is only safe for copies performed while
// holding it (ring pages, headers). The hot data path must Acquire so
// a concurrent evict cannot pull the mapping out from under a backend
// write; see Acquire.
func (t *IOTLB) Lookup(addr, length uint64) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	r := t.find(addr)
	if r == nil {
		return nil, ErrNotMapped
	}
	off := addr - r.Start
	if length == 0 || off+length > uint64(len(r.Mem)) {
		return nil, ErrNotMapped
	}
	return r.Mem[off : off+length], nil
}

// Acquire pins the region covering [addr, addr+length) and returns a
// reference valid until Release. Callers that touch guest memory
// across a suspension point (a backend round-trip, a worker queue hop)
// must pin it: an Evict arriving in the meantime then only unlinks,
// and the mapping survives until the last pin goes away.
func (t *IOTLB) Acquire(addr, length uint64) (*RegionRef, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrNotMapped
	}
	r := t.find(addr)
	if r == nil {
		return nil, ErrNotMapped
	}
	off := addr - r.Start
	if length == 0 || off+length > uint64(len(r.Mem)) {
		return nil, ErrNotMapped
	}
	r.refs++
	return &RegionRef{t: t, r: r}, nil
}

// Release unpins a RegionRef. The region's memory stays mapped for the
// lifetime of the reference: pages are munmapped strictly after the
// last Release returns, so Slice results taken before Release are safe
// for the duration of the caller's use.
func (p *RegionRef) Release() {
	if p == nil || p.r == nil {
		return
	}
	t := p.t
	r := p.r
	p.r = nil
	t.mu.Lock()
	r.refs--
	var dead []*region
	if r.refs == 0 && !r.alive {
		// Last pin gone: drop it from the zombie list and munmap
		// outside the lock.
		for i, z := range t.zombies {
			if z == r {
				t.zombies = append(t.zombies[:i], t.zombies[i+1:]...)
				break
			}
		}
		dead = append(dead, r)
	}
	t.mu.Unlock()
	unmapAll(dead)
}

// Slice returns a host view of [addr, addr+length) inside the pinned
// region. It must be called before Release.
func (p *RegionRef) Slice(addr, length uint64) ([]byte, bool) {
	if p == nil || p.r == nil {
		return nil, false
	}
	r := p.r
	if addr < r.Start {
		return nil, false
	}
	off := addr - r.Start
	if length == 0 || off+length > uint64(len(r.Mem)) {
		return nil, false
	}
	return r.Mem[off : off+length], true
}

// find locates the live region containing addr. Callers hold mu.
func (t *IOTLB) find(addr uint64) *region {
	i := sort.Search(len(t.rs), func(i int) bool { return t.rs[i].Start > addr }) - 1
	if i < 0 {
		return nil
	}
	r := t.rs[i]
	if addr > r.Last {
		return nil
	}
	return r
}

// drop unlinks x from the live table, appending it to dead when it
// carries no pins (pinned regions are parked as zombies; their last
// Release munmaps them). Callers hold the write lock.
func (t *IOTLB) drop(x *region, dead []*region) []*region {
	x.alive = false
	if x.refs > 0 {
		t.zombies = append(t.zombies, x)
		return dead
	}
	return append(dead, x)
}

func unmapAll(rs []*region) {
	for _, r := range rs {
		if len(r.Mem) > 0 {
			syscall.Munmap(r.Mem)
		}
	}
}

// Snapshot returns a copy of the current live regions (inspection and
// diagnostics; the table owns the memory).
func (t *IOTLB) Snapshot() []Region {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]Region, 0, len(t.rs))
	for _, r := range t.rs {
		out = append(out, r.Region)
	}
	return out
}

// ReleaseAll unmaps every mapping (teardown). Call only once no worker
// can touch guest memory any more; regions still pinned at this point
// are left mapped (a bug guard, not a feature: pins belong to requests
// the caller has drained). After ReleaseAll the table refuses Acquire
// and lookups miss.
func (t *IOTLB) ReleaseAll() {
	t.mu.Lock()
	dead := t.rs
	dead = append(dead, t.zombies...)
	t.rs, t.zombies = nil, nil
	t.closed = true
	t.mu.Unlock()
	unmapAll(dead)
}

// Exact returns the region mapped exactly at [start,last], if any.
func (t *IOTLB) Exact(start, last uint64) (Region, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, r := range t.rs {
		if r.Start == start && r.Last == last {
			return r.Region, true
		}
	}
	return Region{}, false
}

// Replace drops (unmapping) every region not keyed by a Start present
// in keep. Regions in keep stay registered untouched; pinned drops are
// parked (see Add).
func (t *IOTLB) Replace(keep map[uint64]Region) {
	t.mu.Lock()
	var dead []*region
	var held []*region
	for _, x := range t.rs {
		if _, ok := keep[x.Start]; ok {
			held = append(held, x)
		} else {
			dead = t.drop(x, dead)
		}
	}
	t.rs = held
	t.mu.Unlock()
	unmapAll(dead)
}

// Evict removes the region containing addr. A region still pinned by
// in-flight work is only unlinked (parked): it is munmapped by the
// last Release, so a completion path evicting buffers it just used
// cannot race a worker on another queue sharing the same mapping. It
// reports whether the region was removed from the live table.
//
// Evict-on-completion exists because the driver DMA-unmaps request
// buffers as soon as it sees the used entry and recycles the IOVA, so
// keeping the mapping would risk writing into detached pages.
func (t *IOTLB) Evict(addr uint64, keepLargerThan uint64) bool {
	t.mu.Lock()
	i := sort.Search(len(t.rs), func(i int) bool { return t.rs[i].Start > addr }) - 1
	if i < 0 {
		t.mu.Unlock()
		return false
	}
	r := t.rs[i]
	if addr > r.Last {
		t.mu.Unlock()
		return false
	}
	if keepLargerThan > 0 && uint64(len(r.Mem)) > keepLargerThan {
		t.mu.Unlock()
		return false
	}
	t.rs = append(t.rs[:i], t.rs[i+1:]...)
	dead := t.drop(r, nil)
	t.mu.Unlock()
	unmapAll(dead)
	return true
}

// Pinned reports the number of live pins on mappings covering addr,
// summing the live region and any evicted-but-pinned (parked) ones
// (diagnostics/tests: the point of the count is "who is still touching
// a mapping of this address").
func (t *IOTLB) Pinned(addr uint64) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := 0
	if r := t.find(addr); r != nil {
		n += r.refs
	}
	for _, z := range t.zombies {
		if z.Start <= addr && addr <= z.Last {
			n += z.refs
		}
	}
	return n
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

// SliceGuestMem extends GuestMem with the zero-copy and pinning
// primitives of the hot I/O path. Slice hands guest buffers straight
// to the backend with no copying — this is what keeps the datapath at
// vhost speed — but the view is UNPINNED: it is safe only for copies
// that happen before the next suspension point. Code that keeps a view
// across a backend round-trip must Acquire the segment first: the pin
// guarantees the mapping survives an evict/refresh for the request's
// lifetime (pages are munmapped strictly after the last Release).
type SliceGuestMem interface {
	GuestMem
	Slice(iova uint64, length int) ([]byte, error)
	// Acquire pins [iova, iova+length) (with pull-on-miss) and returns
	// a reference valid until Release.
	Acquire(iova uint64, length int) (*RegionRef, error)
}

func (m *iovaMem) Slice(iova uint64, length int) ([]byte, error) {
	return m.lookup(iova, uint64(length))
}

func (m *iovaMem) Acquire(iova uint64, length int) (*RegionRef, error) {
	p, err := m.t.Acquire(iova, uint64(length))
	if err != nil && m.pull != nil {
		if perr := m.pull(iova, uint64(length)); perr == nil {
			p, err = m.t.Acquire(iova, uint64(length))
		}
	}
	return p, err
}

var _ SliceGuestMem = (*iovaMem)(nil)
