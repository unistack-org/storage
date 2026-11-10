package vduse

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
)

// IOTLB maps guest IOVA ranges to host virtual addresses.
//
// The VDUSE kernel module hands out one mmap-able fd per IOVA region
// (VDUSE_IOTLB_GET_FD); the runtime mmaps it and registers the resulting
// base address here. QEMU-type VMs expose guest RAM as one region at
// IOVA 0, so lookups are O(log n) over a tiny list.
//
// The virtio-vDPA driver maps request buffers through the DMA API
// (vduse_dev_map_page) WITHOUT sending any message, and DMA-unmaps them
// (vduse_domain_unmap_page) as soon as it sees our used entry — also
// silently, recycling the IOVA into its stream cache. So the hot I/O
// path discovers fresh mappings by lookup miss (pull-on-miss) and must
// drop them at completion (Evict): a stale mapping for a recycled IOVA
// would write into detached/reused guest pages. The table is therefore
// read-mostly (one Acquire + one Evict per request) and reads dominate.
//
// Lock design (this is the perf-critical path — a single global RWMutex
// taken for WRITES on Acquire/Release/Evict capped us well below target
// under 95K IOPS): the live region list is an immutable copy-on-write
// snapshot published with an atomic pointer. Readers (Lookup/Acquire)
// load the snapshot, binary-search it, and pin with an atomic refcount —
// NO lock at all. Only mutators (Add/RemoveRange/Replace/Evict/
// ReleaseAll) take mu, rebuild a fresh snapshot (n is tiny: 1-3 regions
// for a QEMU guest), and publish it.
//
// The table OWNS its mappings: every mutator unmaps the regions it
// removes, and regions pinned by in-flight work are parked as zombies
// instead, munmapped by the last release. Callers never munmap region
// memory themselves — with a parallel data path an immediate munmap
// would pull pages out from under a worker mid-DMA (SIGBUS).
//
// munmap safety is exact-once: a region is munmapped only by the
// goroutine that wins its dead CAS, which happens when refs reaches 0
// AND the region is unlinked. A pin racing an unlink can transiently
// bump refs on an unlinked region (a heap struct the GC keeps alive);
// the pin's release sees !alive and drives refs to 0, at which point
// exactly one of {writer, last-pinner} wins the CAS and munmaps. No
// page of an unmapped region is ever touched on the miss path.
type IOTLB struct {
	mu      sync.Mutex                // serialises mutators (writes to the snapshot)
	snap    atomic.Pointer[[]*region] // immutable, sorted by Start; lock-free reads
	zombies []*region                 // unlinked while pinned; dropped at last release (guarded by mu)
	closed  atomic.Bool               // ReleaseAll ran: refuse new pins (mappings gone)
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
// plus lock-free pin state. refs is an atomic pin count; alive is set
// false when the region is unlinked from the snapshot; dead guards that
// the mapping is munmapped exactly once (see the type comment).
type region struct {
	Region
	refs  atomic.Int32
	alive atomic.Bool
	dead  atomic.Bool
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

// load returns the current snapshot. A zero-value IOTLB (before the
// first Add) has a nil snapshot pointer; it reads as an empty table.
func (t *IOTLB) load() []*region {
	if p := t.snap.Load(); p != nil {
		return *p
	}
	return nil
}

// findIndex returns the index in rs of the region containing addr, or
// -1. rs is sorted by Start and non-overlapping.
func findIndex(rs []*region, addr uint64) int {
	i := sort.Search(len(rs), func(i int) bool { return rs[i].Start > addr }) - 1
	if i < 0 {
		return -1
	}
	if addr > rs[i].Last {
		return -1
	}
	return i
}

// Add registers a region, replacing any region overlapping the same
// range: the table unmaps every region it removes (pinned ones are
// parked, see the type comment).
func (t *IOTLB) Add(r Region) {
	nr := &region{Region: r}
	nr.alive.Store(true)
	t.mu.Lock()
	var dead []*region
	out := make([]*region, 0, len(t.load())+1)
	for _, x := range t.load() {
		if x.Start > r.Last || x.Last < r.Start {
			out = append(out, x)
		} else {
			dead = t.drop(x, dead)
		}
	}
	out = append(out, nr)
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	t.snap.Store(&out)
	t.mu.Unlock()
	unmapAll(dead)
}

// RemoveRange unmaps and removes all regions fully covered by
// [start,last] (pinned ones are parked, not unmapped).
func (t *IOTLB) RemoveRange(start, last uint64) {
	t.mu.Lock()
	var dead []*region
	var keep []*region
	for _, x := range t.load() {
		if x.Start >= start && x.Last <= last {
			dead = t.drop(x, dead)
		} else {
			keep = append(keep, x)
		}
	}
	t.snap.Store(&keep)
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
	if t.closed.Load() {
		return nil, ErrNotMapped
	}
	rs := t.load()
	i := findIndex(rs, addr)
	if i < 0 {
		return nil, ErrNotMapped
	}
	r := rs[i]
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
//
// Lock-free: load the snapshot, locate the region, pin it atomically.
// A pin that races an unlink (region is being evicted) is undone and
// reported as a miss, so the caller's pull-on-miss can refetch a fresh
// mapping.
func (t *IOTLB) Acquire(addr, length uint64) (*RegionRef, error) {
	if t.closed.Load() {
		return nil, ErrNotMapped
	}
	rs := t.load()
	i := findIndex(rs, addr)
	if i < 0 {
		return nil, ErrNotMapped
	}
	r := rs[i]
	off := addr - r.Start
	if length == 0 || off+length > uint64(len(r.Mem)) {
		return nil, ErrNotMapped
	}
	r.refs.Add(1)
	if !r.alive.Load() {
		// Raced an unlink: never hand out a view of a region whose
		// pages may be on their way to munmap. Undo the pin (which
		// munmaps if we are the last) and miss.
		t.unpin(r)
		return nil, ErrNotMapped
	}
	return &RegionRef{t: t, r: r}, nil
}

// unpin drops one pin on r. If r is unlinked and this was its last
// pin, it is removed from the zombie list and munmapped exactly once
// (dead CAS). Safe to call on regions in any state.
func (t *IOTLB) unpin(r *region) {
	if r.refs.Add(-1) == 0 && !r.alive.Load() {
		if r.dead.CompareAndSwap(false, true) {
			t.mu.Lock()
			for i, z := range t.zombies {
				if z == r {
					t.zombies = append(t.zombies[:i], t.zombies[i+1:]...)
					break
				}
			}
			t.mu.Unlock()
			unmap(r)
		}
	}
}

// Release unpins a RegionRef. The region's memory stays mapped for the
// lifetime of the reference: pages are munmapped strictly after the
// last Release returns, so Slice results taken before Release are safe
// for the duration of the caller's use.
func (p *RegionRef) Release() {
	if p == nil || p.r == nil {
		return
	}
	r := p.r
	p.r = nil
	p.t.unpin(r)
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

// drop unlinks x from the live table, appending it to dead when it
// carries no pins (pinned regions are parked as zombies; their last
// release munmaps them). Callers hold mu.
func (t *IOTLB) drop(x *region, dead []*region) []*region {
	x.alive.Store(false)
	if x.refs.Load() > 0 {
		t.zombies = append(t.zombies, x)
		return dead
	}
	if x.dead.CompareAndSwap(false, true) {
		return append(dead, x)
	}
	return dead
}

func unmap(r *region) {
	if len(r.Mem) > 0 {
		syscall.Munmap(r.Mem)
	}
}

func unmapAll(rs []*region) {
	for _, r := range rs {
		unmap(r)
	}
}

// Snapshot returns a copy of the current live regions (inspection and
// diagnostics; the table owns the memory).
func (t *IOTLB) Snapshot() []Region {
	rs := t.load()
	out := make([]Region, 0, len(rs))
	for _, r := range rs {
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
	dead := t.load()
	dead = append(dead, t.zombies...)
	empty := []*region{}
	t.snap.Store(&empty)
	t.zombies = nil
	t.closed.Store(true)
	t.mu.Unlock()
	// Mark every collected region dead so a late unpin (a pin racing
	// teardown) cannot double-munmap: teardown owns the unmapping.
	for _, r := range dead {
		r.alive.Store(false)
		r.dead.Store(true)
	}
	unmapAll(dead)
}

// Exact returns the region mapped exactly at [start,last], if any.
func (t *IOTLB) Exact(start, last uint64) (Region, bool) {
	for _, r := range t.load() {
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
	for _, x := range t.load() {
		if _, ok := keep[x.Start]; ok {
			held = append(held, x)
		} else {
			dead = t.drop(x, dead)
		}
	}
	t.snap.Store(&held)
	t.mu.Unlock()
	unmapAll(dead)
}

// Evict removes the region containing addr. A region still pinned by
// in-flight work is only unlinked (parked): it is munmapped by the
// last release, so a completion path evicting buffers it just used
// cannot race a worker on another queue sharing the same mapping. It
// reports whether the region was removed from the live table.
//
// Evict-on-completion exists because the driver DMA-unmaps request
// buffers as soon as it sees the used entry and recycles the IOVA, so
// keeping the mapping would risk writing into detached pages.
func (t *IOTLB) Evict(addr uint64, keepLargerThan uint64) bool {
	t.mu.Lock()
	removed := t.evictLocked(addr, keepLargerThan)
	t.mu.Unlock()
	return removed
}

// EvictAll removes the regions containing each address in addrs in a
// single table rebuild. The completion path evicts every data segment
// of a request; batching turns k per-segment write-lock acquisitions
// and snapshot rebuilds into one (the hot path's dominant remaining
// lock cost for multi-segment requests).
func (t *IOTLB) EvictAll(addrs []uint64, keepLargerThan uint64) {
	if len(addrs) == 0 {
		return
	}
	t.mu.Lock()
	var dead []*region
	rs := t.load()
	var out []*region
	seen := make(map[*region]bool, len(addrs))
	removed := false
	for _, addr := range addrs {
		i := findIndex(rs, addr)
		if i < 0 {
			continue
		}
		r := rs[i]
		if seen[r] {
			continue
		}
		if keepLargerThan > 0 && uint64(len(r.Mem)) > keepLargerThan {
			continue
		}
		seen[r] = true
		removed = true
	}
	if !removed {
		t.mu.Unlock()
		return
	}
	out = make([]*region, 0, len(rs))
	for _, r := range rs {
		if seen[r] {
			dead = t.drop(r, dead)
			continue
		}
		out = append(out, r)
	}
	t.snap.Store(&out)
	t.mu.Unlock()
	unmapAll(dead)
}

// evictLocked removes the region containing addr. Caller holds mu.
func (t *IOTLB) evictLocked(addr uint64, keepLargerThan uint64) bool {
	rs := t.load()
	i := findIndex(rs, addr)
	if i < 0 {
		return false
	}
	r := rs[i]
	if keepLargerThan > 0 && uint64(len(r.Mem)) > keepLargerThan {
		return false
	}
	out := make([]*region, 0, len(rs)-1)
	out = append(out, rs[:i]...)
	out = append(out, rs[i+1:]...)
	dead := t.drop(r, nil)
	t.snap.Store(&out)
	unmapAll(dead)
	return true
}

// Pinned reports the number of live pins on mappings covering addr,
// summing the live region and any evicted-but-pinned (parked) ones
// (diagnostics/tests: the point of the count is "who is still touching
// a mapping of this address").
func (t *IOTLB) Pinned(addr uint64) int {
	rs := t.load()
	n := 0
	if i := findIndex(rs, addr); i >= 0 {
		n += int(rs[i].refs.Load())
	}
	t.mu.Lock()
	for _, z := range t.zombies {
		if z.Start <= addr && addr <= z.Last {
			n += int(z.refs.Load())
		}
	}
	t.mu.Unlock()
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
// lifetime (pages are munmapped strictly after the last release).
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
