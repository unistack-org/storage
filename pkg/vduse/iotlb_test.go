package vduse

import (
	"sync"
	"testing"
	"time"
)

func TestIOTLBEvictPinnedDefersMunmap(t *testing.T) {
	var tlb IOTLB
	r := Region{Start: 0x1000, Last: 0x1FFF, Mem: make([]byte, 0x1000)}
	tlb.Add(r)

	ref, err := tlb.Acquire(0x1234, 16)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	b, ok := ref.Slice(0x1234, 16)
	if !ok || &b[0] != &r.Mem[0x234] {
		t.Fatalf("Slice on pinned ref returned wrong memory")
	}
	// Evict while pinned: region leaves the table, munmap deferred.
	if !tlb.Evict(0x1000, 0) {
		t.Fatalf("Evict refused a matching region")
	}
	if _, err := tlb.Lookup(0x1000, 16); err == nil {
		t.Fatalf("Lookup found evicted region")
	}
	if got := tlb.Pinned(0x1000); got != 1 {
		t.Fatalf("Pinned = %d, want 1", got)
	}
	// Pinned memory stays valid after eviction.
	b[0] = 0xAB
	if ref.r.Mem[0x234] != 0xAB {
		t.Fatalf("pinned slice detached from region memory")
	}
	ref.Release()
	if got := tlb.Pinned(0x1000); got != 0 {
		t.Fatalf("Pinned = %d after Release, want 0", got)
	}
	// Evict with keepLargerThan spares permanent mappings bigger than
	// the bound (0 disables the bound: everything is evictable).
	big := Region{Start: 0x2000, Last: 0x2FFF, Mem: make([]byte, 0x1000)}
	tlb.Add(big)
	if !tlb.Evict(0x2000, 0) {
		t.Fatalf("Evict with a disabled bound refused")
	}
	tlb.Add(big)
	if tlb.Evict(0x2000, 0xFFF) {
		t.Fatalf("Evict removed a region larger than the keep bound")
	}
	if _, err := tlb.Lookup(0x2000, 16); err != nil {
		t.Fatalf("big mapping was not kept: %v", err)
	}
	if !tlb.Evict(0x2000, 0x1000) {
		t.Fatalf("Evict should drop a region at/below the keep bound")
	}
	// Acquire on a closed table (after ReleaseAll) must fail.
	tlb.Add(big)
	tlb.ReleaseAll()
	if _, err := tlb.Acquire(0x2000, 16); err == nil {
		t.Fatalf("Acquire succeeded on closed table")
	}
}

func TestIOTLBAddOverlappingPinned(t *testing.T) {
	var tlb IOTLB
	old := Region{Start: 0x4000, Last: 0x4FFF, Mem: make([]byte, 0x1000)}
	tlb.Add(old)
	ref, err := tlb.Acquire(0x4000, 8)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	// Replace the pinned region: the old one is parked (zombie), the
	// new one answers lookups.
	fresh := Region{Start: 0x4000, Last: 0x4FFF, Mem: make([]byte, 0x1000)}
	tlb.Add(fresh)
	nb, err := tlb.Lookup(0x4000, 8)
	if err != nil {
		t.Fatalf("Lookup after Add: %v", err)
	}
	if &nb[0] != &fresh.Mem[0] {
		t.Fatalf("Lookup returned the old (parked) region")
	}
	if got := tlb.Pinned(0x4000); got != 1 {
		t.Fatalf("Pinned = %d, want 1", got)
	}
	ref.Release()
	if got := tlb.Pinned(0x4000); got != 0 {
		t.Fatalf("Pinned = %d after Release, want 0", got)
	}
	tlb.ReleaseAll()
}

func TestIOTLBEvictAllBatch(t *testing.T) {
	var tlb IOTLB
	mem := func(n int) []byte { return make([]byte, n) }
	// Three per-request regions and one permanent region that is bigger
	// than the keep bound.
	tlb.Add(Region{Start: 0x1000, Last: 0x1FFF, Mem: mem(0x1000)})
	tlb.Add(Region{Start: 0x2000, Last: 0x2FFF, Mem: mem(0x1000)})
	tlb.Add(Region{Start: 0x3000, Last: 0x3FFF, Mem: mem(0x1000)})
	perm := Region{Start: 0x100000, Last: 0x100FFF, Mem: mem(0x2000)} // 8 KiB > bound 4 KiB
	tlb.Add(perm)

	addrs := []uint64{0x1234, 0x2234, 0x2400 /* same region as 0x2234 */, 0x100010 /* perm */}
	tlb.EvictAll(addrs, 0x1000)

	for _, a := range []uint64{0x1000, 0x2000} {
		if _, err := tlb.Lookup(a, 16); err == nil {
			t.Fatalf("EvictAll left region at %x mapped", a)
		}
	}
	if _, err := tlb.Lookup(0x3000, 16); err != nil {
		t.Fatalf("EvictAll dropped an untouched region: %v", err)
	}
	if _, err := tlb.Lookup(0x100000, 16); err != nil {
		t.Fatalf("EvictAll evicted a region above the keep bound: %v", err)
	}
	// A bound of 0 disables the bound: the permanent region goes too.
	tlb.EvictAll([]uint64{0x100010}, 0)
	if _, err := tlb.Lookup(0x100000, 16); err == nil {
		t.Fatalf("EvictAll with bound 0 spared the permanent region")
	}
	tlb.ReleaseAll()
}

func TestIOTLBConcurrentPinEvict(t *testing.T) {
	// Stress the lock-free reader against evict/mutate writers: pins,
	// lookups and slices racing Add/Evict/EvictAll/RemoveRange. Run
	// under -race. The invariant is "no crash, no use of a region the
	// table has fully dropped" (a returned Slice must stay readable
	// for the ref's lifetime, evicted regions must never be handed out
	// by a fresh Acquire).
	var tlb IOTLB
	const regionSize = 0x1000
	base := uint64(0x10000)
	tlb.Add(Region{Start: base, Last: base + regionSize - 1, Mem: make([]byte, regionSize)})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Readers: acquire, touch a distinct slice, release.
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(off uint64) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ref, err := tlb.Acquire(base+off, 64)
				if err != nil {
					continue
				}
				b, ok := ref.Slice(base+off, 64)
				if ok {
					b[0] = 0xAB // must not fault: pin keeps pages mapped
					_ = b[0]
				}
				ref.Release()
			}
		}(uint64(i) * 64)
	}
	// Writer: churn the mapping the readers target.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			tlb.Evict(base, 0)
			tlb.Add(Region{Start: base, Last: base + regionSize - 1, Mem: make([]byte, regionSize)})
			tlb.EvictAll([]uint64{base, base}, 0)
			tlb.Add(Region{Start: base, Last: base + regionSize - 1, Mem: make([]byte, regionSize)})
		}
	}()
	for i := 0; i < 2000; i++ {
		if ref, err := tlb.Acquire(base, 8); err == nil {
			ref.Release()
		}
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
	if got := tlb.Pinned(base); got != 0 {
		t.Fatalf("Pinned = %d after quiesce, want 0", got)
	}
	tlb.ReleaseAll()
}
