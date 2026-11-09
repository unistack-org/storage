package vduse

import "testing"


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
