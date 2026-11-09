package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

func TestResizeVolumeGrow(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	// Allocate vm0's base through the allocator (a real CreateVolume
	// does this; a hand-picked base would not be recorded in the
	// monotonic top pointer).
	base0, err := NextOIDBase(ctx, s, object.NumObjects(4<<30)+1)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{Name: "vm0", Size: 4 << 30, OIDBase: base0, OIDShift: object.SizeShift, Replicas: 3, Rule: "data", PGNum: 64}
	if _, err := PutVolume(ctx, s, spec); err != nil {
		t.Fatal(err)
	}

	// A second volume created after vm0 takes ids above its range: a
	// later grow of vm0 must not collide with it.
	nextBase, err := NextOIDBase(ctx, s, object.NumObjects(spec.Size)+1)
	if err != nil {
		t.Fatal(err)
	}
	spec2 := VolumeSpec{Name: "vm1", Size: 4 << 30, OIDBase: nextBase, OIDShift: object.SizeShift, Replicas: 3, Rule: "data", PGNum: 64}
	if _, err := PutVolume(ctx, s, spec2); err != nil {
		t.Fatal(err)
	}

	grown, err := ResizeVolume(ctx, s, "vm0", 10<<30)
	if err != nil {
		t.Fatal(err)
	}
	if grown.Size != 10<<30 {
		t.Fatalf("grown size = %d", grown.Size)
	}
	if grown.OIDCount != object.NumObjects(4<<30) {
		t.Fatalf("OIDCount = %d, want %d", grown.OIDCount, object.NumObjects(4<<30))
	}
	if len(grown.Extents) != 1 {
		t.Fatalf("extents = %+v, want 1", grown.Extents)
	}
	want := object.NumObjects(10 << 30)
	if grown.ReservedObjs() < want {
		t.Fatalf("reserved %d < want %d", grown.ReservedObjs(), want)
	}

	// Lookups: base maps unchanged, extent ids land above vm1's base
	// (allocator monotonic: the extent was carved from the top).
	for _, idx := range []uint64{0, grown.OIDCount - 1} {
		id, ok := grown.Lookup(idx)
		if !ok || uint64(id) != spec.OIDBase+idx+1 {
			t.Fatalf("Lookup(%d) = %d,%v", idx, id, ok)
		}
	}
	id, ok := grown.Lookup(grown.OIDCount)
	if !ok {
		t.Fatal("Lookup first extent index failed")
	}
	if uint64(id) <= spec2.OIDBase+object.NumObjects(spec2.Size) {
		t.Fatalf("extent id %d collides with vm1 range starting at %d", id, spec2.OIDBase)
	}
	if _, ok := grown.Lookup(want); ok {
		t.Fatal("Lookup past the last object succeeded")
	}

	// Grow again: a second extent, indices stay contiguous.
	grown2, err := ResizeVolume(ctx, s, "vm0", 18<<30)
	if err != nil {
		t.Fatal(err)
	}
	if len(grown2.Extents) != 2 {
		t.Fatalf("extents = %+v, want 2", grown2.Extents)
	}
	seen := map[uint64]bool{}
	total := object.NumObjects(18 << 30)
	for i := uint64(0); i < total; i++ {
		id, ok := grown2.Lookup(i)
		if !ok {
			t.Fatalf("Lookup(%d) failed", i)
		}
		if seen[uint64(id)] {
			t.Fatalf("duplicate id %d at index %d", id, i)
		}
		seen[uint64(id)] = true
	}

	// Shrink is refused; equal size is a no-op.
	if _, err := ResizeVolume(ctx, s, "vm0", 1<<30); !errors.Is(err, ErrShrinkUnsupported) {
		t.Fatalf("shrink = %v, want ErrShrinkUnsupported", err)
	}
	noop, err := ResizeVolume(ctx, s, "vm0", 18<<30)
	if err != nil || noop.Size != 18<<30 || len(noop.Extents) != 2 {
		t.Fatalf("no-op resize = %+v, %v", noop, err)
	}
}

// A bitmap stored before a grow zero-extends to the new nbits (the new
// objects are truthfully unallocated); a shrink keeps erroring.
func TestAllocMapGrowTolerant(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	rec := AllocMapRecord{Bits: make([]byte, 64)} // 512 objects
	rec.Bits[0] = 0b0000_0011
	if _, err := PutAllocMap(ctx, s, "vm0", rec); err != nil {
		t.Fatal(err)
	}
	got, err := GetAllocMap(ctx, s, "vm0", 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Bits) != 128 {
		t.Fatalf("len = %d, want 128", len(got.Bits))
	}
	if got.Bits[0] != 0b0000_0011 {
		t.Fatalf("prefix lost: %b", got.Bits[0])
	}
	for i := 1; i < len(got.Bits); i++ {
		if got.Bits[i] != 0 {
			t.Fatalf("extension not zero at %d", i)
		}
	}
	if _, err := GetAllocMap(ctx, s, "vm0", 256); err == nil {
		t.Fatal("shrink read succeeded")
	}
}
