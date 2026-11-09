package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// newSnapVolume creates a volume whose base range is allocator-owned
// (the way CreateVolume does) and returns its spec.
func newSnapVolume(t *testing.T, s Store, name string, size uint64) VolumeSpec {
	t.Helper()
	ctx := context.Background()
	n := object.NumObjects(size)
	base, err := NextOIDBase(ctx, s, n+1)
	if err != nil {
		t.Fatal(err)
	}
	spec := VolumeSpec{Name: name, Size: size, OIDBase: base, OIDShift: object.SizeShift, Replicas: 3, Rule: "data", PGNum: 64}
	if _, err := PutVolume(ctx, s, spec); err != nil {
		t.Fatal(err)
	}
	return spec
}

func allocBits(n int) []byte { return make([]byte, (n+7)/8) }

func TestSnapshotCreateSwap(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	spec := newSnapVolume(t, s, "vm0", 4<<30)
	oldBase := spec.OIDBase
	nobjs := object.NumObjects(spec.Size)

	// Allocate objects 0..4 in the live bitmap (quota-style).
	bits := allocBits(int(nobjs))
	setAllocBit(bits, 0)
	setAllocBit(bits, 3)
	if _, err := PutAllocMap(ctx, s, "vm0", AllocMapRecord{Bits: bits}); err != nil {
		t.Fatal(err)
	}

	got, err := CreateSnapshot(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	if got.Gen != 1 || len(got.Snapshots) != 1 {
		t.Fatalf("gen %d snaps %d", got.Gen, len(got.Snapshots))
	}
	g := got.Snapshots[0]
	if g.Gen != 1 || g.OIDBase != oldBase || g.OIDCount != spec.BaseObjs() {
		t.Fatalf("frozen record wrong: %+v", g)
	}
	if !g.GenBit(0) || !g.GenBit(3) || g.GenBit(1) {
		t.Fatal("frozen bitmap wrong")
	}
	if got.OIDBase == oldBase {
		t.Fatal("live range not swapped")
	}
	if got.OIDBase <= oldBase+spec.BaseObjs() {
		t.Fatalf("live range %d overlaps frozen range %d", got.OIDBase, oldBase)
	}
	// The live bitmap survives the snapshot untouched (live writes
	// after the snapshot book themselves anew).
	live, err := GetAllocMap(ctx, s, "vm0", int(nobjs))
	if err != nil {
		t.Fatal(err)
	}
	if !allocBit(live.Bits, 0) {
		t.Fatal("live bitmap wiped by snapshot")
	}
	// Live Lookup now addresses the NEW range.
	oid, ok := got.Lookup(0)
	if !ok || uint64(oid) != got.OIDBase+1 {
		t.Fatalf("live lookup after swap: %d %v", oid, ok)
	}
	// Snapshot Lookup addresses the old range.
	if oid, ok := g.Lookup(3); !ok || uint64(oid) != oldBase+4 {
		t.Fatalf("gen lookup: %d %v", oid, ok)
	}
}

func TestSnapshotLimitAnd300Gens(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	// A 4 MiB volume: 1 object, so 300 snapshots reserve cheap ranges.
	spec := newSnapVolume(t, s, "vm0", object.Size)
	_ = spec
	for i := 1; i <= MaxSnapshots; i++ {
		if _, err := CreateSnapshot(ctx, s, "vm0"); err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
	}
	if _, err := CreateSnapshot(ctx, s, "vm0"); !errors.Is(err, ErrTooManySnapshots) {
		t.Fatalf("limit: %v", err)
	}
	// Gen is uint32 arithmetic: the cap is a policy choice, not an
	// overflow boundary (the plan's 8-bit bug), and generation numbers
	// never rewind - the counter outlives deletion of the snapshots
	// that consumed it (ADR: rollback to N must stay unambiguous).
	// Drop the chain and run past 255 to prove the counter itself is
	// fine and strictly monotonic across chain resets.
	for {
		sp, err := GetVolume(ctx, s, "vm0")
		if err != nil {
			t.Fatal(err)
		}
		if len(sp.Snapshots) == 0 {
			break
		}
		if _, err := DeleteSnapshot(ctx, s, "vm0", sp.Snapshots[0].Gen); err != nil {
			t.Fatal(err)
		}
	}
	prev := uint32(MaxSnapshots) // MaxSnapshots snapshots were created above
	for i := 1; i <= 300; i++ {
		sp, err := CreateSnapshot(ctx, s, "vm0")
		if err != nil {
			t.Fatalf("gen %d: %v", i, err)
		}
		if sp.Gen != prev+1 {
			t.Fatalf("gen counter: want %d got %d", prev+1, sp.Gen)
		}
		prev = sp.Gen
		if len(sp.Snapshots) > MaxSnapshots {
			t.Fatal("cap not enforced")
		}
		// Keep the chain under the cap by deleting the oldest.
		if _, err := DeleteSnapshot(ctx, s, "vm0", sp.Snapshots[0].Gen); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSnapshotRollback(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	spec := newSnapVolume(t, s, "vm0", object.Size*8) // 8 objects
	base1 := spec.OIDBase

	// gen1: objects 0,1 allocated.
	bits := allocBits(8)
	setAllocBit(bits, 0)
	setAllocBit(bits, 1)
	if _, err := PutAllocMap(ctx, s, "vm0", AllocMapRecord{Bits: bits}); err != nil {
		t.Fatal(err)
	}
	sp, err := CreateSnapshot(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	base2 := sp.OIDBase

	// Live writes after gen1: object 2 allocated in the new range.
	bits2 := allocBits(8)
	setAllocBit(bits2, 2)
	if _, err := PutAllocMap(ctx, s, "vm0", AllocMapRecord{Bits: bits2, Rev: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateSnapshot(ctx, s, "vm0"); err != nil { // gen2
		t.Fatal(err)
	}
	sp, err = GetVolume(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	base3 := sp.OIDBase
	if len(sp.Snapshots) != 2 {
		t.Fatalf("snaps %d", len(sp.Snapshots))
	}

	// Roll back to gen1: gen2 dropped, live+gen2 ranges are garbage.
	out, garbage, err := RollbackVolume(ctx, s, "vm0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if out.Gen != 1 || len(out.Snapshots) != 0 {
		t.Fatalf("after rollback: gen %d snaps %d", out.Gen, len(out.Snapshots))
	}
	if out.LastGen != 2 {
		t.Fatalf("LastGen after rollback = %d, want 2 (high-water mark preserved)", out.LastGen)
	}
	// A snapshot taken after the rollback is numbered above every
	// generation that ever existed (monotonic ids).
	sp2, err := CreateSnapshot(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	if sp2.Gen != 3 || sp2.LastGen != 3 {
		t.Fatalf("snapshot after rollback: gen %d lastgen %d, want 3/3", sp2.Gen, sp2.LastGen)
	}
	if out.OIDBase != base1 {
		t.Fatalf("live base %d want %d", out.OIDBase, base1)
	}
	want := map[uint64]bool{base2: true, base3: true}
	if len(garbage) != 2 {
		t.Fatalf("garbage %+v", garbage)
	}
	for _, r := range garbage {
		if !want[r.Base] {
			t.Fatalf("unexpected garbage base %d", r.Base)
		}
	}
	// Live bitmap is the gen1 copy: object 2 (post-snapshot write)
	// reads as unallocated -> zeros, objects 0,1 as allocated.
	live, err := GetAllocMap(ctx, s, "vm0", 8)
	if err != nil {
		t.Fatal(err)
	}
	if !allocBit(live.Bits, 0) || !allocBit(live.Bits, 1) || allocBit(live.Bits, 2) {
		t.Fatalf("bitmap after rollback: %v", live.Bits)
	}
	// Rolling back to a dropped generation fails.
	if _, _, err := RollbackVolume(ctx, s, "vm0", 2); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("rollback to dropped gen: %v", err)
	}
}

func TestSnapshotDelete(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "vm0", object.Size)
	if _, err := CreateSnapshot(ctx, s, "vm0"); err != nil {
		t.Fatal(err)
	}
	sp, err := GetVolume(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	genBase := sp.Snapshots[0].OIDBase
	liveBase := sp.OIDBase
	garbage, err := DeleteSnapshot(ctx, s, "vm0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(garbage) != 1 || garbage[0].Base != genBase {
		t.Fatalf("garbage %+v", garbage)
	}
	sp, err = GetVolume(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	if len(sp.Snapshots) != 0 || sp.OIDBase != liveBase {
		t.Fatalf("live addressing disturbed by delete: %+v", sp)
	}
	if _, err := DeleteSnapshot(ctx, s, "vm0", 1); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("double delete: %v", err)
	}
}

func TestSnapshotResizeRefused(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "vm0", object.Size)
	if _, err := CreateSnapshot(ctx, s, "vm0"); err != nil {
		t.Fatal(err)
	}
	if _, err := ResizeVolume(ctx, s, "vm0", object.Size*2); !errors.Is(err, ErrSnapshotsUnsupported) {
		t.Fatalf("resize with snapshots: %v", err)
	}
}

func TestVolumeGarbageCoversAllRanges(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "vm0", object.Size)
	if _, err := CreateSnapshot(ctx, s, "vm0"); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateSnapshot(ctx, s, "vm0"); err != nil {
		t.Fatal(err)
	}
	sp, err := GetVolume(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	g := VolumeGarbage(sp)
	if len(g) != 3 { // live + gen2 + gen1
		t.Fatalf("garbage ranges: %d", len(g))
	}
	seen := map[uint64]int{}
	for _, r := range g {
		seen[r.Base]++
	}
	for base, n := range seen {
		if n != 1 {
			t.Fatalf("base %d listed %d times", base, n)
		}
	}
}

func TestSnapshotParallelCreateSerializes(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "vm0", object.Size)
	// Two racing creators: CAS serialization must yield gens 1 and 2
	// over two distinct frozen ranges (no lost update, no shared
	// frozen range).
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- func() error { _, err := CreateSnapshot(ctx, s, "vm0"); return err }() }()
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	sp, err := GetVolume(ctx, s, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	if len(sp.Snapshots) != 2 {
		t.Fatalf("snaps %d", len(sp.Snapshots))
	}
	if sp.Snapshots[0].Gen == sp.Snapshots[1].Gen {
		t.Fatal("duplicate gens")
	}
	if sp.Snapshots[0].OIDBase == sp.Snapshots[1].OIDBase {
		t.Fatal("two snapshots froze the same range")
	}
}
