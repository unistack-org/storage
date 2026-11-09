package meta

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

func genRefs(t *testing.T, s Store, base uint64) (int, bool) {
	t.Helper()
	b, _, err := s.Get(context.Background(), PrefixGenRefs, genRefKey(base))
	if errors.Is(err, ErrNotFound) {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var rec GenRefRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		t.Fatal(err)
	}
	return rec.Refs, true
}

func TestCloneCopiesChainWithFreshLive(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	src := newSnapVolume(t, s, "base", 1<<30)
	srcBase := src.OIDBase
	if _, err := CreateSnapshot(ctx, s, "base"); err != nil {
		t.Fatal(err)
	}
	frozenBase := srcBase

	dst, err := CloneVolume(ctx, s, "base", "vm1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if dst.Name != "vm1" || dst.Size != src.Size {
		t.Fatalf("dst geometry: %+v", dst)
	}
	if dst.OIDBase == 0 || dst.OIDBase == frozenBase {
		t.Fatalf("clone live range not fresh: %d", dst.OIDBase)
	}
	if len(dst.Snapshots) != 2 {
		t.Fatalf("chain: %+v", dst.Snapshots)
	}
	if dst.Snapshots[0].OIDBase != frozenBase {
		t.Fatalf("clone carries the source's frozen range: %+v", dst.Snapshots[0])
	}
	// The clone's chain also carries the freeze of the source's live
	// state the clone protocol took (gen2).
	if dst.Snapshots[1].OIDBase == frozenBase || dst.Snapshots[1].OIDBase == dst.OIDBase {
		t.Fatalf("freeze gen wrong: %+v", dst.Snapshots[1])
	}
	// Live lookups address the clone's own range.
	if oid, ok := dst.Lookup(0); !ok || uint64(oid) != dst.OIDBase+1 {
		t.Fatalf("clone live lookup: %d %v", oid, ok)
	}
	// Both holders are counted on the shared range.
	if refs, ok := genRefs(t, s, frozenBase); !ok || refs != 2 {
		t.Fatalf("genrefs after clone: %d %v", refs, ok)
	}
	// Quota 0 inherits the source's quota (0 here).
	if dst.Quota != src.Quota {
		t.Fatalf("quota inherit: %d != %d", dst.Quota, src.Quota)
	}
}

func TestCloneDestinationExists(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "base", 1<<20)
	newSnapVolume(t, s, "vm1", 1<<20)
	if _, err := CloneVolume(ctx, s, "base", "vm1", 0); !errors.Is(err, ErrVolumeExists) {
		t.Fatalf("want ErrVolumeExists, got %v", err)
	}
}

func TestShareDropCounting(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	r := []GenRange{{Base: 1000, Count: 10}}

	// Absent -> 2 (owner + clone).
	if err := ShareGenRanges(ctx, s, r); err != nil {
		t.Fatal(err)
	}
	if refs, ok := genRefs(t, s, 1000); !ok || refs != 2 {
		t.Fatalf("after share: %d %v", refs, ok)
	}
	// Third holder.
	if err := ShareGenRanges(ctx, s, r); err != nil {
		t.Fatal(err)
	}
	if refs, _ := genRefs(t, s, 1000); refs != 3 {
		t.Fatalf("after second share: %d", refs)
	}
	// Drops: first two keep the record, the last one removes it and
	// claims the vacuum.
	for i := 0; i < 2; i++ {
		vac, err := DropGenRanges(ctx, s, r)
		if err != nil {
			t.Fatal(err)
		}
		if len(vac) != 0 {
			t.Fatalf("drop %d vacuumed a referenced range: %+v", i, vac)
		}
	}
	vac, err := DropGenRanges(ctx, s, r)
	if err != nil {
		t.Fatal(err)
	}
	if len(vac) != 1 || vac[0].Base != 1000 {
		t.Fatalf("last drop should claim vacuum: %+v", vac)
	}
	if _, ok := genRefs(t, s, 1000); ok {
		t.Fatal("record should be gone at zero refs")
	}
	// Never-shared range is plain vacuum.
	vac, err = DropGenRanges(ctx, s, []GenRange{{Base: 9999, Count: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if len(vac) != 1 || vac[0].Base != 9999 {
		t.Fatalf("absent record must vacuum: %+v", vac)
	}
}

func TestDeleteSnapshotKeepsClonedGen(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "base", 1<<20)
	if _, err := CreateSnapshot(ctx, s, "base"); err != nil {
		t.Fatal(err)
	}
	src, err := GetVolume(ctx, s, "base")
	if err != nil {
		t.Fatal(err)
	}
	frozenBase := src.Snapshots[0].OIDBase

	if _, err := CloneVolume(ctx, s, "base", "vm1", 0); err != nil {
		t.Fatal(err)
	}
	// Dropping the snapshot from the source must not release the
	// range the clone reads.
	vac, err := DeleteSnapshot(ctx, s, "base", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(vac) != 0 {
		t.Fatalf("delete snapshot vacuumed a cloned gen: %+v", vac)
	}
	if refs, ok := genRefs(t, s, frozenBase); !ok || refs != 1 {
		t.Fatalf("genrefs after src drop: %d %v", refs, ok)
	}
	// The clone still carries both generations (the protocol's freeze
	// of the source's live state plus the explicit gen1).
	dst, err := GetVolume(ctx, s, "vm1")
	if err != nil {
		t.Fatal(err)
	}
	if len(dst.Snapshots) != 2 {
		t.Fatalf("clone chain damaged: %+v", dst.Snapshots)
	}
	found := false
	for _, g := range dst.Snapshots {
		if g.OIDBase == frozenBase {
			found = true
		}
	}
	if !found {
		t.Fatalf("clone lost the shared gen: %+v", dst.Snapshots)
	}
}

func TestDeleteSourceKeepsCloneReadableRanges(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	src := newSnapVolume(t, s, "base", 1<<20)
	if _, err := CreateSnapshot(ctx, s, "base"); err != nil {
		t.Fatal(err)
	}
	src, err := GetVolume(ctx, s, "base")
	if err != nil {
		t.Fatal(err)
	}
	frozenBase := src.Snapshots[0].OIDBase
	srcLive := src.OIDBase // frozen by the clone protocol

	if _, err := CloneVolume(ctx, s, "base", "vm1", 0); err != nil {
		t.Fatal(err)
	}
	// Deleting the source: its whole garbage drops refs; every shared
	// range must stay out of the source's vacuum list.
	garbage := VolumeGarbage(src)
	vac, err := DropGenRanges(ctx, s, garbage)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vac {
		if v.Base == frozenBase || v.Base == srcLive {
			t.Fatalf("source delete vacuumed a cloned range: %+v", vac)
		}
	}
	// The clone's own chain is fully intact.
	dst, err := GetVolume(ctx, s, "vm1")
	if err != nil {
		t.Fatal(err)
	}
	if len(dst.Snapshots) != 2 {
		t.Fatalf("clone chain: %+v", dst.Snapshots)
	}
	for _, v := range vac {
		if _, ok := genRefs(t, s, v.Base); ok {
			t.Fatalf("vacuumed range %d still has a genrefs record", v.Base)
		}
	}
}

func TestRollbackOfClonedSourceKeepsSharedGens(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "base", 1<<20)
	if _, err := CreateSnapshot(ctx, s, "base"); err != nil { // gen1
		t.Fatal(err)
	}
	if _, err := CreateSnapshot(ctx, s, "base"); err != nil { // gen2
		t.Fatal(err)
	}
	src, err := GetVolume(ctx, s, "base")
	if err != nil {
		t.Fatal(err)
	}
	gen1Base := src.Snapshots[0].OIDBase
	gen2Base := src.Snapshots[1].OIDBase

	if _, err := CloneVolume(ctx, s, "base", "vm1", 0); err != nil {
		t.Fatal(err)
	}
	// Clone chain now carries gen1, gen2 and the freeze of the live
	// state (gen3 on the source).
	// Roll the source back to gen1: gen2, gen3 and the old live range
	// are released. gen2/gen3 are cloned (kept); the live range the
	// source was using at rollback time is private to the source
	// (vacuumed).
	srcAfter, err := GetVolume(ctx, s, "base")
	if err != nil {
		t.Fatal(err)
	}
	liveAtRollback := srcAfter.OIDBase
	_, garbage, err := RollbackVolume(ctx, s, "base", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(garbage) != 1 || garbage[0].Base != liveAtRollback {
		t.Fatalf("rollback garbage: want only the private live range %d, got %+v", liveAtRollback, garbage)
	}
	// gen1's record keeps both holders (source-as-live + clone-as-frozen).
	if refs, ok := genRefs(t, s, gen1Base); !ok || refs != 2 {
		t.Fatalf("gen1 refs after rollback: %d %v", refs, ok)
	}
	// gen2/gen3 drop to clone-only.
	for _, base := range []uint64{gen2Base} {
		if refs, ok := genRefs(t, s, base); !ok || refs != 1 {
			t.Fatalf("refs after rollback at %d: %d %v", base, refs, ok)
		}
	}
	// Deleting the rolled-back source must not touch any cloned range.
	src2, err := GetVolume(ctx, s, "base")
	if err != nil {
		t.Fatal(err)
	}
	vac, err := DropGenRanges(ctx, s, VolumeGarbage(src2))
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vac {
		if v.Base == gen1Base || v.Base == gen2Base {
			t.Fatalf("source delete vacuumed cloned range %d (full vac: %+v)", v.Base, vac)
		}
	}
}

func TestCloneQuotaAndDepthLimits(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "base", 1<<20)
	if _, err := CloneVolume(ctx, s, "base", "vm1", 4<<20); err == nil {
		t.Fatal("quota above size must be refused")
	}
	if _, err := CloneVolume(ctx, s, "base", "vm1", 512<<10); err != nil {
		t.Fatal(err)
	}
	dst, err := GetVolume(ctx, s, "vm1")
	if err != nil {
		t.Fatal(err)
	}
	if dst.Quota != 512<<10 {
		t.Fatalf("explicit quota not stored: %d", dst.Quota)
	}
}

func TestCloneChainDepthBound(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	newSnapVolume(t, s, "base", object.Size)
	for i := 0; i < MaxSnapshots; i++ {
		if _, err := CreateSnapshot(ctx, s, "base"); err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
	}
	if _, err := CloneVolume(ctx, s, "base", "vm1", 0); !errors.Is(err, ErrTooManySnapshots) {
		t.Fatalf("want ErrTooManySnapshots, got %v", err)
	}
}
