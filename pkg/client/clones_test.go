package client

// End-to-end clone tests (plan M3, pkg/meta/clones.go protocol) on a
// real 3-OSD cluster: chain reads through a clone, copy-on-write
// isolation, source deletion with the clone surviving (the refcount
// keeps shared generations out of the vacuum), and the clone's own
// delete finally sweeping the objects.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
)

func TestCloneReadWriteAndSourceDelete(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	if _, err := c.cl.CreateVolume(ctx, "base", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	src, err := c.cl.OpenVolume(ctx, "base")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer src.Close()

	// Base image: block 1 = A, block 3 = B.
	a := make([]byte, object.Size)
	fillQuota(a, 0xa1, 0)
	b := make([]byte, object.Size)
	fillQuota(b, 0xb2, 0)
	if _, err := src.WriteAt(ctx, a, 1<<object.SizeShift); err != nil {
		t.Fatalf("write block1: %v", err)
	}
	if _, err := src.WriteAt(ctx, b, 3<<object.SizeShift); err != nil {
		t.Fatalf("write block3: %v", err)
	}
	if _, err := src.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Post-snapshot source write: block 1 = C (newest).
	cc := make([]byte, object.Size)
	fillQuota(cc, 0xc3, 0)
	if _, err := src.WriteAt(ctx, cc, 1<<object.SizeShift); err != nil {
		t.Fatalf("overwrite block1: %v", err)
	}

	// --- clone ---
	if _, err := c.cl.CloneVolume(ctx, "base", "vm1", 0); err != nil {
		t.Fatalf("CloneVolume: %v", err)
	}
	cl, err := c.cl.OpenVolume(ctx, "vm1")
	if err != nil {
		t.Fatalf("OpenVolume clone: %v", err)
	}
	defer cl.Close()

	// The clone sees the source's newest bytes through the chain.
	if got := snapBlock(t, cl, ctx, 1); !bytes.Equal(got, cc) {
		t.Fatalf("clone block1 != source newest data")
	}
	if got := snapBlock(t, cl, ctx, 3); !bytes.Equal(got, b) {
		t.Fatalf("clone block3 != base image")
	}
	if got := snapBlock(t, cl, ctx, 0); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("clone block0 should be zeros")
	}

	// Copy-on-write isolation: clone write does not touch the source.
	d := make([]byte, object.Size)
	fillQuota(d, 0xd4, 0)
	if _, err := cl.WriteAt(ctx, d, 1<<object.SizeShift); err != nil {
		t.Fatalf("clone write block1: %v", err)
	}
	e := make([]byte, object.Size)
	fillQuota(e, 0xe5, 0)
	if _, err := cl.WriteAt(ctx, e, 2<<object.SizeShift); err != nil {
		t.Fatalf("clone write block2: %v", err)
	}
	if got := snapBlock(t, cl, ctx, 1); !bytes.Equal(got, d) {
		t.Fatalf("clone block1 != own data")
	}
	if got := snapBlock(t, src, ctx, 1); !bytes.Equal(got, cc) {
		t.Fatalf("source block1 damaged by clone write")
	}
	if got := snapBlock(t, src, ctx, 2); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("source block2 should stay zeros")
	}

	// Locate the base-image object for block 3 (oldest gen) and the
	// newest-state gen object for block 1 (source's post-clone chain).
	sp, err := meta.GetVolume(ctx, c.cl.meta, "base")
	if err != nil {
		t.Fatalf("GetVolume base: %v", err)
	}
	if len(sp.Snapshots) != 2 {
		t.Fatalf("source gens after clone = %d, want 2", len(sp.Snapshots))
	}
	baseGen := sp.Snapshots[0] // the explicit snapshot (block3's bytes)
	oid3, ok := baseGen.Lookup(3)
	if !ok {
		t.Fatalf("base gen lookup(3)")
	}
	waitForObj(t, c.dir, uint64(oid3), 3)
	// The gen frozen by the clone protocol carries block 1 = C.
	freezeGen := sp.Snapshots[1]
	oid1, ok := freezeGen.Lookup(1)
	if !ok {
		t.Fatalf("freeze gen lookup(1)")
	}
	waitForObj(t, c.dir, uint64(oid1), 3)

	// --- delete the source: the clone must keep working ---
	if err := c.cl.DeleteVolume(ctx, "base"); err != nil {
		t.Fatalf("DeleteVolume source: %v", err)
	}
	res, err := c.cl.VacuumVolume(ctx, "base")
	if err != nil {
		t.Fatalf("VacuumVolume source: %v", err)
	}
	_ = res
	if recs, err := c.cl.PendingVacuum(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("pending vacuum after source delete = %d (%v), want 0", len(recs), err)
	}
	// Shared generation objects survived the vacuum.
	for id := 0; id < 3; id++ {
		if !objExists(objPath(c.dir, id, uint64(oid3))) {
			t.Fatalf("vacuum erased shared object %d on osd %d", oid3, id)
		}
		if !objExists(objPath(c.dir, id, uint64(oid1))) {
			t.Fatalf("vacuum erased shared object %d on osd %d", oid1, id)
		}
	}
	// Reads through the chain still work (the source record is gone;
	// the clone carries the chain).
	if got := snapBlock(t, cl, ctx, 1); !bytes.Equal(got, d) {
		t.Fatalf("clone block1 after source delete != own data")
	}
	if got := snapBlock(t, cl, ctx, 3); !bytes.Equal(got, b) {
		t.Fatalf("clone block3 after source delete != base image")
	}
	// New clone writes keep working.
	f := make([]byte, object.Size)
	fillQuota(f, 0xf6, 0)
	if _, err := cl.WriteAt(ctx, f, 3<<object.SizeShift); err != nil {
		t.Fatalf("clone overwrite block3: %v", err)
	}
	if got := snapBlock(t, cl, ctx, 3); !bytes.Equal(got, f) {
		t.Fatalf("clone block3 != overwritten data")
	}

	// --- delete the clone: last holder, the objects are swept ---
	if err := c.cl.DeleteVolume(ctx, "vm1"); err != nil {
		t.Fatalf("DeleteVolume clone: %v", err)
	}
	if _, err := c.cl.VacuumVolume(ctx, "vm1"); err != nil {
		t.Fatalf("VacuumVolume clone: %v", err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		gone := true
		for id := 0; id < 3; id++ {
			if objExists(objPath(c.dir, id, uint64(oid3))) || objExists(objPath(c.dir, id, uint64(oid1))) {
				gone = false
			}
		}
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shared objects %d/%d survived clone delete", oid1, oid3)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The genrefs records are gone at zero refs.
	keys, err := c.cl.meta.List(ctx, meta.PrefixGenRefs)
	if err != nil {
		t.Fatalf("list genrefs: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("genrefs left after both volumes deleted: %d", len(keys))
	}
}

func TestCloneDestinationExists(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "base", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.cl.CloneVolume(ctx, "base", "dst", 0); err != nil {
		t.Fatalf("first clone: %v", err)
	}
	if _, err := c.cl.CloneVolume(ctx, "base", "dst", 0); err == nil {
		t.Fatal("second clone onto the same name must fail")
	}
	if _, err := c.cl.CloneVolume(ctx, "base", "base", 0); err == nil {
		t.Fatal("clone onto itself must fail")
	}
}
