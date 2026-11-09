package client

// End-to-end snapshot tests (plan M3, todos/adr-snapshots.md): the
// meta freeze + the client chain-read / TRIM-materialization hooks on
// a real 3-OSD cluster.

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
)

// snapBlock reads one 128K object at index and returns it.
func snapBlock(tb testing.TB, v *Volume, ctx context.Context, index uint64) []byte {
	tb.Helper()
	p := make([]byte, object.Size)
	n, err := v.ReadAt(ctx, p, index<<object.SizeShift)
	if err != nil {
		tb.Fatalf("read obj %d: %v", index, err)
	}
	if uint64(n) != object.Size {
		tb.Fatalf("read obj %d = %d bytes, want %d", index, n, object.Size)
	}
	return p
}

// TestSnapshotChainRead proves the core chain behaviour: a block last
// written before a snapshot survives snapshots and new writes to the
// fresh generation.
func TestSnapshotChainRead(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	// Gen 0 content: block 1 = A, block 3 = B.
	a := make([]byte, object.Size)
	fillQuota(a, 0xa1, 0)
	b := make([]byte, object.Size)
	fillQuota(b, 0xb2, 0)
	if _, err := v.WriteAt(ctx, a, 1<<object.SizeShift); err != nil {
		t.Fatalf("write block1: %v", err)
	}
	if _, err := v.WriteAt(ctx, b, 3<<object.SizeShift); err != nil {
		t.Fatalf("write block3: %v", err)
	}

	info, err := v.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if info.Gen != 1 {
		t.Fatalf("snapshot gen = %d, want 1", info.Gen)
	}
	if info.Objects != 2 {
		t.Fatalf("snapshot objects = %d, want 2", info.Objects)
	}

	// Overwrite block 1 in the new generation; add block 2.
	a2 := make([]byte, object.Size)
	fillQuota(a2, 0xc3, 0)
	if _, err := v.WriteAt(ctx, a2, 1<<object.SizeShift); err != nil {
		t.Fatalf("overwrite block1: %v", err)
	}
	c2 := make([]byte, object.Size)
	fillQuota(c2, 0xd4, 0)
	if _, err := v.WriteAt(ctx, c2, 2<<object.SizeShift); err != nil {
		t.Fatalf("write block2: %v", err)
	}

	// Live reads see the newest bytes; block 3 (untouched since the
	// snapshot) must be served through the chain.
	if got := snapBlock(t, v, ctx, 1); !bytes.Equal(got, a2) {
		t.Fatalf("live block1 != newest data")
	}
	if got := snapBlock(t, v, ctx, 3); !bytes.Equal(got, b) {
		t.Fatalf("block3 through chain != snapshot data")
	}
	if got := snapBlock(t, v, ctx, 0); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("block0 should be zeros")
	}

	// A second snapshot: chain is two deep. Block 1 is in gen 1 and
	// gen 2 (newest wins).
	info2, err := v.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if info2.Gen != 2 {
		t.Fatalf("snapshot 2 gen = %d, want 2", info2.Gen)
	}
	if got := snapBlock(t, v, ctx, 3); !bytes.Equal(got, b) {
		t.Fatalf("block3 through 2-deep chain != snapshot data")
	}
	if got := snapBlock(t, v, ctx, 1); !bytes.Equal(got, a2) {
		t.Fatalf("block1 through chain != gen-1 data")
	}

	list, err := v.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots: %v", err)
	}
	if len(list) != 2 || list[0].Gen != 1 || list[1].Gen != 2 {
		t.Fatalf("Snapshots = %+v, want gens 1,2", list)
	}
}

// TestSnapshotRollback proves rollback restores old bytes, discards
// the future, and the vacuum sweeps the discarded generation's
// objects off the OSDs.
func TestSnapshotRollback(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	a := make([]byte, object.Size)
	fillQuota(a, 0x11, 0)
	if _, err := v.WriteAt(ctx, a, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := v.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Post-snapshot writes (the "future" to be discarded).
	z := make([]byte, object.Size)
	fillQuota(z, 0x22, 0)
	if _, err := v.WriteAt(ctx, z, 0); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	future := make([]byte, object.Size)
	fillQuota(future, 0x33, 0)
	if _, err := v.WriteAt(ctx, future, 2<<object.SizeShift); err != nil {
		t.Fatalf("write future: %v", err)
	}
	// Snapshot the "future" so rollback discards a generation whose
	// objects physically exist.
	if _, err := v.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	sp, err := meta.GetVolume(ctx, c.cl.meta, "vm")
	if err != nil {
		t.Fatalf("GetVolume: %v", err)
	}
	if len(sp.Snapshots) != 2 {
		t.Fatalf("specs = %d gens, want 2", len(sp.Snapshots))
	}
	g2 := sp.Snapshots[1]
	futureOID, ok := g2.Lookup(2)
	if !ok {
		t.Fatalf("gen 2 lookup index 2 failed")
	}
	waitForObj(t, c.dir, uint64(futureOID), 3)

	if _, err := v.Rollback(ctx, 1); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := snapBlock(t, v, ctx, 0); !bytes.Equal(got, a) {
		t.Fatalf("after rollback block0 != pre-snapshot data")
	}
	if got := snapBlock(t, v, ctx, 2); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("future block must read as zeros after rollback")
	}
	list, err := v.Snapshots(ctx)
	if err != nil {
		t.Fatalf("Snapshots: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("snapshots after rollback = %+v, want none (gen 1 became live)", list)
	}
	// The discarded generation's objects must be swept off every OSD.
	deadline := time.Now().Add(15 * time.Second)
	for {
		gone := true
		for id := 0; id < 3; id++ {
			if objExists(objPath(c.dir, id, uint64(futureOID))) {
				gone = false
			}
		}
		if gone {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("discarded gen object %d survived vacuum", futureOID)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if recs, err := c.cl.PendingVacuum(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("pending vacuum after rollback = %d (%v), want 0", len(recs), err)
	}

	// Writes after rollback address the restored generation and read
	// back correctly.
	if _, err := v.WriteAt(ctx, future, 2<<object.SizeShift); err != nil {
		t.Fatalf("rewrite after rollback: %v", err)
	}
	if got := snapBlock(t, v, ctx, 2); !bytes.Equal(got, future) {
		t.Fatalf("block2 after rollback rewrite != written data")
	}
}

// TestSnapshotTrimMaterialization proves the ADR rule: after a
// snapshot, a sparse TRIM stores literal zeros - the read returns
// zeros and NOT the snapshot's bytes (no resurrection through a hole).
func TestSnapshotTrimMaterialization(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	a := make([]byte, object.Size)
	fillQuota(a, 0x77, 0)
	if _, err := v.WriteAt(ctx, a, 1<<object.SizeShift); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := v.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// Sparse TRIM of the whole block: must materialize zeros in the
	// live generation, not punch, not fall through to the snapshot.
	if err := v.WriteZeroes(ctx, 1<<object.SizeShift, object.Size, true); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	if got := snapBlock(t, v, ctx, 1); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("TRIM resurrected snapshot data through the chain")
	}
	// The materialized object is allocated: usage must count it.
	u, err := v.Usage(ctx)
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if u < object.Size {
		t.Fatalf("usage after materialized trim = %d, want >= %d", u, object.Size)
	}
	// And the next snapshot must contain the zeros, not the old bytes.
	info, err := v.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if info.Objects != 1 {
		t.Fatalf("snapshot 2 objects = %d, want 1 (the trimmed block)", info.Objects)
	}
	if _, err := v.Rollback(ctx, info.Gen); err != nil {
		t.Fatalf("Rollback to zeroed snapshot: %v", err)
	}
	if got := snapBlock(t, v, ctx, 1); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("snapshot taken after TRIM contains non-zero data")
	}
}

// TestSnapshotFreeTrimPunchRegression: volumes without snapshots keep
// the sparse punch + quota release behaviour untouched.
func TestSnapshotFreeTrimPunchRegression(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	spec, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	a := make([]byte, object.Size)
	fillQuota(a, 0x55, 0)
	if _, err := v.WriteAt(ctx, a, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitForObj(t, c.dir, spec.OIDBase+1, 3)
	if err := v.WriteZeroes(ctx, 0, object.Size, true); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	// Punch (FALLOC_FL_PUNCH_HOLE | KEEP_SIZE, F_PUNCHHOLE on APFS)
	// keeps the file and its apparent size: the honest check is that
	// allocated bytes drop to zero (st_blocks).
	deadline := time.Now().Add(10 * time.Second)
	for {
		punched := true
		for id := 0; id < 3; id++ {
			fi, err := os.Stat(objPath(c.dir, id, spec.OIDBase+1))
			if err != nil {
				punched = false
				break
			}
			if fi.Size() != int64(object.Size) {
				t.Fatalf("punched obj on osd %d: size %d, want %d (KEEP_SIZE punch)", id, fi.Size(), object.Size)
			}
			if punchSupported && testAllocated(fi) != 0 {
				punched = false
			}
		}
		if punched || !punchSupported {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sparse trim did not punch obj on snapshot-free volume")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := snapBlock(t, v, ctx, 0); !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatalf("punched block reads non-zero")
	}
	u, err := v.Usage(ctx)
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if u != 0 {
		t.Fatalf("usage after sparse trim = %d, want 0", u)
	}
}

// TestDeleteVolumeIncludesSnapshotRanges proves the tombstone written
// by DeleteVolume covers every generation, and the vacuum removes
// snapshot objects from disk.
func TestDeleteVolumeIncludesSnapshotRanges(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}

	a := make([]byte, object.Size)
	fillQuota(a, 0x99, 0)
	if _, err := v.WriteAt(ctx, a, 0); err != nil {
		t.Fatalf("write gen0: %v", err)
	}
	if _, err := v.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sp, err := meta.GetVolume(ctx, c.cl.meta, "vm")
	if err != nil {
		t.Fatalf("GetVolume: %v", err)
	}
	if len(sp.Snapshots) != 1 {
		t.Fatalf("want 1 snapshot, got %d", len(sp.Snapshots))
	}
	snapOID, ok := sp.Snapshots[0].Lookup(0)
	if !ok {
		t.Fatalf("snapshot lookup failed")
	}
	b := make([]byte, object.Size)
	fillQuota(b, 0x88, 0)
	if _, err := v.WriteAt(ctx, b, 2<<object.SizeShift); err != nil {
		t.Fatalf("write live: %v", err)
	}
	waitForObj(t, c.dir, uint64(snapOID), 3)
	waitForObj(t, c.dir, sp.OIDBase+3, 3)
	v.Close()

	if err := c.cl.DeleteVolume(ctx, "vm"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	recs, err := c.cl.PendingVacuum(ctx)
	if err != nil || len(recs) != 1 {
		t.Fatalf("PendingVacuum = %d (%v), want 1", len(recs), err)
	}
	rec := recs[0]
	if rec.OIDBase != sp.OIDBase || rec.NumObjs != object.NumObjects(size) {
		t.Fatalf("tombstone base = %d/%d, want %d/%d", rec.OIDBase, rec.NumObjs, sp.OIDBase, object.NumObjects(size))
	}
	if len(rec.Extents) != 1 || rec.Extents[0].Base != sp.Snapshots[0].OIDBase {
		t.Fatalf("tombstone extents = %+v, want snapshot range base %d", rec.Extents, sp.Snapshots[0].OIDBase)
	}
	if _, err := c.cl.VacuumDeletedVolumes(ctx); err != nil {
		t.Fatalf("VacuumDeletedVolumes: %v", err)
	}
	for _, oid := range []uint64{uint64(snapOID), sp.OIDBase + 3} {
		for id := 0; id < 3; id++ {
			if objExists(objPath(c.dir, id, oid)) {
				t.Fatalf("oid %d survived delete+vacuum on osd %d", oid, id)
			}
		}
	}
}

// TestSnapshotRollbackThenSnapshot proves generation numbering stays
// monotonic through a rollback: after rollback to gen 1 (the chain
// emptied), the next snapshot must be numbered above the discarded
// gen 2 (generation numbers are never reused).
func TestSnapshotRollbackThenSnapshot(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	a := make([]byte, object.Size)
	fillQuota(a, 0x01, 0)
	if _, err := v.WriteAt(ctx, a, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := v.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot 1: %v", err)
	}
	if _, err := v.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot 2: %v", err)
	}
	if _, err := v.Rollback(ctx, 1); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	info, err := v.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot 3: %v", err)
	}
	if info.Gen <= 2 {
		t.Fatalf("gen after rollback = %d, want > 2 (monotonic)", info.Gen)
	}
}
