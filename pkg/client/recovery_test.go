package client

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/qos"
)

// TestRecoveryDetectsAndRepairs proves plan M6.3 end to end: an object
// missing on one replica is detected by RecoveryCheck, copied back by
// Recover (byte-identical to the live replica), thin objects are left
// alone, and the scan is healthy afterwards.
func TestRecoveryDetectsAndRepairs(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const size = 32 << 20 // 8 objects
	spec, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 2, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	// Write object 0 and let all three replicas settle.
	d0 := make([]byte, object.Size)
	fillQuota(d0, 0xa1, 0)
	if _, err := v.WriteAt(ctx, d0, 0); err != nil {
		t.Fatalf("write obj0: %v", err)
	}
	waitForObj(t, c.dir, spec.OIDBase+1, 3)

	// Simulate divergence: object 0 vanishes from OSD 0 (disk loss /
	// stale replica). The daemon must restart afterwards: its fd cache
	// would otherwise keep writing into the unlinked inode.
	if err := os.Remove(objPath(c.dir, 0, spec.OIDBase+1)); err != nil {
		t.Fatalf("simulate loss: %v", err)
	}
	c.restartOSD(t, 0)

	rep, err := v.RecoveryCheck(ctx)
	if err != nil {
		t.Fatalf("RecoveryCheck: %v", err)
	}
	if len(rep.Items) != 1 {
		t.Fatalf("items = %+v, want exactly 1 repair item", rep.Items)
	}
	it := rep.Items[0]
	if it.Index != 0 || it.OID != spec.OIDBase+1 || it.OSD != 0 || it.Source < 0 || it.Source == 0 {
		t.Fatalf("item = %+v, want index 0 oid %d osd 0 from a live source", it, spec.OIDBase+1)
	}
	if rep.Thin != 7 {
		t.Fatalf("thin = %d, want 7 untouched objects", rep.Thin)
	}
	if rep.Ok != 0 {
		t.Fatalf("ok = %d, want 0", rep.Ok)
	}
	if len(rep.Down) != 0 {
		t.Fatalf("down = %v, want none", rep.Down)
	}

	st, err := v.Recover(ctx, rep, RecoveryOpts{})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if st.Copied != 1 || st.Failed != 0 || st.Skipped != 0 || st.Bytes != object.Size {
		t.Fatalf("stats = %+v, want 1 copy", st)
	}

	// The repaired object must exist on OSD 0 and match the source.
	if !objExists(objPath(c.dir, 0, spec.OIDBase+1)) {
		t.Fatal("repaired object missing on osd 0")
	}
	got, err := os.ReadFile(objPath(c.dir, 0, spec.OIDBase+1))
	if err != nil {
		t.Fatalf("read repaired: %v", err)
	}
	if !bytes.Equal(got, d0) {
		t.Fatalf("repaired bytes differ from source (%d bytes)", len(got))
	}

	// Re-check: healthy, nothing to do.
	rep2, err := v.RecoveryCheck(ctx)
	if err != nil {
		t.Fatalf("RecoveryCheck after repair: %v", err)
	}
	if !rep2.Healthy() || rep2.Ok != 1 || rep2.Thin != 7 {
		t.Fatalf("post-repair report = %+v, want healthy 1 ok + 7 thin", rep2)
	}
	// Data path reads see the same bytes.
	buf := make([]byte, object.Size)
	if n, err := v.ReadAt(ctx, buf, 0); err != nil || n != int(object.Size) {
		t.Fatalf("read back: %d %v", n, err)
	}
	if !bytes.Equal(buf, d0) {
		t.Fatal("volume read differs from written data")
	}
}

// TestRecoverySurvivesDeadOSD proves the check classifies an
// unreachable OSD as Down (not as repair items), and once it is back
// the repair completes.
func TestRecoverySurvivesDeadOSD(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const size = 8 << 20 // 2 objects
	spec, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 2, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()
	d := make([]byte, object.Size)
	fillQuota(d, 0xb2, 0)
	if _, err := v.WriteAt(ctx, d, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitForObj(t, c.dir, spec.OIDBase+1, 3)

	// Kill OSD 1: check must report it Down with zero items.
	c.daems[1].Close()
	rep, err := v.RecoveryCheck(ctx)
	if err != nil {
		t.Fatalf("RecoveryCheck with dead osd: %v", err)
	}
	if !containsInt32(rep.Down, 1) {
		t.Fatalf("down = %v, want osd 1", rep.Down)
	}
	if len(rep.Items) != 0 {
		t.Fatalf("items = %+v, want none (a down OSD is not a divergence)", rep.Items)
	}

	// Bring it back: the object is still there (the disk survived),
	// so the scan is healthy again.
	c.restartOSD(t, 1)
	rep2, err := v.RecoveryCheck(ctx)
	if err != nil {
		t.Fatalf("RecoveryCheck after restart: %v", err)
	}
	if !rep2.Healthy() {
		t.Fatalf("post-restart report = %+v, want healthy", rep2)
	}
}

// TestRecoveryScanListsVolumes covers the cluster-wide "recovery ls".
func TestRecoveryScanListsVolumes(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const size = 8 << 20
	for _, name := range []string{"b", "a"} {
		if _, err := c.cl.CreateVolume(ctx, name, size, 0, 3, 2, 64, "data"); err != nil {
			t.Fatalf("CreateVolume %s: %v", name, err)
		}
	}
	reports, err := c.cl.RecoveryScan(ctx)
	if err != nil {
		t.Fatalf("RecoveryScan: %v", err)
	}
	if len(reports) != 2 || reports[0].Volume != "a" || reports[1].Volume != "b" {
		t.Fatalf("reports = %+v, want a,b sorted", reports)
	}
	for _, r := range reports {
		if !r.Healthy() || r.Objects != 2 || r.Thin != 2 {
			t.Fatalf("report %s = %+v, want healthy all-thin", r.Volume, r)
		}
	}
}

// TestRecoveryPacedByLimiter proves opts.Limit throttles the copier
// (the "below client IO" pacing of plan M6.3): with 1 copy/second the
// pass needs at least ~1s.
func TestRecoveryPacedByLimiter(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const size = 16 << 20 // 4 objects
	spec, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 2, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()
	d := make([]byte, object.Size)
	fillQuota(d, 0xc3, 0)
	for i := 0; i < 4; i++ {
		if _, err := v.WriteAt(ctx, d, uint64(i)*object.Size); err != nil {
			t.Fatalf("write obj %d: %v", i, err)
		}
	}
	for i := uint64(1); i <= 4; i++ {
		waitForObj(t, c.dir, spec.OIDBase+i, 3)
	}
	for i := uint64(1); i <= 4; i++ {
		if err := os.Remove(objPath(c.dir, 2, spec.OIDBase+i)); err != nil {
			t.Fatalf("simulate loss obj %d: %v", i, err)
		}
	}
	c.restartOSD(t, 2)

	rep, err := v.RecoveryCheck(ctx)
	if err != nil || len(rep.Items) != 4 {
		t.Fatalf("check = %+v (%v), want 4 items", rep, err)
	}
	// 1 IOPS / 4 MiB-s on the write side: bucket capacity is
	// 100ms*rate, so the first copy rides the burst and every
	// further 4 MiB object costs ~1s of refill.
	lim := qos.NewPair(0, 0, 1, 4<<20)
	start := time.Now()
	st, err := v.Recover(ctx, rep, RecoveryOpts{Limit: lim})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if st.Copied != 4 {
		t.Fatalf("stats = %+v, want 4 copies", st)
	}
	if d := time.Since(start); d < 2500*time.Millisecond {
		t.Fatalf("paced recover finished in %v, want >= ~2.5s for 4 copies", d)
	}
	rep2, err := v.RecoveryCheck(ctx)
	if err != nil || !rep2.Healthy() {
		t.Fatalf("post-recover check = %+v (%v), want healthy", rep2, err)
	}
}
