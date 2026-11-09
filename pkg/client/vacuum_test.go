package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
)

// objPath is the on-disk object file of an OSD in the test cluster.
func objPath(dir string, osd int, oid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("osd%d", osd), fmt.Sprintf("%016x", oid))
}

func objExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// waitForObj polls until oid is present on all OSDs (background
// straggler replication is asynchronous: the write acknowledges on a
// quorum, so the third replica lands a moment later).
func waitForObj(tb testing.TB, dir string, oid uint64, n int) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		all := true
		for id := 0; id < n; id++ {
			if !objExists(objPath(dir, id, oid)) {
				all = false
			}
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("precondition: obj %d missing on some osd in %s", oid, dir)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestVacuumDeletedVolumes proves plan M1.4: DeleteVolume leaves
// a tombstone with the volume's OID range, VacuumDeletedVolumes removes
// the whole range from every OSD, drops the tombstone, and is
// idempotent (a second pass finds nothing).
func TestVacuumDeletedVolumes(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20 // 4 objects
	spec, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, object.Size)
	fillQuota(data, 0x5a, 0)
	// Touch the first and last objects; middle objects stay thin.
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write obj0: %v", err)
	}
	if _, err := v.WriteAt(ctx, data, size-object.Size); err != nil {
		t.Fatalf("write obj3: %v", err)
	}
	v.Close()

	base := spec.OIDBase
	nobj := object.NumObjects(size)
	written := []uint64{base + 1, base + nobj}
	for _, oid := range written {
		waitForObj(t, c.dir, oid, 3)
	}

	if err := c.cl.DeleteVolume(ctx, "vm"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	recs, err := c.cl.PendingVacuum(ctx)
	if err != nil || len(recs) != 1 {
		t.Fatalf("PendingVacuum = %d (%v), want 1", len(recs), err)
	}
	if recs[0].Name != "vm" || recs[0].OIDBase != base || recs[0].NumObjs != nobj {
		t.Fatalf("tombstone = %+v, want vm base %d objs %d", recs[0], base, nobj)
	}

	res, err := c.cl.VacuumDeletedVolumes(ctx)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if res.Vacuums != 1 || res.Deleted != 1 {
		t.Fatalf("vacuum result = %+v, want 1 tombstone 1 deleted", res)
	}
	if len(res.Errs) != 0 {
		t.Fatalf("vacuum errors: %v", res.Errs)
	}

	// Every OID of the range must be gone from every OSD (including
	// thin ones: the vacuum is range-wide, not bitmap-driven).
	for i := uint64(1); i <= nobj; i++ {
		for id := 0; id < 3; id++ {
			if objExists(objPath(c.dir, id, base+i)) {
				t.Fatalf("obj %d survived on osd %d", base+i, id)
			}
		}
	}
	for _, oid := range written {
		for id := 0; id < 3; id++ {
			p := objPath(c.dir, id, oid)
			if _, err := os.Stat(p); !os.IsNotExist(err) {
				t.Fatalf("obj %d osd %d: stat = %v, want not-exist", oid, id, err)
			}
		}
	}

	if recs, err := c.cl.PendingVacuum(ctx); err != nil || len(recs) != 0 {
		t.Fatalf("tombstones after vacuum = %d (%v), want 0", len(recs), err)
	}

	// Idempotent replay: nothing pending, nothing to do, no errors.
	res2, err := c.cl.VacuumDeletedVolumes(ctx)
	if err != nil {
		t.Fatalf("second vacuum: %v", err)
	}
	if res2.Vacuums != 0 || res2.Deleted != 0 {
		t.Fatalf("second vacuum = %+v, want no-op", res2)
	}
}

// TestVacuumSurvivesUnreachableOSD proves a tombstone is kept while
// any OSD is down (its garbage will be vacuumed on the next pass).
func TestVacuumSurvivesUnreachableOSD(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 8 << 20
	spec, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, object.Size)
	fillQuota(data, 0x77, 0)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	v.Close()
	waitForObj(t, c.dir, spec.OIDBase+1, 3)
	if err := c.cl.DeleteVolume(ctx, "vm"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	// Kill OSD 2 (the vacuum must fail on it and keep the tombstone).
	// The daemon object survives Close (Addr keeps reporting the last
	// listen address), which is what restartOSD needs.
	c.daems[2].Close()

	res, err := c.cl.VacuumDeletedVolumes(ctx)
	if res == nil {
		t.Fatalf("vacuum with dead OSD: nil result: %v", err)
	}
	if res.Deleted != 0 {
		t.Fatalf("tombstone consumed while an OSD was down: %+v", res)
	}
	if recs, err := c.cl.PendingVacuum(ctx); err != nil || len(recs) != 1 {
		t.Fatalf("pending after failed vacuum = %d (%v), want 1", len(recs), err)
	}

	// Restart it and vacuum again: everything completes.
	c.restartOSD(t, 2)
	res, err = c.cl.VacuumDeletedVolumes(ctx)
	if err != nil {
		t.Fatalf("vacuum after restart: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("vacuum after restart = %+v, want 1 deleted", res)
	}
	for i := uint64(1); i <= object.NumObjects(size); i++ {
		if objExists(objPath(c.dir, 2, spec.OIDBase+i)) {
			t.Fatalf("obj %d survived on restarted osd 2", spec.OIDBase+i)
		}
	}
}

// TestVacuumNameReuse proves a volume recreated under the same name
// is not touched by the pending vacuum of its deleted predecessor: the
// new incarnation addresses a disjoint OID range.
func TestVacuumNameReuse(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 8 << 20
	old, err := c.cl.CreateVolume(ctx, "tpl", size, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "tpl")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, object.Size)
	fillQuota(data, 0x01, 0)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write old: %v", err)
	}
	v.Close()
	waitForObj(t, c.dir, old.OIDBase+1, 3)
	if err := c.cl.DeleteVolume(ctx, "tpl"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}

	fresh, err := c.cl.CreateVolume(ctx, "tpl", size, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if fresh.OIDBase == old.OIDBase {
		t.Fatalf("OID range reused: base %d", fresh.OIDBase)
	}
	v2, err := c.cl.OpenVolume(ctx, "tpl")
	if err != nil {
		t.Fatalf("OpenVolume new: %v", err)
	}
	fillQuota(data, 0x02, 0)
	if _, err := v2.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write new: %v", err)
	}
	v2.Close()

	res, err := c.cl.VacuumDeletedVolumes(ctx)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("vacuum = %+v, want old tombstone vacuumed", res)
	}
	for i := uint64(1); i <= object.NumObjects(size); i++ {
		if objExists(objPath(c.dir, 0, old.OIDBase+i)) {
			t.Fatalf("old obj %d survived the vacuum", old.OIDBase+i)
		}
	}
	// The new volume's first object must be intact.
	v3, err := c.cl.OpenVolume(ctx, "tpl")
	if err != nil {
		t.Fatalf("reopen new: %v", err)
	}
	defer v3.Close()
	got := make([]byte, object.Size)
	if n, err := v3.ReadAt(ctx, got, 0); err != nil || n != int(object.Size) {
		t.Fatalf("read new: %d %v", n, err)
	}
	if got[0] != 0x02 {
		t.Fatalf("new volume data corrupted by vacuum: first byte %#x", got[0])
	}
}

// TestVacuumTombstoneHelpers covers the meta plumbing directly:
// Put/List/Delete round-trip.
func TestVacuumTombstoneHelpers(t *testing.T) {
	dir := t.TempDir()
	m, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	ctx := context.Background()
	if _, err := meta.PutVacuum(ctx, m, meta.VacuumRecord{Name: "a", OIDBase: 10, NumObjs: 4}); err != nil {
		t.Fatalf("PutVacuum: %v", err)
	}
	if _, err := meta.PutVacuum(ctx, m, meta.VacuumRecord{Name: "b", OIDBase: 20, NumObjs: 8}); err != nil {
		t.Fatalf("PutVacuum b: %v", err)
	}
	recs, err := meta.ListVacuum(ctx, m)
	if err != nil || len(recs) != 2 {
		t.Fatalf("ListVacuum = %d (%v), want 2", len(recs), err)
	}
	if recs[0].Name != "a" || recs[0].OIDBase != 10 || recs[0].NumObjs != 4 {
		t.Fatalf("rec a = %+v", recs[0])
	}
	if err := meta.DeleteVacuum(ctx, m, "a"); err != nil {
		t.Fatalf("DeleteVacuum: %v", err)
	}
	if _, err := meta.GetVacuum(ctx, m, "a"); err != meta.ErrNotFound {
		t.Fatalf("GetVacuum(a) = %v, want ErrNotFound", err)
	}
	if err := meta.DeleteVacuum(ctx, m, "ghost"); err != nil {
		t.Fatalf("DeleteVacuum of a missing key: %v", err)
	}
	recs, err = meta.ListVacuum(ctx, m)
	if err != nil || len(recs) != 1 || recs[0].Name != "b" {
		t.Fatalf("ListVacuum after delete = %+v (%v)", recs, err)
	}
}

// TestVacuumVolumeTargeted proves the pinpoint form of tombstone
// vacuum: one deleted volume's garbage is removed while a second
// deleted volume keeps both its objects and its tombstone.
func TestVacuumVolumeTargeted(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 8 << 20 // 2 objects
	data := make([]byte, object.Size)
	fillQuota(data, 0x5a, 0)

	base := map[string]uint64{}
	for _, name := range []string{"a", "b"} {
		spec, err := c.cl.CreateVolume(ctx, name, size, 0, 3, 0, 64, "data")
		if err != nil {
			t.Fatalf("CreateVolume %s: %v", name, err)
		}
		v, err := c.cl.OpenVolume(ctx, name)
		if err != nil {
			t.Fatalf("OpenVolume %s: %v", name, err)
		}
		if _, err := v.WriteAt(ctx, data, 0); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		v.Close()
		base[name] = spec.OIDBase
		waitForObj(t, c.dir, spec.OIDBase+1, 3)
	}
	for _, name := range []string{"a", "b"} {
		if err := c.cl.DeleteVolume(ctx, name); err != nil {
			t.Fatalf("DeleteVolume %s: %v", name, err)
		}
	}

	res, err := c.cl.VacuumVolume(ctx, "a")
	if err != nil {
		t.Fatalf("VacuumVolume a: %v", err)
	}
	if res.Vacuums != 1 || res.Deleted != 1 || len(res.Errs) != 0 {
		t.Fatalf("VacuumVolume a = %+v, want 1/1/no errors", res)
	}
	for id := 0; id < 3; id++ {
		if objExists(objPath(c.dir, id, base["a"]+1)) {
			t.Fatalf("a's object survived on osd %d", id)
		}
		if !objExists(objPath(c.dir, id, base["b"]+1)) {
			t.Fatalf("b's object removed by a targeted vacuum of a (osd %d)", id)
		}
	}
	recs, err := c.cl.PendingVacuum(ctx)
	if err != nil || len(recs) != 1 || recs[0].Name != "b" {
		t.Fatalf("pending after targeted vacuum = %+v (%v), want only b", recs, err)
	}

	// Unknown names and live volumes have nothing to vacuum.
	if _, err := c.cl.VacuumVolume(ctx, "ghost"); !errors.Is(err, ErrNoVacuum) {
		t.Fatalf("VacuumVolume ghost = %v, want ErrNoVacuum", err)
	}
	spec, err := c.cl.CreateVolume(ctx, "live", size, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume live: %v", err)
	}
	_ = spec
	if _, err := c.cl.VacuumVolume(ctx, "live"); !errors.Is(err, ErrNoVacuum) {
		t.Fatalf("VacuumVolume live = %v, want ErrNoVacuum", err)
	}
}

// TestVolumeVacuumLive proves the operator-side fstrim: objects that
// are entirely zeroes are punched out on every replica (the file
// stays, fully sparse), objects with real data are left untouched,
// and a second pass is a no-op.
func TestVolumeVacuumLive(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 16 << 20 // 4 objects
	if _, err := c.cl.CreateVolume(ctx, "vm", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	data := make([]byte, object.Size)
	fillQuota(data, 0x5a, 0)
	if _, err := v.WriteAt(ctx, data, 0); err != nil { // obj0: real data
		t.Fatalf("write obj0: %v", err)
	}
	if _, err := v.WriteAt(ctx, make([]byte, object.Size), object.Size); err != nil { // obj1: literal zeroes
		t.Fatalf("write obj1: %v", err)
	}
	// obj2, obj3 stay thin.
	o0, ok := v.oid(0)
	if !ok {
		t.Fatal("oid(0) outside ranges")
	}
	o1, ok := v.oid(1)
	if !ok {
		t.Fatal("oid(1) outside ranges")
	}
	oid0 := uint64(o0)
	waitForObj(t, c.dir, oid0, 3)
	oid1 := uint64(o1)
	waitForObj(t, c.dir, oid1, 3)

	res, err := v.Vacuum(ctx)
	if err != nil {
		t.Fatalf("live vacuum: %v", err)
	}
	if res.Checked != 2 || res.Zeroed != 1 || res.Freed != object.Size {
		t.Fatalf("live vacuum = %+v, want checked 2 zeroed 1 freed %d", res, object.Size)
	}

	// The punched object keeps its file on every replica (a punch
	// preserves the apparent size), the data object is untouched.
	// Where punching is real (Linux), the punched file's allocated
	// bytes drop to 0; the honest metric is st_blocks, not the size.
	for id := 0; id < 3; id++ {
		fi, err := os.Stat(objPath(c.dir, id, oid1))
		if err != nil {
			t.Fatalf("punched obj on osd %d: %v", id, err)
		}
		if fi.Size() != int64(object.Size) {
			t.Fatalf("punched obj on osd %d: size %d, want %d (KEEP_SIZE punch)", id, fi.Size(), object.Size)
		}
		if punchSupported {
			if a := testAllocated(fi); a != 0 {
				t.Fatalf("punched obj on osd %d: allocated %d, want 0 (hole punched)", id, a)
			}
		}
		fi, err = os.Stat(objPath(c.dir, id, oid0))
		if err != nil {
			t.Fatalf("data obj on osd %d: %v", id, err)
		}
		if fi.Size() != int64(object.Size) {
			t.Fatalf("data obj on osd %d: size %d, want %d", id, fi.Size(), object.Size)
		}
		if punchSupported {
			if a := testAllocated(fi); a == 0 {
				t.Fatalf("data obj on osd %d: allocated 0, want >0 (not punched)", id)
			}
		}
	}

	got := make([]byte, object.Size)
	if n, err := v.ReadAt(ctx, got, 0); err != nil || n != int(object.Size) || got[0] != (0x5a^0) {
		t.Fatalf("data object corrupted: n=%d err=%v first=%#x", n, err, got[0])
	}
	if n, err := v.ReadAt(ctx, got, object.Size); err != nil || n != int(object.Size) {
		t.Fatalf("read punched: %d %v", n, err)
	}
	for _, b := range got {
		if b != 0 {
			t.Fatalf("punched object not zero: %#x", b)
			break
		}
	}

	// Idempotent second pass (Linux): the punched file now reports
	// alloc 0, so the scan skips it and the data object is not zero.
	// Where punching degrades to a zero-fill (darwin), blocks stay
	// allocated, so the pass re-zeroes (a no-op on content): Zeroed
	// only drops to 0 once real punching is available.
	res2, err := v.Vacuum(ctx)
	if err != nil {
		t.Fatalf("second live vacuum: %v", err)
	}
	if punchSupported && (res2.Zeroed != 0 || res2.Freed != 0) {
		t.Fatalf("second live vacuum = %+v, want no-op on punch-capable platform", res2)
	}

	// A quota-capped volume also releases the allocation bit, so Usage
	// drops by the freed object.
	if _, err := c.cl.CreateVolume(ctx, "q", size, 8<<20, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume q: %v", err)
	}
	vq, err := c.cl.OpenVolume(ctx, "q")
	if err != nil {
		t.Fatalf("OpenVolume q: %v", err)
	}
	defer vq.Close()
	if _, err := vq.WriteAt(ctx, make([]byte, object.Size), 2*object.Size); err != nil {
		t.Fatalf("write obj2 under quota: %v", err)
	}
	if _, err := vq.WriteAt(ctx, data, 3*object.Size); err != nil {
		t.Fatalf("write obj3 under quota: %v", err)
	}
	before, err := vq.Usage(ctx)
	if err != nil || before != 2*object.Size {
		t.Fatalf("usage before quota vacuum = %d (%v), want %d", before, err, 2*object.Size)
	}
	qres, err := vq.Vacuum(ctx)
	if err != nil {
		t.Fatalf("quota vacuum: %v", err)
	}
	if qres.Zeroed != 1 {
		t.Fatalf("quota vacuum = %+v, want 1 zeroed", qres)
	}
	after, err := vq.Usage(ctx)
	if err != nil {
		t.Fatalf("usage after: %v", err)
	}
	if after != object.Size {
		t.Fatalf("quota vacuum did not release allocation: %d -> %d", before, after)
	}
	// The volume is back under its cap: the data object remains and a
	// fresh object still fits.
	if _, err := vq.WriteAt(ctx, data, object.Size); err != nil {
		t.Fatalf("write under freed quota: %v", err)
	}
}

// TestVacuumBacklog checks the queue gauge math: empty queue reports
// Oldest -1, and the aggregate counts base ranges plus resize extents
// across tombstones, with the age of the oldest record.
func TestVacuumBacklog(t *testing.T) {
	dir := t.TempDir()
	m, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	defer m.Close()
	ctx := context.Background()
	cl := NewCluster(m)
	defer cl.Close()

	vb, err := cl.VacuumBacklog(ctx)
	if err != nil {
		t.Fatalf("VacuumBacklog: %v", err)
	}
	if vb.Tombstones != 0 || vb.Objects != 0 || vb.Bytes != 0 || vb.Oldest != -1 {
		t.Fatalf("empty backlog = %+v", vb)
	}

	old := time.Now().Add(-2 * time.Hour).Unix()
	if _, err := meta.PutVacuum(ctx, m, meta.VacuumRecord{
		Name: "a", OIDBase: 10, NumObjs: 4,
		Extents:   []meta.OIDExtent{{Base: 100, Count: 2}},
		DeletedAt: old,
	}); err != nil {
		t.Fatalf("PutVacuum a: %v", err)
	}
	if _, err := meta.PutVacuum(ctx, m, meta.VacuumRecord{
		Name: "b", OIDBase: 20, NumObjs: 8, DeletedAt: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("PutVacuum b: %v", err)
	}
	vb, err = cl.VacuumBacklog(ctx)
	if err != nil {
		t.Fatalf("VacuumBacklog: %v", err)
	}
	if vb.Tombstones != 2 || vb.Objects != 14 {
		t.Fatalf("backlog = %+v, want 2 tombstones / 14 objects", vb)
	}
	if vb.Bytes != 14*object.Size {
		t.Fatalf("bytes = %d, want %d", vb.Bytes, 14*object.Size)
	}
	if vb.Oldest < 7100 || vb.Oldest > 7300 {
		t.Fatalf("oldest = %v, want ~7200", vb.Oldest)
	}
}
