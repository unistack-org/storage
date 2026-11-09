package client

import (
	"context"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
)

// waitForSize polls until the open handle's live geometry shows want
// (the volume watch refreshes it; DirStore polls at 250ms).
func waitForSize(t *testing.T, v *Volume, want uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v.Size() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("live size = %d, want %d (watch did not refresh)", v.Size(), want)
}

// An online grow: the open handle sees the new size through the watch,
// writes land in the extent ranges (never in another volume's ids),
// reads return them, and deleting the grown volume vacuums base +
// extents.
func TestResizeOnline(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const oldSize = 8 << 20
	const newSize = 24 << 20
	spec, err := c.cl.CreateVolume(ctx, "grower", oldSize, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "grower")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, object.Size)
	fillQuota(data, 0x0a, 0)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write base: %v", err)
	}

	grown, err := c.cl.ResizeVolume(ctx, "grower", newSize)
	if err != nil {
		t.Fatalf("ResizeVolume: %v", err)
	}
	if len(grown.Extents) != 1 {
		t.Fatalf("extents = %+v, want 1", grown.Extents)
	}
	waitForSize(t, v, newSize)

	// Write into the grown area (object index 3 = offset 12 MiB, beyond
	// the old 8 MiB end) and read it back.
	tail := make([]byte, object.Size)
	fillQuota(tail, 0x2b, 3)
	if _, err := v.WriteAt(ctx, tail, 12<<20); err != nil {
		t.Fatalf("write grown area: %v", err)
	}
	back := make([]byte, object.Size)
	if _, err := v.ReadAt(ctx, back, 12<<20); err != nil {
		t.Fatalf("read grown area: %v", err)
	}
	for i := range back {
		if back[i] != tail[i] {
			t.Fatalf("grown read mismatch at %d: %02x != %02x", i, back[i], tail[i])
		}
	}
	// The base-area write must still read back intact.
	if _, err := v.ReadAt(ctx, back, 0); err != nil {
		t.Fatalf("read base: %v", err)
	}
	if back[0] != 0x0a {
		t.Fatalf("base read corrupted: %02x", back[0])
	}

	// The grown object must live at the extent id, not inside the base
	// range (index 3 >= BaseObjs(8MiB)=2).
	extID, ok := grown.Lookup(3)
	if !ok {
		t.Fatal("Lookup(3) failed")
	}
	if extID <= object.OID(spec.OIDBase+object.NumObjects(oldSize)) {
		t.Fatalf("extent id %d inside base range ending at %d",
			extID, spec.OIDBase+object.NumObjects(oldSize))
	}
	waitForObj(t, c.dir, uint64(extID), 3)

	// Reading/writing one byte past the new end must fail.
	if _, err := v.WriteAt(ctx, make([]byte, 2), newSize-1); err == nil {
		t.Fatal("write past new end succeeded")
	}
	v.Close()

	// Delete + vacuum: base range and extent range all go away.
	if err := c.cl.DeleteVolume(ctx, "grower"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	rec, err := meta.GetVacuum(ctx, c.meta, "grower")
	if err != nil {
		t.Fatalf("GetVacuum: %v", err)
	}
	if len(rec.Extents) != 1 {
		t.Fatalf("tombstone extents = %+v, want 1", rec.Extents)
	}
	res, err := c.cl.VacuumDeletedVolumes(ctx)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if res.Deleted != 1 {
		t.Fatalf("vacuum = %+v", res)
	}
	for _, id := range []uint64{spec.OIDBase + 1, uint64(extID)} {
		for id3 := 0; id3 < 3; id3++ {
			if objExists(objPath(c.dir, id3, id)) {
				t.Fatalf("obj %d on osd %d survived the vacuum", id, id3)
			}
		}
	}
}

// A second client handle picks the new size up from the watch, and a
// volume created while a grown one is open never collides: allocation
// stays monotonic.
func TestResizeNoCollision(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "a", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	a, err := c.cl.OpenVolume(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	// b is created after a: its ids sit directly above a's base range.
	if _, err := c.cl.CreateVolume(ctx, "b", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.cl.ResizeVolume(ctx, "a", 32<<20); err != nil {
		t.Fatal(err)
	}
	waitForSize(t, a, 32<<20)

	bv, err := c.cl.OpenVolume(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, object.Size)
	fillQuota(data, 0x5c, 0)
	if _, err := bv.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write b: %v", err)
	}
	// Write every object of grown a, including the extents.
	for i := 0; i < int(object.NumObjects(32<<20)); i++ {
		chunk := make([]byte, object.Size)
		fillQuota(chunk, byte(0x70+i), uint64(i))
		if _, err := a.WriteAt(ctx, chunk, uint64(i)*object.Size); err != nil {
			t.Fatalf("write a obj %d: %v", i, err)
		}
	}
	// b's object 0 must be byte-identical to what b wrote: a's extent
	// ids cannot alias it.
	back := make([]byte, object.Size)
	if _, err := bv.ReadAt(ctx, back, 0); err != nil {
		t.Fatalf("read b: %v", err)
	}
	for i := range back {
		if back[i] != data[i] {
			t.Fatalf("b obj 0 corrupted at %d: %02x != %02x (OID collision?)", i, back[i], data[i])
		}
	}
	a.Close()
	bv.Close()
}
