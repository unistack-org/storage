package client

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestClusterTwoVolumeIsolation is the multi-tenant regression: two
// volumes share the same OSDs, and each must see only its own bytes.
// Volumes used to address objects as index+1 with no per-volume range,
// so the second volume silently read and overwrote the first one's
// objects (every test VM disk on a cluster sharing a disk = corruption).
func TestClusterTwoVolumeIsolation(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 64 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm-a", size, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume vm-a: %v", err)
	}
	if _, err := c.cl.CreateVolume(ctx, "vm-b", size, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume vm-b: %v", err)
	}
	a, err := c.cl.OpenVolume(ctx, "vm-a")
	if err != nil {
		t.Fatalf("OpenVolume vm-a: %v", err)
	}
	b, err := c.cl.OpenVolume(ctx, "vm-b")
	if err != nil {
		t.Fatalf("OpenVolume vm-b: %v", err)
	}
	if a.Spec().OIDBase == b.Spec().OIDBase {
		t.Fatalf("volumes share object-id range: base %d", a.Spec().OIDBase)
	}

	// Same offsets on both volumes (the colliding addressing made these
	// indistinguishable), plus a thin (never written) region that must
	// stay zero on the volume that never touched it.
	const chunk = 128 << 10
	for _, off := range []uint64{0, 1 << 20, 4<<20 + 4096, 33 << 20} {
		da := make([]byte, chunk)
		db := make([]byte, chunk)
		fill(da, 0xA0, int(off))
		fill(db, 0xB0, int(off))
		if _, err := a.WriteAt(ctx, da, off); err != nil {
			t.Fatalf("write vm-a at %d: %v", off, err)
		}
		if _, err := b.WriteAt(ctx, db, off); err != nil {
			t.Fatalf("write vm-b at %d: %v", off, err)
		}
		ga := make([]byte, chunk)
		gb := make([]byte, chunk)
		if n, err := a.ReadAt(ctx, ga, off); err != nil || n != chunk {
			t.Fatalf("read vm-a at %d: n=%d err=%v", off, n, err)
		}
		if n, err := b.ReadAt(ctx, gb, off); err != nil || n != chunk {
			t.Fatalf("read vm-b at %d: n=%d err=%v", off, n, err)
		}
		if !bytes.Equal(ga, da) {
			t.Fatalf("vm-a data corrupted at %d (cross-volume object collision)", off)
		}
		if !bytes.Equal(gb, db) {
			t.Fatalf("vm-b data corrupted at %d (cross-volume object collision)", off)
		}
	}

	// A region only vm-a wrote must read as zeroes on vm-b.
	zero := make([]byte, chunk)
	const hole = 50 << 20
	if _, err := a.WriteAt(ctx, bytes.Repeat([]byte{0x5A}, chunk), hole); err != nil {
		t.Fatalf("write vm-a hole: %v", err)
	}
	got := make([]byte, chunk)
	if n, err := b.ReadAt(ctx, got, hole); err != nil || n != chunk {
		t.Fatalf("read vm-b hole: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, zero) {
		t.Fatal("vm-b sees vm-a's data at a never-written offset")
	}

	// Trim/punch on one volume must not punch the other.
	if err := b.WriteZeroes(ctx, 4<<20, chunk, true); err != nil {
		t.Fatalf("vm-b write zeroes: %v", err)
	}
	if n, err := a.ReadAt(ctx, got, 4<<20); err != nil || n != chunk {
		t.Fatalf("read vm-a after vm-b trim: n=%d err=%v", n, err)
	}
	if bytes.Equal(got, zero) {
		t.Fatal("vm-b trim punched vm-a's data")
	}
}

// TestClusterVolumeListDelete covers the lifecycle a control plane needs:
// list volumes and delete a registration.
func TestClusterVolumeListDelete(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, n := range []string{"vm-b", "vm-a"} {
		if _, err := c.cl.CreateVolume(ctx, n, 8<<20, 3, 0, 64, "data"); err != nil {
			t.Fatalf("CreateVolume %s: %v", n, err)
		}
	}
	specs, err := c.cl.ListVolumes(ctx)
	if err != nil {
		t.Fatalf("ListVolumes: %v", err)
	}
	if len(specs) != 2 || specs[0].Name != "vm-a" || specs[1].Name != "vm-b" {
		t.Fatalf("ListVolumes = %+v, want vm-a,vm-b sorted", specs)
	}
	if err := c.cl.DeleteVolume(ctx, "vm-a"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if _, err := c.cl.OpenVolume(ctx, "vm-a"); err == nil {
		t.Fatal("open after delete succeeded")
	}
	if err := c.cl.DeleteVolume(ctx, "vm-a"); err == nil {
		t.Fatal("double delete reported success")
	}
}
