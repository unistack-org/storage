package client

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
)

// TestVolumeQoSPacing proves an open volume enforces its registered
// limits: 20 write IOPS (burst 2) makes 4 writes cost ~100ms.
func TestVolumeQoSPacing(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "qos0", 128<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := c.cl.SetVolumeQoS(ctx, "qos0", meta.VolumeSpec{WriteIOPS: 20}); err != nil {
		t.Fatalf("SetVolumeQoS: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "qos0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	data := make([]byte, 512)
	start := time.Now()
	for i := 0; i < 4; i++ {
		if _, err := v.WriteAt(ctx, data, uint64(i*512)); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("4 writes at 20 write-IOPS took %v: limiter not pacing", elapsed)
	}

	// Reads are unlimited on this volume (read axis is 0): 8 reads must
	// stay fast.
	buf := make([]byte, 512)
	start = time.Now()
	for i := 0; i < 8; i++ {
		if _, err := v.ReadAt(ctx, buf, uint64(i*512)); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("8 unlimited reads took %v: limits leaked to the read axis", elapsed)
	}
}

// TestVolumeQoSHotReload proves the owner's requirement: QoS on an
// ALREADY-OPEN volume takes effect live (no remount) via the meta
// watch, and clearing it restores full speed.
func TestVolumeQoSHotReload(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "live0", 128<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "live0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	data := make([]byte, 512)
	writeTimed := func(n int) time.Duration {
		start := time.Now()
		for i := 0; i < n; i++ {
			if _, err := v.WriteAt(ctx, data, uint64(i*512)); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		return time.Since(start)
	}
	if d := writeTimed(4); d > 500*time.Millisecond {
		t.Fatalf("unlimited volume 4 writes took %v", d)
	}

	// Admin throttles the running volume.
	if _, err := c.cl.SetVolumeQoS(ctx, "live0", meta.VolumeSpec{WriteIOPS: 20}); err != nil {
		t.Fatalf("SetVolumeQoS: %v", err)
	}
	// The DirStore watch polls at 250ms; give it room.
	deadline := time.Now().Add(5 * time.Second)
	var paced bool
	for !paced {
		v.reloadQoS(ctx) // force the reload the watch performs
		if v.limitsSnapshot() == nil {
			if time.Now().After(deadline) {
				t.Fatal("limits never applied to the open volume")
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		paced = writeTimed(8) >= 120*time.Millisecond
		if !paced && time.Now().After(deadline) {
			t.Fatal("applied limits do not pace writes")
		}
	}

	// Admin lifts the limit again (the admin class): full speed back.
	if _, err := c.cl.SetVolumeQoS(ctx, "live0", meta.VolumeSpec{}); err != nil {
		t.Fatalf("SetVolumeQoS clear: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for v.limitsSnapshot() != nil {
		v.reloadQoS(ctx)
		if time.Now().After(deadline) {
			t.Fatal("limits never cleared on the open volume")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if d := writeTimed(4); d > 500*time.Millisecond {
		t.Fatalf("unlimited volume 4 writes took %v after clearing", d)
	}

	// Data integrity through the whole episode.
	if _, err := v.WriteAt(ctx, data, 4096); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 512)
	if _, err := v.ReadAt(ctx, got, 4096); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data corrupted across QoS reconfiguration")
	}
}

// TestVolumeQoSTemplate proves named classes: a volume referencing a
// class takes the class rates at open, an admin (missing) class is
// unlimited, and editing the class + reload changes a running volume.
func TestVolumeQoSTemplate(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := c.cl.PutQoSClass(ctx, meta.QoSClassRecord{Name: "std", WriteIOPS: 20}); err != nil {
		t.Fatalf("PutQoSClass: %v", err)
	}
	if _, err := c.cl.CreateVolume(ctx, "t0", 128<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	if _, err := c.cl.SetVolumeQoS(ctx, "t0", meta.VolumeSpec{QoSTemplate: "std"}); err != nil {
		t.Fatalf("SetVolumeQoS: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "t0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()
	if v.limitsSnapshot() == nil {
		t.Fatal("class rates not applied at open")
	}

	data := make([]byte, 512)
	start := time.Now()
	// 8 ops: burst 2, the rest ~50ms each at 20 IOPS (~300ms); a watch
	// reload mid-measurement can donate one extra burst, so the bound
	// is loose.
	for i := 0; i < 8; i++ {
		if _, err := v.WriteAt(ctx, data, uint64(i*512)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Fatalf("class-paced writes took %v", elapsed)
	}

	// The admin class: a volume referencing a missing class is
	// unlimited (open must not fail).
	if _, err := c.cl.SetVolumeQoS(ctx, "t0", meta.VolumeSpec{QoSTemplate: "admin"}); err != nil {
		t.Fatalf("SetVolumeQoS admin: %v", err)
	}
	v.reloadQoS(ctx)
	if v.limitsSnapshot() != nil {
		t.Fatal("missing class must mean unlimited")
	}

	// Editing the class lowers a running volume.
	if err := c.cl.PutQoSClass(ctx, meta.QoSClassRecord{Name: "std", WriteIOPS: 1000000}); err != nil {
		t.Fatalf("PutQoSClass bump: %v", err)
	}
	if _, err := c.cl.SetVolumeQoS(ctx, "t0", meta.VolumeSpec{QoSTemplate: "std"}); err != nil {
		t.Fatalf("SetVolumeQoS rebind: %v", err)
	}
	v.reloadQoS(ctx)
	if v.limitsSnapshot() == nil {
		t.Fatal("rebinding the class produced no limits")
	}
	start = time.Now()
	for i := 0; i < 4; i++ {
		if _, err := v.WriteAt(ctx, data, uint64(i*512)); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("1M-IOPS class paced 4 writes (%v)", elapsed)
	}
}
