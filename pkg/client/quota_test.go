package client

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
)

// fill writes a deterministic byte pattern.
func fillQuota(p []byte, seed byte, off uint64) {
	for i := range p {
		p[i] = seed ^ byte(off+uint64(i))
	}
}

// TestQuotaEnforcement walks the quota lifecycle on a capped volume:
// writes within the cap succeed, a first touch past the cap fails with
// ENOSPC, rewrites of allocated objects stay free, a full-object sparse
// TRIM gives the room back, and a live quota edit (via meta) takes
// effect on the open handle.
func TestQuotaEnforcement(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 128 << 20 // 32 objects
	quota := 2 * object.Size
	if _, err := c.cl.CreateVolume(ctx, "q1", size, uint64(quota), 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "q1")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()
	if v.Quota() != uint64(quota) {
		t.Fatalf("Quota() = %d, want %d", v.Quota(), quota)
	}

	data := make([]byte, object.Size)
	fillQuota(data, 0x5A, 0)
	got := make([]byte, object.Size)

	// First two objects fit exactly under the cap.
	for i := 0; i < 2; i++ {
		if _, err := v.WriteAt(ctx, data, uint64(i)*object.Size); err != nil {
			t.Fatalf("write object %d: %v", i, err)
		}
	}
	if u, err := v.Usage(ctx); err != nil || u != 2*object.Size {
		t.Fatalf("usage after 2 objects = %d (%v), want %d", u, err, 2*object.Size)
	}

	// Object 2 is a first touch past the cap: refused, as ENOSPC.
	if _, err := v.WriteAt(ctx, data, 2*object.Size); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("past-quota write err = %v, want ErrNoSpace", err)
	}
	if _, err := v.WriteAt(ctx, data, 2*object.Size); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("past-quota write err = %v, must wrap syscall.ENOSPC (NBD maps it to the guest)", err)
	}
	// A rewrite of an allocated object stays free even at the cap.
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("rewrite of allocated object: %v", err)
	}
	if _, err := v.ReadAt(ctx, got, 0); err != nil {
		t.Fatalf("read at cap: %v", err)
	}
	// A partial write whose every object is allocated is free too.
	if _, err := v.WriteAt(ctx, data[:512], object.Size-256); err != nil {
		t.Fatalf("straddling rewrite of allocated objects: %v", err)
	}

	// Sparse TRIM of object 0 (full object) frees its room; the write
	// past the cap then succeeds.
	if err := v.WriteZeroes(ctx, 0, object.Size, true); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		u, err := v.Usage(ctx)
		if err != nil {
			t.Fatalf("usage: %v", err)
		}
		if u == object.Size {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("usage after trim = %d, want %d", u, object.Size)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := v.WriteAt(ctx, data, 2*object.Size); err != nil {
		t.Fatalf("write after trim: %v", err)
	}

	// Live quota edit through meta (the admin path): the open handle
	// picks the larger cap up without a remount.
	if _, err := c.cl.SetVolumeQuota(ctx, "q1", 3*object.Size); err != nil {
		t.Fatalf("SetVolumeQuota: %v", err)
	}
	for {
		if v.Quota() == 3*object.Size {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("live quota edit not picked up")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := v.WriteAt(ctx, data, 3*object.Size); err != nil {
		t.Fatalf("write after cap raise: %v", err)
	}
	// Shrinking below usage keeps reads and allocated-object rewrites
	// working; only new first touches are refused.
	if _, err := c.cl.SetVolumeQuota(ctx, "q1", object.Size); err != nil {
		t.Fatalf("SetVolumeQuota shrink: %v", err)
	}
	for v.Quota() != object.Size {
		select {
		case <-ctx.Done():
			t.Fatal("live quota shrink not picked up")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := v.ReadAt(ctx, got, 2*object.Size); err != nil {
		t.Fatalf("read of allocated object under shrunk cap: %v", err)
	}
	if _, err := v.WriteAt(ctx, data, 4*object.Size); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("first touch under shrunk cap err = %v, want ErrNoSpace", err)
	}
}

// TestQuotaUncapped proves an uncapped (quota=0) volume never refuses a
// write for space, yet its allocation bitmap is maintained: usage is
// tracked (the snapshot layer and Usage() depend on it for every
// volume, capped or not).
func TestQuotaUncapped(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 32 << 20
	if _, err := c.cl.CreateVolume(ctx, "free", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "free")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()
	data := make([]byte, object.Size)
	fillQuota(data, 0x11, 0)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The bitmap is now maintained for every volume: usage counts the
	// one allocated object.
	u, err := v.Usage(ctx)
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if u != object.Size {
		t.Fatalf("usage after one write = %d, want %d", u, object.Size)
	}
}

// TestQuotaDeleteCleanup proves deleting a volume drops its allocation
// map, so recreating a volume under the same name starts at zero usage.
func TestQuotaDeleteCleanup(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 32 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm", size, object.Size, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, object.Size)
	fillQuota(data, 0x22, 0)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	v.Close()
	if err := c.cl.DeleteVolume(ctx, "vm"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if _, _, err := c.meta.Get(ctx, meta.PrefixAllocMaps, "vm"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("allocmap survived DeleteVolume: %v", err)
	}
	if _, err := c.cl.CreateVolume(ctx, "vm", size, object.Size, 3, 0, 64, "data"); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	v2, err := c.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer v2.Close()
	if u, err := v2.Usage(ctx); err != nil || u != 0 {
		t.Fatalf("recreated volume usage = %d (%v), want 0", u, err)
	}
}

// TestQuotaConcurrentWriters races two handles of one capped volume
// against the last block of room: exactly one first touch must win, and
// the bitmap must never lose a bit (the CAS merge).
func TestQuotaConcurrentWriters(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 64 << 20
	if _, err := c.cl.CreateVolume(ctx, "race", size, object.Size, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v1, err := c.cl.OpenVolume(ctx, "race")
	if err != nil {
		t.Fatalf("open v1: %v", err)
	}
	defer v1.Close()
	v2, err := c.cl.OpenVolume(ctx, "race")
	if err != nil {
		t.Fatalf("open v2: %v", err)
	}
	defer v2.Close()

	// Same object, two handles, concurrent first touch: one charges,
	// one finds the bit set. Then a distinct object beyond the cap must
	// fail from both.
	done := make(chan error, 2)
	for i, v := range []*Volume{v1, v2} {
		go func(i int, v *Volume) {
			p := make([]byte, 4096)
			fillQuota(p, byte(i), 0)
			_, err := v.WriteAt(ctx, p, 0)
			done <- err
		}(i, v)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatalf("racing first touch %d: %v", i, err)
		}
	}
	if u, err := v1.Usage(ctx); err != nil || u != object.Size {
		t.Fatalf("usage after racing first touches = %d (%v), want %d", u, err, object.Size)
	}
	if _, err := v2.WriteAt(ctx, make([]byte, 4096), object.Size); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("second object under 1-object cap err = %v, want ErrNoSpace", err)
	}
}
