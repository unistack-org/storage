package client

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
)

// TestPoolLifecycle: create with default fill, duplicate refused, list,
// get, delete guarded by referencing volumes.
func TestPoolLifecycle(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rec, err := c.cl.CreatePool(ctx, meta.PoolRecord{Name: "fast", Replicas: 3, MinSize: 2, PGNum: 128, Rule: "replicated"})
	if err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if rec.Replicas != 3 || rec.MinSize != 2 || rec.PGNum != 128 {
		t.Fatalf("CreatePool: explicit fields mangled: %+v", rec)
	}
	// Omitted fields take the compiled cluster defaults (1024/3 from
	// the snapshot the map compiled to).
	rec2, err := c.cl.CreatePool(ctx, meta.PoolRecord{Name: "bydef"})
	if err != nil {
		t.Fatalf("CreatePool bydef: %v", err)
	}
	if rec2.PGNum != 1024 || rec2.Replicas != 3 || rec2.MinSize != 2 || rec2.Rule == "" {
		t.Fatalf("CreatePool bydef: defaults not filled: %+v", rec2)
	}
	// Duplicate name: ErrConflict from meta.
	if _, err := c.cl.CreatePool(ctx, meta.PoolRecord{Name: "fast", Replicas: 3, MinSize: 2, PGNum: 64}); !errors.Is(err, meta.ErrConflict) {
		t.Fatalf("duplicate pool: want ErrConflict, got %v", err)
	}
	// pg_num above the cluster default is refused.
	if _, err := c.cl.CreatePool(ctx, meta.PoolRecord{Name: "big", Replicas: 3, MinSize: 2, PGNum: 2048}); !errors.Is(err, meta.ErrInvalidPool) {
		t.Fatalf("pg_num over default: want ErrInvalidPool, got %v", err)
	}

	all, err := c.cl.ListPools(ctx)
	if err != nil {
		t.Fatalf("ListPools: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListPools: want 2 pools, got %+v", all)
	}
	if _, err := c.cl.GetPool(ctx, "fast"); err != nil {
		t.Fatalf("GetPool: %v", err)
	}

	// A pool a volume references cannot be deleted; after the volume is
	// gone it can.
	if _, err := c.cl.CreateVolumeInPool(ctx, "fast", "pv0", 64<<20, 0, 0, 0, 0, ""); err != nil {
		t.Fatalf("CreateVolumeInPool: %v", err)
	}
	err = c.cl.DeletePool(ctx, "fast")
	if !errors.Is(err, meta.ErrPoolInUse) {
		t.Fatalf("DeletePool in use: want ErrPoolInUse, got %v", err)
	}
	if err := c.cl.DeleteVolume(ctx, "pv0"); err != nil {
		t.Fatalf("DeleteVolume: %v", err)
	}
	if err := c.cl.DeletePool(ctx, "fast"); err != nil {
		t.Fatalf("DeletePool after volume removed: %v", err)
	}
	if _, err := c.cl.GetPool(ctx, "fast"); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("GetPool after delete: want ErrNotFound, got %v", err)
	}
}

// TestCreateVolumeInPool: pool policy fills the spec (including the QoS
// template); explicit flags override the pool; the volume opens and
// reads/writes with the pool-supplied placement.
func TestCreateVolumeInPool(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.cl.PutQoSClass(ctx, meta.QoSClassRecord{Name: "gold", WriteIOPS: 1000}); err != nil {
		t.Fatalf("PutQoSClass: %v", err)
	}
	if _, err := c.cl.CreatePool(ctx, meta.PoolRecord{Name: "gold-pool", Replicas: 3, MinSize: 2, PGNum: 64, Rule: "replicated", QoSTemplate: "gold"}); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	spec, err := c.cl.CreateVolumeInPool(ctx, "gold-pool", "vm0", 128<<20, 0, 0, 0, 0, "")
	if err != nil {
		t.Fatalf("CreateVolumeInPool: %v", err)
	}
	if spec.Replicas != 3 || spec.MinSize != 2 || spec.PGNum != 64 || spec.Rule != "replicated" {
		t.Fatalf("pool policy not applied: %+v", spec)
	}
	if spec.QoSTemplate != "gold" {
		t.Fatalf("pool QoS template not inherited: %+v", spec)
	}
	if spec.Pool != "gold-pool" {
		t.Fatalf("pool provenance not recorded: %+v", spec)
	}

	// An explicit flag beats the pool.
	spec2, err := c.cl.CreateVolumeInPool(ctx, "gold-pool", "vm1", 128<<20, 0, 0, 0, 32, "custom")
	if err != nil {
		t.Fatalf("CreateVolumeInPool override: %v", err)
	}
	if spec2.PGNum != 32 || spec2.Rule != "custom" {
		t.Fatalf("explicit flags lost to the pool: %+v", spec2)
	}

	// The pool volume is a normal volume on the data path.
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()
	data := make([]byte, 512)
	data[0] = 0xAB
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 512)
	if _, err := v.ReadAt(ctx, buf, 0); err != nil {
		t.Fatalf("read: %v", err)
	}
	if buf[0] != 0xAB {
		t.Fatalf("read-back mismatch: %02x", buf[0])
	}

	// A missing pool is ErrNotFound (HTTP maps it to 404).
	if _, err := c.cl.CreateVolumeInPool(ctx, "nope", "vm2", 128<<20, 0, 0, 0, 0, ""); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("missing pool: want ErrNotFound, got %v", err)
	} else if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("missing pool: error should name the pool: %v", err)
	}
}
