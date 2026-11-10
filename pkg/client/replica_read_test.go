package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/sdsnet"
)

// Reads are replica-independent (quorum writes, byte-identical
// copies): any live replica answers. These tests pin the two halves
// of the roadmap item «чтение с любой реплики»: tolerance (survive
// replica deaths on the read path) and preference (the local OSD is
// picked first, so an in-process target absorbs reads without
// touching the network).

// Two of three OSDs die: reads keep succeeding from the survivor,
// exercising the fall-through over the reordered replica list.
func TestReadSurvivesReplicaDeaths(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 128<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, 1<<20)
	fill(data, 0x3C, 11)
	if n, err := v.WriteAt(ctx, data, 0); err != nil || n != len(data) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}

	// Kill daemons 0 and 1 (a full pg row in map3 spans all three
	// hosts, so every pg keeps exactly one live target).
	for _, i := range []int{0, 1} {
		c.daems[i].Close()
		c.daems[i] = nil // cleanup must not double-close
	}
	c.cl.dropPool(0)
	c.cl.dropPool(1)

	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 0); err != nil || n != len(got) {
		t.Fatalf("read after 2/3 deaths: n=%d err=%v", n, err)
	}
	for i := range got {
		if got[i] != data[i] {
			t.Fatalf("corrupt at %d: got %02x want %02x", i, got[i], data[i])
		}
	}
}

// With an in-process (local) transport to OSD 0 and OSD 0 declared
// local, every read chunk must be served by it: map3 puts all three
// OSDs in every pg row, so the TCP replicas are always valid targets
// — if ordering regressed, some pg would lead with OSD 1/2 and their
// pools' idle counts would grow on the read (the write opens all
// three pools, so their post-write stats are the baseline).
func TestReadPrefersLocalReplica(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.cl.SetLocalDial(func(osdID int32) (net.Conn, error) {
		if osdID != 0 {
			// Not hosted here: plain TCP, what a pool would dial
			// without the local transport.
			return net.Dial("tcp", c.daems[osdID].Addr())
		}
		srv := c.daems[0].Server()
		if srv == nil {
			return nil, net.ErrClosed
		}
		mine, peer, err := sdsnet.Socketpair()
		if err != nil {
			return nil, err
		}
		go srv.ServeConn(peer)
		return mine, nil
	})
	c.cl.SetLocalOSDs([]int32{0})

	if _, err := c.cl.CreateVolume(ctx, "vm0", 128<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, 64<<10)
	fill(data, 0x77, 3)
	if n, err := v.WriteAt(ctx, data, 0); err != nil || n != len(data) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	// The write commits at quorum (w=2 of 3): the straggler replica
	// write may check out a pool after WriteAt returns. Let it finish
	// before snapshotting per-pool op counters.
	time.Sleep(150 * time.Millisecond)
	base := c.cl.PoolOps()
	for _, id := range []int32{0, 1, 2} {
		if base[id] == 0 {
			t.Fatalf("pool %d unused by write: %v", id, base)
		}
	}

	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 0); err != nil || n != len(got) {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	for i := range got {
		if got[i] != data[i] {
			t.Fatalf("corrupt at %d", i)
		}
	}
	ops := c.cl.PoolOps()
	if ops[0] <= base[0] {
		t.Fatalf("local OSD 0 served no read: ops %v -> %v", base, ops)
	}
	// The read must not touch the remote pools at all.
	for _, id := range []int32{1, 2} {
		if ops[id] != base[id] {
			t.Fatalf("read escaped the local replica: osd %d ops %v -> %v", id, base[id], ops[id])
		}
	}
}
