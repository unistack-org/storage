package client

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/osd"
)

// map3 is a three-host rep3 map (one osd per host): every pg row spans
// all three hosts, i.e. exactly the three daemons under test.
const map3 = `# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable straw_calc_version 1
tunable chooseleaf_stable 1
# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2
# types
type 0 osd
type 1 host
type 2 root
# buckets
root r {
id -2
alg straw2
hash 0
item host h1
item host h2
item host h3
}
host h1 {
id -3
alg straw2
hash 0
item osd.0 weight 1.000
}
host h2 {
id -4
alg straw2
hash 0
item osd.1 weight 1.000
}
host h3 {
id -5
alg straw2
hash 0
item osd.2 weight 1.000
}
# rules
rule data {
id 0
type replicated
step take r
step chooseleaf firstn 0 type host
step emit
}
# end crush map
`

// cluster is a running in-process test cluster.
type cluster struct {
	t     *testing.T
	meta  meta.Store
	daems []*osd.Daemon
	cl    *Cluster
}

// newCluster starts n OSD daemons over a shared dir meta store and
// returns a cluster client (no map published yet).
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	dir := t.TempDir()
	m, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatalf("meta: %v", err)
	}
	c := &cluster{t: t, meta: m}
	cctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		for _, d := range c.daems {
			d.Close()
		}
		for _, d := range c.daems {
			if err := d.Run(context.Background()); err != nil && err != context.Canceled {
				t.Logf("daemon run: %v", err)
			}
		}
	})
	for i := 0; i < n; i++ {
		d, err := osd.New(osd.Options{
			ID:      int32(i),
			Addr:    "127.0.0.1:0",
			DataDir: filepath.Join(dir, fmt.Sprintf("osd%d", i)),
			Meta:    m,
		})
		if err != nil {
			t.Fatalf("osd new: %v", err)
		}
		c.daems = append(c.daems, d)
		errc := make(chan error, 1)
		go func() { errc <- d.Run(cctx) }()
		// Wait for registration in meta.
		deadline := time.Now().Add(5 * time.Second)
		for {
			infos, err := meta.ListOSDs(cctx, m)
			if err == nil && len(infos) >= i+1 {
				break
			}
			select {
			case derr := <-errc:
				t.Fatalf("daemon %d died: %v", i, derr)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("daemon %d did not register (err=%v)", i, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cl := NewCluster(m)
	t.Cleanup(func() { cl.Close() })
	c.cl = cl
	return c
}

// publishMap publishes epoch as the cluster crush map and waits for the
// OSD fences to rise.
func (c *cluster) publishMap(epoch uint32) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.cl.MapEpoch(ctx, epoch, []byte(map3)); err != nil {
		c.t.Fatalf("MapEpoch: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		fenced := 0
		for _, d := range c.daems {
			if d.Fence() >= epoch {
				fenced++
			}
		}
		if fenced == len(c.daems) {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("osds did not fence at epoch %d (%d/%d)", epoch, fenced, len(c.daems))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestClusterVolumeRoundTrip(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const size = 128 << 20 // 128 MiB = 32 objects
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 3, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}

	// Thin read: untouched volume reads as zeroes.
	p := make([]byte, 4096)
	fill(p, 0xAB, 0)
	n, err := v.ReadAt(ctx, p, 1<<20)
	if err != nil || n != 4096 {
		t.Fatalf("thin read: n=%d err=%v", n, err)
	}
	for i, b := range p {
		if b != 0 {
			t.Fatalf("thin read not zero at %d", i)
		}
	}

	// Write across an object boundary (objects are 4 MiB).
	data := make([]byte, 1<<20)
	fill(data, 0x5A, 7)
	off := object.Size - 512 // straddles objects 0 and 1
	if n, err := v.WriteAt(ctx, data, uint64(off)); err != nil || n != len(data) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, uint64(off)); err != nil || n != len(data) {
		t.Fatalf("ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("round-trip mismatch across object boundary")
	}

	// Untouched neighbours still read zero.
	z := make([]byte, 512)
	if n, err := v.ReadAt(ctx, z, uint64(off)-512); err != nil || n != 512 {
		t.Fatalf("neighbour read: n=%d err=%v", n, err)
	}
	for i, b := range z {
		if b != 0 {
			t.Fatalf("neighbour not zero at %d", i)
		}
	}

	// WriteZeroes punches the head back to zeros.
	if err := v.WriteZeroes(ctx, 0, 1<<20); err != nil {
		t.Fatalf("WriteZeroes: %v", err)
	}
	zeros := make([]byte, 1<<20)
	if n, err := v.ReadAt(ctx, zeros, 0); err != nil || n != 1<<20 {
		t.Fatalf("post-punch read: n=%d err=%v", n, err)
	}
	for i, b := range zeros {
		if b != 0 {
			t.Fatalf("punched region nonzero at %d", i)
		}
	}

	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func TestClusterQuorumKillOne(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 3, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, 64<<10)
	fill(data, 0x11, 3)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("baseline write: %v", err)
	}

	// Kill one OSD: quorum (2/3) must still write and read. Close the
	// daemon and drop its pool so the client redials (and fails fast on
	// connection refused) instead of using dead idle connections.
	c.daems[1].Close()
	c.cl.poolsMu.Lock()
	if pp, ok := c.cl.pools[1]; ok {
		pp.Close()
		delete(c.cl.pools, 1)
	}
	c.cl.poolsMu.Unlock()

	if _, err := v.WriteAt(ctx, data, 1<<20); err != nil {
		t.Fatalf("write with one osd down: %v", err)
	}
	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 1<<20); err != nil || n != len(data) {
		t.Fatalf("read with one osd down: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch with one osd down")
	}
}

func TestClusterStaleEpochRefresh(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 3, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	snap1, err := c.cl.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	data := make([]byte, 64<<10)
	fill(data, 0x22, 5)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write at epoch 1: %v", err)
	}

	// Publish epoch 2 and fence the OSDs, then rewind the client to a
	// cached epoch-1 snapshot with a fresh throttle stamp (so Refresh
	// returns the stale map) and epoch-1 pools. Its requests must be
	// refused with StatusStale, forcing a placement refresh and a
	// successful retry.
	c.publishMap(2)
	c.cl.mu.Lock()
	c.cl.epoch, c.cl.snap = 1, snap1
	c.cl.mu.Unlock()
	c.cl.lastRef.Store(time.Now().UnixNano())
	c.cl.poolsMu.Lock()
	for _, pp := range c.cl.pools {
		pp.SetEpoch(1)
	}
	c.cl.poolsMu.Unlock()

	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 0); err != nil || n != len(data) {
		t.Fatalf("read through stale fence: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after stale-epoch refresh")
	}
	if ep := c.cl.epochSnapshot(); ep != 2 {
		t.Fatalf("client did not move to epoch 2 (at %d)", ep)
	}
}

func fill(p []byte, seed byte, off int) {
	for i := range p {
		p[i] = seed + byte(i+off)
	}
}
