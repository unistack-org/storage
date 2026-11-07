package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
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
	t     testing.TB
	meta  meta.Store
	daems []*osd.Daemon
	cl    *Cluster
}

// newCluster starts n OSD daemons over a shared dir meta store and
// returns a cluster client (no map published yet).
func newCluster(tb testing.TB, n int) *cluster {
	tb.Helper()
	t := tb
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
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 3, 0, 64, "data"); err != nil {
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

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 3, 0, 64, "data"); err != nil {
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

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 3, 0, 64, "data"); err != nil {
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

// setOSDWeight rewrites the OSD record's weight in meta. Weight 0 drops
// the OSD from the client's placement registry (the Registry builder
// treats weight<=0 as out), simulating a lost replica without killing
// the daemon.
func (c *cluster) setOSDWeight(tb testing.TB, ctx context.Context, id int32, w float64) {
	tb.Helper()
	info, err := meta.GetOSD(ctx, c.meta, id)
	if err != nil {
		tb.Fatalf("GetOSD %d: %v", id, err)
	}
	info.Weight = w
	if _, err := meta.PutOSD(ctx, c.meta, info); err != nil {
		tb.Fatalf("PutOSD %d: %v", id, err)
	}
}

// forceRefresh resets the refresh throttle so the next client Refresh
// rebuilds the placement snapshot from meta.
func (c *cluster) forceRefresh() {
	c.cl.lastRef.Store(0)
}

func TestClusterMinSizeCreation(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "bad-high", 1<<20, 3, 4, 64, "data"); err == nil {
		t.Fatal("min_size 4 with replicas 3 accepted")
	}
	if _, err := c.cl.CreateVolume(ctx, "bad-low", 1<<20, 3, -1, 64, "data"); err == nil {
		t.Fatal("negative min_size accepted")
	}
	spec, err := c.cl.CreateVolume(ctx, "def", 1<<20, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume default: %v", err)
	}
	if spec.MinSize != 2 {
		t.Fatalf("default min_size = %d, want 2", spec.MinSize)
	}
	if spec, err = c.cl.CreateVolume(ctx, "one", 1<<20, 3, 1, 64, "data"); err != nil {
		t.Fatalf("CreateVolume min_size=1: %v", err)
	}
	if spec.MinSize != 1 {
		t.Fatalf("min_size = %d, want 1", spec.MinSize)
	}
}

func TestClusterMinSizeDegraded(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 3, 1, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, 64<<10)
	fill(data, 0x33, 7)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("baseline write: %v", err)
	}

	// vm1 uses the default quorum (2/3). Exercise it while all
	// replicas are in.
	if _, err := c.cl.CreateVolume(ctx, "vm1", 64<<20, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume vm1: %v", err)
	}
	v1, err := c.cl.OpenVolume(ctx, "vm1")
	if err != nil {
		t.Fatalf("OpenVolume vm1: %v", err)
	}
	if _, err := v1.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("write vm1 healthy: %v", err)
	}

	// Two replicas out: min_size 1 keeps the volume writable and
	// readable on the single survivor.
	c.setOSDWeight(t, ctx, 1, 0)
	c.setOSDWeight(t, ctx, 2, 0)
	c.forceRefresh()

	if _, err := v.WriteAt(ctx, data, 1<<20); err != nil {
		t.Fatalf("write with min_size=1 and two osds out: %v", err)
	}
	got := make([]byte, len(data))
	for _, off := range []uint64{0, 1 << 20} {
		if n, err := v.ReadAt(ctx, got, off); err != nil || n != len(data) {
			t.Fatalf("read at %d with two osds out: n=%d err=%v", off, n, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("data mismatch at %d with two osds out", off)
		}
	}

	// vm1 under the same topology: writes must fail fast with
	// ErrDegraded (1 live target < min_size 2) while reads keep
	// answering from the survivor.
	start := time.Now()
	if _, err := v1.WriteAt(ctx, data, 1<<20); !errors.Is(err, ErrDegraded) {
		t.Fatalf("degraded write err = %v, want ErrDegraded", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("degraded write took %v, want fast fail", d)
	}
	if n, err := v1.ReadAt(ctx, got, 0); err != nil || n != len(data) {
		t.Fatalf("degraded read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("degraded read data mismatch")
	}
}

// --- out_time (Vitastor osd_out_time analogue) --------------------------

// waitFor polls cond until it holds or the deadline passes.
func waitFor(tb testing.TB, d time.Duration, cond func() bool) bool {
	tb.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func TestClusterOutTimeEvict(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	// Healthy traffic first (registry-wide weight changes degrade
	// placement for every volume, so exercise full replication now).
	data := make([]byte, 64<<10)
	fill(data, 0x5a, 1)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("healthy write: %v", err)
	}

	// Fast dwell: evict 40ms after the first transport failure, probe
	// every 10ms.
	c.cl.SetOutTime(40 * time.Millisecond)
	c.cl.probeEvery = 10 * time.Millisecond

	// Kill OSD 1 and drop its pool so the client redials and fails
	// fast (connection refused) — a transport error, not a reply.
	if err := c.daems[1].Close(); err != nil {
		t.Fatalf("kill osd 1: %v", err)
	}
	c.cl.dropPool(1)

	// Under map3 every write spans all three OSDs, so this degraded
	// write records the failure against OSD 1 while the 2/3 quorum
	// commits it.
	if _, err := v.WriteAt(ctx, data, 1<<20); err != nil {
		t.Fatalf("degraded write: %v", err)
	}

	if !waitFor(t, 5*time.Second, func() bool {
		wd, err := meta.GetOSD(ctx, c.meta, 1)
		return err == nil && wd.Weight <= 0
	}) {
		wd, _ := meta.GetOSD(ctx, c.meta, 1)
		t.Fatalf("osd 1 was not evicted: weight=%v", wd.Weight)
	}

	// Placement must exclude the out OSD everywhere.
	c.forceRefresh()
	snap, err := c.cl.Refresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := snap.Refs()[1]; ok {
		t.Fatal("evicted OSD 1 still in registry snapshot")
	}
	for pg := uint32(0); pg < 64; pg++ {
		for _, id := range snap.Row(pg) {
			if id == 1 {
				t.Fatalf("out OSD 1 in placement row at pg %d", pg)
			}
		}
	}

	// Data written before the eviction stays readable through the
	// surviving replicas.
	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 0); err != nil || n != len(data) {
		t.Fatalf("degraded read after eviction: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch after eviction")
	}
}

func TestClusterOutTimeFlapNoEvict(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	c.cl.SetOutTime(60 * time.Second) // generous dwell
	c.cl.probeEvery = 10 * time.Millisecond

	// A transport failure the OSD recovers from within the dwell.
	c.cl.noteFailure(1)
	if !c.cl.failing(1) {
		t.Fatal("failure not tracked")
	}
	if c.cl.hasOut(1) {
		t.Fatal("evicted before the dwell elapsed")
	}
	// Any protocol answer proves liveness: the clock resets.
	c.cl.noteSuccess(1)
	if c.cl.failing(1) {
		t.Fatal("failure clock not cleared by success")
	}
	time.Sleep(100 * time.Millisecond)
	if c.cl.hasOut(1) {
		t.Fatal("flapping OSD was evicted")
	}
	wd, err := meta.GetOSD(context.Background(), c.meta, 1)
	if err != nil {
		t.Fatal(err)
	}
	if wd.Weight <= 0 {
		t.Fatal("flapping OSD weight was zeroed")
	}
}

func TestClusterOutTimeRestore(t *testing.T) {
	dir := t.TempDir()
	m, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	daem := func(id int32, addr string) *osd.Daemon {
		d, err := osd.New(osd.Options{
			ID: id, Addr: addr, DataDir: filepath.Join(dir, fmt.Sprintf("osd%d", id)), Meta: m,
		})
		if err != nil {
			t.Fatal(err)
		}
		return d
	}
	d0, d1, d2 := daem(0, "127.0.0.1:0"), daem(1, "127.0.0.1:0"), daem(2, "127.0.0.1:0")
	var wg sync.WaitGroup
	for _, d := range []*osd.Daemon{d0, d1, d2} {
		wg.Add(1)
		go func() { defer wg.Done(); _ = d.Run(ctx) }()
	}
	t.Cleanup(func() {
		for _, d := range []*osd.Daemon{d0, d1, d2} {
			_ = d.Close()
		}
		wg.Wait()
	})

	cl := NewCluster(m)
	cl.probeEvery = 10 * time.Millisecond
	t.Cleanup(func() { _ = cl.Close() })
	if !waitFor(t, 5*time.Second, func() bool {
		ls, err := meta.ListOSDs(ctx, m)
		return err == nil && len(ls) == 3
	}) {
		t.Fatal("OSDs did not register")
	}
	// Publish map3 (the daemons fence; the prober does not need maps).
	if err := cl.MapEpoch(ctx, 1, []byte(map3)); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Refresh(ctx); err != nil {
		t.Fatal(err)
	}

	// Evict OSD 1: it must stop answering (probes use fresh
	// connections), so kill the daemon first, then record failures
	// past the dwell.
	if err := d1.Close(); err != nil {
		t.Fatalf("close osd 1: %v", err)
	}
	cl.SetOutTime(40 * time.Millisecond)
	cl.noteFailure(1)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		cl.noteFailure(1)
		time.Sleep(20 * time.Millisecond)
	}
	if !waitFor(t, 5*time.Second, func() bool {
		wd, err := meta.GetOSD(ctx, m, 1)
		return err == nil && wd.Weight <= 0
	}) {
		t.Fatal("osd 1 was not evicted")
	}

	// Start a replacement that registers at the same id: the prober
	// pings the address from meta, the replacement answers, and the
	// weight returns to 1.
	addr := d1.Addr()
	repl := daem(1, addr)
	rwg := sync.WaitGroup{}
	rwg.Add(1)
	go func() { defer rwg.Done(); _ = repl.Run(ctx) }()
	t.Cleanup(func() {
		_ = repl.Close()
		rwg.Wait()
	})
	if !waitFor(t, 10*time.Second, func() bool {
		wd, err := meta.GetOSD(ctx, m, 1)
		return err == nil && wd.Weight > 0
	}) {
		wd, _ := meta.GetOSD(ctx, m, 1)
		t.Fatalf("evicted OSD was not restored: weight=%v", wd.Weight)
	}
	if cl.hasOut(1) {
		t.Fatal("OSD still marked out after restore")
	}
}
