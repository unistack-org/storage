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
	"github.com/unistack-org/storage/pkg/sdsnet"
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
	dir   string // temp dir holding meta/ and osdN/ data dirs
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
	c := &cluster{t: t, meta: m, dir: dir}
	cctx, cancel := context.WithCancel(context.Background())
	var runs []chan error
	t.Cleanup(func() {
		cancel()
		for _, d := range c.daems {
			if d == nil {
				continue
			}
			d.Close()
		}
		// Wait for every Run to return (its defers close the store and
		// listener) before t.TempDir() removes the data dirs; re-running
		// Run here would recreate files during removal and fail on Linux.
		for _, rc := range runs {
			<-rc
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
		runs = append(runs, errc)
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
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 0, 3, 0, 64, "data"); err != nil {
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
	if err := v.WriteZeroes(ctx, 0, 1<<20, true); err != nil {
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

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 0, 64, "data"); err != nil {
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

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 0, 64, "data"); err != nil {
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

	if _, err := c.cl.CreateVolume(ctx, "bad-high", 1<<20, 0, 3, 4, 64, "data"); err == nil {
		t.Fatal("min_size 4 with replicas 3 accepted")
	}
	if _, err := c.cl.CreateVolume(ctx, "bad-low", 1<<20, 0, 3, -1, 64, "data"); err == nil {
		t.Fatal("negative min_size accepted")
	}
	spec, err := c.cl.CreateVolume(ctx, "def", 1<<20, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatalf("CreateVolume default: %v", err)
	}
	if spec.MinSize != 2 {
		t.Fatalf("default min_size = %d, want 2", spec.MinSize)
	}
	if spec, err = c.cl.CreateVolume(ctx, "one", 1<<20, 0, 3, 1, 64, "data"); err != nil {
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

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 1, 64, "data"); err != nil {
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
	if _, err := c.cl.CreateVolume(ctx, "vm1", 64<<20, 0, 3, 0, 64, "data"); err != nil {
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

	// vm1 under the same topology, in SOFT mode: writes must fail fast
	// with ErrDegraded (1 live target < min_size 2) while reads keep
	// answering from the survivor. (The cluster default is hard mode —
	// "client waits" — exercised by the hard-semantics tests below.)
	c.cl.SetHardSemantics(false)
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

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 0, 64, "data"); err != nil {
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
	c.cl.setProbeEvery(10 * time.Millisecond)

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
	c.cl.setProbeEvery(10 * time.Millisecond)

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
	cl.setProbeEvery(10 * time.Millisecond)
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

// --- hard/soft semantics (the Vitastor "client waits" lesson) ----------

// restartOSD relaunches OSD id on the address it last served (its
// on-disk state and meta record, including the operator weight,
// survive the restart) and drops the client's cached pool so the next
// request redials.
func (c *cluster) restartOSD(tb testing.TB, id int32) {
	tb.Helper()
	ctx := context.Background()
	addr := c.daems[id].Addr()
	// Shut the old daemon down first: its listener must be released
	// before the replacement binds the same address, and any pooled
	// connection the client still holds must stop serving (a live old
	// daemon would answer writes to files deleted from disk behind its
	// back, landing them in unlinked inodes).
	c.daems[id].Close()
	c.daems[id] = nil
	d, err := osd.New(osd.Options{
		ID: id, Addr: addr,
		DataDir: filepath.Join(c.dir, fmt.Sprintf("osd%d", id)),
		Meta:    c.meta,
	})
	if err != nil {
		tb.Fatalf("osd %d restart: %v", id, err)
	}
	c.daems[id] = d
	runErr := make(chan error, 1)
	cctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)
	go func() { runErr <- d.Run(cctx) }()
	c.cl.dropPool(id)
	if !waitFor(tb, 5*time.Second, func() bool {
		select {
		case err := <-runErr:
			tb.Fatalf("osd %d restart died: %v", id, err)
			return true
		default:
		}
		if _, err := meta.GetOSD(ctx, c.meta, id); err != nil {
			return false
		}
		// BootedAt has second granularity, so probe the listener
		// itself: the replacement daemon answers only once it is up
		// on the address the old one released.
		pp := sdsnet.NewPool(addr, "vol")
		pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
		err := pp.Ping(pctx)
		pcancel()
		pp.Close()
		return err == nil
	}) {
		tb.Fatalf("osd %d did not come back on %s", id, addr)
	}
}

func TestClusterHardSemanticsWaits(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !c.cl.HardSemantics() {
		t.Fatal("hard semantics must be the default")
	}
	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, 64<<10)
	fill(data, 0x77, 5)
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatalf("baseline write: %v", err)
	}

	// Two replicas out (weight 0): 1 live target < min_size 2. Kill
	// the daemons too, so a write cannot slip through before the
	// client observes the weights.
	c.setOSDWeight(t, ctx, 1, 0)
	c.setOSDWeight(t, ctx, 2, 0)
	c.daems[1].Close()
	c.daems[2].Close()
	c.cl.dropPool(1)
	c.cl.dropPool(2)
	c.forceRefresh()

	// Hard mode: the write parks — it must NOT return an error, not
	// even ErrDegraded, and it must not return at all.
	werr := make(chan error, 1)
	go func() {
		_, err := v.WriteAt(ctx, data, 1<<20)
		werr <- err
	}()
	select {
	case err := <-werr:
		t.Fatalf("hard-mode degraded write returned early: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Heal: weight back, daemons back on their ports. The parked
	// write must complete on its own, with no call from the user.
	c.setOSDWeight(t, ctx, 1, 1)
	c.setOSDWeight(t, ctx, 2, 1)
	c.restartOSD(t, 1)
	c.restartOSD(t, 2)
	select {
	case err := <-werr:
		if err != nil {
			t.Fatalf("write after recovery: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hard-mode write did not resume after recovery")
	}

	// The data is where the parked write put it, readable on the
	// healed cluster.
	data2 := make([]byte, len(data))
	fill(data2, 0x88, 6)
	if _, err := v.WriteAt(ctx, data2, 1<<20); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 1<<20); err != nil || n != len(data) {
		t.Fatalf("post-recovery read: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data2) {
		t.Fatal("post-recovery data mismatch")
	}
}

func TestClusterHardSemanticsCancel(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	c.setOSDWeight(t, ctx, 1, 0)
	c.setOSDWeight(t, ctx, 2, 0)
	c.forceRefresh()

	// The wait is not a trap: expiring the caller's context (a VM
	// teardown, a hung I/O the orchestrator gives up on) releases it
	// promptly with the caller's error.
	cctx, ccancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer ccancel()
	start := time.Now()
	_, err = v.WriteAt(cctx, make([]byte, 4096), 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled hard write err = %v, want context.DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("cancel took %v, want prompt release", d)
	}
}

func TestClusterReadWaitsForTransports(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := c.cl.CreateVolume(ctx, "vm0", 64<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	data := make([]byte, 64<<10)
	fill(data, 0x91, 7)
	if _, err := v.WriteAt(ctx, data, 1<<20); err != nil {
		t.Fatalf("baseline write: %v", err)
	}

	// Soft mode with every replica dead mid-flight: the map still
	// lists three targets (no weight changes), so placement is
	// healthy — only the transports are gone. Soft mode surfaces the
	// last transport error; the caller (NBD/VDUSE queue) decides.
	c.cl.SetHardSemantics(false)
	c.daems[0].Close()
	c.daems[1].Close()
	c.daems[2].Close()
	c.cl.dropPool(0)
	c.cl.dropPool(1)
	c.cl.dropPool(2)
	start := time.Now()
	_, err = v.ReadAt(ctx, make([]byte, len(data)), 1<<20)
	if err == nil {
		t.Fatal("read from three dead OSDs succeeded")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("soft read took %v, want fast fail", d)
	}

	// Hard mode: the same read parks while the OSDs are gone and
	// completes by itself once they return — the guest sees a paused
	// disk, not an I/O error.
	c.cl.SetHardSemantics(true)
	rerr := make(chan error, 1)
	go func() {
		_, err := v.ReadAt(ctx, make([]byte, len(data)), 1<<20)
		rerr <- err
	}()
	select {
	case err := <-rerr:
		t.Fatalf("hard-mode read returned early: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	c.restartOSD(t, 0)
	c.restartOSD(t, 1)
	c.restartOSD(t, 2)
	select {
	case err := <-rerr:
		if err != nil {
			t.Fatalf("read after recovery: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("hard-mode read did not resume after recovery")
	}

	// And it came back with the right bytes.
	got := make([]byte, len(data))
	if n, err := v.ReadAt(ctx, got, 1<<20); err != nil || n != len(data) {
		t.Fatalf("read after recovery: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("read-after-recovery data mismatch")
	}
}
