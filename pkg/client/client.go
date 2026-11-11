// Package client is the cluster volume client: it assembles a Volume from
// cluster metadata (volume spec + crush map epochs + OSD registry), keeps
// an atomically-swapped placement snapshot per epoch, and serves the
// volume's byte range over the OSDs the map selects.
//
// Data path (perVitastor/Ceph object addressing): a volume is a flat range
// of fixed 4 MiB objects, OID = index (OID 0 is reserved and never used).
//   - reads: hash -> pg -> row, try replicas in preference order; a
//     missing object (thin provisioning) zero-fills and stops (the
//     authoritative replica for an object is the first target, so a
//     NotFound from it means zeros),
//   - writes: fan out to every replica of the pg, acknowledge on a write
//     quorum (2 of 3), stragglers finish in the background,
//   - epoch fencing: replicas answer StatusStale when the client works
//     off a stale map; the client refreshes placement from meta and
//     retries.
//
// Metadata plane: pkg/meta (dir-backed today, etcd-shaped interface).
// MapEpoch pushes a new crush map epoch to meta, which every OSD (and
// every client Watch) picks up.
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/unistack-org/storage/crush"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/qos"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// Errors returned by the client.
var (
	// ErrNotEnoughOSDs means the registry has fewer live OSDs than the
	// volume's replica count.
	ErrNotEnoughOSDs = errors.New("client: not enough live osds")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("client: closed")
	// ErrNoSpace means a replica's filesystem refused the write
	// (ENOSPC/EDQUOT). It wraps syscall.ENOSPC so the NBD/VDUSE front
	// ends hand the guest a real ENOSPC, and it is deliberately NOT a
	// quorum failure: the refusing OSD answered (it is healthy — it
	// said "no"), hard semantics must not retry a full disk forever,
	// and the OSD must not be noted as failed (that would mark a
	// perfectly alive node out of the cluster on every full disk).
	ErrNoSpace = fmt.Errorf("client: no space left on device: %w", syscall.ENOSPC)
)

// Sentinels distinguishing a write that could not reach its quorum
// (the replicas were reachable but refused/failed, or too few answered)
// from a placement that cannot even be attempted (ErrDegraded). Hard
// semantics waits on both; soft semantics surfaces both. They are
// unexported: callers branch on hard/soft, not on these.
var (
	errQuorumFailed = errors.New("client: quorum failed")
	errQuorumLost   = errors.New("client: quorum lost")
)

// isQuorumFailure reports a write that reached its targets but could not
// commit a quorum — the transient class hard semantics rides out.
func isQuorumFailure(err error) bool {
	return errors.Is(err, errQuorumFailed) || errors.Is(err, errQuorumLost)
}

// Cluster is the shared connection/placement context for one process
// talking to one cluster: a meta store, one pool set per OSD, and the
// live placement epoch. Dialing is per target and lazy; pools are reused
// across every volume of the cluster.
type Cluster struct {
	meta meta.Store

	poolsMu sync.Mutex
	pools   map[int32]*sdsnet.Pool // per OSD id, keyed for reuse

	// poolOpts customises every per-OSD connection pool (see
	// SetPoolOptions). The default caps a pool at defaultPoolLimit
	// connections: an unlimited pool dials one TCP connection (and
	// keeps 256KB of bufio on it) per in-flight op, so a deep-QD
	// benchmark with 4x128 requests per target used ~1GB of pool
	// buffers. Deep queues are served by the NBD read workers and the
	// OSD itself, not by per-op client connections, so a few fat
	// connections per OSD are enough.
	poolOpts func(*sdsnet.Pool)

	// localDial, when set via SetLocalDial, is installed on every
	// per-OSD pool as Pool.Local: an in-process transport for OSDs
	// served by this same process (see SetLocalDial).
	localDial func(osdID int32) (net.Conn, error)

	// localOSDs (SetLocalOSDs) marks the OSD ids reachable in-process;
	// read-replica selection prefers them over network targets. Stored
	// as an atomic snapshot so the read hot path stays lock-free.
	localOSDs atomic.Pointer[map[int32]bool]

	// poolTLS, when set via SetTLS, makes every per-OSD pool dial over
	// mutual TLS (tlsconf.Config.ClientConfig).
	poolTLS *tls.Config

	// refreshMin bounds how often Refresh may rebuild placement.
	refreshMin time.Duration
	lastRef    atomic.Int64 // unix nanos

	mu    sync.RWMutex
	snap  *placement.Snapshot
	epoch uint32
	// Refresh cache: compiled-state inputs. A refresh that finds the
	// same map bytes and the same registry reuses the snapshot, so a
	// periodic refresh of a static cluster costs two meta reads and
	// nothing else (no map recompile, no pg table rebuild).
	mapSrc   []byte
	regCache placement.Registry
	closed   atomic.Bool // read under mu, failMu (prober) and hot paths

	// out_time dwell (osd_out_time analogue): an OSD that fails to
	// answer keeps its weight for outTime; only if it stays down that
	// long is it set weight 0 (out of placement, triggering rebalance).
	// An OSD that flaps back within the dwell never triggers churn.
	// Stored as nanos so the hot path (noteFailure) and prober read
	// them lock-free while SetOutTime / tests may write at any time.
	outTimeNs    atomic.Int64
	probeEveryNs atomic.Int64 // prober tick (tests shrink it)
	failMu       sync.Mutex
	failures     map[int32]time.Time // transport failures, first-seen
	outs         map[int32]bool      // evicted, awaiting recovery
	proberOn     bool
	probeStopCh  chan struct{}

	// hardMode is the Vitastor "client waits" default: when a volume is
	// below its write quorum (fewer live targets than min_size) or a
	// write loses its quorum to transport failures, the call blocks —
	// refreshing placement on capped backoff — until the cluster
	// recovers, instead of surfacing an I/O error. A guest VM sees a
	// paused disk (its own filesystem/timeout logic handles it) and
	// resumes cleanly when the OSD returns; it never sees a spurious
	// write error that fsck cannot undo. SetHardSemantics(false)
	// restores fast-fail (ErrDegraded / quorum errors) for soft users
	// that prefer to observe degradation and decide for themselves.
	// Atomic so the hot path (writeChunk/readChunk) reads it lock-free.
	hardMode atomic.Bool
}

// SetRefreshInterval overrides the placement refresh interval. The
// default (5ms) targets fast failure detection on a live meta watcher;
// benchmarks and static clusters can raise it to cut meta traffic
// (every interval costs a couple of meta reads on the dir backend).
func (c *Cluster) SetRefreshInterval(d time.Duration) {
	if d > 0 {
		c.refreshMin = d
	}
}

// NewCluster wires a client to the cluster metadata. Placement is built
// on first use (Refresh/OpenVolume).
func NewCluster(s meta.Store) *Cluster {
	c := &Cluster{
		meta:       s,
		pools:      map[int32]*sdsnet.Pool{},
		refreshMin: 5 * time.Millisecond,
		failures:   map[int32]time.Time{},
		outs:       map[int32]bool{},
	}
	c.outTimeNs.Store(int64(600 * time.Second))
	c.probeEveryNs.Store(int64(15 * time.Second))
	c.hardMode.Store(true) // Vitastor "client waits" default
	c.SetPoolLimit(defaultPoolLimit)
	return c
}

// SetHardSemantics controls what a write does when the volume is below
// its write quorum. hard=true (the default) blocks until the cluster
// recovers: a degraded volume pauses the caller (refreshing placement
// on capped backoff, ctx-cancelable) rather than returning an error, so
// a guest VM sees a stalled disk it can ride out. hard=false restores
// fast-fail: a degraded write returns ErrDegraded immediately and a
// lost quorum returns a quorum error, for callers that want to detect
// degradation and act on it. Safe to call at any time.
func (c *Cluster) SetHardSemantics(hard bool) { c.hardMode.Store(hard) }

// HardSemantics reports the current hard/soft setting.
func (c *Cluster) HardSemantics() bool { return c.hardMode.Load() }

// SetOutTime sets the dwell (osd_out_time analogue): an OSD whose
// requests fail transport-wise stays in placement for this long before
// being marked out (weight 0). 0 disables auto-eviction entirely
// (noout). Safe to call at any time; takes effect from the next
// failure. The dwell prevents rebalance churn when an OSD flaps: it
// returns to service unseen if it answers a probe before the dwell
// elapses.
func (c *Cluster) SetOutTime(d time.Duration) {
	if d >= 0 {
		c.outTimeNs.Store(int64(d))
	}
}

// setProbeEvery tunes the prober tick (tests shrink it; production
// keeps the 15s default).
func (c *Cluster) setProbeEvery(d time.Duration) {
	if d > 0 {
		c.probeEveryNs.Store(int64(d))
	}
}

// Meta exposes the metadata store (for volume creation, map epochs).
func (c *Cluster) Meta() meta.Store { return c.meta }

// dropPool closes and forgets the cached pool for an OSD so the next
// use redials (a restarted OSD listens on a new address; a dead one
// must not keep a poisoned connection).
func (c *Cluster) dropPool(id int32) {
	c.poolsMu.Lock()
	if pp, ok := c.pools[id]; ok {
		pp.Close()
		delete(c.pools, id)
	}
	c.poolsMu.Unlock()
}

// noteFailure records a transport-level failure against an OSD and,
// once the failure has persisted past the out_time dwell, marks the
// OSD out (weight 0) so placement drops it. Callers must only report
// connection-level failures (dial/IO), never protocol replies: a
// StatusStale answer proves the OSD is alive.
func (c *Cluster) noteFailure(id int32) {
	outTime := time.Duration(c.outTimeNs.Load())
	if outTime <= 0 {
		return // noout
	}
	now := time.Now()
	c.failMu.Lock()
	if c.outs[id] {
		c.failMu.Unlock()
		return // already evicted
	}
	first, seen := c.failures[id]
	if !seen {
		c.failures[id] = now
		c.ensureProber()
		c.failMu.Unlock()
		return
	}
	if now.Sub(first) < outTime {
		c.failMu.Unlock()
		return // still inside the dwell
	}
	c.outs[id] = true
	delete(c.failures, id)
	c.failMu.Unlock()
	c.dropPool(id)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := meta.SetOSDWeight(ctx, c.meta, id, 0); err != nil {
		c.failMu.Lock()
		delete(c.outs, id) // meta write failed: retry next failure
		c.failMu.Unlock()
	}
}

// noteSuccess clears a pending (in-dwell) failure clock for an OSD that
// answered. Evicted OSDs are handled by the prober, not the data path.
func (c *Cluster) noteSuccess(id int32) {
	c.failMu.Lock()
	if _, seen := c.failures[id]; seen && !c.outs[id] {
		delete(c.failures, id)
	}
	c.failMu.Unlock()
}

// ensureProber starts the background prober if any OSD is in the dwell
// queue. Call with failMu held.
func (c *Cluster) ensureProber() {
	if c.proberOn || c.closed.Load() {
		return
	}
	c.proberOn = true
	c.probeStopCh = make(chan struct{})
	stop := c.probeStopCh
	go c.probeLoop(stop)
}

// probeLoop watches in-dwell failures and evicted OSDs: an in-dwell
// failure whose probe answers is cleared; an evicted OSD whose probe
// answers gets its weight restored (the map it returns to is the one
// placement builds from the registry, so the rebalance target is the
// current map, not the pre-out one).
func (c *Cluster) probeLoop(stop <-chan struct{}) {
	tick := time.NewTicker(time.Duration(c.probeEveryNs.Load()))
	defer tick.Stop()
	for {
		select {
		case <-stop:
			c.failMu.Lock()
			c.proberOn = false
			c.failMu.Unlock()
			return
		case <-tick.C:
			c.probeOnce(stop)
		}
	}
}

// probeOnce probes every OSD with outstanding failures/evictions once.
func (c *Cluster) probeOnce(stop <-chan struct{}) {
	c.failMu.Lock()
	type item struct {
		id    int32
		evict bool // in-dwell vs already evicted
	}
	var todo []item
	for id := range c.failures {
		if !c.outs[id] {
			todo = append(todo, item{id, false})
		}
	}
	for id := range c.outs {
		todo = append(todo, item{id, true})
	}
	c.failMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	osds, err := meta.ListOSDs(ctx, c.meta)
	if err != nil {
		return
	}
	addr := map[int32]string{}
	for _, o := range osds {
		addr[o.ID] = o.ControlAddr()
	}
	for _, it := range todo {
		select {
		case <-stop:
			return
		default:
		}
		a, ok := addr[it.id]
		if !ok {
			continue
		}
		// Fresh connection: the cached pool for a dead OSD is stale,
		// and a restarted OSD listens on a new address.
		pingErr := func() error {
			pp := sdsnet.NewPool(a, "vol")
			defer pp.Close()
			pctx, pcancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer pcancel()
			return pp.Ping(pctx)
		}()
		c.failMu.Lock()
		if pingErr == nil {
			if it.evict {
				delete(c.outs, it.id)
			}
			delete(c.failures, it.id)
		} else if !it.evict {
			if first, seen := c.failures[it.id]; seen && time.Since(first) >= time.Duration(c.outTimeNs.Load()) {
				// Dwell elapsed between data-path failures: evict now.
				c.outs[it.id] = true
				delete(c.failures, it.id)
			}
		}
		restore := pingErr == nil && it.evict
		evict := pingErr != nil && !it.evict && c.outs[it.id]
		c.failMu.Unlock()
		if evict {
			ectx, ecancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := meta.SetOSDWeight(ectx, c.meta, it.id, 0); err != nil {
				c.failMu.Lock()
				delete(c.outs, it.id)
				c.failures[it.id] = time.Now()
				c.failMu.Unlock()
			}
			ecancel()
		}
		if restore {
			c.dropPool(it.id)
			rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
			// Restoring into the registry the OSD re-registered with:
			// re-read current weight first; only flip a 0 back to 1.
			if info, err := meta.GetOSD(rctx, c.meta, it.id); err == nil && info.Weight <= 0 {
				_ = meta.SetOSDWeight(rctx, c.meta, it.id, 1)
			}
			rcancel()
		}
	}
}

// hasOut reports whether the prober has evicted an OSD.
func (c *Cluster) hasOut(id int32) bool {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	return c.outs[id]
}

// failing reports whether an OSD has an in-dwell (not yet evicted)
// failure clock running.
func (c *Cluster) failing(id int32) bool {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	_, ok := c.failures[id]
	return ok
}

// Close shuts every pool down.
func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil
	}
	c.closed.Store(true)
	c.mu.Unlock()
	c.failMu.Lock()
	if c.proberOn {
		close(c.probeStopCh)
		c.probeStopCh = nil
		c.proberOn = false
	}
	c.failMu.Unlock()
	c.poolsMu.Lock()
	for _, pp := range c.pools {
		pp.Close()
	}
	c.pools = nil
	c.poolsMu.Unlock()
	return nil
}

// defaultPoolLimit caps connections per OSD. Deep queues live in the
// NBD read workers and the OSD, not in per-op client connections; 64
// connections per target sustain ~10x the throughput a single host
// generates (each connection serialises at roughly 14K ops/s), while an
// uncapped pool dials one connection (256KB of bufio) per in-flight op
// and cost ~1GB of RSS under a QD512 benchmark.
const defaultPoolLimit = 64

// SetPoolLimit caps connections per OSD target (0 = unlimited). Calls
// in flight beyond the cap queue inside the pool. Set before first use:
// pools already dialed keep their previous limit.
func (c *Cluster) SetPoolLimit(n int) {
	c.poolOpts = func(pp *sdsnet.Pool) { pp.Limit = n }
}

// SetTLS makes every per-OSD connection pool dial over mutual TLS (use
// tlsconf.Config.ClientConfig; the ServerName is taken from each OSD
// address). Set before first use: pools already dialed keep plain
// connections.
func (c *Cluster) SetTLS(cfg *tls.Config) {
	c.poolsMu.Lock()
	c.poolTLS = cfg
	c.poolsMu.Unlock()
}

// SetLocalDial installs an in-process transport for OSD connections:
// for every OSD the cluster later dials, f returns a connected stream
// (typically a socketpair whose peer is served by a sdsnet.Server in
// this same process). A nil f (the default) means plain TCP/TLS. The
// wire protocol, fencing, QoS and metrics are identical on both paths;
// only the bytes-in-transit differ, which removes the TCP stack from
// the hot path of a single-process node. Set before first use: pools
// already dialed keep their transport.
func (c *Cluster) SetLocalDial(f func(osdID int32) (net.Conn, error)) {
	c.poolsMu.Lock()
	c.localDial = f
	c.poolsMu.Unlock()
}

// SetLocalOSDs marks the OSD ids that are reachable in-process (the
// same daemon hosts them; pairs SetLocalDial). Read-replica selection
// prefers these targets: an in-process read skips the network
// entirely, so a compute node that also runs OSDs serves reads from
// its own disk when the object lives there (the Vitastor local_reads
// idea, "локальная → наименее загруженная"). Safe to call at any time;
// nil/empty clears the preference.
func (c *Cluster) SetLocalOSDs(ids []int32) {
	if len(ids) == 0 {
		c.localOSDs.Store(nil)
		return
	}
	m := make(map[int32]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	c.localOSDs.Store(&m)
}

// PoolStats returns the connection-pool occupancy of every OSD target
// dialed so far: OSD id -> (idle, in-use) connections. It is a cheap
// snapshot of the cached pools (no dialing, no network), the export seam
// for a metrics endpoint that reports client-side connection pressure
// per target.
// PoolOps returns the cumulative connection-checkout count of every
// dialed OSD pool. Monotonic, so tests (and debug tooling) can
// attribute traffic to targets even when it races with straggler
// replica writes.
func (c *Cluster) PoolOps() map[int32]int64 {
	c.poolsMu.Lock()
	defer c.poolsMu.Unlock()
	out := make(map[int32]int64, len(c.pools))
	for id, pp := range c.pools {
		out[id] = pp.Ops()
	}
	return out
}

func (c *Cluster) PoolStats() map[int32][2]int {
	c.poolsMu.Lock()
	defer c.poolsMu.Unlock()
	out := make(map[int32][2]int, len(c.pools))
	for id, pp := range c.pools {
		idle, inUse := pp.Stats()
		out[id] = [2]int{idle, inUse}
	}
	return out
}

// ProbeOSDs pings every registered OSD over a fresh connection (the
// cached pools may be poisoned for a dead peer) and returns the registry
// it probed plus the per-OSD outcome: nil error means the OSD answered.
// metaErr is the registry read failure (probes is then empty). This is
// the health-check view used by the admin status API; the data-path
// failure tracking (noteFailure / out_time) is independent of it.
func (c *Cluster) ProbeOSDs(ctx context.Context, timeout time.Duration) (infos []meta.OSDInfo, probes map[int32]error, metaErr error) {
	infos, err := meta.ListOSDs(ctx, c.meta)
	if err != nil {
		return nil, nil, err
	}
	probes = make(map[int32]error, len(infos))
	for _, info := range infos {
		pp := sdsnet.NewPool(info.ControlAddr(), "vol")
		pctx, cancel := context.WithTimeout(ctx, timeout)
		err := pp.Ping(pctx)
		cancel()
		pp.Close()
		probes[info.ID] = err
	}
	return infos, probes, nil
}

// DiskUsage is one OSD's raw capacity report (bytes).
type DiskUsage struct {
	Usage uint64 // bytes stored in object files
	Free  uint64 // filesystem free bytes
	Total uint64 // filesystem total bytes
}

// ProbeOSDsDisk is ProbeOSDs plus a StatFS round-trip per answering OSD:
// disk usage/free/total for every OSD in the cluster, not just those
// hosted in this process (the metrics export's capacity view). probes
// keeps the ping outcome (nil = alive); disk only carries entries for
// OSDs that answered StatFS (an old server pings fine but answers
// StatusNotSupp, which simply omits it).
func (c *Cluster) ProbeOSDsDisk(ctx context.Context, timeout time.Duration) (infos []meta.OSDInfo, probes map[int32]error, disk map[int32]DiskUsage, metaErr error) {
	infos, err := meta.ListOSDs(ctx, c.meta)
	if err != nil {
		return nil, nil, nil, err
	}
	probes = make(map[int32]error, len(infos))
	disk = make(map[int32]DiskUsage, len(infos))
	for _, info := range infos {
		pp := sdsnet.NewPool(info.ControlAddr(), "vol")
		pctx, cancel := context.WithTimeout(ctx, timeout)
		pingErr := pp.Ping(pctx)
		probes[info.ID] = pingErr
		if pingErr == nil {
			if usage, free, total, e := pp.StatDisk(pctx); e == nil {
				disk[info.ID] = DiskUsage{Usage: usage, Free: free, Total: total}
			}
		}
		cancel()
		pp.Close()
	}
	return infos, probes, disk, nil
}

// pool returns (dialing on demand) the connection pool for one OSD.
func (c *Cluster) pool(id int32, addr string) (*sdsnet.Pool, error) {
	c.poolsMu.Lock()
	if c.pools == nil {
		c.poolsMu.Unlock()
		return nil, ErrClosed
	}
	pp, ok := c.pools[id]
	if !ok {
		pp = sdsnet.NewPool(addr, "vol")
		if c.poolOpts != nil {
			c.poolOpts(pp)
		}
		pp.TLS = c.poolTLS
		if c.localDial != nil {
			dial := c.localDial
			pp.Local = func() (net.Conn, error) { return dial(id) }
		}
		c.pools[id] = pp
	}
	c.poolsMu.Unlock()
	if !ok {
		pp.SetEpoch(c.epochSnapshot())
	}
	return pp, nil
}

func (c *Cluster) epochSnapshot() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.epoch
}

// poolLoad reports the in-flight op count of the cached pool for an
// OSD (0 when no pool exists yet — an idle target is the cheapest
// first choice). Lock-free: only reads the pools map pointer-safe.
func (c *Cluster) poolLoad(id int32) int64 {
	c.poolsMu.Lock()
	pp := c.pools[id]
	c.poolsMu.Unlock()
	if pp == nil {
		return 0
	}
	return pp.Inflight()
}

// Registry builds a placement registry from the OSD records in meta:
// weight 0 (or missing) OSDs are treated as out.
func (c *Cluster) Registry(ctx context.Context) (placement.Registry, error) {
	infos, err := meta.ListOSDs(ctx, c.meta)
	if err != nil {
		return nil, err
	}
	reg := placement.Registry{}
	for _, info := range infos {
		if info.Weight <= 0 {
			continue
		}
		reg[info.ID] = placement.Ref{OSD: info.ID, Addr: info.Addr, Export: "vol"}
	}
	return reg, nil
}

// Refresh rebuilds the placement snapshot from the latest map epoch in
// meta. Cheap (a table rebuild is ~ms) and throttled so a burst of
// StatusStale replies costs one rebuild.
func (c *Cluster) Refresh(ctx context.Context) (*placement.Snapshot, error) {
	c.mu.RLock()
	snap, closed := c.snap, c.closed.Load()
	c.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if snap != nil && time.Since(time.Unix(0, c.lastRef.Load())) < c.refreshMin {
		return snap, nil // fresh enough for this burst
	}
	c.lastRef.Store(time.Now().UnixNano())

	rec, err := meta.LatestMap(ctx, c.meta)
	if err != nil {
		if snap != nil {
			return snap, nil // keep serving off the last good map
		}
		return nil, fmt.Errorf("client: no crush map in meta: %w", err)
	}
	reg, err := c.Registry(ctx)
	if err != nil {
		if snap != nil {
			return snap, nil
		}
		return nil, err
	}
	if len(reg) == 0 {
		return nil, ErrNotEnoughOSDs
	}
	c.mu.RLock()
	snap, epoch, mapSrc, regCache := c.snap, c.epoch, c.mapSrc, c.regCache
	c.mu.RUnlock()
	if snap != nil && epoch == rec.Epoch && bytes.Equal(mapSrc, rec.Map) && regEqual(regCache, reg) {
		return snap, nil // nothing moved: keep the compiled snapshot
	}
	ns, err := c.compile(rec, reg, snap)
	if err != nil {
		if snap != nil {
			return snap, nil
		}
		return nil, err
	}
	c.mu.Lock()
	c.snap, c.epoch = ns, rec.Epoch
	c.mapSrc = append(c.mapSrc[:0], rec.Map...)
	c.regCache = reg
	c.mu.Unlock()
	// Raise the fence on every pool so new connections carry the epoch.
	c.poolsMu.Lock()
	for _, pp := range c.pools {
		pp.SetEpoch(rec.Epoch)
	}
	c.poolsMu.Unlock()
	return ns, nil
}

// compile builds a snapshot for rec. opts are taken from an existing
// snapshot when available (volumes share the cluster's pg layout).
func (c *Cluster) compile(rec meta.MapRecord, reg placement.Registry, tmpl *placement.Snapshot) (*placement.Snapshot, error) {
	opts := placement.Options{PGNum: 1024, Replicas: 3}
	if tmpl != nil {
		opts = placement.Options{PGNum: tmpl.PGNum, Replicas: tmpl.Replicas, Rule: tmpl.Rule}
	}
	return placement.Compile(rec.Epoch, rec.Map, reg, opts)
}

// VolumeHealth is the placement verdict for one volume at the current
// map epoch: how many of its replicas the live placement provides and
// the quorum width it needs to write, plus the per-PG state census
// (the vitastor_pg_count view: every placement group classified by the
// live width of its row).
type VolumeHealth struct {
	LiveTargets int // live OSDs in this volume's placement rows
	MinSize     int // write quorum width (degraded when LiveTargets < MinSize)
	Writable    bool
	PGs         int // placement groups in the volume's layout
	PGsOK       int // rows with the full replica set live
	PGsDegraded int // rows reduced below the replica count but >= min_size
	PGsDown     int // rows below min_size (writes to these PGs fail)
}

// VolumeHealth compiles the volume's placement at the current epoch and
// counts the live targets (weight > 0, registered OSDs) its rows
// resolve to. It is a map-placement view: it does not ping OSDs (the
// status API combines it with ProbeOSDs).
func (c *Cluster) VolumeHealth(ctx context.Context, spec meta.VolumeSpec) (VolumeHealth, error) {
	rec, err := meta.LatestMap(ctx, c.meta)
	if err != nil {
		return VolumeHealth{}, fmt.Errorf("client: volume health %q: no crush map: %w", spec.Name, err)
	}
	reg, err := c.Registry(ctx)
	if err != nil {
		return VolumeHealth{}, err
	}
	rep := spec.Replicas
	if rep < 1 {
		rep = 3
	}
	snap, err := placement.Compile(rec.Epoch, rec.Map, reg, placement.Options{
		PGNum: uint32(spec.PGNum), Replicas: rep, Rule: spec.Rule,
	})
	if err != nil {
		return VolumeHealth{}, fmt.Errorf("client: volume health %q: %w", spec.Name, err)
	}
	ms := spec.MinSize
	if ms <= 0 {
		ms = rep/2 + 1
	}
	// Census over every PG: the same map and registry through the same
	// rule, so a healthy row is the common case; each row is classified
	// by its live width (full / degraded >= min_size / below min_size).
	live := 0
	h := VolumeHealth{MinSize: ms, PGs: int(snap.PGNum)}
	for pg := 0; pg < h.PGs; pg++ {
		n := 0
		for _, id := range snap.Row(uint32(pg)) {
			if id >= 0 {
				n++
			}
		}
		switch {
		case n >= rep:
			h.PGsOK++
		case n >= ms:
			h.PGsDegraded++
		default:
			h.PGsDown++
		}
		if pg == 0 {
			live = n // Row(0) answers "how many replicas does a write find"
		}
	}
	h.LiveTargets = live
	h.Writable = live >= ms
	return h, nil
}

// MapEpoch publishes a new crush map epoch: the map is stored in meta// under maps/<epoch> (OSDs raise their fence and clients rebuild on
// watch). Format is auto-detected by placement (text/binary/json).
func (c *Cluster) MapEpoch(ctx context.Context, epoch uint32, src []byte) error {
	format := "text"
	switch {
	case len(src) > 0 && src[0] == '{':
		format = "json"
	case len(src) >= 4 && binary.LittleEndian.Uint32(src[:4]) == crush.Magic:
		format = "binary"
	}
	b, err := json.Marshal(meta.MapRecord{Epoch: epoch, Format: format, Map: src})
	if err != nil {
		return err
	}
	_, err = c.meta.Put(ctx, meta.PrefixMaps, fmt.Sprint(epoch), b, 0)
	return err
}

// CreateVolume registers a volume in meta. pgNum must be a power of two;
// the volume is thin: nothing is written to OSDs until first touch.
// minSize is the write quorum width (Vitastor pg_minsize / Ceph
// min_size): commits need minSize replica acks, and a volume whose
// placement row has fewer than minSize live targets is degraded —
// writes fail fast instead of waiting on unreachable replicas. 0
// selects the default Replicas/2+1. quota caps the bytes the volume may
// allocate on the OSDs (thin provisioning, plan M0.4); 0 = the logical
// size (uncapped beyond it).
func (c *Cluster) CreateVolume(ctx context.Context, name string, size uint64, quota uint64, replicas, minSize, pgNum int, rule string) (meta.VolumeSpec, error) {
	return c.createVolume(ctx, name, size, quota, replicas, minSize, pgNum, rule, "", "")
}

// CreateVolumeInPool creates a volume inside a pool: every placement
// parameter left at 0/"" (replicas, minSize, pgNum, rule) is taken from
// the pool record; an explicitly passed value wins. The QoS template
// comes from the pool when the caller does not set one. The resolved
// values are stored on the volume spec (with the pool name recorded),
// so later pool edits never move existing data.
func (c *Cluster) CreateVolumeInPool(ctx context.Context, pool, name string, size uint64, quota uint64, replicas, minSize, pgNum int, rule string) (meta.VolumeSpec, error) {
	rec, err := meta.GetPool(ctx, c.meta, pool)
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: pool %q: %w", pool, err)
	}
	if replicas == 0 {
		replicas = rec.Replicas
	}
	if minSize == 0 {
		minSize = rec.MinSize
	}
	if pgNum == 0 {
		pgNum = rec.PGNum
	}
	if rule == "" {
		rule = rec.Rule
	}
	return c.createVolume(ctx, name, size, quota, replicas, minSize, pgNum, rule, rec.QoSTemplate, pool)
}

func (c *Cluster) createVolume(ctx context.Context, name string, size uint64, quota uint64, replicas, minSize, pgNum int, rule, qosTemplate, pool string) (meta.VolumeSpec, error) {
	if name == "" || size == 0 {
		return meta.VolumeSpec{}, errors.New("client: volume name and size required")
	}
	if pgNum == 0 || bits.OnesCount(uint(pgNum)) != 1 {
		return meta.VolumeSpec{}, fmt.Errorf("client: pg_num %d must be a power of two", pgNum)
	}
	if replicas < 1 {
		replicas = 3
	}
	if minSize == 0 {
		minSize = replicas/2 + 1
	}
	if minSize < 1 || minSize > replicas {
		return meta.VolumeSpec{}, fmt.Errorf("client: volume %q: min_size %d out of range 1..%d", name, minSize, replicas)
	}
	if quota > size {
		return meta.VolumeSpec{}, fmt.Errorf("client: volume %q: quota %d exceeds size %d", name, quota, size)
	}
	// Every volume gets a private, cluster-wide-unique object-id range, so
	// two volumes placed on the same OSD can never address the same
	// object. The range is [OIDBase+1, OIDBase+NumObjects]; one extra id is
	// reserved to keep the OIDBase+N arithmetic inside it.
	base, err := meta.NextOIDBase(ctx, c.meta, object.NumObjects(size)+1)
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: create volume %q: %w", name, err)
	}
	spec := meta.VolumeSpec{
		Name:        name,
		Size:        size,
		Quota:       quota,
		OIDBase:     base,
		OIDShift:    object.SizeShift,
		Replicas:    replicas,
		MinSize:     minSize,
		Rule:        rule,
		PGNum:       pgNum,
		QoSTemplate: qosTemplate,
		Pool:        pool,
	}
	if _, err := meta.PutVolume(ctx, c.meta, spec); err != nil {
		return meta.VolumeSpec{}, err
	}
	return spec, nil
}

// ListVolumes returns the volume specs registered in the cluster.
func (c *Cluster) ListVolumes(ctx context.Context) ([]meta.VolumeSpec, error) {
	return meta.ListVolumes(ctx, c.meta)
}

// ResizeVolume grows a volume online (grow-only; shrinking is refused
// — see meta.ResizeVolume). The extra object ids come from the
// cluster-wide allocator and are appended as extents, so no other
// volume can be holding them. Open handles on this client pick the new
// size up through the volume watch; clients on other hosts refresh
// within their watch latency too. The quota deliberately does not grow
// with the volume: raising the cap is a separate admin decision.
func (c *Cluster) ResizeVolume(ctx context.Context, name string, newSize uint64) (meta.VolumeSpec, error) {
	if name == "" || newSize == 0 {
		return meta.VolumeSpec{}, errors.New("client: volume name and size required")
	}
	return meta.ResizeVolume(ctx, c.meta, name, newSize)
}

// SetVolumeQoS updates a volume's QoS fields (inline rates and/or
// QoSTemplate) in the cluster meta. Open volumes pick the change up on
// their next Open; running NBD targets are updated by calling
// Volume.SetQoS with the returned spec. Passing an all-zero, no-template
// spec makes the volume unlimited (the admin class).
func (c *Cluster) SetVolumeQoS(ctx context.Context, name string, spec meta.VolumeSpec) (meta.VolumeSpec, error) {
	// CAS mutate: a concurrent resize (which rewrites the record to
	// append extents) cannot be lost, and QoS fields are re-applied to
	// the fresh record on a racing edit.
	cur, err := meta.MutateVolume(ctx, c.meta, name, func(cur *meta.VolumeSpec) bool {
		cur.QoSTemplate = spec.QoSTemplate
		cur.ReadIOPS = spec.ReadIOPS
		cur.ReadBPS = spec.ReadBPS
		cur.WriteIOPS = spec.WriteIOPS
		cur.WriteBPS = spec.WriteBPS
		return true
	})
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: set qos volume %q: %w", name, err)
	}
	return cur, nil
}

// SetVolumeFlags updates the serving flags of a volume (read-only,
// shared, owner) with a CAS mutate, the same way SetVolumeQoS edits
// QoS. Exports re-read the spec through their meta watch, so flipping
// ReadOnly live takes effect on the next export creation (an existing
// NBD transmission keeps the flags it negotiated at handshake).
func (c *Cluster) SetVolumeFlags(ctx context.Context, name string, readOnly, shared bool, owner string) (meta.VolumeSpec, error) {
	cur, err := meta.MutateVolume(ctx, c.meta, name, func(cur *meta.VolumeSpec) bool {
		cur.ReadOnly = readOnly
		cur.Shared = shared
		cur.Owner = owner
		return true
	})
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: set flags volume %q: %w", name, err)
	}
	return cur, nil
}

// SetVolumeQuota updates a volume's allocation cap (0 = cap at the
// logical size, the default). Open volumes pick the change up through
// the same live-config watch that carries QoS edits. Shrinking the cap
// never touches existing data: it only refuses the next first-touch
// write when usage is already at or above the new cap.
func (c *Cluster) SetVolumeQuota(ctx context.Context, name string, quota uint64) (meta.VolumeSpec, error) {
	var tooBig uint64
	cur, err := meta.MutateVolume(ctx, c.meta, name, func(cur *meta.VolumeSpec) bool {
		if quota > cur.Size {
			tooBig = cur.Size
			return false
		}
		cur.Quota = quota
		return true
	})
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: set quota volume %q: %w", name, err)
	}
	if tooBig != 0 || quota > cur.Size {
		return meta.VolumeSpec{}, fmt.Errorf("client: set quota volume %q: quota %d exceeds volume size %d", name, quota, cur.Size)
	}
	return cur, nil
}

// PutQoSClass registers (or replaces) a named QoS class.
func (c *Cluster) PutQoSClass(ctx context.Context, rec meta.QoSClassRecord) error {
	if rec.Name == "" {
		return errors.New("client: qos class name required")
	}
	if _, err := meta.PutQoSClass(ctx, c.meta, rec); err != nil {
		return fmt.Errorf("client: put qos class %q: %w", rec.Name, err)
	}
	return nil
}

// CreatePool registers a pool, filling parameters left at 0/"" from the
// compiled cluster defaults (the -pg-num/-replicas/-rule the daemon
// booted with, i.e. the current placement snapshot). A pool whose pg_num
// exceeds the cluster default is refused: a volume may never carry more
// PGs than the snapshot offers, so such a pool could never place one.
// Overwriting an existing pool is refused (meta.ErrConflict): delete and
// re-create to change a policy.
func (c *Cluster) CreatePool(ctx context.Context, rec meta.PoolRecord) (meta.PoolRecord, error) {
	snap, err := c.Refresh(ctx)
	if err != nil {
		return meta.PoolRecord{}, fmt.Errorf("client: create pool %q: %w", rec.Name, err)
	}
	if rec.Replicas == 0 {
		rec.Replicas = snap.Replicas
	}
	if rec.MinSize == 0 {
		rec.MinSize = rec.Replicas/2 + 1
	}
	if rec.PGNum == 0 {
		rec.PGNum = int(snap.PGNum)
	}
	if rec.Rule == "" {
		rec.Rule = snap.Rule
	}
	if _, err := meta.PutPool(ctx, c.meta, rec, int(snap.PGNum)); err != nil {
		return meta.PoolRecord{}, fmt.Errorf("client: create pool %q: %w", rec.Name, err)
	}
	return rec, nil
}

// GetPool reads one pool record.
func (c *Cluster) GetPool(ctx context.Context, name string) (meta.PoolRecord, error) {
	return meta.GetPool(ctx, c.meta, name)
}

// ListPools returns every registered pool.
func (c *Cluster) ListPools(ctx context.Context) ([]meta.PoolRecord, error) {
	return meta.ListPools(ctx, c.meta)
}

// DeletePool removes a pool. It is refused while volumes still
// reference the pool: the name is their recorded provenance, and a pool
// that disappears under live volumes makes their policy unanswerable.
func (c *Cluster) DeletePool(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("client: pool name required")
	}
	n, err := meta.CountPoolVolumes(ctx, c.meta, name)
	if err != nil {
		return fmt.Errorf("client: delete pool %q: %w", name, err)
	}
	if n > 0 {
		return fmt.Errorf("client: pool %q: %d volume(s) still reference it: %w", name, n, meta.ErrPoolInUse)
	}
	if err := meta.DeletePool(ctx, c.meta, name); err != nil {
		return fmt.Errorf("client: delete pool %q: %w", name, err)
	}
	return nil
}

// ListQoSClasses returns every registered QoS class.
func (c *Cluster) ListQoSClasses(ctx context.Context) ([]meta.QoSClassRecord, error) {
	return meta.ListQoSClasses(ctx, c.meta)
}

// DeleteQoSClass removes a class (volumes referencing it become
// unlimited on their next open).
func (c *Cluster) DeleteQoSClass(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("client: qos class name required")
	}
	if err := meta.DeleteQoSClass(ctx, c.meta, name); err != nil {
		return fmt.Errorf("client: delete qos class %q: %w", name, err)
	}
	return nil
}

// DeleteVolume removes a volume's registration. It removes the
// volume record, releases the volume's references on shared snapshot
// ranges (clones, clones.go protocol: refs drop AFTER the record is
// gone, so a clone racing the delete re-reads a chain without the
// ranges it pre-shared), writes a vacuum tombstone (meta.PrefixVacuum)
// covering every range the volume ends up owning (unshared live
// range, resize extents, snapshot generations no clone references —
// deleting a volume discards its snapshots with it, ADR), and
// removes the allocation map. An in-flight writer keeps writing to
// objects it can no longer register — those writes land in the
// tombstoned range and are removed by the vacuum. A crash between
// the record removal and the tombstone leaks the ranges (capacity
// only, never a clone's data); leaking a range into a vacuum a clone
// still reads would destroy the clone, so the order is deliberate.
func (c *Cluster) DeleteVolume(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("client: volume name required")
	}
	spec, err := meta.GetVolume(ctx, c.meta, name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil // idempotent: nothing to unregister
		}
		return fmt.Errorf("client: delete volume %q: %w", name, err)
	}
	if spec.OIDBase != 0 {
		if err := meta.DeleteVolume(ctx, c.meta, name); err != nil {
			return fmt.Errorf("client: delete volume %q: %w", name, err)
		}
		ranges, err := meta.DropGenRanges(ctx, c.meta, meta.VolumeGarbage(spec))
		if err != nil {
			return fmt.Errorf("client: delete volume %q: release refs: %w", name, err)
		}
		// A fully cloned volume (all ranges shared) leaves nothing to
		// vacuum: no tombstone at all.
		if len(ranges) > 0 {
			rec := meta.VacuumRecord{
				Name:      name,
				OIDBase:   ranges[0].Base,
				NumObjs:   ranges[0].Count,
				DeletedAt: time.Now().Unix(),
			}
			for _, r := range ranges[1:] {
				rec.Extents = append(rec.Extents, meta.OIDExtent{Base: r.Base, Count: r.Count})
			}
			if _, err := meta.PutVacuum(ctx, c.meta, rec); err != nil {
				return fmt.Errorf("client: delete volume %q: vacuum tombstone: %w", name, err)
			}
		}
	} else if err := meta.DeleteVolume(ctx, c.meta, name); err != nil {
		return fmt.Errorf("client: delete volume %q: %w", name, err)
	}
	// The allocation map rides with the volume record (accounting
	// garbage is worse than object garbage: it is keyed by name and a
	// recreated volume of the same name would inherit it).
	if err := meta.DeleteAllocMap(ctx, c.meta, name); err != nil {
		return fmt.Errorf("client: delete volume %q allocmap: %w", name, err)
	}
	return nil
}

// OpenVolume opens a registered volume. The cluster placement must
// already exist (call Refresh first); the volume's PG layout is clamped
// to the cluster snapshot's (a shared table serves all volumes).
func (c *Cluster) OpenVolume(ctx context.Context, name string) (*Volume, error) {
	spec, err := meta.GetVolume(ctx, c.meta, name)
	if err != nil {
		return nil, fmt.Errorf("client: open volume %q: %w", name, err)
	}
	if spec.QoSTemplate != "" {
		// Resolve the user tier: a missing class is unlimited (the
		// admin class), matching the record's contract.
		if rec, err := meta.GetQoSClass(ctx, c.meta, spec.QoSTemplate); err == nil {
			spec.ReadIOPS, spec.ReadBPS, spec.WriteIOPS, spec.WriteBPS = rec.Rates()
		} else if !errors.Is(err, meta.ErrNotFound) {
			return nil, fmt.Errorf("client: open volume %q: qos class %q: %w", name, spec.QoSTemplate, err)
		}
	}
	if spec.OIDShift != object.SizeShift {
		return nil, fmt.Errorf("client: volume %q oid_shift %d unsupported (want %d)", name, spec.OIDShift, object.SizeShift)
	}
	if spec.OIDBase == 0 {
		return nil, fmt.Errorf("client: volume %q has no object-id range (created by an older version; recreate it)", name)
	}
	snap, err := c.Refresh(ctx)
	if err != nil {
		return nil, err
	}
	pgNum := uint32(spec.PGNum)
	if pgNum == 0 || pgNum > snap.PGNum {
		pgNum = snap.PGNum
	}
	if spec.Replicas > maxReplicas {
		return nil, fmt.Errorf("client: volume %q: replicas %d > %d", name, spec.Replicas, maxReplicas)
	}
	w := spec.MinSize
	if w == 0 {
		w = spec.Replicas/2 + 1
	}
	if w < 1 || w > spec.Replicas {
		return nil, fmt.Errorf("client: volume %q: min_size %d out of range 1..%d", name, w, spec.Replicas)
	}
	v := &Volume{
		cluster: c,
		spec:    spec,
		pgNum:   pgNum,
		mask:    pgNum - 1,
		w:       w,
		limits: qos.NewPair(spec.ReadIOPS, spec.ReadBPS,
			spec.WriteIOPS, spec.WriteBPS),
		qosTemplate: spec.QoSTemplate,
		qosApplied: qosRates{spec.ReadIOPS, spec.ReadBPS,
			spec.WriteIOPS, spec.WriteBPS},
		quota: spec.Quota,
	}
	if v.quota == 0 {
		v.quota = spec.Size
	}
	v.cur.Store(&spec) // live geometry: resize swaps this pointer
	if v.quota < spec.Size {
		// Capped volume: load the allocation bitmap so the quota check
		// has its counter. A brand-new volume has no record yet (zeros).
		rec, err := meta.GetAllocMap(ctx, c.meta, name, v.nbits())
		if err != nil {
			v.limits.Close()
			return nil, fmt.Errorf("client: open volume %q: %w", name, err)
		}
		v.alloc = rec.Bits
	}
	// Live QoS: watch the volumes prefix so an admin changing this
	// volume's spec (SetVolumeQoS, or a class edit) is picked up on the
	// already-open handle without a remount.
	v.startQoSWatch()
	return v, nil
}

// startQoSWatch launches goroutines that re-read the volume's spec
// whenever the volumes prefix changes, and whenever the QoS class it
// references changes, swapping in the new limits. A store without Watch
// support leaves the volume on its open-time limits (SetQoS still works
// for an in-process caller).
func (v *Volume) startQoSWatch() {
	ctx, cancel := context.WithCancel(context.Background())
	v.stopQoSWatch = cancel
	watch := func(prefix string, match func(key string) bool) {
		w, err := v.cluster.meta.Watch(ctx, prefix)
		if err != nil {
			return
		}
		go func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.Events():
					if !ok {
						return
					}
					if match(ev.Key) {
						v.reloadQoS(ctx)
					}
				}
			}
		}()
	}
	watch(meta.PrefixVolumes, func(key string) bool { return key == v.spec.Name })
	// A class edit must reach every volume bound to it, not only the
	// volume record's own key.
	watch(meta.PrefixQoSClasses, func(key string) bool {
		v.mu.Lock()
		tpl := v.qosTemplate
		v.mu.Unlock()
		return tpl != "" && key == tpl
	})
}

// reloadQoS re-reads this volume's spec (resolving its QoS template)
// and applies the resulting limits via SetQoS. The volume's placement
// spec stays frozen (OIDBase, replicas...): only QoS fields are live. A
// vanished volume is left as-is: the caller is responsible for closing
// the handle.
func (v *Volume) reloadQoS(ctx context.Context) {
	spec, err := meta.GetVolume(ctx, v.cluster.meta, v.spec.Name)
	if err != nil {
		return
	}
	// Resolve the template into the local spec BEFORE publishing it:
	// cur.Store shares this address with every reader, so mutating
	// spec afterwards would race with (and corrupt) live reads.
	if spec.QoSTemplate != "" {
		if rec, err := meta.GetQoSClass(ctx, v.cluster.meta, spec.QoSTemplate); err == nil {
			spec.ReadIOPS, spec.ReadBPS, spec.WriteIOPS, spec.WriteBPS = rec.Rates()
		}
	}
	v.mu.Lock()
	v.qosTemplate = spec.QoSTemplate
	// Live geometry: an online resize (Size, OID extents) travels on
	// the same watch. Swap the whole spec atomically so readers see a
	// consistent size/extents pair.
	v.cur.Store(&spec)
	// A quota edit travels on the same watch: refresh the cap live.
	// Growing the cap needs no bitmap work; shrinking takes effect on
	// the next first-touch charge (existing allocations stay readable).
	if spec.Quota == 0 {
		v.quota = spec.Size
	} else {
		v.quota = spec.Quota
	}
	v.mu.Unlock()
	v.SetQoS(spec)
}

// qosRates is the effective rate quadruple behind a limits pair:
// comparable, so SetQoS can tell a real reconfiguration from a watch
// event that changed nothing about the rates.
type qosRates [4]uint64

// Volume is an open volume: a byte range over replicated objects. Safe
// for concurrent use.
type Volume struct {
	cluster *Cluster
	// spec is the open-time record; Name is immutable and safe to read
	// directly. The live geometry (size and the OID ranges behind it)
	// lives in cur: an online resize swaps it atomically, so readers
	// never observe a torn size/extent pair.
	spec meta.VolumeSpec
	cur  atomic.Pointer[meta.VolumeSpec]

	pgNum uint32
	mask  uint32
	w     int // write quorum

	mu sync.Mutex // guards limits and qosTemplate (SetQoS swaps under load)

	// limits enforces the volume's QoS class (nil = unlimited, the
	// admin class). Every client that opens the volume builds the same
	// pair from the spec, so the ceiling travels with the volume no
	// matter who touches it; the OSD-side aggregate adds the per-disk
	// ceiling on top.
	limits *qos.Pair

	// qosTemplate is the class name this volume resolves its limits
	// from ("" = inline rates only). Kept under mu because a class
	// edit must rebind every open volume bound to it.
	qosTemplate string

	// qosApplied is the rate quadruple the current limits pair was
	// built from; under mu, so SetQoS can skip no-op rebuilds that
	// would donate a fresh burst.
	qosApplied qosRates

	// stopQoSWatch cancels the goroutine that re-reads this volume's
	// spec from meta and calls SetQoS when it changes, so an admin
	// changing QoS (or quota) on an already-open volume takes effect
	// live.
	stopQoSWatch context.CancelFunc

	// quota is the effective allocation cap in bytes (spec.Quota,
	// falling back to spec.Size); under mu, refreshed by the live-
	// config watch. A cap >= Size imposes no restriction beyond the
	// logical volume size.
	quota uint64

	// allocMu guards alloc, this handle's cached copy of the volume's
	// allocation bitmap (nil = not loaded yet). The bitmap is the
	// quota's allocation counter: one bit per object, set = the object
	// holds data on the OSDs. Writes consult the cache first (rewrites
	// of allocated objects skip meta entirely); first touches go
	// through meta CAS so concurrent writers cannot lose bits.
	allocMu sync.Mutex
	alloc   []byte

	// Read micro-batching (volume_batch.go). batchMu guards the
	// per-OSD collector map; batchClosed is set by Close so late
	// senders fall back to the single-op path instead of creating a
	// collector on a dead volume. batchDisabled latches off after a
	// peer without negotiated batch support answers, so a mixed-version
	// cluster pays one probe, not a fallback per read.
	batchMu       sync.Mutex
	batchGroups   map[int32]*batchGroup
	batchClosed   bool
	batchWG       sync.WaitGroup
	batchDisabled atomic.Bool
}

// SetQoS replaces the volume's limits at runtime (admin
// reconfiguration after meta.PutVolume of a new spec). Passing an
// all-zero spec removes limits. In-flight ops keep their charge on the
// pair they entered: the old pair is deliberately NOT closed, so a
// reconfiguration never fails queued writers (a blocked op drains
// under the old rate, the next ops pay the new one).
func (v *Volume) SetQoS(spec meta.VolumeSpec) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// Re-applying the same rates is a no-op: the meta watch fires on any
	// change to the volume record, and rebuilding the pair would hand the
	// volume a fresh full burst every time. Only a real rate change
	// replaces the bucket.
	if v.qosApplied == (qosRates{spec.ReadIOPS, spec.ReadBPS, spec.WriteIOPS, spec.WriteBPS}) {
		return
	}
	v.qosApplied = qosRates{spec.ReadIOPS, spec.ReadBPS, spec.WriteIOPS, spec.WriteBPS}
	v.limits = qos.NewPair(spec.ReadIOPS, spec.ReadBPS, spec.WriteIOPS, spec.WriteBPS)
}

// limitsSnapshot returns the current pair under the lock: SetQoS may
// swap it while an op is in flight.
func (v *Volume) limitsSnapshot() *qos.Pair {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.limits
}

// QoSSustained returns the sustained rates (iops, bps) each QoS axis of
// this volume is currently holding, as the limiters measured them over
// their last refill window. This is the exporter seam for "what speed
// is this volume actually allowed/using" — (0,0) for an unlimited
// volume. Rates, not totals: Prometheus computes the totals from
// counters.
func (v *Volume) QoSSustained() (readIOPS uint32, readBPS uint64, writeIOPS uint32, writeBPS uint64) {
	lim := v.limitsSnapshot()
	if lim == nil {
		return 0, 0, 0, 0
	}
	readIOPS, readBPS = lim.Read.Stats()
	writeIOPS, writeBPS = lim.Write.Stats()
	return readIOPS, readBPS, writeIOPS, writeBPS
}

// Close releases the volume's QoS waiters and stops its live-QoS watch.
// Open volumes in long-running processes (NBD targets) are closed on
// shutdown; forgetting to close a volume leaks nothing but leaves its
// blocked writers parked until their contexts die.
func (v *Volume) Close() error {
	if v.stopQoSWatch != nil {
		v.stopQoSWatch()
	}
	v.closeBatchers()
	v.mu.Lock()
	lim := v.limits
	v.limits = nil
	v.mu.Unlock()
	lim.Close() // nil-safe
	return nil
}

// Spec returns the volume's registered spec.
func (v *Volume) Spec() meta.VolumeSpec { return *v.cur.Load() }

// Size is the logical volume size.
func (v *Volume) Size() uint64 { return v.cur.Load().Size }

// oid maps an object index to its object id: the base range
// OIDBase+1 .. OIDBase+BaseObjs, then the resize extents (see
// meta.VolumeSpec.Lookup). Because ranges are allocated cluster-wide,
// no two volumes ever share an object id on a shared OSD. The lookup
// is false for indices beyond the reserved range, which only a spec
// torn mid-resize could produce.
func (v *Volume) oid(index uint64) (object.OID, bool) { return v.cur.Load().Lookup(index) }

// nbits is the volume's object count (= allocation bitmap size).
func (v *Volume) nbits() int { return int(object.NumObjects(v.cur.Load().Size)) }

// quotaCap returns the effective allocation cap (mu-guarded, live
// refreshed by the config watch).
func (v *Volume) quotaCap() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.quota
}

// chargeQuota books index against the volume's allocation bitmap before
// its first write. The bitmap is maintained for EVERY volume (capped or
// not): it is the snapshot layer's source of truth (a snapshot copies it
// into the frozen generation, Usage() popcounts it). For a capped volume
// a first touch that would exceed the cap is refused with ErrNoSpace
// (which reaches a guest as ENOSPC through the NBD/VDUSE errno mapping);
// for an uncapped volume the charge just sets the bit. A cached-bit hit
// keeps the hot path free of meta traffic.
func (v *Volume) chargeQuota(ctx context.Context, index uint64) error {
	if index >= uint64(v.nbits()) {
		return nil // safety: nothing to charge beyond the volume
	}
	capped := v.quotaCap() < v.Size()
	v.allocMu.Lock()
	cached := v.alloc
	v.allocMu.Unlock()
	if cached != nil && allocBitLocal(cached, int(index)) {
		return nil // already allocated on the OSDs: free space
	}
	overQuota := false
	bits, _, err := meta.MutateAlloc(ctx, v.cluster.meta, v.spec.Name, v.nbits(), func(b []byte) bool {
		if allocBitLocal(b, int(index)) {
			return false
		}
		if capped && uint64(meta.AllocMapPopcount(b))*object.Size >= v.quotaCap() {
			overQuota = true
			return false
		}
		setAllocBitLocal(b, int(index))
		return true
	})
	if err != nil {
		return fmt.Errorf("client: volume %q quota: %w", v.spec.Name, err)
	}
	v.allocMu.Lock()
	v.alloc = bits
	v.allocMu.Unlock()
	if overQuota {
		return fmt.Errorf("client: volume %q is at its quota (%d bytes used): %w",
			v.spec.Name, v.quotaCap(), ErrNoSpace)
	}
	return nil
}

// releaseQuota gives allocation room back after a sparse TRIM cleared a
// whole object (the allocation granularity): only full-object clears
// release, matching "TRIM frees space" for guest discard. Partial
// object zeroes keep the object allocated, as they do on the OSDs. The
// bitmap bit is cleared for every volume (it feeds Usage and snapshots);
// the cached-bit check keeps the hot path free of meta traffic.
func (v *Volume) releaseQuota(ctx context.Context, index uint64) {
	if index >= uint64(v.nbits()) {
		return
	}
	v.allocMu.Lock()
	cached := v.alloc
	v.allocMu.Unlock()
	if cached != nil && !allocBitLocal(cached, int(index)) {
		return // nothing booked locally for this object
	}
	bits, _, err := meta.MutateAlloc(ctx, v.cluster.meta, v.spec.Name, v.nbits(), func(b []byte) bool {
		return clearAllocBitLocal(b, int(index))
	})
	if err != nil {
		return // accounting only: the trim itself already succeeded
	}
	v.allocMu.Lock()
	v.alloc = bits
	v.allocMu.Unlock()
}

// Usage returns the bytes currently allocated on the OSDs for this
// volume (quota accounting granularity: allocated objects x object
// size). It reads the shared bitmap, so it reflects other writers.
func (v *Volume) Usage(ctx context.Context) (uint64, error) {
	rec, err := meta.GetAllocMap(ctx, v.cluster.meta, v.spec.Name, v.nbits())
	if err != nil {
		return 0, fmt.Errorf("client: volume %q usage: %w", v.spec.Name, err)
	}
	return uint64(meta.AllocMapPopcount(rec.Bits)) * object.Size, nil
}

// Quota returns the volume's effective allocation cap in bytes.
func (v *Volume) Quota() uint64 { return v.quotaCap() }

// Bitmap copies are LSB-first per 8-byte group; tiny local helpers keep
// the client free of meta's unexported bit functions. They are
// length-tolerant: a cached bitmap can be shorter than the live nbits()
// after an online resize (the watch refreshes the cached copy shortly
// after the size), and bits beyond the stored prefix are unallocated.
func allocBitLocal(b []byte, i int) bool {
	if i>>3 >= len(b) {
		return false
	}
	return b[i>>3]&(1<<(i&7)) != 0
}
func setAllocBitLocal(b []byte, i int) bool {
	if i>>3 >= len(b) {
		return false
	}
	w := &b[i>>3]
	m := byte(1) << (i & 7)
	if *w&m != 0 {
		return false
	}
	*w |= m
	return true
}
func clearAllocBitLocal(b []byte, i int) bool {
	if i>>3 >= len(b) {
		return false
	}
	w := &b[i>>3]
	m := byte(1) << (i & 7)
	if *w&m == 0 {
		return false
	}
	*w &^= m
	return true
}

// pgFor maps an object index to its placement group, scaled into the
// cluster table's pg space.
func (v *Volume) pgFor(snap *placement.Snapshot, index uint64) uint32 {
	pg := uint32(((index+1)*11400714819323198485)>>32) & v.mask
	return uint32(uint64(pg) * uint64(snap.PGNum) / uint64(v.pgNum))
}

// targets returns the volume's replicas for object index in snapshot
// snap, filling scratch (callers pass a stack array so the hot path
// allocates nothing).
// ErrDegraded reports that a volume has fewer live targets than its
// min_size: writes fail fast (the data is recoverable by restoring
// replicas) instead of hanging on unreachable ones. Reads stay
// tolerant — any live replica answers identically.
var ErrDegraded = errors.New("client: volume degraded: live targets below min_size")

// orderReadRefs reorders the live replica targets of a pg into the
// read preference order (Vitastor lesson: any replica answers a read
// identically, so pick the cheapest one):
//
//  1. in-process (local) OSDs — an in-process read skips the network;
//  2. least in-flight load (Pool.Inflight of the pool dialed so far;
//     an undialed pool counts as 0, a brand-new idle target beats a
//     busy one);
//  3. target order in the pg row (the placement map's own preference;
//     the primary-by-PG head of the row acts as a stable tiebreak).
//
// The order is a preference only: readChunk falls through to the next
// replica on any failure, so a misranked (even dead) first target
// costs one failed round-trip, never correctness.
func (v *Volume) orderReadRefs(refs []placement.Ref) {
	if len(refs) < 2 {
		return
	}
	c := v.cluster
	local := c.localOSDs.Load()
	// Snapshot ranks once (one pool lookup per target), not per compare.
	var rank [maxReplicas]struct {
		local bool
		load  int64
	}
	for i, r := range refs {
		rank[i].load = c.poolLoad(r.OSD)
		if local != nil {
			rank[i].local = (*local)[r.OSD]
		}
	}
	sort.SliceStable(refs, func(i, j int) bool {
		if rank[i].local != rank[j].local {
			return rank[i].local
		}
		return rank[i].load < rank[j].load
	})
}

func (v *Volume) targets(snap *placement.Snapshot, index uint64, scratch []placement.Ref) ([]placement.Ref, error) {
	pg := v.pgFor(snap, index)
	row := snap.Row(pg)
	refs := scratch[:0]
	if len(scratch) >= len(row) {
		// len 0, cap = len(row): append fills from index 0 (a plain
		// scratch[:len(row)] would make append stack on top of it).
		refs = scratch[:0:len(row)]
	}
	for _, id := range row {
		if id < 0 {
			continue
		}
		if ref, ok := snap.LookupRef(id); ok {
			refs = append(refs, ref)
		}
	}
	if len(refs) < v.w {
		return refs, fmt.Errorf("%w: pg %d has %d live targets, min_size %d", ErrDegraded, pg, len(refs), v.w)
	}
	return refs, nil
}

// ReadAt reads len(p) bytes of the volume at off. Reads go to the first
// live replica that answers (replicas are byte-identical at quorum
// commit); a missing object is thin and reads as zeros.
func (v *Volume) ReadAt(ctx context.Context, p []byte, off uint64) (int, error) {
	if size := v.Size(); off >= size {
		return 0, fmt.Errorf("client: read at %d past volume end %d", off, size)
	}
	if size := v.Size(); uint64(len(p)) > size-off {
		p = p[:size-off]
	}
	// One guest request = one op token on the read axis, charged before
	// the (possibly multi-chunk, multi-replica) work below.
	if lim := v.limitsSnapshot(); lim != nil {
		if err := lim.Read.Wait(ctx, uint64(len(p))); err != nil {
			if errors.Is(err, qos.ErrClosed) {
				return 0, fmt.Errorf("client: volume %q closed", v.spec.Name)
			}
			return 0, err
		}
	}
	total := 0
	for total < len(p) {
		cur := off + uint64(total)
		index, inObj := object.Split(cur)
		n := len(p) - total
		if uint64(n) > object.Size-inObj {
			n = int(object.Size - inObj)
		}
		if err := v.readChunk(ctx, p[total:total+n], index, inObj); err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (v *Volume) readChunk(ctx context.Context, p []byte, index, inObj uint64) error {
	oid, ok := v.oid(index)
	if !ok {
		return fmt.Errorf("client: volume %q: object index %d outside the reserved ranges", v.spec.Name, index)
	}
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return err
	}
	hard := v.cluster.hardMode.Load()
	var backoff time.Duration
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for attempt := 0; attempt < 2; attempt++ {
		var scratch [maxReplicas]placement.Ref
		refs, terr := v.targets(snap, index, scratch[:])
		if terr != nil && len(refs) == 0 {
			if !hard || !errors.Is(terr, ErrDegraded) {
				return terr
			}
			// Hard mode: every replica of this pg is out of the
			// registry. A read has no authoritative answer to fall
			// back on, so (like Vitastor) wait for one to return.
			// Below min_size but above zero, reads stay tolerant:
			// any live replica answers identically.
			for {
				var ok bool
				if backoff, ok = v.cluster.sleep(ctx, timer, backoff, hardBackoffMin, hardBackoffMax); !ok {
					return ctx.Err()
				}
				s, rerr := v.cluster.forceRefresh(ctx)
				if rerr != nil {
					return rerr
				}
				snap = s
				if refs, terr = v.targets(s, index, scratch[:]); len(refs) > 0 {
					break
				}
			}
		}
		// Any replica answers a read identically (quorum writes,
		// byte-identical copies): reorder by local → least loaded →
		// map order. The fall-through loop below keeps full tolerance.
		v.orderReadRefs(refs)
		lastErr := error(terr)
		var sawMissing bool
	refresh:
		for _, ref := range refs {
			pp, err := v.cluster.pool(ref.OSD, ref.Addr)
			if err != nil {
				return err
			}
			// Micro-batched read: coalesces concurrent chunk reads for
			// this OSD into one round-trip. handled=false means "not
			// applicable" (batching off/snapshot volume/legacy peer/
			// closing) and the single-op path below answers verbatim.
			n, err, handled := v.batchRead(ctx, ref, uint64(oid), inObj, p)
			if !handled {
				n, err = pp.Read(ctx, uint64(oid), inObj, p)
			}
			switch {
			case err == nil:
				v.cluster.noteSuccess(ref.OSD)
				if n < len(p) {
					// Short read = sparse tail inside the object.
					for i := n; i < len(p); i++ {
						p[i] = 0
					}
				}
				return nil
			case sdsnet.IsObjectMissing(err), isStatus(err, sdsnet.StatusEOF):
				// Thin/sparse on THIS replica: not authoritative. A
				// quorum write commits on W replicas and stragglers
				// finish in the background, so a lagging replica can
				// report an object missing that the quorum already
				// stored. Fall through to the next replica; only if
				// every replica says missing is the region zeros.
				sawMissing = true
				lastErr = nil
				continue
			case isStale(err):
				lastErr = err
				break refresh // refresh placement, try the fresh map
			default:
				if _, isStatus := sdsnet.AsError(err); isStatus {
					v.cluster.noteSuccess(ref.OSD) // it answered
				} else {
					v.cluster.noteFailure(ref.OSD)
				}
				lastErr = err
				continue // next replica
			}
		}
		if sawMissing {
			// All replicas report the region absent. On a volume with
			// snapshots the live range only covers writes after the
			// last generation swap: walk the frozen chain
			// newest-to-oldest before declaring zeros.
			if v.hasSnapshots() {
				found, serr := v.readSnapshotChain(ctx, p, index, inObj)
				if serr != nil {
					return serr
				}
				if found {
					return nil
				}
			}
			// Authoritative zeros (thin provisioning).
			for i := range p {
				p[i] = 0
			}
			return nil
		}
		if !isStale(lastErr) {
			// Hard mode: every target existed in placement but none
			// answered (transport deaths the map has not caught up
			// with, e.g. before the out_time dwell evicts them).
			// There is no authoritative answer to fall back on, so
			// wait out the outage: a protocol reply means the OSD is
			// alive and the error is real — only transport failures
			// are worth waiting on.
			if hard && len(refs) > 0 {
				if _, isProtocol := sdsnet.AsError(lastErr); !isProtocol {
					next, ok := v.cluster.sleep(ctx, timer, backoff, hardBackoffMin, hardBackoffMax)
					if !ok {
						return ctx.Err()
					}
					backoff = next
					if snap, err = v.cluster.forceRefresh(ctx); err != nil {
						return err
					}
					attempt-- // waiting does not consume a stale retry
					continue
				}
			}
			return lastErr
		}
		if snap, err = v.cluster.forceRefresh(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("client: read oid %d: %w", oid, errStaleRead(uint64(oid)))
}

func errStaleRead(oid uint64) error {
	return fmt.Errorf("client: read oid %d: still stale after refresh", oid)
}

// WriteAt writes p at off, fanning out to every replica and committing
// on the write quorum (W = R/2+1).
func (v *Volume) WriteAt(ctx context.Context, p []byte, off uint64) (int, error) {
	if size := v.Size(); off+uint64(len(p)) > size {
		return 0, fmt.Errorf("client: write at %d len %d past volume end %d", off, len(p), size)
	}
	if lim := v.limitsSnapshot(); lim != nil {
		if err := lim.Write.Wait(ctx, uint64(len(p))); err != nil {
			if errors.Is(err, qos.ErrClosed) {
				return 0, fmt.Errorf("client: volume %q closed", v.spec.Name)
			}
			return 0, err
		}
	}
	total := 0
	for total < len(p) {
		cur := off + uint64(total)
		index, inObj := object.Split(cur)
		n := len(p) - total
		if uint64(n) > object.Size-inObj {
			n = int(object.Size - inObj)
		}
		if err := v.writeChunk(ctx, p[total:total+n], uint64(n), index, inObj, false, false); err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// writeChunk writes (or zeroes) one object-sized chunk. hole selects the
// zeroes mode (no payload on the wire); sparse asks the replicas to punch a
// real hole so guest TRIM vacuums space (it implies hole).
func (v *Volume) writeChunk(ctx context.Context, p []byte, n uint64, index, inObj uint64, hole, sparse bool) error {
	oid, ok := v.oid(index)
	if !ok {
		return fmt.Errorf("client: volume %q: object index %d outside the reserved ranges", v.spec.Name, index)
	}
	// Quota charge (plan M0.4): a real payload write into an object the
	// volume has not allocated yet must fit under the cap. Zeroes modes
	// do not allocate (hole punches and literal-zero writes of an
	// already-covered range never grow usage), so they skip the charge;
	// the sparse-trim path releases room separately.
	if !hole {
		if err := v.chargeQuota(ctx, index); err != nil {
			return err
		}
	}
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return err
	}
	hard := v.cluster.hardMode.Load()
	// Capped backoff for the hard-semantics wait. The timer object is
	// reused (Reset/Stop) rather than time.After, whose timers pile up
	// until they fire when the loop runs for a long time.
	var backoff time.Duration
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	// Soft mode caps total attempts (the original bounded retry); hard
	// mode loops until success, ctx cancellation, or a non-transient
	// error. A persistently stale reply is bounded in both modes: it
	// means the placement loop itself is broken, and spinning on a
	// falsified map forever would mask a bug.
	for attempt, stale := 0, 0; ; attempt++ {
		if !hard && attempt >= 3 {
			break
		}
		var scratch [maxReplicas]placement.Ref
		refs, terr := v.targets(snap, index, scratch[:])
		if terr != nil {
			if !hard || !errors.Is(terr, ErrDegraded) {
				return terr
			}
			// Hard mode: the volume is below its write quorum. Pause
			// here, watching meta, until enough replicas return —
			// the VM's I/O resumes on its own.
			backoff = 0
			if err := v.awaitQuorum(ctx, index, &snap, timer, &backoff); err != nil {
				return err
			}
			continue
		}
		err = v.quorumWrite(ctx, uint64(oid), inObj, p, n, hole, sparse, refs)
		if err == nil {
			return nil
		}
		if isStale(err) {
			if stale >= 2 {
				break
			}
			stale++
			if snap, err = v.cluster.forceRefresh(ctx); err != nil {
				return err
			}
			continue
		}
		if !hard || !isQuorumFailure(err) {
			return err
		}
		// Hard mode: the targets were in the map but the quorum did
		// not answer (transports died mid-write, e.g. an OSD process
		// vanished before the out_time dwell marked it out). Refresh
		// and retry on backoff; if the map itself marks the OSDs out,
		// targets() takes over the wait above.
		next, ok := v.cluster.sleep(ctx, timer, backoff, hardBackoffMin, hardBackoffMax)
		if !ok {
			return ctx.Err()
		}
		backoff = next
		if snap, err = v.cluster.forceRefresh(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("client: write oid %d: %w", oid, err)
}

// sleep grows a capped backoff (lo..hi on first use, doubling to hi)
// and waits for it. ok=false means ctx was cancelled during the wait.
func (c *Cluster) sleep(ctx context.Context, timer *time.Timer, cur, lo, hi time.Duration) (time.Duration, bool) {
	if cur == 0 {
		cur = lo
	} else if cur *= 2; cur > hi {
		cur = hi
	}
	timer.Reset(cur)
	select {
	case <-ctx.Done():
		return cur, false
	case <-timer.C:
	}
	return cur, true
}

// Backoff bounds for the hard-semantics wait: a refresh costs two meta
// reads, so the floor keeps meta traffic sane while the cap keeps
// recovery detection prompt (the watcher path makes a plain Refresh
// observe a healed OSD the moment meta changes; the backoff mainly
// paces the forced rebuilds).
const (
	hardBackoffMin = 10 * time.Millisecond
	hardBackoffMax = 200 * time.Millisecond
)

// awaitQuorum blocks until pg for index has min_size live targets
// (stores the fresh snapshot in *snapp), ctx is cancelled, or the
// snapshot itself fails to rebuild. It is the hard-semantics
// degraded-write wait.
func (v *Volume) awaitQuorum(ctx context.Context, index uint64, snapp **placement.Snapshot, timer *time.Timer, backoff *time.Duration) error {
	for {
		var ok bool
		if *backoff, ok = v.cluster.sleep(ctx, timer, *backoff, hardBackoffMin, hardBackoffMax); !ok {
			return ctx.Err()
		}
		s, err := v.cluster.forceRefresh(ctx)
		if err != nil {
			return err
		}
		*snapp = s
		var scratch [maxReplicas]placement.Ref
		if refs, terr := v.targets(s, index, scratch[:]); terr == nil && len(refs) >= v.w {
			return nil
		}
	}
}

// maxReplicas bounds the fan-out scratch of a chunk write; volumes
// with more replicas are rejected at OpenVolume.
const maxReplicas = 8

// payload is the straggler-owned copy of a chunk write's data.
// Reference-counted: every replica worker drops one reference when
// its write finishes, and the last one recycles the buffer. Pooling
// (pointer-typed, so no boxing on Put) keeps the hot path at zero
// allocations in steady state.
type payload struct {
	b []byte
	n atomic.Int32
}

var payloadPool = sync.Pool{New: func() any { return new(payload) }}

func (v *Volume) acquirePayload(p []byte, refs int) *payload {
	pl := payloadPool.Get().(*payload)
	if cap(pl.b) < len(p) {
		pl.b = make([]byte, len(p))
	}
	pl.b = pl.b[:len(p)]
	copy(pl.b, p)
	pl.n.Store(int32(refs))
	return pl
}

// release drops one reference; the last worker recycles the payload.
func (pl *payload) release() {
	if pl.n.Add(-1) == 0 {
		pl.b = pl.b[:0] // keep capacity for reuse
		payloadPool.Put(pl)
	}
}

// doneChanPool recycles quorumWrite completion channels. A channel is
// returned to the pool only after every worker has sent its index; on
// the quorum-commit fast path (and on caller cancel) stragglers are
// still sending, so the channel is deliberately dropped there — it is
// buffered to maxReplicas, so those sends never block and the channel
// is collected with the straggler goroutines.
var doneChanPool = sync.Pool{New: func() any { return make(chan int, maxReplicas) }}

// detachedCtx is the quorum-straggler context: never cancelled, with
// no deadline. sdsnet.Conn uses only Deadline() on the data path, so
// this is exactly what context.WithoutCancel(ctx) yields in the common
// case (ctx without a deadline) — at zero allocation cost, because the
// empty struct needs no boxing in the context.Context interface.
type detachedCtx struct{}

func (detachedCtx) Deadline() (time.Time, bool) { return time.Time{}, false }
func (detachedCtx) Done() <-chan struct{}       { return nil }
func (detachedCtx) Err() error                  { return nil }
func (detachedCtx) Value(any) any               { return nil }

// deadlineCtx is detachedCtx carrying the caller's deadline (like
// WithoutCancel, a deadline survives detachment). Boxed once per
// write, but only on the rare caller-with-deadline path.
type deadlineCtx struct{ d time.Time }

func (c deadlineCtx) Deadline() (time.Time, bool) { return c.d, true }
func (deadlineCtx) Done() <-chan struct{}         { return nil }
func (deadlineCtx) Err() error                    { return nil }
func (deadlineCtx) Value(any) any                 { return nil }

// quorumWrite fans the chunk out to refs and commits on v.w acks.
//
// Allocation shape matters: this is the hot path of every VM write.
// One completion channel carrying worker indices plus one shared
// result array replaces the per-replica channels of the first
// implementation. Stragglers keep working after a quorum commit,
// detached from the caller's context (bounded by a grace deadline),
// so a caller cancel never abandons an in-flight replica write.
func (v *Volume) quorumWrite(ctx context.Context, oid, inObj uint64, p []byte, n uint64, hole, sparse bool, refs []placement.Ref) error {
	type res struct {
		status sdsnet.Status
		err    error
	}
	// Payload ownership: stragglers keep writing after a quorum commit,
	// but the WriteAt contract says p is not retained past the call —
	// the caller may reuse it the moment we return. One refcounted
	// pooled copy feeds every replica; the last worker recycles it.
	// Not needed when the quorum IS every replica (commit means all
	// workers finished), and zeroes writes carry no payload.
	var pl *payload
	if !hole && v.w < len(refs) {
		pl = v.acquirePayload(p, len(refs))
		p = pl.b
	}
	// Single replica (the single-node MVP): no fan-out machinery at
	// all — one synchronous write, no goroutine, channel, detached
	// context, or payload copy.
	if len(refs) == 1 && v.w == 1 {
		status, err := v.replicaWrite(ctx, refs[0], oid, inObj, p, n, hole, sparse)
		switch {
		case err != nil:
			return err
		case status == sdsnet.StatusOK:
			return nil
		case status == sdsnet.StatusStale:
			return &sdsnet.Error{Op: sdsnet.OpWrite, Status: sdsnet.StatusStale}
		case status == sdsnet.StatusNoSpace:
			return ErrNoSpace
		default:
			return fmt.Errorf("client: write oid %d: replica status %d", oid, status)
		}
	}
	var results [maxReplicas]res
	// Straggler context: detached from the caller's cancellation, so
	// a quorum commit does not cancel the remaining replicas and a
	// caller cancel never abandons an in-flight replica write.
	// sdsnet.Conn uses only ctx.Deadline() on the data path (never
	// ctx.Value), so a plain detached context is exactly what
	// context.WithoutCancel(ctx) yields in the common case — at zero
	// allocation cost. A caller-set deadline is preserved; stragglers
	// must not be cancelled early, so it is carried as a value-only
	// deadline (sdsnet bounds the socket at the transport level).
	var ctx2 context.Context = detachedCtx{}
	if d, ok := ctx.Deadline(); ok {
		ctx2 = deadlineCtx{d: d}
	}
	done := doneChanPool.Get().(chan int)
	for i := range refs {
		// ref passed by value: the caller's scratch backing array must
		// not escape through the goroutine closure.
		go func(i int, ref placement.Ref) {
			status, err := v.replicaWrite(ctx2, ref, oid, inObj, p, n, hole, sparse)
			results[i] = res{status, err}
			if pl != nil {
				pl.release()
			}
			done <- i
		}(i, refs[i])
	}
	var errs []error
	var firstStale error
	var firstNoSpace error
	ok := 0
	seen := 0
	for i := 0; i < len(refs); i++ {
		select {
		case idx := <-done:
			seen++
			r := results[idx]
			switch {
			case r.err != nil:
				errs = append(errs, r.err)
			case r.status == sdsnet.StatusOK:
				ok++
				if ok >= v.w {
					// Quorum reached: stragglers finish under ctx2,
					// which is detached from the caller's ctx.
					if v.w == len(refs) {
						// The quorum is every replica: this is the
						// last index and no send is pending, so the
						// channel goes straight back to the pool.
						doneChanPool.Put(done)
					}
					// Otherwise stragglers may still send into it;
					// it is buffered to maxReplicas, so their sends
					// never block and it is collected with them.
					return nil
				}
			case r.status == sdsnet.StatusStale:
				// Synthetic StatusStale error keeps the retry loop in
				// writeChunk (isStale -> forceRefresh) working.
				if firstStale == nil {
					firstStale = &sdsnet.Error{Op: sdsnet.OpWrite, Status: sdsnet.StatusStale}
				}
			case r.status == sdsnet.StatusNoSpace:
				// A replica's disk is full. The quorum can no longer be
				// reached (a refusing replica will not become healthy by
				// retrying), and a full disk is not a transient condition
				// hard semantics should ride out: surface ENOSPC.
				if firstNoSpace == nil {
					firstNoSpace = ErrNoSpace
				}
			default:
				errs = append(errs, fmt.Errorf("client: write oid %d: replica status %d", oid, r.status))
			}
			if firstNoSpace != nil && ok+(len(refs)-seen) < v.w {
				// Even if every outstanding replica acks, the quorum is
				// unreachable: fail fast with ENOSPC instead of waiting
				// out the stragglers. The done channel is buffered to
				// maxReplicas, so their late sends never block.
				doneChanPool.Put(done)
				return firstNoSpace
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Every worker's index has been received here: no send is pending,
	// so the channel can be reused.
	doneChanPool.Put(done)
	if firstStale != nil {
		return firstStale
	}
	if firstNoSpace != nil {
		return firstNoSpace
	}
	if len(errs) == 0 {
		return fmt.Errorf("client: %w on oid %d: %d/%d acks", errQuorumLost, oid, ok, len(refs))
	}
	return fmt.Errorf("client: %w for oid %d: %w", errQuorumFailed, oid, errors.Join(errs...))
}

// replicaWrite performs one replica's write (data or zeroes mode) and
// maps the reply status.
func (v *Volume) replicaWrite(ctx context.Context, ref placement.Ref, oid, inObj uint64, p []byte, n uint64, hole, sparse bool) (sdsnet.Status, error) {
	pp, err := v.cluster.pool(ref.OSD, ref.Addr)
	if err != nil {
		return 0, err
	}
	if hole {
		err = pp.WriteZeroes(ctx, oid, inObj, n, sparse, false)
	} else {
		err = pp.Write(ctx, oid, inObj, p, false)
	}
	if err != nil {
		if e, ok := sdsnet.AsError(err); ok {
			// A protocol reply: the OSD answered, so it is alive.
			v.cluster.noteSuccess(ref.OSD)
			return e.Status, nil
		}
		v.cluster.noteFailure(ref.OSD)
		return 0, err
	}
	v.cluster.noteSuccess(ref.OSD)
	return sdsnet.StatusOK, nil
}

// WriteZeroes makes [off, off+n) read as zeroes on every replica. When
// sparse is set the replicas punch a hole (blocks return to the NVMe),
// which is what a guest TRIM/DISCARD wants; with sparse=false the replicas
// store literal zeroes (NBD Write_Zeroes with the NO_HOLE flag), which is
// the byte-exact, allocation-keeping variant.
func (v *Volume) WriteZeroes(ctx context.Context, off, n uint64, sparse bool) error {
	if size := v.Size(); off+n > size {
		return fmt.Errorf("client: write zeroes at %d len %d past volume end %d", off, n, size)
	}
	if lim := v.limitsSnapshot(); lim != nil {
		// Discard is coarse (a guest FITRIM can span the whole volume in
		// one op); it charges the write axis as one op.
		if err := lim.Write.Wait(ctx, n); err != nil {
			if errors.Is(err, qos.ErrClosed) {
				return fmt.Errorf("client: volume %q closed", v.spec.Name)
			}
			return err
		}
	}
	// TRIM materialization rule (ADR): on a volume with snapshots a
	// discard must be VISIBLE against the frozen chain. Punching holes
	// (or leaving objects absent) would make reads fall through to
	// older generations and resurrect pre-trim data, so discards store
	// literal zeros in the live range instead: the object becomes
	// allocated, its live bit is charged to quota, and no quota is
	// released. Volumes without snapshots keep punch-and-release
	// semantics unchanged.
	materialize := sparse && v.hasSnapshots()
	if materialize {
		sparse = false
	}
	for done := uint64(0); done < n; {
		cur := off + done
		index, inObj := object.Split(cur)
		ch := n - done
		if ch > object.Size-inObj {
			ch = object.Size - inObj
		}
		if materialize {
			// Conservative order: reserve the allocation first, then
			// store the zeros. An over-count after a failed write is
			// safe; an under-count would hide a live object from the
			// next snapshot bitmap.
			if qerr := v.chargeQuota(ctx, index); qerr != nil {
				return qerr
			}
		}
		if err := v.writeChunk(ctx, nil, ch, index, inObj, true, sparse); err != nil {
			return err
		}
		// A sparse TRIM that cleared an entire object frees its quota
		// room (guest discard vacuums allocation at object granularity).
		if sparse && inObj == 0 && ch == object.Size {
			v.releaseQuota(ctx, index)
		}
		done += ch
	}
	return nil
}

// Flush syncs every OSD the volume may touch (cluster-wide sync: cheap
// and correct for MVP; per-pg sync is a later optimization).
func (v *Volume) Flush(ctx context.Context) error {
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for id, ref := range snap.Refs() {
		pp, err := v.cluster.pool(id, ref.Addr)
		if err != nil {
			firstErr = err
			continue
		}
		if err := pp.Sync(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// forceRefresh bypasses the refresh throttle (after a StatusStale).
func (c *Cluster) forceRefresh(ctx context.Context) (*placement.Snapshot, error) {
	c.lastRef.Store(0)
	return c.Refresh(ctx)
}

// regEqual compares two placement registries.
func regEqual(a, b placement.Registry) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		w, ok := b[k]
		if !ok || w != v {
			return false
		}
	}
	return true
}

// isStale reports a StatusStale reply from a fenced OSD replica.
func isStale(err error) bool { return isStatus(err, sdsnet.StatusStale) }

// isStatus reports err as a specific remote status reply.
func isStatus(err error, st sdsnet.Status) bool {
	if e, ok := sdsnet.AsError(err); ok {
		return e.Status == st
	}
	return false
}
