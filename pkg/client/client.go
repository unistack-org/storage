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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unistack-org/crushkv/crushmap"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// Errors returned by the client.
var (
	// ErrNotEnoughOSDs means the registry has fewer live OSDs than the
	// volume's replica count.
	ErrNotEnoughOSDs = errors.New("client: not enough live osds")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("client: closed")
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
	closed   bool

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
	if c.proberOn || c.closed {
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
		addr[o.ID] = o.Addr
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
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
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
	snap, closed := c.snap, c.closed
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

// MapEpoch publishes a new crush map epoch: the map is stored in meta
// under maps/<epoch> (OSDs raise their fence and clients rebuild on
// watch). Format is auto-detected by placement (text/binary/json).
func (c *Cluster) MapEpoch(ctx context.Context, epoch uint32, src []byte) error {
	format := "text"
	switch {
	case len(src) > 0 && src[0] == '{':
		format = "json"
	case len(src) >= 4 && binary.LittleEndian.Uint32(src[:4]) == crushmap.Magic:
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
// selects the default Replicas/2+1.
func (c *Cluster) CreateVolume(ctx context.Context, name string, size uint64, replicas, minSize, pgNum int, rule string) (meta.VolumeSpec, error) {
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
	// Every volume gets a private, cluster-wide-unique object-id range, so
	// two volumes placed on the same OSD can never address the same
	// object. The range is [OIDBase+1, OIDBase+NumObjects]; one extra id is
	// reserved to keep the OIDBase+N arithmetic inside it.
	base, err := meta.NextOIDBase(ctx, c.meta, object.NumObjects(size)+1)
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: create volume %q: %w", name, err)
	}
	spec := meta.VolumeSpec{
		Name:     name,
		Size:     size,
		OIDBase:  base,
		OIDShift: object.SizeShift,
		Replicas: replicas,
		MinSize:  minSize,
		Rule:     rule,
		PGNum:    pgNum,
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

// DeleteVolume removes a volume's registration. It does not delete the
// volume's objects: they stay on the OSDs (garbage) until a reclaim pass
// is implemented. Callers must close open handles first; an in-flight
// writer keeps writing to objects it can no longer register.
func (c *Cluster) DeleteVolume(ctx context.Context, name string) error {
	if name == "" {
		return errors.New("client: volume name required")
	}
	if err := meta.DeleteVolume(ctx, c.meta, name); err != nil {
		return fmt.Errorf("client: delete volume %q: %w", name, err)
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
	return &Volume{
		cluster: c,
		spec:    spec,
		pgNum:   pgNum,
		mask:    pgNum - 1,
		w:       w,
	}, nil
}

// Volume is an open volume: a byte range over replicated objects. Safe
// for concurrent use.
type Volume struct {
	cluster *Cluster
	spec    meta.VolumeSpec

	pgNum uint32
	mask  uint32
	w     int // write quorum
}

// Spec returns the volume's registered spec.
func (v *Volume) Spec() meta.VolumeSpec { return v.spec }

// Size is the logical volume size.
func (v *Volume) Size() uint64 { return v.spec.Size }

// oid maps an object index to the volume's private OID range:
// OIDBase+1 .. OIDBase+NumObjects (OIDBase itself is unused, mirroring the
// reserved NilOID). Because bases are allocated cluster-wide, no two
// volumes ever share an object id on a shared OSD.
func (v *Volume) oid(index uint64) uint64 { return v.spec.OIDBase + index + 1 }

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
	if off >= v.spec.Size {
		return 0, fmt.Errorf("client: read at %d past volume end %d", off, v.spec.Size)
	}
	if uint64(len(p)) > v.spec.Size-off {
		p = p[:v.spec.Size-off]
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
	oid := v.oid(index)
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
		lastErr := error(terr)
		var sawMissing bool
	refresh:
		for _, ref := range refs {
			pp, err := v.cluster.pool(ref.OSD, ref.Addr)
			if err != nil {
				return err
			}
			n, err := pp.Read(ctx, oid, inObj, p)
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
			// All replicas report the region absent: authoritative
			// zeros (thin provisioning).
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
	return fmt.Errorf("client: read oid %d: %w", oid, errStaleRead(oid))
}

func errStaleRead(oid uint64) error {
	return fmt.Errorf("client: read oid %d: still stale after refresh", oid)
}

// WriteAt writes p at off, fanning out to every replica and committing
// on the write quorum (W = R/2+1).
func (v *Volume) WriteAt(ctx context.Context, p []byte, off uint64) (int, error) {
	if off+uint64(len(p)) > v.spec.Size {
		return 0, fmt.Errorf("client: write at %d len %d past volume end %d", off, len(p), v.spec.Size)
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
// real hole so guest TRIM reclaims space (it implies hole).
func (v *Volume) writeChunk(ctx context.Context, p []byte, n uint64, index, inObj uint64, hole, sparse bool) error {
	oid := v.oid(index)
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
		err = v.quorumWrite(ctx, oid, inObj, p, n, hole, sparse, refs)
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
	ok := 0
	for i := 0; i < len(refs); i++ {
		select {
		case idx := <-done:
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
			default:
				errs = append(errs, fmt.Errorf("client: write oid %d: replica status %d", oid, r.status))
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
	if off+n > v.spec.Size {
		return fmt.Errorf("client: write zeroes at %d len %d past volume end %d", off, n, v.spec.Size)
	}
	for done := uint64(0); done < n; {
		cur := off + done
		index, inObj := object.Split(cur)
		ch := n - done
		if ch > object.Size-inObj {
			ch = object.Size - inObj
		}
		if err := v.writeChunk(ctx, nil, ch, index, inObj, true, sparse); err != nil {
			return err
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
