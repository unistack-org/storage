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

// Cluster is the shared connection/placement context for one process
// talking to one cluster: a meta store, one pool set per OSD, and the
// live placement epoch. Dialing is per target and lazy; pools are reused
// across every volume of the cluster.
type Cluster struct {
	meta meta.Store

	poolsMu sync.Mutex
	pools   map[int32]*sdsnet.Pool // per OSD id, keyed for reuse

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
	return &Cluster{meta: s, pools: map[int32]*sdsnet.Pool{}, refreshMin: 5 * time.Millisecond}
}

// Meta exposes the metadata store (for volume creation, map epochs).
func (c *Cluster) Meta() meta.Store { return c.meta }

// Close shuts every pool down.
func (c *Cluster) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.poolsMu.Lock()
	for _, pp := range c.pools {
		pp.Close()
	}
	c.pools = nil
	c.poolsMu.Unlock()
	return nil
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
	spec := meta.VolumeSpec{
		Name:     name,
		Size:     size,
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

func (v *Volume) oid(index uint64) uint64 { return index + 1 } // OID 0 reserved

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
	for attempt := 0; attempt < 2; attempt++ {
		var scratch [maxReplicas]placement.Ref
		refs, terr := v.targets(snap, index, scratch[:])
		if terr != nil && len(refs) == 0 {
			return terr
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
		if err := v.writeChunk(ctx, p[total:total+n], uint64(n), index, inObj, false); err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

func (v *Volume) writeChunk(ctx context.Context, p []byte, n uint64, index, inObj uint64, hole bool) error {
	oid := v.oid(index)
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		var scratch [maxReplicas]placement.Ref
		refs, terr := v.targets(snap, index, scratch[:])
		if terr != nil {
			return terr
		}
		err = v.quorumWrite(ctx, oid, inObj, p, n, hole, refs)
		if err == nil {
			return nil
		}
		if !isStale(err) {
			return err
		}
		if snap, err = v.cluster.forceRefresh(ctx); err != nil {
			return err
		}
	}
	return fmt.Errorf("client: write oid %d: %w", oid, err)
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
func (v *Volume) quorumWrite(ctx context.Context, oid, inObj uint64, p []byte, n uint64, hole bool, refs []placement.Ref) error {
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
		status, err := v.replicaWrite(ctx, refs[0], oid, inObj, p, n, hole)
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
			status, err := v.replicaWrite(ctx2, ref, oid, inObj, p, n, hole)
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
		return fmt.Errorf("client: quorum lost on oid %d: %d/%d acks", oid, ok, len(refs))
	}
	return fmt.Errorf("client: quorum failed for oid %d: %w", oid, errors.Join(errs...))
}

// replicaWrite performs one replica's write (data or zeroes mode) and
// maps the reply status.
func (v *Volume) replicaWrite(ctx context.Context, ref placement.Ref, oid, inObj uint64, p []byte, n uint64, hole bool) (sdsnet.Status, error) {
	pp, err := v.cluster.pool(ref.OSD, ref.Addr)
	if err != nil {
		return 0, err
	}
	if hole {
		err = pp.WriteZeroes(ctx, oid, inObj, n, true, false)
	} else {
		err = pp.Write(ctx, oid, inObj, p, false)
	}
	if err != nil {
		if e, ok := sdsnet.AsError(err); ok {
			return e.Status, nil
		}
		return 0, err
	}
	return sdsnet.StatusOK, nil
}

// WriteZeroes punches n zero bytes (sparse where supported) at off.
func (v *Volume) WriteZeroes(ctx context.Context, off, n uint64) error {
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
		if err := v.writeChunk(ctx, nil, ch, index, inObj, true); err != nil {
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
