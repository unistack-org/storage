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

	mu     sync.RWMutex
	snap   *placement.Snapshot
	epoch  uint32
	closed bool
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
	ns, err := c.compile(rec, reg, snap)
	if err != nil {
		if snap != nil {
			return snap, nil
		}
		return nil, err
	}
	c.mu.Lock()
	c.snap, c.epoch = ns, rec.Epoch
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
func (c *Cluster) CreateVolume(ctx context.Context, name string, size uint64, replicas, pgNum int, rule string) (meta.VolumeSpec, error) {
	if name == "" || size == 0 {
		return meta.VolumeSpec{}, errors.New("client: volume name and size required")
	}
	if pgNum == 0 || bits.OnesCount(uint(pgNum)) != 1 {
		return meta.VolumeSpec{}, fmt.Errorf("client: pg_num %d must be a power of two", pgNum)
	}
	if replicas < 1 {
		replicas = 3
	}
	spec := meta.VolumeSpec{
		Name:     name,
		Size:     size,
		OIDShift: object.SizeShift,
		Replicas: replicas,
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
	return &Volume{
		cluster: c,
		spec:    spec,
		pgNum:   pgNum,
		mask:    pgNum - 1,
		w:       spec.Replicas/2 + 1,
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

// targets returns the volume's replicas for object index in snapshot snap.
func (v *Volume) targets(snap *placement.Snapshot, index uint64) ([]placement.Ref, error) {
	pg := v.pgFor(snap, index)
	row := snap.Row(pg)
	refs := make([]placement.Ref, 0, len(row))
	for _, id := range row {
		if id < 0 {
			continue
		}
		if ref, ok := snap.LookupRef(id); ok {
			refs = append(refs, ref)
		}
	}
	if len(refs) < v.spec.Replicas {
		return refs, fmt.Errorf("client: pg %d has %d/%d live replicas", pg, len(refs), v.spec.Replicas)
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
		refs, terr := v.targets(snap, index)
		if terr != nil && len(refs) == 0 {
			return terr
		}
		lastErr := error(terr)
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
			case sdsnet.IsObjectMissing(err):
				// Thin object: authoritative zeros.
				for i := range p {
					p[i] = 0
				}
				return nil
			case isStatus(err, sdsnet.StatusEOF):
				// Sparse tail of the object: zeros beyond the stored extent.
				for i := range p {
					p[i] = 0
				}
				return nil
			case isStale(err):
				lastErr = err
				break refresh // refresh placement, try the fresh map
			default:
				lastErr = err
				continue // next replica
			}
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
		refs, terr := v.targets(snap, index)
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

// quorumWrite fans the chunk out to refs and commits on v.w acks.
func (v *Volume) quorumWrite(ctx context.Context, oid, inObj uint64, p []byte, n uint64, hole bool, refs []placement.Ref) error {
	type res struct {
		osd int32
		err error
	}
	var (
		wg      sync.WaitGroup
		ackCh   = make(chan struct{}, len(refs))
		staleCh = make(chan res, len(refs))
		failCh  = make(chan res, len(refs))
		// Stragglers keep working after a quorum commit, detached from
		// the caller's context, but bounded by a grace deadline.
		ctx2, cancel = context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	)
	// Cancel the stragglers when the write fails; on a quorum commit
	// they run on (bounded by the grace deadline above).
	committed := false
	defer func() {
		if !committed {
			cancel()
		}
	}()

	for _, ref := range refs {
		wg.Add(1)
		go func(ref placement.Ref) {
			defer wg.Done()
			pp, err := v.cluster.pool(ref.OSD, ref.Addr)
			if err != nil {
				failCh <- res{ref.OSD, err}
				return
			}
			var werr error
			if hole {
				werr = pp.Do(ctx2, func(cc *sdsnet.Conn) error {
					return cc.WriteZeroes(ctx2, oid, inObj, n, true, false)
				})
			} else {
				werr = pp.Write(ctx2, oid, inObj, p, false)
			}
			switch {
			case werr == nil:
				ackCh <- struct{}{}
			case isStale(werr):
				staleCh <- res{ref.OSD, werr}
			default:
				failCh <- res{ref.OSD, werr}
			}
		}(ref)
	}

	// Close the collectors once every replica settles.
	go func() { wg.Wait(); close(ackCh); close(staleCh); close(failCh) }()

	acks := 0
	var firstFail, firstStale error
	for acks < v.w {
		select {
		case _, ok := <-ackCh:
			if !ok {
				ackCh = nil
			} else {
				acks++
			}
		case r := <-staleCh:
			if firstStale == nil {
				firstStale = r.err
			}
		case r := <-failCh:
			if firstFail == nil {
				firstFail = r.err
			}
		case <-ctx.Done():
			// Parent context cancelled: drain to the quorum check.
			ackCh, staleCh, failCh = nil, nil, nil
		}
		if ackCh == nil && staleCh == nil && failCh == nil {
			break
		}
	}
	if acks >= v.w {
		committed = true
		return nil // quorum; stragglers finish under ctx2
	}
	// Quorum lost.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if firstStale != nil {
		return firstStale
	}
	if firstFail != nil {
		return firstFail
	}
	return fmt.Errorf("client: quorum lost on oid %d: %d/%d acks", oid, acks, len(refs))
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

// isStale reports a StatusStale reply from a fenced OSD replica.
func isStale(err error) bool { return isStatus(err, sdsnet.StatusStale) }

// isStatus reports err as a specific remote status reply.
func isStatus(err error, st sdsnet.Status) bool {
	if e, ok := sdsnet.AsError(err); ok {
		return e.Status == st
	}
	return false
}
