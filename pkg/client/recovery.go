package client

// Recovery v1 (plan M6.3): detect diverged/missing replicas and copy
// from a live replica.
//
// RecoveryCheck walks every object index of a volume, resolves its pg
// row in the current placement snapshot (the FULL registry: down OSDs
// stay in the row, unlike the data path, which drops them and demands
// a quorum), and stats the object on every target. Classification per
// object:
//
//   - present on at least one live target and missing on another live
//     target  -> a repair item (copy source = first target that has it);
//   - present on no target at all -> thin object (reads as zeros): ok,
//     nothing to copy;
//   - an OSD that does not answer at all is recorded once in Down and
//     excluded from repair items (it will be repaired when it returns).
//
// Recover copies each repair item whole (one object, 4 MiB) from the
// source replica to the target, paced by its own QoS pair so it always
// runs below client IO (recovery_tune_interval analogue: set rates low,
// the copier trickles). The pass is idempotent: re-checking after a
// completed run yields no items, and a re-run over already-repaired
// targets only pays the stat.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/qos"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// RecoveryItem is one object that must be copied to one target.
type RecoveryItem struct {
	Index  uint64 // object index inside the volume
	OID    uint64 // cluster object id (OIDBase+Index+1)
	OSD    int32  // target missing (or lagging behind on) the object
	Source int32  // replica to copy from
}

// RecoveryReport is the result of RecoveryCheck for one volume.
type RecoveryReport struct {
	Volume    string
	Objects   int            // objects in the volume
	Thin      int            // objects absent everywhere (zeros: fine)
	Ok        int            // objects present on every live target
	Items     []RecoveryItem // copies to make, in index order
	Down      []int32        // OSDs that did not answer (unique, sorted)
	CheckedAt time.Time
}

// Healthy reports a volume with nothing to repair.
func (r *RecoveryReport) Healthy() bool {
	return len(r.Items) == 0 && len(r.Down) == 0
}

// RecoveryOpts tunes a Recover run.
type RecoveryOpts struct {
	// Limit, when non-nil, paces the copier (recovery bandwidth
	// headroom for client IO). Build one with qos.NewPair(0, iops,
	// bps, 0) style rates; a nil pair runs at full speed (admins
	// only — the polygon default should be a bounded rate).
	Limit *qos.Pair
	// Logf receives per-copy progress (may be nil).
	Logf func(string, ...any)
	// Record, when non-nil, is called once per completed copy with the
	// bytes moved (metrics hook; nil in tools and tests).
	Record func(copies int, bytes uint64)
}

// RecoveryStats counts what a Recover run did.
type RecoveryStats struct {
	Copied   int
	Bytes    uint64
	Failed   int
	Skipped  int // items that turned out already repaired on the way
	Started  time.Time
	Finished time.Time
}

// RecoveryCheck scans the volume's replicas. It must not be routed
// through the normal data path (targets() refuses below min_size on
// purpose): it reads placement directly, so it works on degraded
// volumes — that is exactly when it is needed.
func (v *Volume) RecoveryCheck(ctx context.Context) (*RecoveryReport, error) {
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return nil, fmt.Errorf("client: recovery check %q: %w", v.spec.Name, err)
	}
	rep := &RecoveryReport{Volume: v.spec.Name, Objects: v.nbits(), CheckedAt: time.Now()}
	down := map[int32]bool{}
	epoch := v.cluster.epochSnapshot()
	n := v.nbits()
	scratch := make([]int32, 0, maxReplicas)

	for index := 0; index < n; index++ {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		oid, ok := v.oid(uint64(index))
		if !ok {
			continue // index outside the reserved ranges (spec mid-resize)
		}
		row := v.rowFor(snap, uint64(index), scratch)
		// Stat every live target of the row.
		type obs struct {
			ref     placement.Ref
			present bool
		}
		obses := make([]obs, 0, len(row))
		anyLive := false
		for _, id := range row {
			ref, ok := snap.LookupRef(id)
			if !ok {
				continue // not in the registry at all: nothing to stat
			}
			if down[id] {
				continue // known dead since an earlier object: skip cheaply
			}
			pp, err := v.cluster.pool(id, ref.Addr)
			if err != nil {
				return rep, err
			}
			pp.SetEpoch(epoch)
			_, present, err := pp.Stat(ctx, uint64(oid))
			if err != nil {
				// Transport-level failure: the OSD is down for
				// recovery purposes; record once, do not spam items.
				down[id] = true
				if log := rep.Down; !containsInt32(log, id) {
					rep.Down = append(rep.Down, id)
				}
				continue
			}
			anyLive = true
			obses = append(obses, obs{ref: ref, present: present})
		}
		if !anyLive {
			// Nothing answered: cannot classify this object; leave it
			// out of Items (a check against a fully-down cluster is a
			// connectivity problem, not a divergence).
			continue
		}
		source := -1
		for i, o := range obses {
			if o.present {
				source = i
				break
			}
		}
		if source < 0 {
			rep.Thin++ // thin everywhere: the volume reads zeros here
			continue
		}
		good := true
		for _, o := range obses {
			if !o.present {
				good = false
				rep.Items = append(rep.Items, RecoveryItem{
					Index: uint64(index), OID: uint64(oid),
					OSD: o.ref.OSD, Source: obses[source].ref.OSD,
				})
			}
		}
		if good {
			rep.Ok++
		}
	}
	sort.Slice(rep.Items, func(i, j int) bool {
		if rep.Items[i].Index != rep.Items[j].Index {
			return rep.Items[i].Index < rep.Items[j].Index
		}
		return rep.Items[i].OSD < rep.Items[j].OSD
	})
	sort.Slice(rep.Down, func(i, j int) bool { return rep.Down[i] < rep.Down[j] })
	return rep, nil
}

// RecoveryScan checks every registered volume and returns the reports
// (sorted by name). It is the "recovery ls" of plan M6.3.
func (c *Cluster) RecoveryScan(ctx context.Context) ([]RecoveryReport, error) {
	specs, err := meta.ListVolumes(ctx, c.meta)
	if err != nil {
		return nil, fmt.Errorf("client: recovery scan: %w", err)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	out := make([]RecoveryReport, 0, len(specs))
	for _, spec := range specs {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		v, err := c.OpenVolume(ctx, spec.Name)
		if err != nil {
			// Unopenable volume (no map, bad spec): report it as a
			// one-item pseudo-report so the scan stays useful.
			out = append(out, RecoveryReport{
				Volume: spec.Name, CheckedAt: time.Now(),
				Items: []RecoveryItem{{OSD: -1, Source: -1}},
			})
			continue
		}
		rep, err := v.RecoveryCheck(ctx)
		v.Close()
		if err != nil {
			return out, err
		}
		out = append(out, *rep)
	}
	return out, nil
}

// Recover copies every repair item of rep (source replica -> missing
// target), one object at a time, paced by opts.Limit. It stops early
// on ctx cancellation; completed copies are durable, so re-running
// RecoveryCheck + Recover resumes where the pass stopped.
func (v *Volume) Recover(ctx context.Context, rep *RecoveryReport, opts RecoveryOpts) (*RecoveryStats, error) {
	if rep.Volume != v.spec.Name {
		return nil, fmt.Errorf("client: recover: report is for volume %q, not %q", rep.Volume, v.spec.Name)
	}
	st := &RecoveryStats{Started: time.Now()}
	defer func() { st.Finished = time.Now() }()
	epoch := v.cluster.epochSnapshot()
	buf := make([]byte, object.Size)

	for _, it := range rep.Items {
		if err := ctx.Err(); err != nil {
			return st, err
		}
		srcRef, dstRef, err := v.cluster.replicaRefs(it)
		if err != nil {
			st.Failed++
			if opts.Logf != nil {
				opts.Logf("recovery %s obj %d: refs: %v", v.spec.Name, it.Index, err)
			}
			continue
		}
		if opts.Limit != nil && opts.Limit.Write != nil {
			if err := opts.Limit.Write.Wait(ctx, object.Size); err != nil {
				return st, err
			}
		}
		sp, err := v.cluster.pool(srcRef.OSD, srcRef.Addr)
		if err != nil {
			return st, err
		}
		sp.SetEpoch(epoch)
		// Re-stat on the way: an item can be stale (auto-healed by a
		// concurrent writer, e.g. a full-object rewrite).
		if _, exists, err := sp.Stat(ctx, it.OID); err != nil {
			st.Failed++
			continue
		} else if !exists {
			st.Skipped++ // thin on its own source now: nothing to copy
			continue
		}
		n, err := sp.Read(ctx, it.OID, 0, buf)
		if err != nil && !isEOFShort(n, err) {
			st.Failed++
			if opts.Logf != nil {
				opts.Logf("recovery %s obj %d: read from osd %d: %v", v.spec.Name, it.Index, it.Source, err)
			}
			continue
		}
		// Normalise to the full object: the tail beyond a short (thin)
		// source file is zeros, so every replica ends byte-identical.
		for i := n; i < len(buf); i++ {
			buf[i] = 0
		}
		dp, err := v.cluster.pool(dstRef.OSD, dstRef.Addr)
		if err != nil {
			return st, err
		}
		dp.SetEpoch(epoch)
		if err := dp.Write(ctx, it.OID, 0, buf, true); err != nil {
			st.Failed++
			if opts.Logf != nil {
				opts.Logf("recovery %s obj %d: write to osd %d: %v", v.spec.Name, it.Index, it.OSD, err)
			}
			continue
		}
		st.Copied++
		st.Bytes += object.Size
		if opts.Record != nil {
			opts.Record(1, object.Size)
		}
		if opts.Logf != nil {
			opts.Logf("recovery %s: obj %d copied osd %d -> osd %d", v.spec.Name, it.Index, it.Source, it.OSD)
		}
	}
	// One flush per destination OSD persists the repaired objects.
	for _, id := range dstsOf(rep.Items) {
		if ref, ok := v.cluster.replicaRef(id); ok {
			if pp, err := v.cluster.pool(id, ref.Addr); err == nil {
				pp.SetEpoch(epoch)
				_ = pp.Sync(ctx)
			}
		}
	}
	return st, nil
}

// ObjectPresent reports whether object index of the volume occupies space
// on any replica: it stats the placement row directly (like RecoveryCheck,
// bypassing the quorum rules of targets()) and returns present=true when
// at least one answering replica has the object. A thin object (absent on
// every answering replica) reads as zeroes, so present=false is a hole.
// It is the base:allocation primitive behind NBD BLOCK_STATUS over
// clustered volumes; granularity is one object (4 MiB).
//
// If no replica answers (fully down row), an error is returned: the
// allocation state is unknown, which must not be reported as a hole.
func (v *Volume) ObjectPresent(ctx context.Context, index uint64) (bool, error) {
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return false, fmt.Errorf("client: object present %q/%d: %w", v.spec.Name, index, err)
	}
	oid, ok := v.oid(index)
	if !ok {
		return false, fmt.Errorf("client: object present %q/%d: index outside volume", v.spec.Name, index)
	}
	epoch := v.cluster.epochSnapshot()
	scratch := make([]int32, 0, maxReplicas)
	row := v.rowFor(snap, index, scratch)
	answered := false
	for _, id := range row {
		ref, ok := snap.LookupRef(id)
		if !ok {
			continue
		}
		pp, err := v.cluster.pool(id, ref.Addr)
		if err != nil {
			return false, err
		}
		pp.SetEpoch(epoch)
		_, present, err := pp.Stat(ctx, uint64(oid))
		if err != nil {
			continue // this replica is down: try the next one
		}
		answered = true
		if present {
			return true, nil
		}
	}
	// Distinguish "every replica answered: thin" from "nobody answered".
	if !answered && len(row) > 0 {
		return false, fmt.Errorf("client: object present %q/%d: no replica answered", v.spec.Name, index)
	}
	return false, nil
}

// rowFor is targets() without the min_size quorum rule: recovery must
// classify replicas on degraded volumes, where targets() (rightly)
// refuses to serve. Negative row entries (CRUSH-out devices) are
// skipped: their rejoin is a reweight story, not recovery-v1.
func (v *Volume) rowFor(snap *placement.Snapshot, index uint64, scratch []int32) []int32 {
	row := snap.Row(v.pgFor(snap, index))
	out := scratch[:0]
	for _, id := range row {
		if id >= 0 {
			out = append(out, id)
		}
	}
	return out
}

// replicaRefs resolves the source/target refs of one item off the
// current snapshot (addresses can move between daemons).
func (c *Cluster) replicaRefs(it RecoveryItem) (src, dst placement.Ref, err error) {
	c.mu.RLock()
	snap := c.snap
	c.mu.RUnlock()
	if snap == nil {
		return src, dst, errors.New("client: recovery: no placement snapshot")
	}
	src, ok := snap.LookupRef(it.Source)
	if !ok {
		return src, dst, fmt.Errorf("source osd %d not in registry", it.Source)
	}
	dst, ok = snap.LookupRef(it.OSD)
	if !ok {
		return src, dst, fmt.Errorf("target osd %d not in registry", it.OSD)
	}
	return src, dst, nil
}

func (c *Cluster) replicaRef(id int32) (placement.Ref, bool) {
	c.mu.RLock()
	snap := c.snap
	c.mu.RUnlock()
	if snap == nil {
		return placement.Ref{}, false
	}
	return snap.LookupRef(id)
}

// isEOFShort tolerates a short read at the end of a sparse object file
// (the store reports io.EOF when the tail was never written; the wire
// maps it to StatusEOF when the read starts at/after the end).
func isEOFShort(n int, err error) bool {
	if n == 0 {
		return false
	}
	if e, ok := sdsnet.AsError(err); ok {
		return e.Status == sdsnet.StatusEOF
	}
	return errors.Is(err, io.EOF)
}

func dstsOf(items []RecoveryItem) []int32 {
	seen := map[int32]bool{}
	out := make([]int32, 0, len(items))
	for _, it := range items {
		if !seen[it.OSD] {
			seen[it.OSD] = true
			out = append(out, it.OSD)
		}
	}
	return out
}

func containsInt32(s []int32, x int32) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}
