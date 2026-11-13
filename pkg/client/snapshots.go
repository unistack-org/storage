package client

// Volume snapshots (plan M3, todos/adr-snapshots.md) - the client side.
//
// The meta layer (pkg/meta/snapshots.go) freezes generations: each
// snapshot moves the live volume to a fresh private OID range and
// copies the allocation bitmap into the frozen GenRecord. This file
// adds the two data-plane hooks the scheme needs:
//
//   - chain reads: a live-range miss (object missing on every replica)
//     falls back through the frozen generations newest-to-oldest;
//     nothing there means the block is zeros;
//   - TRIM materialization: on a volume that has snapshots, a sparse
//     whole-object discard writes literal zeroes instead of punching a
//     hole (a hole is indistinguishable from "never allocated", and the
//     chain would resurrect the snapshot's bytes through it).
//
// Control-plane methods (Snapshot/Rollback/DeleteSnapshot) drive the
// meta CAS operations and hand the discarded OID ranges to the vacuum
// machinery via the standard tombstone.

import (
	"context"
	"fmt"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// hasSnapshots reports whether the volume carries frozen generations.
// The live spec pointer is swapped atomically by the config watch, so
// this is a lock-free snapshot of the chain state.
func (v *Volume) hasSnapshots() bool { return len(v.cur.Load().Snapshots) > 0 }

// Snapshots lists the volume's frozen generations (oldest first).
func (v *Volume) Snapshots(ctx context.Context) ([]meta.SnapshotInfo, error) {
	out, err := meta.ListSnapshots(ctx, v.cluster.meta, v.spec.Name)
	if err != nil {
		return nil, fmt.Errorf("client: volume %q snapshots: %w", v.spec.Name, err)
	}
	return out, nil
}

// Snapshot freezes the volume's current data as a new read-only
// generation and moves the live volume to a fresh OID range. No object
// data moves: the snapshot costs a few meta writes. The guest should be
// quiesced (fsync inside, then this call): Snapshot flushes the OSDs
// first so every acknowledged write is durable inside the generation.
func (v *Volume) Snapshot(ctx context.Context) (meta.SnapshotInfo, error) {
	if err := v.Flush(ctx); err != nil {
		return meta.SnapshotInfo{}, fmt.Errorf("client: volume %q snapshot flush: %w", v.spec.Name, err)
	}
	spec, err := meta.CreateSnapshot(ctx, v.cluster.meta, v.spec.Name)
	if err != nil {
		return meta.SnapshotInfo{}, fmt.Errorf("client: volume %q snapshot: %w", v.spec.Name, err)
	}
	// The live bitmap is cumulative across the chain: bit i set means
	// the volume has data at block i somewhere (this generation's range
	// or an older one the chain reads fall through). The frozen record
	// copied it, and the copy stays true for the live volume too (data
	// at block i is still readable through the chain), so the bitmap
	// needs no reset and the local cache stays valid. New writes into
	// the fresh range charge (set) their bits as usual.
	v.applySpec(spec)
	n := len(spec.Snapshots)
	g := spec.Snapshots[n-1]
	return meta.SnapshotInfo{
		Gen:       g.Gen,
		CreatedAt: g.CreatedAt,
		Size:      g.Size,
		Objects:   meta.AllocMapPopcount(g.Bits),
	}, nil
}

// Rollback makes snapshot gen the live generation again: its frozen
// addressing and bitmap become live, every newer snapshot is discarded
// (rolling back discards the future) and its OID ranges are handed to
// the vacuum. The volume's logical size never shrinks: blocks beyond
// the frozen bitmap read as zeroes.
func (v *Volume) Rollback(ctx context.Context, gen uint32) (meta.VolumeSpec, error) {
	// Quiesce the writeback cache first: after the spec swap a cached
	// page would be written into the new generation's ranges (wrong
	// addressing) or into ranges the vacuum is about to free.
	if v.wb != nil {
		if err := v.wb.flushAll(ctx); err != nil {
			return meta.VolumeSpec{}, fmt.Errorf("client: volume %q rollback to gen %d: flush: %w", v.spec.Name, gen, err)
		}
	}
	spec, garbage, err := meta.RollbackVolume(ctx, v.cluster.meta, v.spec.Name, gen)
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: volume %q rollback to gen %d: %w", v.spec.Name, gen, err)
	}
	v.applySpec(spec)
	// The live bitmap was replaced by the generation's copy in meta;
	// drop the local cache so the next quota charge re-reads it.
	v.allocMu.Lock()
	v.alloc = nil
	v.allocMu.Unlock()
	if err := v.cluster.vacuumGenRanges(ctx, v.spec.Name, garbage); err != nil {
		return spec, fmt.Errorf("client: volume %q rollback to gen %d: rolled back, vacuum pending: %w", v.spec.Name, gen, err)
	}
	return spec, nil
}

// DeleteSnapshot drops one frozen generation; its private OID ranges
// go to the vacuum. The live data is untouched.
func (v *Volume) DeleteSnapshot(ctx context.Context, gen uint32) error {
	garbage, err := meta.DeleteSnapshot(ctx, v.cluster.meta, v.spec.Name, gen)
	if err != nil {
		return fmt.Errorf("client: volume %q delete snapshot %d: %w", v.spec.Name, gen, err)
	}
	if err := v.cluster.vacuumGenRanges(ctx, v.spec.Name, garbage); err != nil {
		return fmt.Errorf("client: volume %q delete snapshot %d: snapshot dropped, vacuum pending: %w", v.spec.Name, gen, err)
	}
	return nil
}

// CloneVolume creates dstName as a copy-on-write clone of srcName
// (todos/adr-snapshots.md, clone protocol in pkg/meta/clones.go). The
// source is flushed and frozen first, so every acknowledged write is
// inside the chain the clone reads; the clone gets its own live OID
// range and inherits geometry and QoS verbatim (the chain layout must
// match). quota 0 inherits the source's quota. The clone is an ordinary
// volume afterwards: it can be opened, written, snapshotted, exported
// and deleted, and deleting the source keeps the clone readable.
func (c *Cluster) CloneVolume(ctx context.Context, srcName, dstName string, quota uint64) (meta.VolumeSpec, error) {
	src, err := c.OpenVolume(ctx, srcName)
	if err != nil {
		return meta.VolumeSpec{}, err
	}
	if err := src.Flush(ctx); err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: clone %q of %q: flush: %w", dstName, srcName, err)
	}
	spec, err := meta.CloneVolume(ctx, c.meta, srcName, dstName, quota)
	if err != nil {
		return meta.VolumeSpec{}, fmt.Errorf("client: clone %q of %q: %w", dstName, srcName, err)
	}
	return spec, nil
}

// applySpec swaps the live spec (the config watch converges to the
// same record shortly; storing it here makes the change immediate for
// this handle).
func (v *Volume) applySpec(spec meta.VolumeSpec) {
	s := spec
	v.cur.Store(&s)
}

// vacuumGenRanges sweeps whole frozen ranges off the OSDs. It reuses
// the deleted-volume tombstone machinery: the ranges ride one
// tombstone record (base range + extents), the standard vacuum engine
// removes every OID (idempotent OpRemove trickle), and the tombstone
// is dropped on completion. On a partial failure the tombstone stays
// pending for the background VacuumLoop (or the next unistorctl
// vacuum), exactly like a deleted volume's range.
func (c *Cluster) vacuumGenRanges(ctx context.Context, name string, ranges []meta.GenRange) error {
	if len(ranges) == 0 {
		return nil
	}
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
		return fmt.Errorf("vacuum tombstone: %w", err)
	}
	if _, err := c.VacuumVolume(ctx, name); err != nil {
		return err
	}
	// VacuumVolume removes the tombstone when the sweep completes; the
	// delete here is the same idempotent call for the engine paths
	// that leave it (missing tombstone is not an error).
	return meta.DeleteVacuum(ctx, c.meta, name)
}

// readSnapshotChain walks the frozen generations newest-to-oldest
// looking for logical object index (the live range reported it
// missing). It reports found=false when no generation has the object
// (the block is zeros) and a non-nil error only when a generation's
// replicas refused to answer at all (real I/O error, not a miss).
func (v *Volume) readSnapshotChain(ctx context.Context, p []byte, index, inObj uint64) (bool, error) {
	cur := v.cur.Load()
	for i := len(cur.Snapshots) - 1; i >= 0; i-- {
		g := &cur.Snapshots[i]
		if !g.GenBit(index) {
			continue // this generation never wrote the object
		}
		oid, ok := g.Lookup(index)
		if !ok {
			continue // index beyond the frozen geometry: zeros
		}
		found, err := v.readGenChunk(ctx, oid, index, inObj, p)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

// readGenChunk reads one frozen-generation object from its replicas.
// Placement is by logical index (a generation's object for index
// lives on the same PG), so targets() is reused unchanged. Generations
// are immutable: a bounded two-attempt loop with a placement refresh
// is enough (no hard-mode outage wait - the data cannot move).
func (v *Volume) readGenChunk(ctx context.Context, oid object.OID, index, inObj uint64, p []byte) (bool, error) {
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return false, err
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		var scratch [maxReplicas]placement.Ref
		refs, terr := v.targets(snap, index, scratch[:])
		lastErr = terr
		if terr != nil {
			return false, terr
		}
		// Frozen generations are immutable: any replica is
		// authoritative, so read from the cheapest one.
		v.orderReadRefs(refs)
		var sawMissing bool
	refresh:
		for _, ref := range refs {
			pp, err := v.cluster.pool(ref.OSD, ref.Addr)
			if err != nil {
				return false, err
			}
			n, err := pp.Read(ctx, uint64(oid), inObj, p)
			switch {
			case err == nil:
				v.cluster.noteSuccess(ref.OSD)
				if n < len(p) {
					// Sparse tail inside the frozen object: zeroes.
					for i := n; i < len(p); i++ {
						p[i] = 0
					}
				}
				return true, nil
			case sdsnet.IsObjectMissing(err), isStatus(err, sdsnet.StatusEOF):
				sawMissing = true
				lastErr = nil
				continue
			case isStale(err):
				lastErr = err
				break refresh
			default:
				if _, isStatusErr := sdsnet.AsError(err); isStatusErr {
					v.cluster.noteSuccess(ref.OSD) // it answered
				} else {
					v.cluster.noteFailure(ref.OSD)
				}
				lastErr = err
				continue
			}
		}
		if sawMissing {
			// Every replica says the frozen object is absent: this
			// generation does not have it (its bitmap bit was stale
			// for a write that raced the snapshot flip). Fall through
			// to the older generation.
			return false, nil
		}
		if !isStale(lastErr) {
			return false, lastErr
		}
		if snap, err = v.cluster.forceRefresh(ctx); err != nil {
			return false, err
		}
	}
	return false, fmt.Errorf("client: volume %q snapshot read oid %d: %w", v.spec.Name, oid, lastErr)
}
