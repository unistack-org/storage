package client

// Per-volume vacuum (plan M1.4, "вакуум по тому").
//
// Two forms:
//
//  1. Targeted tombstone vacuum (Cluster.VacuumVolume): the pinpoint
//     version of VacuumDeletedVolumes — consume ONE deleted volume's
//     tombstone instead of the whole queue. Same machinery, same
//     guarantees (idempotent OpRemove trickle per OSD, tombstone
//     dropped only when every OSD answered cleanly).
//
//  2. Live-volume vacuum (Volume.Vacuum): reclaim the space a guest
//     freed without discarding. Objects whose content is entirely
//     zeroes are punched out (sparse full-object TRIM on every
//     replica + allocation bit released), exactly like guest discard
//     does from inside. Data objects are never touched: an object is
//     reclaimed only after it was read back and found all-zero, and
//     the TRIM is idempotent, so racing writers can only see their
//     bytes rewritten as zeroes — the state vacuum was told to
//     produce.

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/placement"
)

// ErrNoVacuum records that a name has nothing to vacuum: it is not a
// registered live volume and has no pending tombstone.
var ErrNoVacuum = errors.New("client: nothing to vacuum")

// VolumeVacuumResult summarises one live-volume vacuum.
type VolumeVacuumResult struct {
	Checked int    // allocated objects read and inspected
	Zeroed  int    // objects found all-zero and punched out
	Freed   uint64 // bytes returned to the pool (Zeroed * object size)
}

// VacuumDeletedVolumes vacuums the object ranges of deleted volumes
// from every registered OSD and removes the tombstones it completed.
// It returns the result of this pass; tombstones whose vacuum hit an
// unreachable OSD (or any error) stay pending for the next call.
//
// The pass is bounded work, not a daemon: callers run it on demand
// (unistorctl vacuum) or on a schedule.
func (c *Cluster) VacuumDeletedVolumes(ctx context.Context) (*VacuumResult, error) {
	recs, err := c.PendingVacuum(ctx)
	if err != nil {
		return nil, fmt.Errorf("client: list vacuums: %w", err)
	}
	return c.vacuumRecs(ctx, recs)
}

// VacuumVolume runs one tombstone vacuum pass over the single deleted
// volume called name. It reports ErrNoVacuum when no tombstone with
// that name is pending.
func (c *Cluster) VacuumVolume(ctx context.Context, name string) (*VacuumResult, error) {
	rec, err := meta.GetVacuum(ctx, c.meta, name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			return nil, fmt.Errorf("%w: volume %q has no pending tombstone", ErrNoVacuum, name)
		}
		return nil, fmt.Errorf("client: get vacuum %q: %w", name, err)
	}
	return c.vacuumRecs(ctx, []meta.VacuumRecord{rec})
}

// vacuumRecs is the shared engine of both tombstone-vacuum entry
// points: it walks recs over every registered OSD, one OSD at a time.
func (c *Cluster) vacuumRecs(ctx context.Context, recs []meta.VacuumRecord) (*VacuumResult, error) {
	res := &VacuumResult{Vacuums: len(recs)}
	if len(recs) == 0 {
		return res, nil
	}
	// Pull the current map epoch onto every pool: a restarted OSD
	// fences requests that carry an older epoch, and a cluster kept
	// only for admin duties (like the admin HTTP server's) has no
	// write traffic to refresh it.
	if _, err := c.Refresh(ctx); err != nil {
		return res, fmt.Errorf("client: vacuum refresh: %w", err)
	}
	// Every OSD that ever served a volume can hold its garbage: vacuum
	// the full registry (meta), not the live/weighted one.
	infos, err := meta.ListOSDs(ctx, c.meta)
	if err != nil {
		return res, fmt.Errorf("client: list osds: %w", err)
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].ID < infos[j].ID })

	// Per-OSD tallies aggregate across tombstones of this pass.
	tally := map[int32]*VacuumOSDResult{}
	perOSD := func(id int32) *VacuumOSDResult {
		r, ok := tally[id]
		if !ok {
			r = &VacuumOSDResult{OSD: id}
			tally[id] = r
		}
		return r
	}

	for _, rec := range recs {
		if err := ctx.Err(); err != nil {
			res.Errs = append(res.Errs, err)
			break
		}
		allDone := true
		// One OSD at a time (plan M1.4: "по одному OSD, с очередью"):
		// each inner loop is sequential over the OID range, so a vacuum
		// is a slow trickle, never a burst.
		for _, info := range infos {
			n, err := vacuumOSD(ctx, c, info, rec)
			r := perOSD(info.ID)
			r.Removed += n
			if err != nil {
				r.Err = err
				allDone = false
				res.Errs = append(res.Errs, fmt.Errorf("client: vacuum %q on osd %d: %w", rec.Name, info.ID, err))
			}
		}
		if allDone {
			if err := meta.DeleteVacuum(ctx, c.meta, rec.Name); err != nil && !errors.Is(err, meta.ErrNotFound) {
				res.Errs = append(res.Errs, fmt.Errorf("client: drop vacuum %q: %w", rec.Name, err))
			} else {
				res.Deleted++
			}
		}
	}
	for _, r := range tally {
		res.OSDs = append(res.OSDs, *r)
	}
	sort.Slice(res.OSDs, func(i, j int) bool { return res.OSDs[i].OSD < res.OSDs[j].OSD })
	return res, nil
}

// Vacuum reclaims the space of a registered volume that is nothing
// but zeroes: every object that physically exists on the replicas is
// read back and, when it contains no non-zero byte, punched out on all
// replicas (sparse full-object TRIM) and released from the volume's
// allocation. It is the operator-side equivalent of running fstrim
// inside the guest — it works even for guests that never discard.
//
// The scan is a per-object Stat over the placement targets (the
// physical view), not the allocation bitmap: the bitmap is only
// maintained for quota-capped volumes, while every volume keeps its
// data on the OSDs. Objects allocated while the pass runs are simply
// missed and caught by the next pass. Read/trim traffic charges the
// volume's QoS limits, so a vacuum of a hot volume is naturally paced.
func (v *Volume) Vacuum(ctx context.Context) (*VolumeVacuumResult, error) {
	res := &VolumeVacuumResult{}
	snap, err := v.cluster.Refresh(ctx)
	if err != nil {
		return res, fmt.Errorf("client: volume %q vacuum refresh: %w", v.spec.Name, err)
	}
	buf := make([]byte, 1<<20)
	var scratch [maxReplicas]placement.Ref
	n := int(object.NumObjects(v.spec.Size))
	for index := 0; index < n; index++ {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		alloc, present, err := v.objectAllocated(ctx, snap, uint64(index), scratch[:])
		if err != nil {
			return res, fmt.Errorf("client: volume %q vacuum object %d: %w", v.spec.Name, index, err)
		}
		if !present || alloc == 0 {
			// Thin on every replica, or fully sparse (punched) files:
			// physically nothing to free. Skipping before Checked++ is
			// what makes a second pass over punched objects a no-op.
			continue
		}
		res.Checked++
		zero, err := v.objectIsZero(ctx, buf, uint64(index))
		if err != nil {
			return res, fmt.Errorf("client: volume %q vacuum object %d: %w", v.spec.Name, index, err)
		}
		if !zero {
			continue
		}
		// Sparse TRIM of the whole object punches the hole on every
		// replica and releases the allocation bit (releaseQuota).
		// Idempotent — racing writers see zeroes, the state vacuum
		// was asked to produce.
		if err := v.WriteZeroes(ctx, uint64(index)*object.Size, object.Size, true); err != nil {
			return res, fmt.Errorf("client: volume %q vacuum punch object %d: %w", v.spec.Name, index, err)
		}
		res.Zeroed++
		res.Freed += object.Size
	}
	return res, nil
}

// objectAllocated asks the live targets of object index how many bytes
// the object physically occupies on a replica (OpStatInfo:
// st_blocks*512). Any replica answers authoritatively: replicas are
// byte-identical at quorum commit, a straggler only lags behind with a
// thin copy (a lagging "absent" simply defers the object to the next
// pass). alloc 0 means fully sparse (punched) files.
func (v *Volume) objectAllocated(ctx context.Context, snap *placement.Snapshot, index uint64, scratch []placement.Ref) (alloc uint64, present bool, err error) {
	refs, terr := v.targets(snap, index, scratch)
	if len(refs) == 0 {
		return 0, false, terr
	}
	for _, ref := range refs {
		pp, err := v.cluster.pool(ref.OSD, ref.Addr)
		if err != nil {
			return 0, false, err
		}
		alloc, exists, err := pp.StatInfo(ctx, v.oid(index))
		if err != nil {
			continue // this replica unreachable: ask the next one
		}
		return alloc, exists, nil
	}
	if terr != nil {
		return 0, false, terr
	}
	return 0, false, fmt.Errorf("no replica answered for object %d", index)
}

// objectIsZero reads the whole object index through the data path and
// reports whether every byte is zero. A thin (never-written) object
// reads as zeroes and counts as zero, though it is not allocated (the
// bitmap gates the loop, so we never pay the read for it).
func (v *Volume) objectIsZero(ctx context.Context, buf []byte, index uint64) (bool, error) {
	base := index * object.Size
	for done := uint64(0); done < object.Size; {
		ch := uint64(len(buf))
		if object.Size-done < ch {
			ch = object.Size - done
		}
		n, err := v.ReadAt(ctx, buf[:ch], base+done)
		if err != nil {
			return false, err
		}
		for _, b := range buf[:n] {
			if b != 0 {
				return false, nil
			}
		}
		done += uint64(n)
	}
	return true, nil
}
