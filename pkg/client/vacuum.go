package client

// Vacuum (plan M1.4): removing a volume's object garbage from the
// OSDs after DeleteVolume.
//
// DeleteVolume leaves a tombstone (meta.VacuumRecord: name + private
// OID range) before it unregisters the volume. VacuumDeletedVolumes
// consumes tombstones: for every OSD of the cluster (one OSD at a
// time, so a vacuum never floods the network) it sends OpRemove over
// the whole range [OIDBase+1, OIDBase+NumObjs] and drops the tombstone
// once every OSD answered cleanly. OpRemove is idempotent, so the
// vacuum is safely replayable — it costs a stats-less unlink per OID
// where the object is absent.
//
// An unreachable OSD keeps the tombstone pending: its objects are
// vacuumed on the next pass (the cluster keeps running in the
// meantime; the garbage only wastes disk).

import (
	"context"
	"fmt"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// VacuumOSDResult is one OSD's share of a vacuum pass.
type VacuumOSDResult struct {
	OSD     int32
	Removed int   // objects the OSD reported removed (all OIDs are removed, existing or not)
	Err     error // transport/status error: the tombstone stays pending
}

// VacuumResult summarises one VacuumDeletedVolumes pass.
type VacuumResult struct {
	Vacuums int // tombstones seen
	Deleted int // tombstones fully vacuumed (every OSD done)
	OSDs    []VacuumOSDResult
	Errs    []error // per-tombstone/OSD failures (non-fatal to the pass)
}

// PendingVacuum lists tombstones awaiting vacuum.
func (c *Cluster) PendingVacuum(ctx context.Context) ([]meta.VacuumRecord, error) {
	return meta.ListVacuum(ctx, c.meta)
}

// VacuumDeletedVolumes lives in vacuum_volume.go: it lists the
// pending tombstones and hands them to the shared vacuumRecs engine,
// which VacuumVolume (one tombstone by name) reuses.

// vacuumOSD removes rec's whole OID range on one OSD and returns the
// number of OIDs it processed.
func vacuumOSD(ctx context.Context, c *Cluster, info meta.OSDInfo, rec meta.VacuumRecord) (int, error) {
	pp, err := c.pool(info.ID, info.Addr)
	if err != nil {
		return 0, err
	}
	done := 0
	for i := uint64(1); i <= rec.NumObjs; i++ {
		if err := ctx.Err(); err != nil {
			return done, err
		}
		// Keep the pool's epoch current so the remove is not fenced.
		pp.SetEpoch(c.epochSnapshot())
		if err := pp.Remove(ctx, rec.OIDBase+i); err != nil {
			if !vacuumSupported(err) {
				return done, fmt.Errorf("osd %d does not support OpRemove (upgrade it)", info.ID)
			}
			return done, err
		}
		done++
	}
	// Persist the unlinks in one flush per pass per OSD.
	if err := pp.Sync(ctx); err != nil {
		return done, fmt.Errorf("objects removed, sync failed: %w", err)
	}
	return done, nil
}

// VacuumLoop runs VacuumDeletedVolumes periodically until ctx is done.
// It is the "queue" of plan M1.4: a slow background trickle so newly
// deleted volumes are vacuumed without an operator kick. Errors are
// reported through logf and never stop the loop.
func (c *Cluster) VacuumLoop(ctx context.Context, interval time.Duration, logf func(string, ...any)) {
	if interval <= 0 {
		interval = time.Minute
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			res, err := c.VacuumDeletedVolumes(ctx)
			if err != nil {
				if logf != nil {
					logf("vacuum: pass failed: %v", err)
				}
				continue
			}
			if res.Vacuums > 0 && logf != nil {
				logf("vacuum: %d tombstone(s), %d vacuumed, %d object(s) removed",
					res.Vacuums, res.Deleted, removedTotal(res))
			}
		}
	}
}

func removedTotal(res *VacuumResult) int {
	n := 0
	for _, o := range res.OSDs {
		n += o.Removed
	}
	return n
}

// vacuumSupported probes whether an OSD understands OpRemove: an old
// server answers StatusNotSupp. Vacuum treats that OSD as "keep the
// tombstone pending" (it will be swept after the OSD is upgraded).
func vacuumSupported(err error) bool {
	if e, ok := sdsnet.AsError(err); ok && e.Status == sdsnet.StatusNotSupp {
		return false
	}
	return true
}
