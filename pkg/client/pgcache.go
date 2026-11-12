package client

import (
	"sync"

	"github.com/unistack-org/storage/pkg/placement"
)

// pgCache is the per-volume placement-string cache (vitastor-ideas
// §3.2): resolving an object index to its replica refs costs a hash, a
// pg-row read and one registry lookup per replica — on a 4K randread
// path that is per-I/O work over the same handful of pgs. The cache is
// keyed by pg and valid for exactly one placement snapshot: snapshots
// are immutable and Refresh returns the same pointer while nothing
// moved, so pointer identity is the whole invalidation rule — a new
// epoch (a map change, a reweight, a registry edit) simply misses the
// cache and the first I/O per pg refills it.
type pgCache struct {
	mu   sync.Mutex
	snap *placement.Snapshot
	m    map[uint32]pgEntry
}

type pgEntry struct {
	refs []placement.Ref // resolved live targets, map order
	err  error           // targets' verdict for the row (nil / ErrDegraded)
}

// pgCacheCap bounds the map: a cluster table has at most PGNum pgs,
// and volumes that touch tens of thousands of distinct pgs per epoch
// (a vacuum sweep) get a plain map walk instead of unbounded growth.
const pgCacheCap = 1 << 16

// resolve returns the volume's replica refs for pg under snap, from
// the cache when it matches (pointer identity) and computing through
// fn otherwise. The refs are copied into scratch (callers keep their
// allocation-free hot path); the returned slice aliases scratch.
func (c *pgCache) resolve(snap *placement.Snapshot, pg uint32, scratch []placement.Ref, fn func() ([]placement.Ref, error)) ([]placement.Ref, error) {
	c.mu.Lock()
	if c.snap != snap {
		c.snap = snap
		if c.m == nil {
			c.m = make(map[uint32]pgEntry, 64)
		} else {
			clear(c.m)
		}
	}
	e, ok := c.m[pg]
	c.mu.Unlock()
	if !ok {
		refs, err := fn()
		e = pgEntry{refs: append([]placement.Ref(nil), refs...), err: err}
		c.mu.Lock()
		if c.snap == snap { // still current? (a concurrent epoch swap
			if len(c.m) < pgCacheCap { // makes this entry stale — skip it)
				c.m[pg] = e
			}
		}
		c.mu.Unlock()
	}
	n := copy(scratch, e.refs)
	return scratch[:n], e.err
}
