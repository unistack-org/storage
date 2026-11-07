// Package replication fans writes out to a set of replica targets and
// commits them on a write quorum (phase 2: RF=3, W=2).
//
// Semantics, mirroring Ceph's primary/replica contract without a primary:
//   - a write is acknowledged once W of N targets have durably accepted
//     it; the remaining fan-out continues in the background,
//   - a write fails with ErrQuorumLost when fewer than W targets can be
//     reached; nothing is acknowledged in that case,
//   - epoch (ReqHeader.Epoch) is the placement epoch: targets serving a
//     stale map answer StatusStale... (server-side fencing lands with
//     the OSD/map distribution work; the field is carried today).
//
// Replication runs on sdsnet pools: one pool per target, connections
// reused across writes. The fan-out goroutines are per-write and short
// lived; the hot path allocations are the per-target request buffers.
package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unistack-org/storage/pkg/sdsnet"
)

// ErrQuorumLost: fewer than W targets accepted the write.
var ErrQuorumLost = errors.New("replication: write quorum lost")

// Target is one replica destination.
type Target struct {
	Addr   string // "host:port" of the sdsnet server
	Export string // volume/export name
}

// Replicator writes one object stream to N targets with quorum W.
// It owns one sdsnet pool per target.
type Replicator struct {
	targets []Target
	pools   []*sdsnet.Pool
	w       int

	// per-target result counters (telemetry/tests)
	acked  [maxTargets]atomic.Uint64
	failed [maxTargets]atomic.Uint64
}

const maxTargets = 16

// New builds a replicator for targets with write quorum w (1 <= w <=
// len(targets) <= maxTargets). Typical: RF=3 W=2.
func New(targets []Target, w int) *Replicator {
	if w < 1 || w > len(targets) || len(targets) > maxTargets {
		panic("replication: bad (targets, w) configuration")
	}
	r := &Replicator{targets: targets, w: w, pools: make([]*sdsnet.Pool, len(targets))}
	for i, t := range targets {
		r.pools[i] = sdsnet.NewPool(t.Addr, t.Export)
	}
	return r
}

// W returns the write quorum.
func (r *Replicator) W() int { return r.w }

// N returns the replica count.
func (r *Replicator) N() int { return len(r.targets) }

// Stats returns acked/failed write counters per target (monotonic).
func (r *Replicator) Stats() (acked, failed []uint64) {
	acked = make([]uint64, len(r.targets))
	failed = make([]uint64, len(r.targets))
	for i := range r.targets {
		acked[i] = r.acked[i].Load()
		failed[i] = r.failed[i].Load()
	}
	return acked, failed
}

// Write fans p out to all targets and returns nil once W of them have
// accepted it. Late/failed stragglers finish in the background (the
// call blocks at most grace after quorum).
func (r *Replicator) Write(ctx context.Context, oid, off uint64, p []byte, fua bool) error {
	return r.fanout(ctx, func(i int) error {
		return r.pools[i].Write(ctx, oid, off, p, fua)
	})
}

// WriteZeroes fans a WRITE_ZEROES/TRIM (sparse) out with the same
// quorum rules.
func (r *Replicator) WriteZeroes(ctx context.Context, oid, off, n uint64, hole, fua bool) error {
	return r.fanout(ctx, func(i int) error {
		return r.pools[i].Do(ctx, func(cc *sdsnet.Conn) error {
			return cc.WriteZeroes(ctx, oid, off, n, hole, fua)
		})
	})
}

// grace bounds how long we wait for stragglers after the quorum is met,
// so a slow replica cannot extend every write's latency indefinitely.
const grace = 2 * time.Second

// fanout runs fn against every target concurrently and reports the
// first quorum. ctx cancellation/timeout aborts the whole fan-out and
// is reported as the error (the write is not acknowledged).
func (r *Replicator) fanout(ctx context.Context, fn func(i int) error) error {
	var (
		wg      sync.WaitGroup
		ackCh   = make(chan int, len(r.targets))
		failCh  = make(chan int, len(r.targets))
		started = time.Now()
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := range r.targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := fn(i)
			if ctx.Err() != nil {
				return // parent aborted; result is moot
			}
			if err == nil {
				r.acked[i].Add(1)
				ackCh <- i
				return
			}
			r.failed[i].Add(1)
			failCh <- i
		}(i)
	}

	acks, fails := 0, 0
	for acks < r.w {
		select {
		case <-ackCh:
			acks++
		case <-failCh:
			fails++
		case <-ctx.Done():
			return ctx.Err()
		}
		if fails > len(r.targets)-r.w {
			// Too many targets already failed: reaching w acks is
			// arithmetically impossible.
			return ErrQuorumLost
		}
	}

	// Quorum reached. Drain straggler results for at most the remaining
	// grace window, then return; wg goroutines that outlive us finish
	// on their own (pools are per-target, results are counters).
	deadline := time.After(time.Until(started.Add(grace)))
drain:
	for {
		select {
		case <-ackCh:
		case <-failCh:
		case <-deadline:
			break drain
		}
		if len(ackCh) == 0 && len(failCh) == 0 {
			break drain
		}
	}
	return nil
}

// Sync persists prior writes on every target (best effort, all
// targets). Used by flush paths; a quorum-sync variant lands with the
// journal/OSD integration.
func (r *Replicator) Sync(ctx context.Context) error {
	var firstErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range r.targets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := r.pools[i].Do(ctx, func(cc *sdsnet.Conn) error { return cc.Sync(ctx) })
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

// Close closes all pools.
func (r *Replicator) Close() {
	for _, p := range r.pools {
		if p != nil {
			p.Close()
		}
	}
}
