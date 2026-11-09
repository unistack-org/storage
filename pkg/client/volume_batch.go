// volume_batch.go — transparent read micro-batching.
//
// The VDUSE/NBD data paths drive dozens of parallel 4K reads through
// Volume.ReadAt. One chunk = one request = one round-trip, and at high
// queue depth the per-request syscalls (one write + one read per op)
// dominate. The batcher coalesces concurrently in-flight chunk reads
// aimed at the SAME OSD into one sdsnet OpBatch round-trip: N
// payload-carrying replies per two syscalls.
//
// Only reads are batched. Writes fan out R replicas per chunk and
// commit on quorum: the fan-out, not the round-trip, is their cost, and
// they are latency-critical. Snapshot-backed volumes keep the single-op
// path, where the copy-on-read chain walk lives.
//
// Structure: one collector goroutine per (volume, OSD). A read enqueues
// a slot and parks on its reply channel; the collector dispatches when
// the window fills, the window timer expires, or the volume starts
// closing. Each dispatch is exactly ONE round-trip against that OSD,
// i.e. one level of readChunk's replica loop. The caller keeps its
// replica-fallthrough switch untouched, so semantics are the single-op
// path's verbatim: NotFound on this replica means "try the next
// replica", all-replicas-missing means authoritative zeros, Stale
// refreshes placement and restarts, a transport failure rotates the
// replica via noteFailure. A degraded slot falls through to the next
// replica's group, where it is batched again — the fallthrough itself
// is batched.
//
// SDS_CLIENT_BATCH=0 disables the coalescing (A/B benchmark switch);
// it never changes behaviour, only batching.

package client

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

const (
	// batchWindow matches sdsnet.MaxBatchOps: 64 x 4K = 256K per
	// round-trip.
	batchWindow = sdsnet.MaxBatchOps
	// batchIntakeCap bounds one collector's intake queue; senders block
	// when it is full, which is strictly better than unbounded queueing.
	batchIntakeCap = 512
	// batchWindowDelay caps the coalescing latency: an isolated read
	// pays at most this; under sustained load the window fills first.
	batchWindowDelay = 15 * time.Microsecond
)

// batchEnabled gates the micro-batcher.
var batchEnabled = os.Getenv("SDS_CLIENT_BATCH") != "0"

// Batch coalescing counters, for tests and for a pprof/health readout:
// dispatches counts round-trips, slots counts chunk reads served.
// slots/dispatch is the average window fill.
var (
	batchDispatches atomic.Int64
	batchSlots      atomic.Int64
)

// BatchStats reports (round-trips, chunk reads served since process
// start) for the read micro-batcher.
func BatchStats() (dispatches, slots int64) {
	return batchDispatches.Load(), batchSlots.Load()
}

// batchSlot is one chunk read handed to a collector for one OSD.
type batchSlot struct {
	oid   uint64
	inObj uint64
	p     []byte
	reply chan batchSlotResult
	// abandoned is set by the submitter when it stops waiting (ctx
	// cancel, volume closing). The dispatch then drops the read
	// destination: the reply bytes are discarded, so a late fill can
	// never write into a buffer the caller has reused.
	abandoned atomic.Bool
}

type batchSlotResult struct {
	n      int
	status sdsnet.Status
	// transport is set when the whole round-trip failed. The caller
	// treats it exactly like a Conn.Read transport error (noteFailure +
	// next replica), which is why it is reported, not hidden.
	transport error
}

// batchGroup collects reads for one OSD of one volume.
type batchGroup struct {
	pp   *sdsnet.Pool
	done chan struct{} // closeBatchers: stop collecting

	intake chan *batchSlot

	// inflight bounds concurrent windows per OSD (pipeline depth):
	// while one round-trip drains, the collector keeps forming the
	// next window. Pool.Get hands each dispatch its own connection
	// (pool limit is unlimited by default).
	inflight chan struct{}
}

// batchGroupFor returns (creating on demand) the collector for an OSD.
func (v *Volume) batchGroupFor(ref placement.Ref) (*batchGroup, error) {
	v.batchMu.Lock()
	defer v.batchMu.Unlock()
	if v.batchClosed {
		return nil, ErrClosed
	}
	if v.batchGroups == nil {
		v.batchGroups = map[int32]*batchGroup{}
	}
	bg, ok := v.batchGroups[ref.OSD]
	if ok && bg.pp.Closed() {
		// Cluster.dropPool replaced this OSD's pool (restart/recovery):
		// the collector is retired (its dispatches get the closed-pool
		// error, which readChunk treats as a transport failure) and a
		// fresh one is built over the new pool.
		delete(v.batchGroups, ref.OSD)
		close(bg.done)
		ok = false
	}
	if ok {
		return bg, nil
	}
	pp, err := v.cluster.pool(ref.OSD, ref.Addr)
	if err != nil {
		return nil, err
	}
	bg = &batchGroup{
		pp:       pp,
		done:     make(chan struct{}),
		intake:   make(chan *batchSlot, batchIntakeCap),
		inflight: make(chan struct{}, 2),
	}
	v.batchGroups[ref.OSD] = bg
	v.batchWG.Add(1)
	go v.runBatchGroup(bg)
	return bg, nil
}

// runBatchGroup forms windows and issues one Pool.Batch per window.
func (v *Volume) runBatchGroup(bg *batchGroup) {
	defer v.batchWG.Done()
	var win []*batchSlot
	var timer *time.Timer
	var fire <-chan time.Time
	lastArrival := time.Now().Add(-2 * batchWindowDelay)
	for {
		select {
		case <-bg.done:
			// Release anyone queued so no sender parks forever.
			for {
				select {
				case s := <-bg.intake:
					s.reply <- batchSlotResult{transport: ErrClosed}
				default:
					return
				}
			}
		case s := <-bg.intake:
			isolated := time.Since(lastArrival) > batchWindowDelay
			lastArrival = time.Now()
			win = append(win, s)
			if timer == nil {
				timer = time.NewTimer(batchWindowDelay)
				fire = timer.C
			}
			// Opportunistic dispatch: an arrival that is isolated in
			// time (a quiet stream, e.g. queue-depth 1) has nothing to
			// coalesce with, so sending it now is pure latency saved —
			// the timer window only pays off when arrivals are dense.
			// len(intake)==0 keeps bursts together: the collector must
			// not cut a window while more slots are already queued.
			if isolated && len(bg.intake) == 0 && len(win) < batchWindow {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer, fire = nil, nil
				v.launch(bg, win)
				win = make([]*batchSlot, 0, batchWindow)
				continue
			}
			if len(win) >= batchWindow {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer, fire = nil, nil
				v.launch(bg, win)
				win = make([]*batchSlot, 0, batchWindow)
			}
		case <-fire:
			timer, fire = nil, nil
			if len(win) > 0 {
				v.launch(bg, win)
				win = make([]*batchSlot, 0, batchWindow)
			}
		}
	}
}

// launch dispatches a window without blocking the collector: at most
// two windows per OSD are in flight; when both slots are busy the
// collector blocks here, which is the natural backpressure (the intake
// queue absorbs arrivals meanwhile).
func (v *Volume) launch(bg *batchGroup, win []*batchSlot) {
	select {
	case bg.inflight <- struct{}{}:
	case <-bg.done:
		for _, s := range win {
			s.reply <- batchSlotResult{transport: ErrClosed}
		}
		return
	}
	v.batchWG.Add(1)
	go func() {
		defer v.batchWG.Done()
		v.dispatch(bg, win)
		<-bg.inflight
	}()
}

// dispatch issues the batch and reports per-slot status verbatim.
func (v *Volume) dispatch(bg *batchGroup, win []*batchSlot) {
	reqs := make([]sdsnet.BatchReq, len(win))
	res := make([]sdsnet.BatchResult, len(win))
	for i, s := range win {
		reqs[i] = sdsnet.BatchReq{
			Op:     sdsnet.OpRead,
			OID:    s.oid,
			Offset: s.inObj,
			Len:    uint32(len(s.p)),
		}
		if !s.abandoned.Load() {
			reqs[i].P = s.p
		}
	}
	batchDispatches.Add(1)
	batchSlots.Add(int64(len(win)))
	err := bg.pp.Batch(context.Background(), reqs, res)
	if err != nil {
		// Whole round-trip failed: every slot sees the transport error,
		// exactly as if its own Conn.Read had failed, and readChunk
		// applies its normal noteFailure + next-replica logic. Pool.Do
		// already retried the window once on a dead connection.
		for _, s := range win {
			s.reply <- batchSlotResult{transport: err}
		}
		return
	}
	for i, s := range win {
		if i >= len(res) {
			// Defensive: a short reply array means a desyncing peer.
			s.reply <- batchSlotResult{transport: sdsnet.ErrProtocol}
			continue
		}
		s.reply <- batchSlotResult{n: int(res[i].Value), status: res[i].Status}
	}
}

// batchRead performs one batched read of p at oid/inObj against ref.
// handled=false means "batcher unavailable, use the single-op path"
// (disabled, snapshots active, pool not dialable, volume closing); the
// caller then falls back to Pool.Read, so nothing is lost — including
// against a legacy peer, whose ErrBatchNotSupported preflight makes
// batchRead report the error once and the NEXT chunk read go
// single-op (see the ErrBatchNotSupported case below). Otherwise it
// returns exactly what Pool.Read would have returned, so the caller's
// status switch needs no batch-specific cases.
func (v *Volume) batchRead(ctx context.Context, ref placement.Ref, oid, inObj uint64, p []byte) (n int, err error, handled bool) {
	if !batchEnabled || v.hasSnapshots() {
		return 0, nil, false
	}
	if v.batchDisabled.Load() {
		return 0, nil, false
	}
	bg, gerr := v.batchGroupFor(ref)
	if gerr != nil {
		return 0, nil, false
	}
	s := &batchSlot{
		oid:   oid,
		inObj: inObj,
		p:     p,
		reply: make(chan batchSlotResult, 1),
	}
	select {
	case bg.intake <- s:
	case <-bg.done:
		return 0, nil, false
	case <-ctx.Done():
		return 0, ctx.Err(), true
	}
	select {
	case r := <-s.reply:
		if r.transport != nil {
			// A peer without negotiated batch support: stop batching
			// for this volume (the pool may hand out more such
			// connections) and let the caller fall back so THIS read
			// still succeeds via the single-op path.
			if r.transport == sdsnet.ErrBatchNotSupported {
				v.batchDisabled.Store(true)
				return 0, nil, false
			}
			return 0, r.transport, true
		}
		if r.status != sdsnet.StatusOK {
			return 0, &sdsnet.Error{Op: sdsnet.OpRead, Status: r.status}, true
		}
		return r.n, nil, true
	case <-bg.done:
		// Closing mid-flight: abandon the slot. Its reply is a
		// buffered channel, so the collector's write cannot block.
		s.abandoned.Store(true)
		return 0, nil, false
	case <-ctx.Done():
		s.abandoned.Store(true)
		return 0, ctx.Err(), true
	}
}

// closeBatchers stops every collector. Called from Volume.Close.
func (v *Volume) closeBatchers() {
	v.batchMu.Lock()
	groups := v.batchGroups
	v.batchGroups = nil
	v.batchClosed = true
	v.batchMu.Unlock()
	for _, bg := range groups {
		close(bg.done)
	}
	v.batchWG.Wait()
}
