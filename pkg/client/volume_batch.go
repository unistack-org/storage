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
// Slots circulate through the owning group's free list. The buffered
// reply channel is the ownership token: whoever receives a posted
// reply recycles the slot (the submitter normally; dispatch itself
// when the submitter walked away), so a slot is put back exactly once
// and its channel is always empty on pool entry.
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

	// work queues finished windows for the group's two persistent
	// workers: pipeline depth 2 (while one round-trip drains, the
	// collector forms the next window; Pool.Get hands each dispatch
	// its own connection, pool limit is unlimited by default). A
	// send blocks when both slots are busy: the natural backpressure.
	work chan []*batchSlot

	// slotFree recycles slots (reply channel included); winFree
	// recycles window slices; scratchFree the reqs/res arrays. They are
	// channels, not sync.Pools: a Pool caches per-P and is flushed at
	// every GC, so on a contended runner (workload goroutines migrate,
	// collectors are descheduled) Get misses its cache and New re-allocs
	// per operation. A capacity-bounded channel queue recycles across
	// goroutines reliably; the caps bound residency, and a full free
	// list just hands the object to the GC.
	slotFree    chan *batchSlot
	winFree     chan []*batchSlot
	scratchFree chan *batchScratch
}

// batchScratch is one dispatch's request/reply arrays.
type batchScratch struct {
	reqs []sdsnet.BatchReq
	res  []sdsnet.BatchResult
}

func (bg *batchGroup) takeSlot() *batchSlot {
	var s *batchSlot
	select {
	case s = <-bg.slotFree:
	default:
		s = &batchSlot{reply: make(chan batchSlotResult, 1)}
	}
	s.oid, s.inObj, s.p = 0, 0, nil
	s.abandoned.Store(false)
	select {
	case <-s.reply: // defensive: a posted reply nobody drained
	default:
	}
	return s
}

func (bg *batchGroup) putSlot(s *batchSlot) {
	s.p = nil
	select {
	case bg.slotFree <- s:
	default: // free list full: let the GC take it
	}
}

func (bg *batchGroup) takeWin() []*batchSlot {
	select {
	case w := <-bg.winFree:
		return w[:0]
	default:
		return make([]*batchSlot, 0, batchWindow)
	}
}

func (bg *batchGroup) putWin(win []*batchSlot) {
	for i := range win {
		win[i] = nil
	}
	select {
	case bg.winFree <- win[:0]:
	default: // free list full (2 workers): let the GC take it
	}
}

func (bg *batchGroup) takeScratch(n int) *batchScratch {
	var sc *batchScratch
	select {
	case sc = <-bg.scratchFree:
		if cap(sc.reqs) >= n {
			return sc
		}
	default:
	}
	// Cold list (or a window larger than anything seen): allocate the
	// arrays for this size; the next dispatch of the same shape takes
	// the recycled pair.
	return &batchScratch{
		reqs: make([]sdsnet.BatchReq, n),
		res:  make([]sdsnet.BatchResult, n),
	}
}

func (bg *batchGroup) putScratch(sc *batchScratch) {
	select {
	case bg.scratchFree <- sc:
	default: // free list full: let the GC take it
	}
}

// post hands a slot its result. The reply value sitting in the
// buffered channel is the ownership token: the submitter recycles the
// slot when it takes the reply; a submitter that walked away (sets
// abandoned before leaving) cedes it, and post steals its own reply
// back and recycles. Exactly one party ever wins the receive, so the
// slot is put exactly once and the channel is empty on pool entry.
func (bg *batchGroup) post(s *batchSlot, r batchSlotResult) {
	s.reply <- r
	if s.abandoned.Load() {
		select {
		case <-s.reply:
			bg.putSlot(s)
		default: // the submitter took the reply: it owns the slot
		}
	}
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
		pp:      pp,
		done:    make(chan struct{}),
		intake:  make(chan *batchSlot, batchIntakeCap),
		work:    make(chan []*batchSlot, 2),
		winFree: make(chan []*batchSlot, 2),
		// Every in-flight slot is held by exactly one of: the intake
		// queue, a window (collector + 2 work slots + in-flight),
		// transiently by a submitter, or a cancelled sender awaiting
		// service — this cap covers them all, so steady-state puts
		// never overflow.
		slotFree:    make(chan *batchSlot, batchIntakeCap+4*batchWindow+16),
		scratchFree: make(chan *batchScratch, 4),
	}
	v.batchGroups[ref.OSD] = bg
	v.batchWG.Add(3) // collector + two persistent workers
	go v.runBatchGroup(bg)
	go v.batchWorker(bg)
	go v.batchWorker(bg)
	return bg, nil
}

// runBatchGroup forms windows and issues one Pool.Batch per window.
func (v *Volume) runBatchGroup(bg *batchGroup) {
	defer v.batchWG.Done()
	var win []*batchSlot
	// One timer object for the collector's life: an isolated stream
	// (queue depth 1) publishes every arrival, so creating a fresh
	// timer per window would allocate one timer per operation.
	timer := time.NewTimer(batchWindowDelay)
	timer.Stop()
	fire := timer.C
	armed := false
	lastArrival := time.Now().Add(-2 * batchWindowDelay)
	for {
		select {
		case <-bg.done:
			// Release anyone queued so no sender parks forever; the
			// workers see the closed work queue and exit after their
			// current window.
			for {
				select {
				case s := <-bg.intake:
					bg.post(s, batchSlotResult{transport: ErrClosed})
				default:
					close(bg.work)
					return
				}
			}
		case s := <-bg.intake:
			isolated := time.Since(lastArrival) > batchWindowDelay
			lastArrival = time.Now()
			win = append(win, s)
			if len(win) >= batchWindow {
				if armed {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					armed = false
				}
				win = v.publish(bg, win)
				continue
			}
			if !armed {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(batchWindowDelay)
				armed = true
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
				armed = false
				win = v.publish(bg, win)
				continue
			}
		case <-fire:
			armed = false
			win = v.publish(bg, win)
		}
	}
}

// publish hands a finished window to the workers (the bounded work
// queue IS the pipeline limit: a send blocks while both worker slots
// are busy, the intake queue absorbs arrivals meanwhile) and returns
// the recycled slice the collector fills next. Only the collector
// publishes, and only the collector closes bg.work, so the send can
// never race the close.
func (v *Volume) publish(bg *batchGroup, win []*batchSlot) []*batchSlot {
	if len(win) == 0 {
		return win
	}
	select {
	case bg.work <- win:
	case <-bg.done:
		for _, s := range win {
			bg.post(s, batchSlotResult{transport: ErrClosed})
		}
		bg.putWin(win)
	}
	return bg.takeWin()
}

// batchWorker runs windows off the work queue: persistent goroutines
// (one per group, two groups' worth of slots), one Batch round-trip at
// a time each, so forming a window costs no goroutine.
func (v *Volume) batchWorker(bg *batchGroup) {
	defer v.batchWG.Done()
	for win := range bg.work {
		v.dispatch(bg, win)
	}
}

// dispatch issues the batch and reports per-slot status verbatim.
// Every slot leaves through bg.post, which routes the slot back to
// the pool exactly once (submitter-owned slots stay with the
// submitter).
func (v *Volume) dispatch(bg *batchGroup, win []*batchSlot) {
	sc := bg.takeScratch(len(win))
	reqs, res := sc.reqs[:len(win)], sc.res[:len(win)]
	defer bg.putScratch(sc)
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
			bg.post(s, batchSlotResult{transport: err})
		}
		bg.putWin(win)
		return
	}
	for i, s := range win {
		if i >= len(res) {
			// Defensive: a short reply array means a desyncing peer.
			bg.post(s, batchSlotResult{transport: sdsnet.ErrProtocol})
			continue
		}
		bg.post(s, batchSlotResult{n: int(res[i].Value), status: res[i].Status})
	}
	bg.putWin(win)
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
	s := bg.takeSlot()
	s.oid, s.inObj, s.p = oid, inObj, p
	select {
	case bg.intake <- s:
	case <-bg.done:
		bg.putSlot(s)
		return 0, nil, false
	case <-ctx.Done():
		bg.putSlot(s)
		return 0, ctx.Err(), true
	}
	select {
	case r := <-s.reply:
		// The reply was ours, so the slot is: recycling here is the
		// normal (non-abandoned) path and pairs with post's rule.
		bg.putSlot(s)
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
		// Closing (or cancelling) mid-flight: abandon the slot. Its
		// reply is a buffered channel, so the reply write cannot
		// block, and abandoned transfers recycling ownership to post.
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
