// Package qos is a token-bucket rate limiter for IOPS and bandwidth,
// shared by the client (per-volume limits) and the OSD server (per-disk
// aggregate limits).
//
// Semantics: two independent buckets (ops, bytes) drained by each
// request and refilled continuously at the configured rates. A request
// waits until both buckets cover it, so a limiter configured with
// (IOPS, Bps) enforces the conjunction: 4k random reads hit the IOPS
// ceiling, 1 MiB sequential ones hit the bandwidth ceiling, and neither
// shape can cheat the other. Zero rate = unlimited on that axis; a
// limiter with both axes zero is a no-op (admin class).
//
// A single request may exceed the bucket capacity (a 1 MiB read on a
// 256 KiB bucket): it is admitted alone once the bucket reaches its full
// capacity, going temporarily negative. Refusing it outright would
// deadlock a volume whose IO is coarser than its limit.
package qos

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Quantum is the burst window: capacity equals rate*Quantum, i.e. a full
// idle Limiter allows ~100 ms of unlimited traffic before the sustained
// rate clamps down. Small enough that VM guests see the configured rate
// as a steady ceiling, large enough to absorb cluster-typical burstiness
// (a page-cache flush, a filesystem's metadata burst).
const Quantum = 100 * time.Millisecond

// ErrClosed is returned by Wait after Close: callers translate a closed
// limiter into "the volume is going away", never into an I/O error.
var ErrClosed = errors.New("qos: limiter closed")

// Limiter is a token bucket over both axes. A nil *Limiter allows
// everything (all methods are nil-safe), which keeps call sites free of
// nil checks.
type Limiter struct {
	mu    sync.Mutex
	iops  float64 // ops per second (0 = unlimited)
	bps   float64 // bytes per second (0 = unlimited)
	ops   float64 // available op tokens
	bytes float64 // available byte tokens
	capOp float64 // bucket capacity: rate * Quantum
	capBy float64
	last  time.Time
	wg    sync.WaitGroup // live Wait() calls
	quit  chan struct{}  // closed by Close; wakes every waiter
}

// New returns a limiter allowing iops ops/s and bps bytes/s; 0 on an
// axis means unlimited there. If both are zero it returns nil: no
// limiting at all (the admin class).
func New(iops uint32, bps uint64) *Limiter {
	if iops == 0 && bps == 0 {
		return nil
	}
	l := &Limiter{
		iops:  float64(iops),
		bps:   float64(bps),
		capOp: float64(iops) * Quantum.Seconds(),
		capBy: float64(bps) * Quantum.Seconds(),
		last:  time.Now(),
		quit:  make(chan struct{}),
	}
	if l.capOp < 1 {
		l.capOp = 1
	}
	if l.capBy < 1 {
		l.capBy = 1
	}
	l.ops, l.bytes = l.capOp, l.capBy
	return l
}

// refill credits tokens for elapsed time. Callers hold l.mu.
func (l *Limiter) refill(now time.Time) {
	dt := now.Sub(l.last).Seconds()
	if dt <= 0 {
		return
	}
	l.last = now
	if l.iops > 0 {
		l.ops += dt * l.iops
		if l.ops > l.capOp {
			l.ops = l.capOp
		}
	}
	if l.bps > 0 {
		l.bytes += dt * l.bps
		if l.bytes > l.capBy {
			l.bytes = l.capBy
		}
	}
}

// admitted reports whether nOps/nBytes fit the buckets. A request larger
// than a bucket's capacity passes when that bucket is full (it then goes
// negative): refusing it outright would deadlock coarse IO. Callers hold
// l.mu.
func (l *Limiter) admitted(nOps, nByt float64) bool {
	if l.iops > 0 && l.ops < nOps && !(nOps > l.capOp && l.ops >= l.capOp) {
		return false
	}
	if l.bps > 0 && l.bytes < nByt && !(nByt > l.capBy && l.bytes >= l.capBy) {
		return false
	}
	return true
}

// charge deducts tokens for an admitted request. Callers hold l.mu.
func (l *Limiter) charge(nOps, nByt float64) {
	if l.iops > 0 {
		l.ops -= nOps
	}
	if l.bps > 0 {
		l.bytes -= nByt
	}
}

// nextReady estimates when the buckets can admit nOps/nBytes (full
// capacity for oversized requests). Callers hold l.mu.
func (l *Limiter) nextReady(nOps, nByt float64) time.Duration {
	var need time.Duration
	if l.iops > 0 {
		want := nOps
		if want > l.capOp {
			want = l.capOp
		}
		if d := want - l.ops; d > 0 {
			need = maxDur(need, time.Duration(d/l.iops*float64(time.Second)))
		}
	}
	if l.bps > 0 {
		want := nByt
		if want > l.capBy {
			want = l.capBy
		}
		if d := want - l.bytes; d > 0 {
			need = maxDur(need, time.Duration(d/l.bps*float64(time.Second)))
		}
	}
	return need
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

// Wait blocks until one op of nBytes bytes is admissible, then charges
// it. A nil limiter returns immediately. It returns ctx.Err() on
// cancellation and ErrClosed after Close.
func (l *Limiter) Wait(ctx context.Context, nBytes uint64) error {
	if l == nil {
		return nil
	}
	l.wg.Add(1)
	defer l.wg.Done()
	l.mu.Lock()
	defer l.mu.Unlock()
	for {
		if l.quit == nil { // Close() before the first refill: stay honest
			return ErrClosed
		}
		select {
		case <-l.quit:
			return ErrClosed
		default:
		}
		l.refill(time.Now())
		if l.admitted(1, float64(nBytes)) {
			l.charge(1, float64(nBytes))
			return nil
		}
		wait := l.nextReady(1, float64(nBytes))
		quit := l.quit // captured under the lock: Close may clear it
		l.mu.Unlock()
		timer := time.NewTimer(wait)
		err := sleepCtx(ctx, timer, quit)
		timer.Stop()
		l.mu.Lock()
		if err != nil {
			return err
		}
	}
}

// sleepCtx waits for the timer, ctx cancellation, or quit. The timer is
// owned by the caller so its channel cannot leak into a later select.
func sleepCtx(ctx context.Context, timer *time.Timer, quit chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-quit:
		return ErrClosed
	case <-timer.C:
		return nil
	}
}

// Close wakes every waiter with ErrClosed: a volume being closed must
// not leave blocked writers sitting until their ctx dies.
func (l *Limiter) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	if l.quit != nil {
		close(l.quit)
		l.quit = nil
	}
	l.mu.Unlock()
	l.wg.Wait()
}

// Stats reports the sustained rates (0 = unlimited axis).
func (l *Limiter) Stats() (iops uint32, bps uint64) {
	if l == nil {
		return 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return uint32(l.iops), uint64(l.bps)
}

// Pair is the read/write limiter pair enforced for one class of traffic
// (a volume, or a whole disk). A nil *Pair limits nothing.
type Pair struct {
	Read  *Limiter
	Write *Limiter
}

// NewPair builds a Pair from rates (0 = unlimited axis); nil when every
// axis is zero (the admin class: no limits at all).
func NewPair(readIOPS, readBPS, writeIOPS, writeBPS uint64) *Pair {
	p := &Pair{
		Read:  New(uint32(readIOPS), readBPS),
		Write: New(uint32(writeIOPS), writeBPS),
	}
	if p.Read == nil && p.Write == nil {
		return nil
	}
	return p
}

// Limited reports whether the pair enforces anything (nil-safe).
func (p *Pair) Limited() bool { return p != nil && (p.Read != nil || p.Write != nil) }

// Close releases both limiters (nil-safe).
func (p *Pair) Close() {
	if p == nil {
		return
	}
	p.Read.Close()
	p.Write.Close()
}
