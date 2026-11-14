package store

import (
	"io"
	"sync"

	"github.com/unistack-org/storage/pkg/iouring"
)

// ringIO runs one pread/pwrite through the async I/O ring and blocks for the
// completion. The result is byte-identical to the plain os.File calls:
// a short transfer is reported as (n, io.EOF) for reads, errno as the
// error (ENOSPC mapped to ErrNoSpace by the caller's classifier).
//
// With a registered-buffer arena configured (Options.RingArenaSlots),
// the I/O moves through an arena slot: the kernel pinned the arena's
// pages once at registration, so the op runs as READ_FIXED/WRITE_FIXED
// and skips the per-I/O page pinning a plain op repeats on the
// caller's buffer; the price is one memcpy of the block, which is far
// cheaper. A read's data lands in the slot and is copied out; a
// write's data is copied in. Buffers larger than a slot take the
// plain path.
//
// The per-op completion and []Op wrap are pooled (ioComp) and submitted
// via SubmitSingle (no per-op slice), so the steady-state I/O performs no
// per-op heap allocation: the completion and its callback are allocated
// once at pool construction, and SubmitSingle copies the already-bound
// callback pointer.
func (s *Store) ringIO(e *entry, p []byte, off int64, write bool) (int, error) {
	c := s.borrowComp()
	defer s.returnComp(c)
	c.wg.Add(1)
	op := iouring.Op{
		Fd:    e.fd,
		Off:   off,
		Write: write,
		Done:  c.done,
	}
	if a := s.arena; a != nil && len(p) <= a.ar.Size() {
		i, slot := a.acquire()
		defer a.release(i)
		if write {
			copy(slot, p)
		}
		op.Buf = slot
		op.BufIndex = a.idx
	} else {
		op.Buf = p
	}
	// SubmitSingle calls `done` (0, err) on ALL paths — including a
	// synchronous ErrClosed rejection — so the comp is always resolved by
	// the ring; we just wait, then surface the submit error if any.
	submitErr := s.ring.SubmitSingle(op)
	c.wg.Wait()
	if submitErr != nil {
		return 0, submitErr
	}
	return finishRing(c.n, c.err, p, write, s.arena != nil && op.BufIndex != 0, op.Buf)
}

// borrowComp / returnComp hand a pooled completion out and back. The free
// set is a buffered channel — the same reuse idiom as ringArena.free.
func (s *Store) borrowComp() *ioComp {
	return <-s.ioComps.free
}

func (s *Store) returnComp(c *ioComp) {
	s.ioComps.free <- c
}

// finishRing applies the shared post-completion semantics to a completed op:
// arena-slot copy-out, short-read -> io.EOF, and ENOSPC -> ErrNoSpace.
func finishRing(n int, err error, p []byte, write, viaArena bool, slot []byte) (int, error) {
	if err == nil && !write && viaArena {
		if n > len(p) {
			n = len(p) // slot-aligned completion; the caller asked for len(p)
		}
		copy(p, slot[:n])
	}
	if n == 0 && err == nil && !write && len(p) > 0 {
		// A ring completion with zero bytes is a short read at EOF;
		// os.File.ReadAt surfaces it as io.EOF.
		err = io.EOF
	}
	if err != nil && isNoSpace(err) {
		err = ErrNoSpace
	}
	return n, err
}

// ioComp is a pooled ring completion. Its `done`
// callback is bound ONCE at pool construction, not per I/O: `op.Done =
// c.done` then copies a pre-existing func pointer, so a steady-state op
// that reports into a pooled completion allocates nothing. Each comp is
// checked out for the lifetime of one in-flight op and returned
// afterwards, so no completion (or its callback) is created on the
// steady-state I/O path.
//
// INVARIANT: the ring's Submit/SubmitSingle contract is that `done` fires
// exactly once on every submission path, including a synchronous ErrClosed
// rejection (via DoneGo on a fresh goroutine). ringIO relies on that:
// it never balances the WaitGroup itself, it only waits. A double `Done`
// (e.g. manually balancing on the submit-error path) would panic.
type ioComp struct {
	wg   sync.WaitGroup
	n    int
	err  error
	done func(int, error) // bound once in newIOCompPool
}

// ioCompPool hands out ioComp instances via a buffered free channel — the
// same reuse idiom as ringArena.free (arena.go).
type ioCompPool struct {
	free chan *ioComp
}

// newIOCompPool builds n completions. n should be at least the expected
// number of concurrent in-flight ringIO calls; a caller that finds the free
// set empty blocks on borrowComp (backpressure) rather than allocating.
func newIOCompPool(n int) *ioCompPool {
	p := &ioCompPool{free: make(chan *ioComp, n)}
	for i := 0; i < n; i++ {
		c := &ioComp{}
		c.done = func(n int, e error) {
			c.n, c.err = n, e
			c.wg.Done()
		}
		p.free <- c
	}
	return p
}
