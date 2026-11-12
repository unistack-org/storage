package store

import (
	"io"

	"github.com/unistack-org/storage/pkg/iouring"
)

// ringIO runs one pread/pwrite through the async ring and blocks for the
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
// The stack-allocated completion is passed to the ring by pointer; the
// ring contract guarantees Done fires exactly once, so after it the
// stack frame is safe to unwind (the worker never touches it again) —
// the slot is released exactly there, before this frame unwinds.
func (s *Store) ringIO(e *entry, p []byte, off int64, write bool) (int, error) {
	compl := &ringCompletion{signal: make(chan struct{}, 1)}
	op := iouring.Op{
		Fd:    e.fd,
		Off:   off,
		Write: write,
		Done:  compl.done,
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
	if err := s.ring.Submit([]iouring.Op{op}); err != nil {
		return 0, err
	}
	<-compl.signal
	n, err := compl.n, compl.err
	if err == nil && !write && s.arena != nil && op.BufIndex != 0 {
		if n > len(p) {
			n = len(p) // slot-aligned completion; the caller asked for len(p)
		}
		copy(p, op.Buf[:n])
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

type ringCompletion struct {
	n      int
	err    error
	signal chan struct{}
}

func (c *ringCompletion) done(n int, err error) {
	c.n, c.err = n, err
	c.signal <- struct{}{}
}
