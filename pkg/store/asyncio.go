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
// The stack-allocated completion is passed to the ring by pointer; the
// ring contract guarantees Done fires exactly once, so after it the
// stack frame is safe to unwind (the worker never touches it again).
func (s *Store) ringIO(e *entry, p []byte, off int64, write bool) (int, error) {
	compl := &ringCompletion{signal: make(chan struct{}, 1)}
	op := iouring.Op{
		Fd:    e.fd,
		Buf:   p,
		Off:   off,
		Write: write,
		Done:  compl.done,
	}
	if err := s.ring.Submit([]iouring.Op{op}); err != nil {
		return 0, err
	}
	<-compl.signal
	n, err := compl.n, compl.err
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
