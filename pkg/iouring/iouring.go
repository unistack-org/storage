// Package iouring is the async I/O ring for the data path.
//
// On Linux it wraps io_uring (IORING_SETUP_SQPOLL, registered files and
// buffers). On other platforms it degrades to a worker pool executing
// pread/pwrite — the API is identical so the data-path code is portable.
//
// Contract for all implementations:
//
//   - Submissions are batched; the caller owns p until the completion
//     callback fires exactly once.
//   - The hot path performs no allocations beyond the fixed ring slots.
package iouring

import (
	"errors"
	"os"
)

// ErrClosed is reported to queued operations when the ring is closed.
var ErrClosed = errors.New("iouring: ring closed")

// Op is an I/O operation submitted to the ring.
type Op struct {
	// Fd is the file descriptor (as int, matching os.File.Fd()).
	Fd int32
	// Buf is the transfer buffer. The kernel/caller contract: the buffer
	// must remain valid and unmodified until Done fires.
	Buf []byte
	// Off is the file offset.
	Off int64
	// Write selects pwrite (true) or pread (false).
	Write bool
	// Done is invoked with the result (bytes transferred, errno/nil).
	// It runs on a ring worker goroutine and must not block.
	Done func(n int, err error)
}

// Done0 calls Done(0, err) when set (synchronous error paths).
func (op Op) Done0(err error) {
	if op.Done != nil {
		op.Done(0, err)
	}
}

// DoneGo calls Done(0, err) on a fresh goroutine, keeping Submit
// non-blocking on closed/empty paths.
func (op Op) DoneGo(err error) {
	if op.Done != nil {
		go op.Done(0, err)
	}
}

// Ring submits and completes operations.
//
// The Linux implementation uses io_uring (kernel >= 5.6,
// IORING_OP_READ/WRITE); elsewhere it falls back to a worker pool over
// ReadAt/WriteAt. Semantics are identical from the caller's perspective.
type Ring interface {
	// Submit queues ops for execution. Implementations may coalesce them
	// into a single kernel submission. It does not block beyond the
	// bounded queue and allocates nothing on the hot path.
	Submit(ops []Op) error
	// RegisterFile associates an open file with the ring so its fd can
	// be used in Op.Fd (fixed-file registration on Linux; keeping the
	// *os.File alive elsewhere).
	RegisterFile(fd int32, f *os.File)
	// Close stops the ring; all queued callbacks fire with ErrClosed.
	Close() error
}
