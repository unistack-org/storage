// Package iouring is the async I/O ring for the data path.
//
// On Linux it wraps io_uring (plain submit-on-enter, no SQPOLL, so the
// producer may reset an unconsumed SQ tail). On other platforms it
// degrades to a worker pool executing
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
	// BufIndex selects a buffer registered with Ring.RegisterBuffer
	// (> 0): the op is then submitted as READ_FIXED/WRITE_FIXED over
	// the registered buffer, so the kernel skips per-I/O page pinning
	// (Buf must alias the registered bytes). 0 = plain buffer (the
	// zero value keeps plain ops correct).
	BufIndex int
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
	// SubmitSingle submits one op. Semantics are exactly
	// Submit([]Op{op}) — the same empty-buffer completion, the same
	// closed-ring handling, and the same outstanding accounting. It
	// exists so a caller that already has one op in hand does not need
	// to wrap it in a slice (avoiding that per-I/O allocation); it is
	// not required to be faster than Submit.
	SubmitSingle(op Op) error
	// RegisterFile associates an open file with the ring so its fd can
	// be used in Op.Fd (fixed-file registration on Linux; keeping the
	// *os.File alive elsewhere). On Linux the fd is added to the
	// ring's fixed-file table, so later ops on it use fixed-file I/O.
	RegisterFile(fd int32, f *os.File)
	// UnregisterFile drops fd from the fixed-file table. Call it before
	// closing the fd so a slot is never reused under an in-flight op;
	// ops already in flight complete against the old table. No-op on
	// the portable implementation.
	UnregisterFile(fd int32)
	// RegisterBuffer registers data as a fixed buffer and returns its
	// index (>= 1) for Op.BufIndex. On Linux the pages are pinned once at
	// registration; the caller must keep data alive and stable in
	// placement until UnregisterBuffer. The registered length is
	// padded to a page boundary, keeping indexes page-aligned. The
	// portable implementation only assigns an index.
	RegisterBuffer(data []byte) (int, error)
	// UnregisterBuffer drops a registered buffer. No-op on the portable
	// implementation.
	UnregisterBuffer(index int)
	// Close stops the ring; all queued callbacks fire with ErrClosed.
	Close() error
}
