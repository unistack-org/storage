package nbd

import (
	"context"

	"github.com/unistack-org/storage/pkg/qos"
)

// qosBackend throttles an export's traffic through a QoS pair. It is
// the standalone-daemon form of per-volume limits: clustered exports
// get their limits inside the client (from the volume spec) and the OSD
// aggregate, while a plain unistord export (file/store backend) has no meta
// to read, so the daemon wraps the backend.
//
// The NBD Backend API is synchronous and context-free; waits use the
// background context, so a throttled guest request blocks its worker
// (bounded by MaxReadWorkers) until its tokens arrive — the intended
// behavior (the guest sees latency, never errors). Close on the
// underlying backend also closes the limiters, waking anything parked.
type qosBackend struct {
	Backend
	limits *qos.Pair
}

// WithQoS wraps b so reads pay limits.Read and writes/trim/zeroes/flush
// pay limits.Write. A nil pair returns b unchanged.
func WithQoS(b Backend, limits *qos.Pair) Backend {
	if !limits.Limited() {
		return b
	}
	return &qosBackend{Backend: b, limits: limits}
}

func (q *qosBackend) ReadAt(p []byte, off int64) (int, error) {
	if err := q.limits.Read.Wait(context.Background(), uint64(len(p))); err != nil {
		return 0, err
	}
	return q.Backend.ReadAt(p, off)
}

func (q *qosBackend) WriteAt(p []byte, off int64) (int, error) {
	if err := q.limits.Write.Wait(context.Background(), uint64(len(p))); err != nil {
		return 0, err
	}
	return q.Backend.WriteAt(p, off)
}

func (q *qosBackend) Trim(off int64, length uint32) error {
	if err := q.limits.Write.Wait(context.Background(), uint64(length)); err != nil {
		return err
	}
	return q.Backend.Trim(off, length)
}

func (q *qosBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	if err := q.limits.Write.Wait(context.Background(), uint64(length)); err != nil {
		return err
	}
	return q.Backend.WriteZeroes(off, length, punchHole)
}

func (q *qosBackend) Flush() error {
	if err := q.limits.Write.Wait(context.Background(), 0); err != nil {
		return err
	}
	return q.Backend.Flush()
}

func (q *qosBackend) Close() error {
	err := q.Backend.Close()
	q.limits.Close()
	return err
}
