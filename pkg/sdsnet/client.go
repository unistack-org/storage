package sdsnet

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn is one protocol connection bound to an export. A single
// request/response exchange is not interleaved with another on the same
// Conn: requests are serialised by the caller owning the Conn (the pool
// checks one Conn out to one user at a time), and replies match
// requests by seq (FIFO). The common patterns are "one Conn per worker"
// or a Pool shared by many goroutines.
type Conn struct {
	c    net.Conn
	rd   *bufio.Reader
	wr   *bufio.Writer
	seq  uint32
	dead bool // transport error: the stream state is unknown
	ep   atomic.Uint32
	// Per-connection wire scratch: a stack array handed to io.ReadFull
	// (io.Reader interface) escapes to the heap, so reusing these keeps
	// the request loop allocation-free.
	sreq  [ReqHeaderSize]byte
	sresp [RespHeaderSize]byte
	// h is a reusable request header: do() takes it by value semantics
	// (we stamp Seq/Epoch then encode immediately), so one scratch
	// header per Conn avoids the &ReqHeader{...} argument allocation.
	h ReqHeader
}

// SetEpoch stamps every subsequent request with the given placement
// epoch (0 disables fencing for this connection).
func (cc *Conn) SetEpoch(epoch uint32) { cc.ep.Store(epoch) }

// Epoch returns the epoch stamped on outgoing requests.
func (cc *Conn) Epoch() uint32 { return cc.ep.Load() }

// Dead reports whether the connection saw a transport error and must
// not be reused: the request/response stream may be desynchronised.
func (cc *Conn) Dead() bool { return cc.dead }

// Dial connects and performs the handshake for export.
func Dial(addr, export string, timeout time.Duration) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	cc := &Conn{
		c:  c,
		rd: bufio.NewReaderSize(c, 128<<10),
		wr: bufio.NewWriterSize(c, 128<<10),
	}
	if err := cc.handshake(export, timeout); err != nil {
		c.Close()
		return nil, err
	}
	return cc, nil
}

func (cc *Conn) handshake(export string, timeout time.Duration) error {
	if timeout > 0 {
		if err := cc.c.SetDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		defer cc.c.SetDeadline(time.Time{})
	}
	if _, err := cc.c.Write(EncodeHandshake(export)); err != nil {
		return err
	}
	var b [RespHeaderSize]byte
	if _, err := io.ReadFull(cc.c, b[:]); err != nil {
		return err
	}
	h, err := DecodeRespHeader(b[:])
	if err != nil {
		return err
	}
	if h.Status != StatusOK {
		return &Error{Op: 0, Status: h.Status}
	}
	return nil
}

// Close closes the connection.
func (cc *Conn) Close() error { return cc.c.Close() }

// do sends one request (with optional payload) and reads the response;
// for reads it fills p and returns the byte count. Any transport-level
// failure marks the Conn dead: bytes may be half-sent/half-read, so the
// stream can no longer be trusted.
func (cc *Conn) do(ctx context.Context, h *ReqHeader, payload, p []byte) (int, error) {
	if cc.dead {
		return 0, ErrClosed
	}
	if cc.seq == 0 {
		cc.seq = 1
	}
	h.Seq = cc.seq
	h.Epoch = cc.ep.Load()
	cc.seq++

	fail := func(err error) (int, error) {
		if _, ok := err.(*Error); !ok {
			cc.dead = true // protocol errors carry a valid frame; transport ones don't
		}
		return 0, err
	}
	h.Encode(cc.sreq[:])
	if _, err := cc.wr.Write(cc.sreq[:]); err != nil {
		return fail(err)
	}
	if len(payload) > 0 {
		if _, err := cc.wr.Write(payload); err != nil {
			return fail(err)
		}
	}
	if err := cc.wr.Flush(); err != nil {
		return fail(err)
	}
	if d, ok := ctx.Deadline(); ok {
		cc.c.SetDeadline(d)
		defer cc.c.SetDeadline(time.Time{})
	}

	if _, err := io.ReadFull(cc.rd, cc.sresp[:]); err != nil {
		return fail(err)
	}
	rh, err := DecodeRespHeader(cc.sresp[:])
	if err != nil {
		return fail(err)
	}
	if rh.Seq != h.Seq {
		return 0, ErrProtocol // stream desync: connection is unusable
	}
	if rh.Status != StatusOK {
		return 0, &Error{Seq: h.Seq, Op: h.Op, Status: rh.Status, Value: rh.Value}
	}
	if h.Op == OpRead && rh.Value > 0 {
		n := int(rh.Value)
		if n > len(p) {
			n = len(p)
		}
		if _, err := io.ReadFull(cc.rd, p[:n]); err != nil {
			return fail(err)
		}
		return n, nil
	}
	return int(rh.Value), nil
}

// op fills the connection's scratch request header and runs one exchange.
// Callers pass the caller-owned payload/p directly: no allocation per op.
func (cc *Conn) op(ctx context.Context, op Op, oid, off uint64, length uint32, flags uint16, payload, p []byte) (int, error) {
	h := &cc.h
	h.Op, h.Flags, h.OID, h.Offset, h.Length = op, flags, oid, off, length
	return cc.do(ctx, h, payload, p)
}

// Ping checks the connection round-trip.
func (cc *Conn) Ping(ctx context.Context) error {
	_, err := cc.op(ctx, OpPing, 0, 0, 0, 0, nil, nil)
	return err
}

// Read reads len(p) bytes of object oid at off.
func (cc *Conn) Read(ctx context.Context, oid, off uint64, p []byte) (int, error) {
	return cc.op(ctx, OpRead, oid, off, uint32(len(p)), 0, nil, p)
}

// Write writes p to object oid at off.
func (cc *Conn) Write(ctx context.Context, oid, off uint64, p []byte, fua bool) error {
	var flags uint16
	if fua {
		flags |= FlagsFUA
	}
	_, err := cc.op(ctx, OpWrite, oid, off, uint32(len(p)), flags, p, nil)
	return err
}

// WriteZeroes zeroes [off, off+n) on object oid.
func (cc *Conn) WriteZeroes(ctx context.Context, oid, off, n uint64, hole, fua bool) error {
	var flags uint16
	if hole {
		flags |= FlagsHole
	}
	if fua {
		flags |= FlagsFUA
	}
	_, err := cc.op(ctx, OpWriteZeroes, oid, off, uint32(n), flags, nil, nil)
	return err
}

// Trim punches a hole over [off, off+n) on object oid.
func (cc *Conn) Trim(ctx context.Context, oid, off, n uint64) error {
	_, err := cc.op(ctx, OpTrim, oid, off, uint32(n), 0, nil, nil)
	return err
}

// Sync persists all prior writes on this connection.
func (cc *Conn) Sync(ctx context.Context) error {
	_, err := cc.op(ctx, OpSync, 0, 0, 0, 0, nil, nil)
	return err
}

// IsProtocolError reports a framing/desync failure (connection must be
// discarded), as opposed to a per-request Status error.
func IsProtocolError(err error) bool { return err == ErrProtocol || err == ErrShortHeader }

// Pool is a set of reusable connections to one address. Get dials on
// demand (up to Limit, 0 = unlimited); Put returns healthy connections
// and drops broken ones. Get blocks while the pool is at Limit, so
// pool.Limit doubles as a per-target concurrency throttle.
type Pool struct {
	Addr   string
	Export string
	Limit  int // max connections (0 = unlimited)

	epoch atomic.Uint32 // stamped onto connections at Get

	mu     sync.Mutex
	idle   []*Conn
	opened int
	closed bool
	done   chan struct{} // closed by Close
	wake   chan struct{} // buffered(1): an idle conn became available
}

// NewPool returns a pool for addr/export.
func NewPool(addr, export string) *Pool {
	return &Pool{
		Addr:   addr,
		Export: export,
		done:   make(chan struct{}),
		wake:   make(chan struct{}, 1),
	}
}

// SetEpoch stamps every connection checked out of the pool with the
// given placement epoch (see Conn.SetEpoch).
func (pp *Pool) SetEpoch(epoch uint32) { pp.epoch.Store(epoch) }

// Epoch returns the pool's placement epoch.
func (pp *Pool) Epoch() uint32 { return pp.epoch.Load() }

// Get returns a connection, dialing a new one if none is idle. When the
// pool is at Limit it blocks until a connection is returned, ctx is
// cancelled, or the pool is closed.
func (pp *Pool) Get(ctx context.Context) (*Conn, error) {
	for {
		pp.mu.Lock()
		if pp.closed {
			pp.mu.Unlock()
			return nil, ErrClosed
		}
		if n := len(pp.idle); n > 0 {
			cc := pp.idle[n-1]
			pp.idle = pp.idle[:n-1]
			ep := pp.epoch.Load()
			pp.mu.Unlock()
			if cc.Dead() {
				cc.Close()
				pp.mu.Lock()
				pp.opened--
				pp.mu.Unlock()
				continue // try the next idle conn
			}
			cc.SetEpoch(ep)
			return cc, nil
		}
		if pp.Limit > 0 && pp.opened >= pp.Limit {
			pp.mu.Unlock()
			select {
			case <-pp.wake:
			case <-pp.done:
				return nil, ErrClosed
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		pp.opened++
		pp.mu.Unlock()

		cc, err := Dial(pp.Addr, pp.Export, dialTimeout)
		if err != nil {
			pp.mu.Lock()
			pp.opened--
			pp.mu.Unlock()
			return nil, err
		}
		cc.SetEpoch(pp.epoch.Load())
		pp.mu.Lock()
		if pp.closed {
			pp.opened--
			pp.mu.Unlock()
			cc.Close()
			return nil, ErrClosed
		}
		pp.mu.Unlock()
		return cc, nil
	}
}

// Put returns cc to the pool (cc must not be in use). If the pool was
// closed in the meantime, cc is closed instead of being kept.
func (pp *Pool) Put(cc *Conn) {
	pp.mu.Lock()
	if pp.closed {
		pp.mu.Unlock()
		cc.Close()
		return
	}
	pp.idle = append(pp.idle, cc)
	pp.mu.Unlock()
	pp.signal()
}

// Discard closes cc and frees its pool slot.
func (pp *Pool) Discard(cc *Conn) {
	cc.Close()
	pp.mu.Lock()
	pp.opened--
	pp.mu.Unlock()
	pp.signal()
}

// signal nudges one blocked Get (buffered, coalescing).
func (pp *Pool) signal() {
	select {
	case pp.wake <- struct{}{}:
	default:
	}
}

// Close closes every idle connection and marks the pool closed: Get
// fails with ErrClosed afterwards and Put/Discard close returned
// connections. In-use connections close when they come back.
func (pp *Pool) Close() {
	pp.mu.Lock()
	idle := pp.idle
	pp.idle = nil
	if !pp.closed {
		pp.closed = true
		close(pp.done)
	}
	pp.mu.Unlock()
	for _, cc := range idle {
		cc.Close()
	}
}

// Stats reports idle/in-use counts (tests/telemetry).
func (pp *Pool) Stats() (idle, inUse int) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return len(pp.idle), pp.opened - len(pp.idle)
}

// Do runs fn with a pooled connection, discarding it on protocol errors.
func (pp *Pool) Do(ctx context.Context, fn func(*Conn) error) error {
	cc, err := pp.Get(ctx)
	if err != nil {
		return err
	}
	err = fn(cc)
	if err != nil && (IsProtocolError(err) || cc.Dead()) {
		pp.Discard(cc)
		return err
	}
	pp.Put(cc)
	return err
}

// do runs fn with a pooled connection, discarding it on protocol errors.
func (pp *Pool) do(ctx context.Context, fn func(*Conn) error) error {
	return pp.Do(ctx, fn)
}

// Ping runs a ping over a pooled connection.
func (pp *Pool) Ping(ctx context.Context) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.Ping(ctx) })
}

// Write runs a pooled write.
func (pp *Pool) Write(ctx context.Context, oid, off uint64, p []byte, fua bool) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.Write(ctx, oid, off, p, fua) })
}

// Read runs a pooled read.
func (pp *Pool) Read(ctx context.Context, oid, off uint64, p []byte) (int, error) {
	var n int
	err := pp.Do(ctx, func(cc *Conn) error {
		var err error
		n, err = cc.Read(ctx, oid, off, p)
		return err
	})
	return n, err
}

// WriteZeroes runs a pooled write-zeroes.
func (pp *Pool) WriteZeroes(ctx context.Context, oid, off, n uint64, hole, fua bool) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.WriteZeroes(ctx, oid, off, n, hole, fua) })
}

// Sync runs a pooled sync.
func (pp *Pool) Sync(ctx context.Context) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.Sync(ctx) })
}

const dialTimeout = 5 * time.Second
