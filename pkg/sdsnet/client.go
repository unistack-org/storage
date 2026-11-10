package sdsnet

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
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
	// zcState caches the MSG_ZEROCOPY support probe (zerocopy_linux.go).
	zcState atomic.Int32
	// batchOK is set from the handshake reply's Value field: a
	// batch-capable server answers the handshake with Value=1. Old
	// servers (Value=0) must never receive a batch frame — a pre-batch
	// server would answer the outer header with StatusNotSupp and then
	// misread the sub-op headers in the payload as top-level requests,
	// desyncing the stream. That is why batch support is negotiated at
	// handshake, not discovered from a StatusNotSupp reply.
	batchOK bool
	// Per-connection wire scratch: a stack array handed to io.ReadFull
	// (io.Reader interface) escapes to the heap, so reusing these keeps
	// the request loop allocation-free.
	sreq  [ReqHeaderSize]byte
	sresp [RespHeaderSize]byte
	sfs   [24]byte // OpStatFS payload (usage, free, total)
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
	return DialTLS(addr, export, timeout, nil)
}

// DialTLS is Dial over TLS when tlsCfg is set (mutual TLS: use
// tlsconf.Config.ClientConfig; the ServerName is taken from addr). A nil
// tlsCfg behaves exactly like Dial.
func DialTLS(addr, export string, timeout time.Duration, tlsCfg *tls.Config) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	setNoDelay(c)
	if tlsCfg != nil {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		cfg := tlsCfg.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = host
		}
		tc := tls.Client(c, cfg)
		if timeout > 0 {
			_ = tc.SetDeadline(time.Now().Add(timeout))
		}
		if err := tc.Handshake(); err != nil {
			tc.Close()
			return nil, err
		}
		_ = tc.SetDeadline(time.Time{})
		c = tc
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

// NewConn builds a protocol connection over an already-established
// transport (the in-process socketpair path; see Socketpair). The
// handshake runs exactly as over TCP, so the server side is unchanged.
func NewConn(c net.Conn, export string, timeout time.Duration) (*Conn, error) {
	setNoDelay(c)
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
	cc.batchOK = h.Value != 0
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

	// Every op is bounded: the caller's deadline if it has one, else a
	// generous default so a detached straggler (context.WithoutCancel)
	// can never hang on a wedged peer forever.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(opTimeout)
	}
	cc.c.SetDeadline(deadline)
	defer cc.c.SetDeadline(time.Time{})

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
	if h.Op == OpStatFS {
		// Fixed 24-byte payload (usage, free, total). Callers pass a
		// 24-byte p; a short buffer is a programming error, not a
		// transport one, so it must not mark the Conn dead.
		if len(p) < 24 {
			return 0, ErrProtocol
		}
		if _, err := io.ReadFull(cc.rd, p[:24]); err != nil {
			return fail(err)
		}
		return 24, nil
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

// ErrBatchNotSupported reports that the server answered a batch request
// with StatusNotSupp (old server). No sub-op ran: callers should fall
// back to single ops. The connection is intact.
var ErrBatchNotSupported = errors.New("sdsnet: batch not supported")

// BatchReq describes one sub-op of a Batch call. For reads, Len/P
// receive the data; for writes, Payload carries the data; the remaining
// ops take neither. Op must be one of Read, Write, WriteZeroes, Trim,
// Sync, Remove, Stat, StatInfo (no nested batches).
type BatchReq struct {
	Op      Op
	OID     uint64
	Offset  uint64
	Flags   uint16
	Len     uint32 // range length for WriteZeroes/Trim
	Payload []byte // write payload (ignored for non-writes)
	P       []byte // read destination (ignored for non-reads)
}

// subLength returns the wire Length for the sub-op.
func (r *BatchReq) subLength() uint32 {
	switch r.Op {
	case OpRead:
		return uint32(len(r.P))
	case OpWrite:
		return uint32(len(r.Payload))
	case OpWriteZeroes, OpTrim:
		return r.Len
	}
	return 0
}

// BatchResult is the reply for one sub-op, positional.
type BatchResult struct {
	Status Status
	Value  uint64 // read: byte count; stat: size/alloc; write: length
	Err    error  // *Error for a failed sub-op (nil when Status == StatusOK)
}

// allowedBatchOp reports whether op may appear inside a batch.
func allowedBatchOp(op Op) bool {
	switch op {
	case OpRead, OpWrite, OpWriteZeroes, OpTrim, OpSync, OpRemove, OpStat, OpStatInfo:
		return true
	}
	return false
}

// Batch sends len(reqs) ops in one request frame and fills results (same
// length) positionally. It returns a transport/protocol error (the Conn
// is then dead and must be discarded), ErrBatchNotSupported (server
// lacks batch support, no sub-op ran), or nil — sub-op failures are
// carried per-slot in results[i].Err, never collapsed into the return.
//
// Like do(), the whole exchange runs under one deadline. The reply
// stream is consumed strictly positionally, so the connection stays
// consistent even when individual sub-ops fail.
func (cc *Conn) Batch(ctx context.Context, reqs []BatchReq, results []BatchResult) error {
	if cc.dead {
		return ErrClosed
	}
	if len(reqs) == 0 || len(reqs) > MaxBatchOps || len(results) != len(reqs) {
		return ErrProtocol
	}
	for i := range reqs {
		if !allowedBatchOp(reqs[i].Op) {
			return ErrProtocol
		}
	}
	if !cc.batchOK {
		// Server predates batches: sending one would desync the stream
		// (it would read the sub-op headers as top-level requests). The
		// caller falls back to single ops.
		return ErrBatchNotSupported
	}
	if cc.seq == 0 {
		cc.seq = 1
	}
	ep := cc.ep.Load()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(opTimeout)
	}
	cc.c.SetDeadline(deadline)
	defer cc.c.SetDeadline(time.Time{})

	// Encode: one outer header (Length = sub-op count), then each
	// sub-op's header and payload. Everything goes through the bufio
	// writer, so the flush below is a single write syscall for a batch
	// that fits the 128 KiB buffer — the whole point of batching.
	var sh [BatchReqHeaderSize]byte
	outer := &cc.h
	outer.Op, outer.Flags, outer.Seq = OpBatch, 0, cc.seq
	outer.OID, outer.Offset, outer.Length, outer.Epoch = 0, 0, uint32(len(reqs)), ep
	cc.seq += uint32(len(reqs))
	fail := func(err error) error {
		if _, ok := err.(*Error); !ok {
			cc.dead = true
		}
		return err
	}
	outer.Encode(cc.sreq[:])
	if _, err := cc.wr.Write(cc.sreq[:]); err != nil {
		return fail(err)
	}
	for i := range reqs {
		r := &reqs[i]
		sub := ReqHeader{
			Op:     r.Op,
			Flags:  r.Flags,
			Seq:    uint32(i),
			OID:    r.OID,
			Offset: r.Offset,
			Length: r.subLength(),
			Epoch:  ep,
		}
		if sub.Length > MaxPayload {
			return ErrTooBig
		}
		sub.Encode(sh[:])
		if _, err := cc.wr.Write(sh[:]); err != nil {
			return fail(err)
		}
		if r.Op == OpWrite && len(r.Payload) > 0 {
			if _, err := cc.wr.Write(r.Payload); err != nil {
				return fail(err)
			}
		}
	}
	if err := cc.wr.Flush(); err != nil {
		return fail(err)
	}

	// Decode: len(reqs) positional replies.
	var sb [BatchRespHeaderSize]byte
	for i := range results {
		if _, err := io.ReadFull(cc.rd, sb[:]); err != nil {
			return fail(err)
		}
		rh, err := DecodeRespHeader(sb[:])
		if err != nil {
			return fail(err)
		}
		if rh.Seq != uint32(i) {
			return fail(ErrProtocol) // stream desync
		}
		results[i].Status = rh.Status
		results[i].Value = rh.Value
		if rh.Status != StatusOK {
			results[i].Err = &Error{Seq: uint32(i), Op: reqs[i].Op, Status: rh.Status, Value: rh.Value}
			continue
		}
		results[i].Err = nil
		if reqs[i].Op == OpRead && rh.Value > 0 {
			n := int(rh.Value)
			if n > len(reqs[i].P) {
				n = len(reqs[i].P)
			}
			if _, err := io.ReadFull(cc.rd, reqs[i].P[:n]); err != nil {
				return fail(err)
			}
		}
	}
	return nil
}

// Ping checks the connection round-trip.
func (cc *Conn) Ping(ctx context.Context) error {
	_, err := cc.op(ctx, OpPing, 0, 0, 0, 0, nil, nil)
	return err
}

// StatDisk reports the filesystem behind the server's store: bytes used
// by the store's data, and the filesystem's free/total bytes. A server
// whose backend has no filesystem returns StatusNotSupp (an *Error).
func (cc *Conn) StatDisk(ctx context.Context) (usage, free, total uint64, err error) {
	if _, err = cc.op(ctx, OpStatFS, 0, 0, 0, 0, nil, cc.sfs[:]); err != nil {
		return 0, 0, 0, err
	}
	return binary.BigEndian.Uint64(cc.sfs[0:8]), binary.BigEndian.Uint64(cc.sfs[8:16]), binary.BigEndian.Uint64(cc.sfs[16:24]), nil
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

// Remove deletes object oid; a missing object is reported as success
// (the server-side remove is idempotent, so vacuums can be replayed).
func (cc *Conn) Remove(ctx context.Context, oid uint64) error {
	_, err := cc.op(ctx, OpRemove, oid, 0, 0, 0, nil, nil)
	return err
}

// Stat reports the size of object oid. It returns (0, false, nil) when
// the object does not exist and (0, false, err) on a real failure, so
// recovery can tell a thin object from an unreachable OSD.
func (cc *Conn) Stat(ctx context.Context, oid uint64) (size uint64, exists bool, err error) {
	n, err := cc.op(ctx, OpStat, oid, 0, 0, 0, nil, nil)
	switch {
	case err == nil:
		return uint64(n), true, nil
	case IsObjectMissing(err):
		return 0, false, nil
	default:
		return 0, false, err
	}
}

// StatInfo is Stat plus physical accounting: it reports the bytes
// actually allocated on disk for object oid (st_blocks*512), so a
// punched/fully-sparse object answers alloc==0. Absent objects map to
// (0, false, nil) as in Stat.
func (cc *Conn) StatInfo(ctx context.Context, oid uint64) (alloc uint64, exists bool, err error) {
	n, err := cc.op(ctx, OpStatInfo, oid, 0, 0, 0, nil, nil)
	switch {
	case err == nil:
		return uint64(n), true, nil
	case IsObjectMissing(err):
		return 0, false, nil
	default:
		return 0, false, err
	}
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
	// TLS, when set, dials every connection over TLS (mutual; see
	// DialTLS).
	TLS *tls.Config
	// Local, when set, replaces DialTLS for establishing connections:
	// it returns an already-connected transport (the in-process
	// socketpair of a Server in this process). The handshake, headers
	// and framing are unchanged, so server-side QoS, epoch fencing and
	// metrics all apply exactly as over TCP.
	Local func() (net.Conn, error)

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

		cc, err := pp.dial()
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

// dial establishes one connection: the Local transport when set
// (in-process), else TCP/TLS to Addr.
func (pp *Pool) dial() (*Conn, error) {
	if pp.Local != nil {
		c, err := pp.Local()
		if err != nil {
			return nil, err
		}
		return NewConn(c, pp.Export, dialTimeout)
	}
	return DialTLS(pp.Addr, pp.Export, dialTimeout, pp.TLS)
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
// Closed reports whether the pool has been closed (dropPool/Cluster
// close). A closed pool never dials again; callers caching a pool must
// re-resolve it when this returns true.
func (pp *Pool) Closed() bool {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	return pp.closed
}

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
//
// A transport-level failure on an idle connection (the peer restarted
// while the socket sat in the pool) is retried once over a fresh
// connection: every op in the protocol is idempotent (writes are
// file+offset, removes/syncs/stats are repeatable), so a retry cannot
// double-apply. The second attempt's error is returned as-is.
func (pp *Pool) Do(ctx context.Context, fn func(*Conn) error) error {
	cc, err := pp.Get(ctx)
	if err != nil {
		return err
	}
	err = fn(cc)
	if err == nil {
		pp.Put(cc)
		return nil
	}
	if IsProtocolError(err) {
		pp.Discard(cc)
		return err
	}
	if !cc.Dead() {
		// Remote status error (*Error) or a ctx failure: the stream is
		// intact, keep the connection.
		pp.Put(cc)
		return err
	}
	pp.Discard(cc)
	cc, err2 := pp.Get(ctx)
	if err2 != nil {
		return err // report the original transport failure
	}
	err2 = fn(cc)
	if err2 != nil && (IsProtocolError(err2) || cc.Dead()) {
		pp.Discard(cc)
		return err2
	}
	pp.Put(cc)
	return err2
}

// do runs fn with a pooled connection, discarding it on protocol errors.
func (pp *Pool) do(ctx context.Context, fn func(*Conn) error) error {
	return pp.Do(ctx, fn)
}

// Ping runs a ping over a pooled connection.
func (pp *Pool) Ping(ctx context.Context) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.Ping(ctx) })
}

// StatDisk runs a pooled filesystem stat (see Conn.StatDisk).
func (pp *Pool) StatDisk(ctx context.Context) (usage, free, total uint64, err error) {
	err = pp.Do(ctx, func(cc *Conn) error {
		var e error
		usage, free, total, e = cc.StatDisk(ctx)
		return e
	})
	if err != nil {
		return 0, 0, 0, err
	}
	return usage, free, total, nil
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

// Remove runs a pooled remove.
func (pp *Pool) Remove(ctx context.Context, oid uint64) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.Remove(ctx, oid) })
}

// Stat runs a pooled stat (see Conn.Stat).
func (pp *Pool) Stat(ctx context.Context, oid uint64) (size uint64, exists bool, err error) {
	err = pp.Do(ctx, func(cc *Conn) error {
		var e error
		size, exists, e = cc.Stat(ctx, oid)
		return e
	})
	if err != nil {
		return 0, false, err
	}
	return size, exists, nil
}

// StatInfo runs a pooled allocated-bytes stat (see Conn.StatInfo).
func (pp *Pool) StatInfo(ctx context.Context, oid uint64) (alloc uint64, exists bool, err error) {
	err = pp.Do(ctx, func(cc *Conn) error {
		var e error
		alloc, exists, e = cc.StatInfo(ctx, oid)
		return e
	})
	if err != nil {
		return 0, false, err
	}
	return alloc, exists, nil
}

// Batch runs a batched exchange over a pooled connection (see
// Conn.Batch). Like Do, it retries once over a fresh connection when an
// idle conn turned out dead. A StatusNotSupp reply is surfaced as
// ErrBatchNotSupported so callers can fall back to single ops; per-op
// results are positional in results.
func (pp *Pool) Batch(ctx context.Context, reqs []BatchReq, results []BatchResult) error {
	return pp.Do(ctx, func(cc *Conn) error { return cc.Batch(ctx, reqs, results) })
}

const dialTimeout = 5 * time.Second

// opTimeout bounds an operation whose context carries no deadline
// (detached stragglers), so a wedged peer can never hang a Conn
// forever. Generous: a healthy OSD answers in milliseconds.
const opTimeout = 30 * time.Second
