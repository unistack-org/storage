package sdsnet

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/unistack-org/storage/pkg/qos"
)

// handshakeTLS bounds the TLS handshake on an accepted connection so a
// stuck peer cannot hold the accept goroutine's resources open.
const handshakeTLS = 10 * time.Second

// Backend is the storage a Server exposes over the wire. store.Backend
// satisfies it; tests use an in-memory implementation.
type Backend interface {
	// Size returns the volume size in bytes.
	Size() uint64
	// ReadAt reads len(p) bytes at off; io.EOF at/after end maps to
	// StatusEOF.
	ReadAt(ctx context.Context, oid uint64, off uint64, p []byte) (int, error)
	// WriteAt writes p at off.
	WriteAt(ctx context.Context, oid uint64, off uint64, p []byte) error
	// WriteZeroes zeroes [off, off+n); sparse requests a punched hole.
	WriteZeroes(ctx context.Context, oid uint64, off uint64, n uint64, sparse bool) error
	// Sync persists prior writes.
	Sync(ctx context.Context) error
	// Remove deletes the object; a missing object is not an error
	// (vacuum vacuums idempotently).
	Remove(ctx context.Context, oid uint64) error
	// Stat returns the object's size, or os.ErrNotExist when the
	// object is absent (recovery uses it to find thin/missing
	// replicas; unlike store.Lookup it never creates).
	Stat(ctx context.Context, oid uint64) (uint64, error)
}

// statInfoer is the optional Backend extension for allocated-byte
// accounting (OpStatInfo, live vacuum). It returns the apparent size
// and the bytes actually allocated on disk (st_blocks*512): a
// punched/fully-sparse object has size>0 and alloc==0. Backends that
// omit it get StatusNotSupp on OpStatInfo (per-op mixed-version
// degradation), which is exactly how an old server behaves.
type statInfoer interface {
	StatInfo(ctx context.Context, oid uint64) (uint64, uint64, error)
}

// statFSer is the optional Backend extension that exposes the filesystem
// behind the store (OpStatFS, cluster capacity metrics): bytes used by
// the store's data, plus the filesystem's free and total bytes. Backends
// without a filesystem answer StatusNotSupp (per-op mixed-version
// degradation, exactly like an old server).
type statFSer interface {
	StatFS(ctx context.Context) (usage, free, total uint64, err error)
}

// Server serves Backends over accepted connections. Every connection is
// bound to a single export name by the handshake. Exactly one of
// Backend (single anonymous export: any requested name maps to it) or
// Exports (name-routed) should be set.
type Server struct {
	// Backend serves every export name when Exports is nil.
	Backend Backend
	// Exports routes handshake names to backends; an unknown name is
	// rejected with StatusUnknown.
	Exports map[string]Backend
	// ReadOnly rejects all mutating operations (status read-only).
	ReadOnly bool
	// Limits, when set, is consulted per handshake: it returns the QoS
	// limiter pair for an export name (nil = unlimited). The server
	// throttles every op of the connection through it. An OSD uses it
	// to enforce per-volume classes (per-export = per-volume in the
	// cluster data path, where exports are named by volume).
	Limits func(export string) *qos.Pair
	// Aggregate, when set, throttles the whole disk: every op of every
	// connection passes it before the per-export pair. This is the
	// "limit iops on the physical device" knob — the per-OSD ceiling
	// that keeps one noisy volume from starving its neighbours.
	Aggregate *qos.Pair
	// TLS, when set, wraps every accepted connection in TLS (mutual:
	// use tlsconf.Config.ServerConfig, which requires and verifies a
	// client certificate). The handshake must complete within
	// handshakeTLS; plain clients are then rejected, TLS clients are
	// rejected by plain servers — a mixed fleet must migrate at once.
	TLS *tls.Config
	// Metrics, when set, receives one sample per served request and
	// per connection close (see ServerMetrics). Called on the request's
	// serving goroutine right before the reply is flushed: keep the
	// callbacks cheap (counter/histogram updates) and never blocking.
	// It is the exporter seam: pkg/sdsnet stays dependency-free, and
	// the binary maps the events onto its metrics library.
	Metrics *ServerMetrics
	// Logf receives connection lifecycle and request errors (may be nil).
	Logf func(format string, args ...any)

	minEpoch atomic.Uint32

	mu       sync.Mutex
	listener net.Listener
	closed   chan struct{}
	closedF  bool
	conns    map[net.Conn]struct{}
	wg       sync.WaitGroup
}

// ServerMetrics is the optional observation seam of a Server: one
// callback per served request (after the reply status is known, before
// the payload/reply is flushed) and one per connection close. The
// server itself keeps no counters — a binary that exposes Prometheus/
// VictoriaMetrics metrics plugs in here without pkg/sdsnet gaining a
// dependency. All callbacks run on the serving goroutines; a nil
// callback is skipped.
type ServerMetrics struct {
	// Request records one completed request: the op, its status, the
	// payload bytes moved in the request direction (write payload) and
	// out of the server (read reply), and the time from header decode
	// to dispatch completion (throttle/queue time included).
	Request func(op Op, status Status, bytesIn, bytesOut uint64, dur time.Duration)
	// ConnAccepted reports a connection that completed the handshake
	// (paired with ConnClosed when the connection ends).
	ConnAccepted func()
	// ConnClosed reports a connection that ended, with the number of
	// requests it served.
	ConnClosed func(ops uint64)
}

// SetMinEpoch enables placement-epoch fencing: requests carrying an
// epoch below min are rejected with StatusStale (response value = min),
// so clients learn to refresh their cluster map. min == 0 disables
// fencing (requests are accepted at any epoch).
func (s *Server) SetMinEpoch(min uint32) { s.minEpoch.Store(min) }

// MinEpoch returns the current fencing floor.
func (s *Server) MinEpoch() uint32 { return s.minEpoch.Load() }

// lookup resolves an export name to its backend.
func (s *Server) lookup(name string) (Backend, bool) {
	if s.Exports != nil {
		b, ok := s.Exports[name]
		return b, ok
	}
	return s.Backend, s.Backend != nil
}

// Serve listens on addr ("host:port") and serves until Close. It returns
// nil after Close stops the accept loop.
func (s *Server) Serve(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.ServeListener(ln)
}

// ServeListener serves on an already-bound listener. Callers that must
// know the resolved address (port 0) before serving — to advertise it in
// cluster metadata, say — bind it themselves and use this, so the
// advertised address is the one actually served.
func (s *Server) ServeListener(ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	if s.closed == nil {
		s.closed = make(chan struct{})
	}
	closed := s.closed
	s.mu.Unlock()
	var wg sync.WaitGroup
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-closed:
				wg.Wait()
				s.wg.Wait()
				return nil
			default:
			}
			return err
		}
		if s.TLS != nil {
			tc := tls.Server(c, s.TLS)
			_ = tc.SetDeadline(time.Now().Add(handshakeTLS))
			if err := tc.Handshake(); err != nil {
				_ = c.Close()
				if s.Logf != nil {
					s.Logf("sdsnet: tls handshake: %v", err)
				}
				continue
			}
			_ = tc.SetDeadline(time.Time{})
			c = tc
		}
		wg.Add(1)
		s.wg.Add(1)
		s.mu.Lock()
		if s.conns == nil {
			s.conns = make(map[net.Conn]struct{})
		}
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		go func() {
			defer wg.Done()
			defer s.wg.Done()
			s.handle(c)
			s.mu.Lock()
			delete(s.conns, c)
			s.mu.Unlock()
		}()
	}
}

// ServeConn serves one already-established connection: the same
// handshake + request loop a served socket gets. It is the in-process
// entry point — pair it with Socketpair so a client and this server
// live in one process (see Pool.Local). The connection is tracked like
// a listener-accepted one: Close closes it.
func (s *Server) ServeConn(c net.Conn) {
	s.mu.Lock()
	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	s.wg.Add(1)
	defer s.wg.Done()
	s.handle(c)
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// Addr returns the listen address (valid once Serve has started).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Close stops accepting and waits for per-connection goroutines. Call it
// concurrently with Serve.
func (s *Server) Close() error {
	s.mu.Lock()
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	if s.closed == nil {
		s.closed = make(chan struct{})
	}
	if !s.closedF {
		s.closedF = true
		close(s.closed)
	}
	// Close live connections: handlers blocked in Read must unblock,
	// otherwise Close waits forever on idle/pooled clients.
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

// handle runs one connection: handshake, then the request loop.
func (s *Server) handle(c net.Conn) {
	defer c.Close()
	setNoDelay(c)

	// Handshake: 16-byte fixed part, then the export name.
	var fixed [16]byte
	if _, err := io.ReadFull(c, fixed[:]); err != nil {
		s.logf("sdsnet: handshake short read: %v", err)
		return
	}
	f, err := DecodeHandshakeFixed(fixed[:])
	if err != nil || !f.HasMagic {
		s.logf("sdsnet: handshake bad magic")
		s.reply(c, &RespHeader{Status: StatusInvalid})
		return
	}
	if f.Version != Version {
		s.reply(c, &RespHeader{Status: StatusNotSupp})
		return
	}
	n := int(f.NameLen)
	name := make([]byte, n)
	if n > 0 {
		if _, err := io.ReadFull(c, name); err != nil {
			s.logf("sdsnet: handshake name read: %v", err)
			return
		}
	}
	s.logf("sdsnet: %s connected export=%q", c.RemoteAddr(), string(name))
	backend, ok := s.lookup(string(name))
	if !ok {
		s.logf("sdsnet: %s unknown export %q", c.RemoteAddr(), string(name))
		s.reply(c, &RespHeader{Status: StatusUnknown})
		return
	}
	if err := s.reply(c, &RespHeader{Status: StatusOK}); err != nil {
		return
	}
	// Per-connection request count for the ConnClosed metric hook.
	// Accepted/Closed fire as a pair (only for connections that got a
	// StatusOK handshake reply), so a gauge of accepted-minus-closed
	// tracks live connections.
	var connOps uint64
	if s.Metrics != nil {
		if s.Metrics.ConnAccepted != nil {
			s.Metrics.ConnAccepted()
		}
		if s.Metrics.ConnClosed != nil {
			defer func() { s.Metrics.ConnClosed(connOps) }()
		}
	}

	rd := bufio.NewReaderSize(c, 128<<10)
	wr := bufio.NewWriterSize(c, 128<<10)
	ctx := context.Background()
	// QoS pair for this export (nil = unlimited). Resolved once per
	// connection: the handshake binds it, matching the export lookup.
	var limits *qos.Pair
	if s.Limits != nil {
		limits = s.Limits(string(name))
	}
	// Per-connection reusable buffers: stack arrays passed to
	// io.ReadFull/bufio.Writer escape (bufio.Writer.Write forwards to an
	// io.Writer interface), costing one allocation per request. Reusing
	// these makes the warm request loop allocation-free.
	rw := &connRW{rh: make([]byte, ReqHeaderSize), hdr: make([]byte, RespHeaderSize)}
	for {
		if _, err := io.ReadFull(rd, rw.rh); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				s.logf("sdsnet: read request header: %v", err)
			}
			return
		}
		h, err := DecodeReqHeader(rw.rh)
		if err != nil {
			// Echo the raw seq (decoded headers zero it on error) so
			// the pipelined client stays in sync with our replies.
			rawSeq := binary.BigEndian.Uint32(rw.rh[4:8])
			s.replyRW(wr, rw, &RespHeader{Seq: rawSeq, Status: statusFromErr(err)}, nil)
			if wr.Flush() != nil {
				return
			}
			continue
		}
		// QoS: charge the disk aggregate, then this export's pair, for
		// the request's payload. Throttling happens before dispatch, so
		// the limiter paces the op the guest actually issued (reads paced
		// on the read axis, writes/zeroes/trim on the write axis).
		if s.Aggregate != nil || limits != nil {
			status, proceed, alive := s.throttle(ctx, limits, &h)
			if !alive {
				return // client went away mid-wait
			}
			if !proceed {
				// A rejected write must still consume its payload from
				// the socket, or the request stream desyncs.
				if h.Op == OpWrite && h.Length > 0 {
					if _, err := io.CopyN(io.Discard, rd, int64(h.Length)); err != nil {
						return
					}
				}
				s.replyRW(wr, rw, &RespHeader{Seq: h.Seq, Status: status}, nil)
				if wr.Flush() != nil {
					return
				}
				continue
			}
		}
		var payload []byte
		if h.Op == OpWrite && h.Length > 0 {
			payload = rw.getBuf(int(h.Length))
			if _, err := io.ReadFull(rd, payload); err != nil {
				s.logf("sdsnet: read request payload: %v", err)
				return
			}
		}
		start := time.Now()
		resp, out := s.dispatch(ctx, backend, &h, payload, rw)
		if m := s.Metrics; m != nil && m.Request != nil {
			connOps++
			var bin, bout uint64
			if h.Op == OpWrite {
				bin = uint64(len(payload))
			}
			if resp.Status == StatusOK {
				bout = uint64(len(out))
			}
			m.Request(h.Op, resp.Status, bin, bout, time.Since(start))
		}
		if err := s.replyRW(wr, rw, &resp, out); err != nil {
			return
		}
		if err := wr.Flush(); err != nil {
			return
		}
	}
}

// connRW holds per-connection reusable wire buffers (see handle).
type connRW struct {
	rh  []byte // request header scratch
	hdr []byte // response header scratch
	b   []byte // payload scratch (write payload / read reply), grown on demand
}

func (rw *connRW) getBuf(n int) []byte {
	if cap(rw.b) >= n {
		return rw.b[:n]
	}
	rw.b = make([]byte, n)
	return rw.b
}

// throttle charges the QoS limiters for one request: the disk
// aggregate first (the device ceiling), then the export's pair.
// proceed=false means reply with status (the export's limiter is
// closed — the volume is going away); alive=false means the client
// context was cancelled while waiting and the connection should drop.
func (s *Server) throttle(ctx context.Context, limits *qos.Pair, h *ReqHeader) (status Status, proceed, alive bool) {
	read := h.Op == OpRead || h.Op == OpStat || h.Op == OpStatInfo
	n := uint64(h.Length)
	if h.Op == OpSync || h.Op == OpRemove {
		n = 0 // a flush or an unlink costs an op, not bytes
	}
	wait := func(lim *qos.Limiter) (ok, alive bool) {
		if lim == nil {
			return true, true
		}
		if err := lim.Wait(ctx, n); err != nil {
			if errors.Is(err, qos.ErrClosed) {
				return false, true
			}
			return false, false // ctx done: the client is gone
		}
		return true, true
	}
	if s.Aggregate != nil {
		lim := s.Aggregate.Write
		if read {
			lim = s.Aggregate.Read
		}
		if ok, al := wait(lim); !al {
			return 0, false, false
		} else if !ok {
			return StatusIO, false, true
		}
	}
	if limits != nil {
		lim := limits.Write
		if read {
			lim = limits.Read
		}
		if ok, al := wait(lim); !al {
			return 0, false, false
		} else if !ok {
			return StatusIO, false, true
		}
	}
	return StatusOK, true, true
}

// dispatch executes one request. The returned out slice (read payload)
// is streamed by the caller right after the response header.
func (s *Server) dispatch(ctx context.Context, backend Backend, h *ReqHeader, payload []byte, rw *connRW) (RespHeader, []byte) {
	if h.Length > MaxPayload {
		return RespHeader{Seq: h.Seq, Status: StatusTooBig}, nil
	}
	if s.ReadOnly {
		switch h.Op {
		case OpWrite, OpWriteZeroes, OpTrim, OpSync, OpRemove:
			return RespHeader{Seq: h.Seq, Status: StatusReadOnly}, nil
		}
	}
	// Epoch fencing: a node that has been replaced (its epoch is below
	// the cluster's floor) must not be used; the reply tells the client
	// the epoch it needs to advance to. Reads are fenced too: after a
	// placement change an old target may no longer hold the
	// authoritative replica, and the StatusStale reply is what drives
	// clients to rebuild their map.
	if min := s.minEpoch.Load(); min != 0 && h.Epoch < min {
		// OpStat is deliberately not fenced: recovery diagnostics must
		// work against any replica, and a stat cannot corrupt anything.
		switch h.Op {
		case OpRead, OpWrite, OpWriteZeroes, OpTrim, OpSync, OpRemove:
			s.logf("sdsnet: stale request epoch %d < %d (op %s oid %d)", h.Epoch, min, h.Op, h.OID)
			return RespHeader{Seq: h.Seq, Status: StatusStale, Value: uint64(min)}, nil
		}
	}
	switch h.Op {
	case OpPing:
		return RespHeader{Seq: h.Seq, Status: StatusOK}, nil

	case OpRead:
		buf := rw.getBuf(int(h.Length))
		n, err := backend.ReadAt(ctx, h.OID, h.Offset, buf)
		switch {
		case err == nil:
			return RespHeader{Seq: h.Seq, Status: StatusOK, Value: uint64(n)}, buf[:n]
		case errors.Is(err, os.ErrNotExist):
			return RespHeader{Seq: h.Seq, Status: StatusNotFound}, nil
		case errors.Is(err, io.EOF) && n > 0:
			// Partial read up to the (sparse) end of the object.
			return RespHeader{Seq: h.Seq, Status: StatusOK, Value: uint64(n)}, buf[:n]
		case errors.Is(err, io.EOF):
			return RespHeader{Seq: h.Seq, Status: StatusEOF}, nil
		default:
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}

	case OpWrite:
		if len(payload) != int(h.Length) {
			return RespHeader{Seq: h.Seq, Status: StatusInvalid}, nil
		}
		if err := backend.WriteAt(ctx, h.OID, h.Offset, payload); err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		if h.Flags&FlagsFUA != 0 {
			if err := backend.Sync(ctx); err != nil {
				return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
			}
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK, Value: uint64(h.Length)}, nil

	case OpWriteZeroes:
		if err := backend.WriteZeroes(ctx, h.OID, h.Offset, uint64(h.Length), h.Flags&FlagsHole != 0); err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		if h.Flags&FlagsFUA != 0 {
			if err := backend.Sync(ctx); err != nil {
				return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
			}
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK, Value: uint64(h.Length)}, nil

	case OpTrim:
		if err := backend.WriteZeroes(ctx, h.OID, h.Offset, uint64(h.Length), true); err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK, Value: uint64(h.Length)}, nil

	case OpSync:
		if err := backend.Sync(ctx); err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK}, nil

	case OpRemove:
		if err := backend.Remove(ctx, h.OID); err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK}, nil

	case OpStat:
		size, err := backend.Stat(ctx, h.OID)
		if err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK, Value: size}, nil

	case OpStatInfo:
		si, ok := backend.(statInfoer)
		if !ok {
			return RespHeader{Seq: h.Seq, Status: StatusNotSupp}, nil
		}
		_, alloc, err := si.StatInfo(ctx, h.OID)
		if err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		return RespHeader{Seq: h.Seq, Status: StatusOK, Value: alloc}, nil

	case OpStatFS:
		sf, ok := backend.(statFSer)
		if !ok {
			return RespHeader{Seq: h.Seq, Status: StatusNotSupp}, nil
		}
		usage, free, total, err := sf.StatFS(ctx)
		if err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
		}
		out := rw.getBuf(24)
		binary.BigEndian.PutUint64(out[0:8], usage)
		binary.BigEndian.PutUint64(out[8:16], free)
		binary.BigEndian.PutUint64(out[16:24], total)
		return RespHeader{Seq: h.Seq, Status: StatusOK}, out
	}
	return RespHeader{Seq: h.Seq, Status: StatusNotSupp}, nil
}

func (s *Server) reply(c net.Conn, h *RespHeader) error {
	var b [RespHeaderSize]byte
	h.Encode(b[:])
	_, err := c.Write(b[:])
	return err
}

// replyRW writes one response on per-connection scratch: zero
// allocations per reply (a stack [RespHeaderSize]byte handed to
// bufio.Writer escapes to the heap).
func (s *Server) replyRW(w *bufio.Writer, rw *connRW, h *RespHeader, out []byte) error {
	h.Encode(rw.hdr)
	if _, err := w.Write(rw.hdr); err != nil {
		return err
	}
	if len(out) > 0 {
		if _, err := w.Write(out); err != nil {
			return err
		}
	}
	return nil
}

// statusFromErr maps backend and framing errors onto wire statuses.
func statusFromErr(err error) Status {
	switch {
	case err == nil:
		return StatusOK
	case errors.Is(err, ErrTooBig):
		return StatusTooBig
	case errors.Is(err, ErrProtocol), errors.Is(err, ErrShortHeader):
		return StatusInvalid
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return StatusEOF
	case errors.Is(err, os.ErrNotExist):
		return StatusNotFound
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT):
		// store.ErrNoSpace wraps the raw errno, so a full disk (or a
		// quota) reaches the client as StatusNoSpace, not StatusIO.
		return StatusNoSpace
	default:
		return StatusIO
	}
}
