package sdsnet

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
)

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

	rd := bufio.NewReaderSize(c, 128<<10)
	wr := bufio.NewWriterSize(c, 128<<10)
	ctx := context.Background()
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
		var payload []byte
		if h.Op == OpWrite && h.Length > 0 {
			payload = rw.getBuf(int(h.Length))
			if _, err := io.ReadFull(rd, payload); err != nil {
				s.logf("sdsnet: read request payload: %v", err)
				return
			}
		}
		resp, out := s.dispatch(ctx, backend, &h, payload, rw)
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

// dispatch executes one request. The returned out slice (read payload)
// is streamed by the caller right after the response header.
func (s *Server) dispatch(ctx context.Context, backend Backend, h *ReqHeader, payload []byte, rw *connRW) (RespHeader, []byte) {
	if h.Length > MaxPayload {
		return RespHeader{Seq: h.Seq, Status: StatusTooBig}, nil
	}
	if s.ReadOnly {
		switch h.Op {
		case OpWrite, OpWriteZeroes, OpTrim, OpSync:
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
		switch h.Op {
		case OpRead, OpWrite, OpWriteZeroes, OpTrim, OpSync:
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
		return RespHeader{Seq: h.Seq, Status: StatusOK, Value: uint64(h.Length)}, nil

	case OpWriteZeroes:
		if err := backend.WriteZeroes(ctx, h.OID, h.Offset, uint64(h.Length), h.Flags&FlagsHole != 0); err != nil {
			return RespHeader{Seq: h.Seq, Status: statusFromErr(err)}, nil
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
	default:
		return StatusIO
	}
}
