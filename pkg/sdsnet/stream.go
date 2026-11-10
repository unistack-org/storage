package sdsnet

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Stream turns one Conn into a pipelined I/O stream: many goroutines
// submit requests concurrently, a single writer goroutine stamps and
// batches the frames, a single reader goroutine matches replies to
// requests in FIFO order (the protocol answers in request order and
// echoes seq, which the reader verifies). One Flush covers a whole
// batch of submissions, so a fleet of vCPUs sharing one TCP connection
// gets wire coalescing and no per-op syscall storm - the client-side
// counterpart of the server's batch handling.
//
// Contract: the caller owns StreamOp.Payload and StreamOp.P and must
// keep them stable and untouched from Submit until the returned Future
// resolves.

// StreamOp describes one request submitted to a Stream.
type StreamOp struct {
	// Ctx bounds the request: its deadline caps the connection
	// deadline while the request is queued or in flight; cancellation
	// resolves the Future with Ctx.Err() once the reply drains (the
	// stream stays consistent). Nil = the default opTimeout.
	Ctx     context.Context
	Op      Op // Read, Write, WriteZeroes, Trim, Sync, Remove, Stat, StatInfo, StatFS
	OID     uint64
	Offset  uint64
	Flags   uint16 // FlagsHole / FlagsFUA
	Len     uint32 // range length for WriteZeroes/Trim (0 for writes: len(Payload))
	Payload []byte // write payload; stable until the Future resolves
	P       []byte // read destination; stable until the Future resolves
}

// wireLength is the Length field for this op.
func (o *StreamOp) wireLength() uint32 {
	if o.Op == OpWrite {
		return uint32(len(o.Payload))
	}
	return o.Len
}

// Future is a submitted stream request. Wait blocks until the reply
// arrives (or the stream fails) and returns the result.
type Future struct {
	op   StreamOp
	seq  uint32
	done chan struct{}
	once sync.Once
	n    int
	err  error
}

// Wait blocks for the result. It returns the context error if the
// Submit context was cancelled (the reply was drained and discarded,
// the stream stays usable).
func (f *Future) Wait() (int, error) {
	<-f.done
	return f.n, f.err
}

// StreamConfig tunes a Stream.
type StreamConfig struct {
	// Window bounds how many requests may be queued/in flight
	// (submissions beyond it block until replies drain). Zero = 256.
	Window int
	// ZerocopyThreshold: writes of at least this many bytes are sent
	// with MSG_ZEROCOPY on Linux (0 disables). Other platforms and
	// TLS connections always copy. A good starting point is 1<<20.
	ZerocopyThreshold int
}

// Stream is a pipelined session over one Conn. Create with
// Conn.NewStream; the Conn must not be used directly while the Stream
// is open.
type Stream struct {
	cc   *Conn
	in   chan *Future
	sent chan *Future
	cfg  StreamConfig

	closed atomic.Bool
	dead   atomic.Bool
	deadMu sync.Mutex
	deadEr error

	writerDone, readerDone chan struct{}
	closeOnce              sync.Once
}

// NewStream starts the writer/reader goroutines for pipelined traffic
// on cc. Close must be called to release the connection.
func (cc *Conn) NewStream(cfg StreamConfig) *Stream {
	if cfg.Window <= 0 {
		cfg.Window = 256
	}
	s := &Stream{
		cc:         cc,
		in:         make(chan *Future, cfg.Window),
		sent:       make(chan *Future, cfg.Window),
		cfg:        cfg,
		writerDone: make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go s.writeLoop()
	go s.readLoop()
	return s
}

// Submit queues a request. It blocks while the in-flight window is
// full (backpressure). After Close returns, Submit fails with
// ErrClosed.
func (s *Stream) Submit(ctx context.Context, op *StreamOp) (*Future, error) {
	if s.closed.Load() || s.dead.Load() {
		return nil, ErrClosed
	}
	f := &Future{op: *op, done: make(chan struct{})}
	if f.op.Ctx == nil {
		f.op.Ctx = ctx // deadline/cancellation applies even via the ctx arg
	}
	type sendRes struct{ ok bool }
	res := make(chan sendRes, 1)
	go func() {
		select {
		case s.in <- f:
			res <- sendRes{true}
		case <-ctx.Done():
			res <- sendRes{false}
		case <-s.writerDone:
			res <- sendRes{false}
		}
	}()
	if r := <-res; !r.ok {
		return nil, ctx.Err()
	}
	return f, nil
}

// Close stops accepting submissions, waits for all accepted requests
// to complete (bounded by ctx), and closes the connection. On ctx
// expiry the stream is failed and the connection force-closed.
func (s *Stream) Close(ctx context.Context) error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		close(s.in)
		<-s.writerDone
		done := make(chan struct{})
		go func() {
			<-s.readerDone
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			s.fail(ctx.Err())
			<-s.readerDone
			err = ctx.Err()
		}
		if cerr := s.cc.Close(); cerr != nil && err == nil {
			err = cerr
		}
	})
	return err
}

// Dead reports a transport failure: the stream is unusable.
func (s *Stream) Dead() bool { return s.dead.Load() }

func (s *Stream) fail(err error) {
	s.deadMu.Lock()
	if s.deadEr == nil {
		s.deadEr = err
	}
	first := !s.dead.Swap(true)
	s.deadMu.Unlock()
	if first {
		_ = s.cc.c.Close() // unblock a reader/writer parked in the kernel
	}
}

func (s *Stream) failErr() error {
	s.deadMu.Lock()
	defer s.deadMu.Unlock()
	if s.deadEr != nil {
		return s.deadEr
	}
	return ErrClosed
}

// resolve completes the future exactly once: a writer/reader racing
// with fail() must not close the done channel twice.
func (f *Future) resolve(n int, err error) {
	f.once.Do(func() {
		f.n, f.err = n, err
		close(f.done)
	})
}

// deadlineOf returns the request deadline (op ctx deadline, else
// now+opTimeout when a ctx is set, else none).
func deadlineOf(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	if dl, ok := ctx.Deadline(); ok {
		return dl, true
	}
	return time.Now().Add(opTimeout), true
}

// writeLoop stamps, encodes and flushes requests, coalescing many
// submissions into one write syscall per batch.
func (s *Stream) writeLoop() {
	defer close(s.writerDone)
	var batch []*Future
	next := (*Future)(nil)
	hbuf := make([]byte, ReqHeaderSize)
	var deadline time.Time

	failAll := func(cause error, cur *Future) {
		s.fail(cause)
		if cur != nil {
			cur.resolve(0, s.failErr())
		}
		for _, f := range batch {
			f.resolve(0, s.failErr())
		}
		batch = nil
		go func() {
			// s.in may be closed concurrently by Close: recover instead
			// of panicking on "range over closed channel".
			defer func() { _ = recover() }()
			for f := range s.in {
				f.resolve(0, s.failErr())
			}
		}()
	}

	flush := func() bool {
		if len(batch) == 0 {
			deadline = time.Time{}
			return true
		}
		if !deadline.IsZero() {
			_ = s.cc.c.SetWriteDeadline(deadline)
		}
		err := s.cc.wr.Flush()
		_ = s.cc.c.SetWriteDeadline(time.Time{})
		if err != nil {
			failAll(err, nil)
			return false
		}
		batch = batch[:0]
		deadline = time.Time{}
		return true
	}

	// Flush the tail before closing sent: every published future must
	// already be on the wire, and closing sent is what lets the
	// reader finish.
	defer func() {
		flush()
		close(s.sent)
	}()

	for {
		f := next
		next = nil
		if f == nil {
			var ok bool
			f, ok = <-s.in
			if !ok {
				break
			}
		}
		if s.dead.Load() {
			f.resolve(0, s.failErr())
			continue
		}
		op := &f.op
		if s.cc.seq == 0 {
			s.cc.seq = 1
		}
		f.seq = s.cc.seq
		s.cc.seq++
		h := ReqHeader{Seq: f.seq, Epoch: s.cc.ep.Load(), Op: op.Op, OID: op.OID,
			Offset: op.Offset, Length: op.wireLength(), Flags: op.Flags}
		h.Encode(hbuf)
		if _, err := s.cc.wr.Write(hbuf); err != nil {
			failAll(err, f)
			return
		}
		if op.Op == OpWrite && len(op.Payload) > 0 {
			if s.cfg.ZerocopyThreshold > 0 && len(op.Payload) >= s.cfg.ZerocopyThreshold &&
				s.cc.canZeroCopy() {
				// Big write: flush the small-frame tail first so the
				// payload lands on the wire in request order, then
				// send it zero-copy (completion synchronised on
				// MSG_ERRQUEUE inside sendZeroCopy).
				if err := s.cc.wr.Flush(); err != nil {
					failAll(err, f)
					return
				}
				batch = batch[:0]
				zcDeadline := deadline
				if zcDeadline.IsZero() {
					zcDeadline = time.Now().Add(opTimeout)
				}
				deadline = time.Time{}
				if err := s.cc.sendZeroCopy(op.Payload, zcDeadline); err != nil {
					failAll(err, f)
					return
				}
			} else if _, err := s.cc.wr.Write(op.Payload); err != nil {
				failAll(err, f)
				return
			}
		}
		batch = append(batch, f)
		if dl, ok := deadlineOf(op.Ctx); ok && (deadline.IsZero() || dl.Before(deadline)) {
			deadline = dl
		}
		// Flush before publishing: the reader blocks on the socket
		// until bytes hit the wire, so publishing into a full sent
		// window ahead of the flush would deadlock. The threshold is
		// capped by Window so the publish below never blocks.
		flushBatch := s.cfg.Window
		if flushBatch > 64 {
			flushBatch = 64
		}
		if len(batch) >= flushBatch || s.cc.wr.Buffered() >= 128<<10 {
			if !flush() {
				return
			}
		} else {
			// Tail latency: when no submission is queued behind this
			// one, push the batch out immediately (a client waiting on
			// a lone request must never sit behind an unflushed frame).
			select {
			case next = <-s.in:
			default:
				if !flush() {
					return
				}
			}
		}
		// Publish to the reader once the frame is fully handed to the
		// buffered writer: the reader's FIFO is exactly this channel,
		// and the writer never reorders frames.
		s.sent <- f
	}
}

// readLoop consumes replies in FIFO order, matching seq and draining
// payloads into the caller-provided buffers.
func (s *Stream) readLoop() {
	defer close(s.readerDone)
	resp := make([]byte, RespHeaderSize)
	for f := range s.sent {
		if s.dead.Load() {
			f.resolve(0, s.failErr())
			continue
		}
		if dl, ok := deadlineOf(f.op.Ctx); ok {
			_ = s.cc.c.SetReadDeadline(dl)
		}
		n, err := s.readReply(f, resp)
		_ = s.cc.c.SetReadDeadline(time.Time{})
		if err != nil {
			if _, protocol := err.(*Error); !protocol {
				s.fail(err) // transport error: byte stream is desynced
				f.resolve(0, s.failErr())
				for g := range s.sent {
					g.resolve(0, s.failErr())
				}
				return
			}
			// Protocol-level status: the frame boundaries are intact,
			// the stream stays usable for everything behind this op.
			f.resolve(0, s.errFor(f, err))
			continue
		}
		f.resolve(n, s.errFor(f, nil))
	}
}

// errFor turns a completed exchange into the caller-visible result: a
// protocol *Error passes through, a transport failure becomes the
// stream error, and a healthy reply is reported as the (possibly
// cancelled) context outcome.
func (s *Stream) errFor(f *Future, exchangeErr error) error {
	if exchangeErr != nil {
		return exchangeErr
	}
	if ctx := f.op.Ctx; ctx != nil {
		return ctx.Err()
	}
	return nil
}

func (s *Stream) readReply(f *Future, resp []byte) (int, error) {
	if _, err := io.ReadFull(s.cc.rd, resp); err != nil {
		return 0, err
	}
	rh, err := DecodeRespHeader(resp)
	if err != nil {
		return 0, err
	}
	if rh.Seq != f.seq {
		return 0, ErrProtocol
	}
	if rh.Status != StatusOK {
		return 0, &Error{Seq: rh.Seq, Op: f.op.Op, Status: rh.Status, Value: rh.Value}
	}
	switch f.op.Op {
	case OpRead:
		n := int(rh.Value)
		if n > len(f.op.P) {
			n = len(f.op.P)
		}
		if n > 0 {
			if _, err := io.ReadFull(s.cc.rd, f.op.P[:n]); err != nil {
				return 0, err
			}
		}
		return n, nil
	case OpStatFS:
		p := f.op.P
		if len(p) < 24 {
			return 0, ErrProtocol
		}
		if _, err := io.ReadFull(s.cc.rd, p[:24]); err != nil {
			return 0, err
		}
		return 24, nil
	}
	return int(rh.Value), nil
}
