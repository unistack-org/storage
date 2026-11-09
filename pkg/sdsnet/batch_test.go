package sdsnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"net"
	"sync"
	"testing"
	"time"
)

func TestBatchRoundTrip(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	// A write batch: 16 objects, 512 B each, distinct patterns.
	const n = 16
	reqs := make([]BatchReq, n)
	payloads := make([][]byte, n)
	for i := 0; i < n; i++ {
		payloads[i] = make([]byte, 512)
		payloads[i][0] = byte(i)
		payloads[i][511] = byte(i * 7)
		reqs[i] = BatchReq{Op: OpWrite, OID: uint64(100 + i), Offset: 0, Payload: payloads[i]}
	}
	results := make([]BatchResult, n)
	if err := cc.Batch(ctx, reqs, results); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	for i, r := range results {
		if r.Status != StatusOK || r.Err != nil {
			t.Fatalf("write sub-op %d: %+v", i, r)
		}
	}

	// A read batch against the same objects.
	reqs = make([]BatchReq, n)
	bufs := make([][]byte, n)
	for i := 0; i < n; i++ {
		bufs[i] = make([]byte, 512)
		reqs[i] = BatchReq{Op: OpRead, OID: uint64(100 + i), Offset: 0, P: bufs[i]}
	}
	results = make([]BatchResult, n)
	if err := cc.Batch(ctx, reqs, results); err != nil {
		t.Fatalf("read batch: %v", err)
	}
	for i, r := range results {
		if r.Status != StatusOK || r.Err != nil {
			t.Fatalf("read sub-op %d: %+v", i, r)
		}
		if r.Value != 512 {
			t.Fatalf("read sub-op %d: value %d", i, r.Value)
		}
		if !bytes.Equal(bufs[i], payloads[i]) {
			t.Fatalf("read sub-op %d: data mismatch", i)
		}
	}

	// Mixed batch: stat, statinfo, write_zeroes, trim, sync, ping-less ops.
	reqs = []BatchReq{
		{Op: OpStat, OID: 100},
		{Op: OpStatInfo, OID: 101},
		{Op: OpWriteZeroes, OID: 102, Offset: 0, Len: 4096},
		{Op: OpTrim, OID: 103, Offset: 0, Len: 4096},
		{Op: OpSync},
		{Op: OpRemove, OID: 104},
	}
	results = make([]BatchResult, len(reqs))
	if err := cc.Batch(ctx, reqs, results); err != nil {
		t.Fatalf("mixed batch: %v", err)
	}
	if results[0].Status != StatusOK || results[0].Value == 0 {
		t.Fatalf("stat: %+v", results[0])
	}
	if results[1].Status != StatusOK {
		t.Fatalf("statinfo: %+v", results[1])
	}
	for i := 2; i < len(results); i++ {
		if results[i].Status != StatusOK {
			t.Fatalf("mixed sub-op %d: %+v", i, results[i])
		}
	}

	// The connection must still serve single ops (seq stayed in sync).
	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("ping after batch: %v", err)
	}
	if err := cc.Write(ctx, 200, 0, []byte("hello"), false); err != nil {
		t.Fatalf("write after batch: %v", err)
	}
}

// strictReadBackend surfaces missing objects as fs.ErrNotExist (the
// real store's thin-provisioning behaviour) instead of the memBackend's
// read-zeroes default, so per-slot StatusNotFound can be exercised.
type strictReadBackend struct{ *memBackend }

func (b *strictReadBackend) ReadAt(ctx context.Context, oid, off uint64, p []byte) (int, error) {
	b.mu.Lock()
	live := b.live[oid]
	b.mu.Unlock()
	if !live {
		return 0, fs.ErrNotExist
	}
	return b.memBackend.ReadAt(ctx, oid, off, p)
}

func TestBatchPerOpStatus(t *testing.T) {
	be := &strictReadBackend{memBackend: newMemBackend(1 << 20)}
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	// Object 1 exists, object 2 never written: a read of a missing
	// object must fail per-slot (StatusNotFound) while its neighbours
	// succeed, and the stream must stay usable.
	if err := cc.Write(ctx, 1, 0, []byte("data"), false); err != nil {
		t.Fatal(err)
	}
	b1 := make([]byte, 4)
	b2 := make([]byte, 4)
	reqs := []BatchReq{
		{Op: OpRead, OID: 1, P: b1},
		{Op: OpRead, OID: 2, P: b2},
		{Op: OpRead, OID: 1, P: b1},
	}
	results := make([]BatchResult, len(reqs))
	if err := cc.Batch(ctx, reqs, results); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if results[0].Status != StatusOK || string(b1) != "data" {
		t.Fatalf("slot 0: %+v", results[0])
	}
	if results[1].Status != StatusNotFound {
		t.Fatalf("slot 1: want NotFound, got %+v", results[1])
	}
	var re *Error
	if !errors.As(results[1].Err, &re) || re.Status != StatusNotFound {
		t.Fatalf("slot 1 err: %v", results[1].Err)
	}
	if results[2].Status != StatusOK {
		t.Fatalf("slot 2: %+v", results[2])
	}
	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("ping after partial failure: %v", err)
	}
}

func TestBatchLimits(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	tooMany := make([]BatchReq, MaxBatchOps+1)
	res := make([]BatchResult, MaxBatchOps+1)
	if err := cc.Batch(ctx, tooMany, res); !errors.Is(err, ErrProtocol) {
		t.Fatalf("over-count batch: want ErrProtocol, got %v", err)
	}
	if cc.Dead() {
		t.Fatal("over-count batch is a pre-flight check: the conn must stay usable")
	}
	// Single ops must still work on it.
	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("ping after over-count batch: %v", err)
	}
}

func TestBatchEmptyAndBadOp(t *testing.T) {
	cc := &Conn{c: &net.TCPConn{}} // never used: Batch must fail pre-flight
	if err := cc.Batch(context.Background(), nil, nil); !errors.Is(err, ErrProtocol) {
		t.Fatalf("empty batch: %v", err)
	}
	if err := cc.Batch(context.Background(), []BatchReq{{Op: OpPing}}, make([]BatchResult, 1)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("ping in batch: %v", err)
	}
	if err := cc.Batch(context.Background(), []BatchReq{{Op: OpBatch}}, make([]BatchResult, 1)); !errors.Is(err, ErrProtocol) {
		t.Fatalf("nested batch: %v", err)
	}
	if cc.Dead() {
		t.Fatal("pre-flight validation must not mark the conn dead")
	}
}

// TestBatchNotSupportedFallback simulates an old (pre-batch) server:
// its handshake reply advertises no batch support (Value=0). The client
// must refuse to send batch frames (ErrBatchNotSupported) on an intact
// connection and keep serving single ops.
func TestBatchNotSupportedFallback(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// Handshake: consume magic frame, reply OK with Value=0 (no
		// batch support — exactly what a pre-batch server sends).
		var fixed [16]byte
		if _, err := io.ReadFull(c, fixed[:]); err != nil {
			return
		}
		f, err := DecodeHandshakeFixed(fixed[:])
		if err != nil || !f.HasMagic {
			return
		}
		name := make([]byte, f.NameLen)
		if f.NameLen > 0 {
			if _, err := io.ReadFull(c, name); err != nil {
				return
			}
		}
		var rep [RespHeaderSize]byte
		(&RespHeader{Status: StatusOK}).Encode(rep[:])
		if _, err := c.Write(rep[:]); err != nil {
			return
		}
		// Old-server loop: ordinary ops only, answered OK.
		for {
			var rh [ReqHeaderSize]byte
			if _, err := io.ReadFull(c, rh[:]); err != nil {
				return
			}
			h, err := DecodeReqHeader(rh[:])
			if err != nil {
				return
			}
			var opReply [RespHeaderSize]byte
			(&RespHeader{Seq: h.Seq, Status: StatusOK}).Encode(opReply[:])
			if _, err := c.Write(opReply[:]); err != nil {
				return
			}
		}
	}()

	cc, err := Dial(ln.Addr().String(), "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	reqs := []BatchReq{{Op: OpRead, OID: 1, P: make([]byte, 4)}, {Op: OpRead, OID: 2, P: make([]byte, 4)}}
	results := make([]BatchResult, len(reqs))
	err = cc.Batch(ctx, reqs, results)
	if !errors.Is(err, ErrBatchNotSupported) {
		t.Fatalf("want ErrBatchNotSupported, got %v", err)
	}
	if cc.Dead() {
		t.Fatal("batch refusal must keep the connection")
	}
	// Single ops must keep working on the same conn (fallback path).
	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("ping after fallback: %v", err)
	}
}

func TestBatchPoolConcurrent(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	pp := NewPool(addr, "vol0")
	pp.Limit = 8
	defer pp.Close()
	ctx := context.Background()

	const workers = 8
	const perWorker = 50
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			reqs := make([]BatchReq, 16)
			results := make([]BatchResult, 16)
			buf := make([]byte, 256)
			for round := 0; round < perWorker; round++ {
				for i := range reqs {
					oid := uint64(w*1000 + i)
					reqs[i] = BatchReq{Op: OpWrite, OID: oid, Payload: buf}
				}
				if err := pp.Batch(ctx, reqs, results); err != nil {
					errCh <- err
					return
				}
				for i := range reqs {
					reqs[i] = BatchReq{Op: OpRead, OID: uint64(w*1000 + i), P: make([]byte, 256)}
				}
				if err := pp.Batch(ctx, reqs, results); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent batch: %v", err)
	}
}

// TestBatchLargeReads exercises batches whose replies exceed the
// server's 128 KiB write buffer, forcing bufio write-through.
func TestBatchLargeReads(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	const n = 40
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte(i)
	}
	reqs := make([]BatchReq, n)
	results := make([]BatchResult, n)
	for i := 0; i < n; i++ {
		reqs[i] = BatchReq{Op: OpWrite, OID: uint64(500 + i), Payload: payload}
	}
	if err := cc.Batch(ctx, reqs, results); err != nil {
		t.Fatalf("write batch: %v", err)
	}
	reqs = make([]BatchReq, n)
	bufs := make([][]byte, n)
	for i := 0; i < n; i++ {
		bufs[i] = make([]byte, 8192)
		reqs[i] = BatchReq{Op: OpRead, OID: uint64(500 + i), P: bufs[i]}
	}
	results = make([]BatchResult, n)
	if err := cc.Batch(ctx, reqs, results); err != nil {
		t.Fatalf("read batch: %v", err)
	}
	for i := range results {
		if results[i].Status != StatusOK || !bytes.Equal(bufs[i], payload) {
			t.Fatalf("sub-op %d: %+v", i, results[i])
		}
	}
}

// TestBatchServerDropsNestedBatch crafts (by hand, on the wire) a batch
// that nests another batch: the server must close the connection.
func TestBatchServerDropsNestedBatch(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write(EncodeHandshake("vol0")); err != nil {
		t.Fatal(err)
	}
	var rep [RespHeaderSize]byte
	if _, err := io.ReadFull(c, rep[:]); err != nil {
		t.Fatal(err)
	}

	// Outer batch with two sub-ops, the second being OpBatch itself.
	var outer [ReqHeaderSize]byte
	(&ReqHeader{Op: OpBatch, Seq: 7, Length: 2}).Encode(outer[:])
	var sub [ReqHeaderSize]byte
	(&ReqHeader{Op: OpPing, Seq: 0}).Encode(sub[:])
	nested := sub
	nested[1] = byte(OpBatch)
	if _, err := c.Write(outer[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(sub[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(nested[:]); err != nil {
		t.Fatal(err)
	}
	// The server must drop us: the first reply (ping OK) may arrive,
	// then the connection closes.
	n, err := c.Read(rep[:])
	if err == nil && n == RespHeaderSize {
		rh, derr := DecodeRespHeader(rep[:])
		if derr != nil || rh.Status != StatusOK {
			t.Fatalf("expected ping OK first, got %+v (%v)", rh, derr)
		}
	}
	if _, err := io.ReadFull(c, rep[:]); err == nil {
		t.Fatal("expected the server to close on a nested batch")
	}
}

// TestLazyFlushPipelining verifies the single-op loop's lazy flush:
// pipelined requests all get replies (the deferred flush must not
// strand one), and the batched path interleaves cleanly with them.
func TestLazyFlushPipelining(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(EncodeHandshake("vol0")); err != nil {
		t.Fatal(err)
	}
	var rep [RespHeaderSize]byte
	if _, err := io.ReadFull(c, rep[:]); err != nil {
		t.Fatal(err)
	}

	// Send 64 pings back-to-back (one write syscall at most, given the
	// small frame size) and read 64 replies.
	const n = 64
	var buf bytes.Buffer
	for i := 1; i <= n; i++ {
		var h [ReqHeaderSize]byte
		(&ReqHeader{Op: OpPing, Seq: uint32(i)}).Encode(h[:])
		buf.Write(h[:])
	}
	if _, err := c.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if _, err := io.ReadFull(c, rep[:]); err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		rh, err := DecodeRespHeader(rep[:])
		if err != nil || rh.Status != StatusOK || rh.Seq != uint32(i) {
			t.Fatalf("reply %d: %+v (%v)", i, rh, err)
		}
	}
}
