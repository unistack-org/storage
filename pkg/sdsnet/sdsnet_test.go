package sdsnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReqHeaderRoundTrip(t *testing.T) {
	in := ReqHeader{
		Op:     OpWrite,
		Flags:  FlagsFUA | FlagsHole,
		Seq:    0xDEADBEEF,
		OID:    0x0123456789ABCDEF,
		Offset: 1 << 40,
		Length: 4096,
		Epoch:  42,
	}
	var b [ReqHeaderSize]byte
	in.Encode(b[:])
	out, err := DecodeReqHeader(b[:])
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, in)
	}
}

func TestRespHeaderRoundTrip(t *testing.T) {
	in := RespHeader{Seq: 7, Status: StatusIO, Value: 1 << 50}
	var b [RespHeaderSize]byte
	in.Encode(b[:])
	out, err := DecodeRespHeader(b[:])
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, in)
	}
}

func TestHeaderValidation(t *testing.T) {
	var b [ReqHeaderSize]byte
	(&ReqHeader{Op: OpRead}).Encode(b[:])
	if _, err := DecodeReqHeader(b[:20]); !errors.Is(err, ErrShortHeader) {
		t.Fatalf("short header: got %v", err)
	}
	b[0] = 99
	if _, err := DecodeReqHeader(b[:]); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("bad version: got %v", err)
	}
	(&ReqHeader{Op: OpWrite, Length: MaxPayload + 1}).Encode(b[:])
	if _, err := DecodeReqHeader(b[:]); !errors.Is(err, ErrTooBig) {
		t.Fatalf("oversize: got %v", err)
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	msg := EncodeHandshake("my-export")
	name, err := DecodeHandshake(msg)
	if err != nil {
		t.Fatal(err)
	}
	if name != "my-export" {
		t.Fatalf("name = %q", name)
	}
	bad := append([]byte(nil), msg...)
	bad[3] ^= 0xFF
	if _, err := DecodeHandshake(bad); !errors.Is(err, ErrProtocol) {
		t.Fatalf("bad magic: got %v", err)
	}
	bad = append([]byte(nil), msg...)
	bad[11] = 7 // version
	if _, err := DecodeHandshake(bad); err == nil {
		t.Fatal("bad version: no error")
	}
}

// memBackend is a tiny in-memory Backend for tests.
type memBackend struct {
	mu   sync.Mutex
	data map[[2]uint64][]byte // (oid, aligned chunk) → bytes; key: oid, off>>12
	size uint64
	sync sync.Mutex
}

func newMemBackend(size uint64) *memBackend {
	return &memBackend{data: map[[2]uint64][]byte{}, size: size}
}

func (m *memBackend) Size() uint64 { return m.size }

func (m *memBackend) ReadAt(_ context.Context, oid, off uint64, p []byte) (int, error) {
	if off >= m.size {
		return 0, io.EOF
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for n < len(p) {
		chunk := off >> 12
		co := int(off & 4095)
		key := [2]uint64{oid, chunk}
		cell := m.data[key]
		if cell == nil {
			cell = make([]byte, 4096)
			m.data[key] = cell
		}
		take := 4096 - int(co)
		if take > len(p)-n {
			take = len(p) - n
		}
		copy(p[n:n+take], cell[co:co+take])
		n += take
		off += uint64(take)
	}
	return n, nil
}

func (m *memBackend) WriteAt(_ context.Context, oid, off uint64, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for n < len(p) {
		chunk := off >> 12
		co := int(off & 4095)
		key := [2]uint64{oid, chunk}
		cell := m.data[key]
		if cell == nil {
			cell = make([]byte, 4096)
			m.data[key] = cell
		}
		take := 4096 - int(co)
		if take > len(p)-n {
			take = len(p) - n
		}
		copy(cell[co:co+take], p[n:n+take])
		n += take
		off += uint64(take)
	}
	return nil
}

func (m *memBackend) WriteZeroes(_ context.Context, oid, off, n uint64, _ bool) error {
	return m.WriteAt(context.Background(), oid, off, make([]byte, n))
}

func (m *memBackend) Sync(context.Context) error { return nil }

// startServer boots a Server on a random port with a mem backend.
func startServer(t *testing.T, be Backend) (addr string, shutdown func()) {
	t.Helper()
	s := &Server{Backend: be, Logf: t.Logf}
	go func() {
		if err := s.Serve("127.0.0.1:0"); err != nil {
			t.Logf("serve: %v", err)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not start")
	}
	return s.Addr(), func() { s.Close() }
}

func TestEndToEnd(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Write across a chunk boundary, read it back.
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := cc.Write(ctx, 1, 2048, payload, false); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	n, err := cc.Read(ctx, 1, 2048, got)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) || !bytes.Equal(got, payload) {
		t.Fatalf("read-back mismatch n=%d", n)
	}

	// Hole read returns zeroes.
	zeros := make([]byte, 4096)
	n, err = cc.Read(ctx, 2, 0, zeros)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4096 || !bytes.Equal(zeros, make([]byte, 4096)) {
		t.Fatal("hole not zero")
	}

	// Write zeroes + trim.
	if err := cc.WriteZeroes(ctx, 1, 2048, 4096, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := cc.Read(ctx, 1, 2048, got[:4096]); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:4096], make([]byte, 4096)) {
		t.Fatal("write zeroes failed")
	}
	if err := cc.Trim(ctx, 1, 2048, 4096); err != nil {
		t.Fatal(err)
	}

	// EOF status past the end.
	if _, err := cc.Read(ctx, 1, be.Size(), got); statusOf(t, err) != StatusEOF {
		t.Fatalf("eof: got %v", err)
	}

	// Sync.
	if err := cc.Sync(ctx); err != nil {
		t.Fatal(err)
	}
}

func statusOf(t *testing.T, err error) Status {
	t.Helper()
	var e *Error
	if errors.As(err, &e) {
		return e.Status
	}
	if err != nil {
		t.Fatalf("not a status error: %v", err)
	}
	return StatusOK
}

func TestReadOnly(t *testing.T) {
	be := newMemBackend(1 << 20)
	s := &Server{Backend: be, ReadOnly: true, Logf: t.Logf}
	go s.Serve("127.0.0.1:0")
	for s.Addr() == "" {
		time.Sleep(time.Millisecond)
	}
	defer s.Close()

	cc, err := Dial(s.Addr(), "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()
	if err := cc.Write(ctx, 1, 0, []byte("x"), false); statusOf(t, err) != StatusReadOnly {
		t.Fatalf("write on read-only: got %v", err)
	}
	buf := make([]byte, 4)
	if _, err := cc.Read(ctx, 1, 0, buf); err != nil {
		t.Fatalf("read on read-only: %v", err)
	}
}

func TestConcurrentClients(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	pool := NewPool(addr, "vol0")
	defer pool.Close()
	ctx := context.Background()

	const workers = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			oid := uint64(w)
			payload := bytes.Repeat([]byte{byte('a' + w)}, 4096)
			for i := 0; i < 50; i++ {
				off := uint64(i%16) * 4096
				if err := pool.Write(ctx, oid, off, payload, false); err != nil {
					t.Errorf("worker %d write: %v", w, err)
					return
				}
				got := make([]byte, 4096)
				n, err := pool.Read(ctx, oid, off, got)
				if err != nil {
					t.Errorf("worker %d read: %v", w, err)
					return
				}
				if n != 4096 || !bytes.Equal(got, payload) {
					t.Errorf("worker %d mismatch", w)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestPoolLimit(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()

	pool := NewPool(addr, "vol0")
	pool.Limit = 1
	defer pool.Close()

	cc1, err := pool.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// At the limit: Get blocks until ctx expires.
	bctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := pool.Get(bctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("limit: got %v", err)
	}
	pool.Put(cc1)
	cc2, err := pool.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cc2 != cc1 {
		t.Fatal("pool did not reuse connection")
	}
	pool.Discard(cc2)
	cc3, err := pool.Get(context.Background())
	if err != nil {
		t.Fatalf("after discard: %v", err)
	}
	pool.Discard(cc3)
}

func TestProtocolDesyncDetected(t *testing.T) {
	// The test plays a rogue server: it answers the client's ping with
	// a response whose seq does not match. The client must detect the
	// desync and mark the connection unusable.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	rawCh := make(chan net.Conn, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			close(rawCh)
			return
		}
		rawCh <- raw

		// Rogue handshake: accept anything, reply StatusOK.
		var fixed [16]byte
		if _, err := io.ReadFull(raw, fixed[:]); err != nil {
			return
		}
		n := int(fixed[14])<<8 | int(fixed[15])
		if n > 0 {
			io.ReadFull(raw, make([]byte, n))
		}
		var ok [RespHeaderSize]byte
		(&RespHeader{Status: StatusOK}).Encode(ok[:])
		raw.Write(ok[:])

		// Answer the ping with the wrong seq.
		var req [ReqHeaderSize]byte
		if _, err := io.ReadFull(raw, req[:]); err != nil {
			return
		}
		var bad [RespHeaderSize]byte
		(&RespHeader{Seq: 999, Status: StatusOK}).Encode(bad[:])
		raw.Write(bad[:])
	}()

	cc, err := Dial(ln.Addr().String(), "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	raw := <-rawCh
	if raw == nil {
		t.Fatal("rogue server did not accept")
	}
	defer raw.Close()

	errCh := make(chan error, 1)
	go func() { errCh <- cc.Ping(context.Background()) }()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrProtocol) {
			t.Fatalf("desync: got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("desync not detected")
	}
}

func BenchmarkClientWrite4K(b *testing.B) {
	be := newMemBackend(1 << 30)
	s := &Server{Backend: be}
	go s.Serve("127.0.0.1:0")
	for s.Addr() == "" {
		time.Sleep(time.Millisecond)
	}
	defer s.Close()
	cc, err := Dial(s.Addr(), "vol0", 2*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()
	payload := make([]byte, 4096)
	b.SetBytes(4096)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := cc.Write(ctx, uint64(i&0xFF), uint64(i%64)*4096, payload, false); err != nil {
			b.Fatal(err)
		}
	}
}
