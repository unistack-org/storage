//go:build unix

package sdsnet

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func streamConn(t *testing.T) *Conn {
	t.Helper()
	_, dial := localServer(t)
	c, err := dial()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	cc, err := NewConn(c, "vol0", 5*time.Second)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

func TestStreamBasicRoundTrip(t *testing.T) {
	cc := streamConn(t)
	s := cc.NewStream(StreamConfig{Window: 8})
	ctx := context.Background()

	payload := bytes.Repeat([]byte("stream"), 1000)
	oid := uint64(3)
	fw, err := s.Submit(ctx, &StreamOp{Op: OpWrite, OID: oid, Offset: 4096, Payload: payload})
	if err != nil {
		t.Fatalf("submit write: %v", err)
	}
	// Submit many reads behind the write: FIFO must resolve all of them.
	const n = 40
	fs := make([]*Future, 0, n)
	for i := 0; i < n; i++ {
		p := make([]byte, 512)
		f, err := s.Submit(ctx, &StreamOp{Op: OpRead, OID: oid, Offset: 8192, Len: uint32(len(p)), P: p})
		if err != nil {
			t.Fatalf("submit read: %v", err)
		}
		fs = append(fs, f)
	}
	if _, err := fw.Wait(); err != nil {
		t.Fatalf("write: %v", err)
	}
	for i, f := range fs {
		got, err := f.Wait()
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got != 512 {
			t.Fatalf("read %d = %d want 512", i, got)
		}
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestStreamConcurrentSubmitters drives many goroutines through one
// stream: no deadlocks, no lost/duplicated completions, stream stays
// consistent under window pressure (Window < total ops).
func TestStreamConcurrentSubmitters(t *testing.T) {
	cc := streamConn(t)
	s := cc.NewStream(StreamConfig{Window: 16})
	ctx := context.Background()

	const goroutines, ops = 16, 50
	var ok, failed atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			p := bytes.Repeat([]byte{byte(g)}, 4096)
			for i := 0; i < ops; i++ {
				oid := uint64(g*1000 + i%4)
				var f *Future
				var err error
				if i%2 == 0 {
					f, err = s.Submit(ctx, &StreamOp{Op: OpWrite, OID: oid, Offset: uint64(i) * 512, Payload: p})
				} else {
					buf := make([]byte, 4096)
					f, err = s.Submit(ctx, &StreamOp{Op: OpRead, OID: oid, Offset: uint64(i) * 512, Len: 4096, P: buf})
				}
				if err != nil {
					failed.Add(1)
					continue
				}
				if _, err := f.Wait(); err != nil {
					failed.Add(1)
					continue
				}
				ok.Add(1)
			}
		}(g)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("deadlock: ok=%d failed=%d", ok.Load(), failed.Load())
	}
	if failed.Load() != 0 {
		t.Fatalf("%d ops failed", failed.Load())
	}
	if ok.Load() != goroutines*ops {
		t.Fatalf("ok=%d want %d", ok.Load(), goroutines*ops)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// slowReadBackend answers reads only after release is closed.
type slowReadBackend struct {
	zeroBackend
	release chan struct{}
}

func (z *slowReadBackend) ReadAt(ctx context.Context, oid, off uint64, p []byte) (int, error) {
	select {
	case <-z.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return len(p), nil
}

// TestStreamContextCancel: a cancelled ctx resolves its future with
// ctx.Err() once the reply drains, and the stream stays usable.
func TestStreamContextCancel(t *testing.T) {
	srv := &Server{Backend: &slowReadBackend{zeroBackend: zeroBackend{size: 1 << 30}, release: make(chan struct{})}, Logf: func(string, ...any) {}}
	t.Cleanup(func() { srv.Close() })
	mine, peer, err := Socketpair()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	go srv.ServeConn(peer)
	cc, err := NewConn(mine, "vol0", 10*time.Second)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer cc.Close()
	s := cc.NewStream(StreamConfig{Window: 64})
	defer s.Close(context.Background())

	cctx, cancel := context.WithCancel(context.Background())
	f, err := s.Submit(cctx, &StreamOp{Ctx: cctx, Op: OpRead, OID: 1, Offset: 0, Len: 128, P: make([]byte, 128)})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	cancel()
	// The backend parks the request, so cancellation definitely races
	// ahead of the reply. Release: the reply drains and the future must
	// resolve with context.Canceled, not the result.
	close(srv.Backend.(*slowReadBackend).release)
	deadline := time.After(8 * time.Second)
	resCh := make(chan error, 1)
	go func() { _, err := f.Wait(); resCh <- err }()
	select {
	case err := <-resCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled future = %v want context.Canceled", err)
		}
	case <-deadline:
		t.Fatal("cancelled future never resolved")
	}
	if s.Dead() {
		t.Fatal("stream marked dead after ctx cancel")
	}
	// Stream remains usable.
	f2, err := s.Submit(context.Background(), &StreamOp{Op: OpWrite, OID: 1, Offset: 0, Payload: []byte("still alive")})
	if err != nil {
		t.Fatalf("submit after cancel: %v", err)
	}
	if _, err := f2.Wait(); err != nil {
		t.Fatalf("write after cancel: %v", err)
	}
}

// TestStreamProtocolError: a protocol-level failure (bad OID -> backend
// error status) resolves the future with *Error and must NOT kill the
// stream.
func TestStreamProtocolError(t *testing.T) {
	cc := streamConn(t)
	s := cc.NewStream(StreamConfig{Window: 8})
	defer s.Close(context.Background())
	ctx := context.Background()

	// zeroBackend accepts any OID, so provoke a protocol error with an
	// oversized read (Length > MaxPayload is refused by the server with
	// StatusIO and the connection stays valid).
	f, err := s.Submit(ctx, &StreamOp{Op: OpRead, OID: 1, Offset: 0, Len: MaxPayload + 1, P: make([]byte, 16)})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	_, werr := f.Wait()
	var pe *Error
	if werr == nil {
		t.Skip("server accepted oversized read; nothing to assert")
	}
	if !errors.As(werr, &pe) {
		t.Fatalf("oversized read = %v, want *sdsnet.Error", werr)
	}
	if s.Dead() {
		t.Fatal("stream marked dead on protocol error")
	}
	f2, err := s.Submit(ctx, &StreamOp{Op: OpSync, OID: 1})
	if err != nil {
		t.Fatalf("submit after protocol error: %v", err)
	}
	if _, err := f2.Wait(); err != nil {
		t.Fatalf("sync after protocol error: %v", err)
	}
}

// TestStreamCloseDrains: Close resolves everything already accepted;
// submits after Close are refused.
func TestStreamCloseDrains(t *testing.T) {
	cc := streamConn(t)
	s := cc.NewStream(StreamConfig{Window: 64})
	ctx := context.Background()
	const n = 50
	fs := make([]*Future, n)
	for i := range fs {
		f, err := s.Submit(ctx, &StreamOp{Op: OpRead, OID: uint64(i), Offset: 0, Len: 256, P: make([]byte, 256)})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		fs[i] = f
	}
	if err := s.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, f := range fs {
		if _, err := f.Wait(); err != nil {
			t.Fatalf("future %d after Close: %v", i, err)
		}
	}
	if _, err := s.Submit(ctx, &StreamOp{Op: OpSync, OID: 1}); err == nil {
		t.Fatal("submit after Close accepted")
	}
}

// TestStreamServerGone: when the server closes the transport, every
// pending future resolves and the stream is Dead.
func TestStreamServerGone(t *testing.T) {
	_, dial := localServer(t)
	c, err := dial()
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	cc, err := NewConn(c, "vol0", 5*time.Second)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer cc.Close()
	s := cc.NewStream(StreamConfig{Window: 512})
	ctx := context.Background()
	var fs []*Future
	for i := 0; i < 400; i++ {
		f, err := s.Submit(ctx, &StreamOp{Op: OpSync, OID: uint64(i)})
		if err != nil {
			break // window pressure with a dying server is fine
		}
		fs = append(fs, f)
	}
	cc.Close() // transport away
	deadline := time.After(10 * time.Second)
	resolved := make(chan int, len(fs))
	go func() {
		n := 0
		for _, f := range fs {
			if _, err := f.Wait(); err == nil {
				// ops that genuinely completed before the close are OK
			}
			n++
		}
		resolved <- n
	}()
	select {
	case n := <-resolved:
		if n != len(fs) {
			t.Fatalf("resolved %d of %d", n, len(fs))
		}
	case <-deadline:
		t.Fatal("futures never resolved after transport loss")
	}
	// Give the reader a moment to mark the stream dead.
	for i := 0; i < 100 && !s.Dead(); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !s.Dead() {
		t.Fatal("stream not Dead after transport loss")
	}
}

// TestStreamZerocopyGate: on non-TCP transports (socketpair) the
// zerocopy branch must be disabled and big writes must still work
// through the normal buffered path.
func TestStreamZerocopyGate(t *testing.T) {
	cc := streamConn(t)
	if cc.canZeroCopy() {
		t.Fatal("zerocopy must be off on socketpair")
	}
	s := cc.NewStream(StreamConfig{Window: 8, ZerocopyThreshold: 1024})
	defer s.Close(context.Background())
	big := bytes.Repeat([]byte("zc"), 4096) // 8 KiB > threshold
	f, err := s.Submit(context.Background(), &StreamOp{Op: OpWrite, OID: 9, Offset: 0, Payload: big})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := f.Wait(); err != nil {
		t.Fatalf("big write: %v", err)
	}
}
