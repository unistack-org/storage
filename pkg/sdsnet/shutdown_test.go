package sdsnet

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// TestShutdownCompletesInFlight: a request that is being served when
// Shutdown starts must complete normally; Shutdown returns once the
// connection drains (well inside the deadline).
func TestShutdownCompletesInFlight(t *testing.T) {
	slow := &slowBackend{delay: 150 * time.Millisecond, entered: make(chan struct{}), memBackend: newMemBackend(1 << 20)}
	s := &Server{Backend: slow}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.ServeListener(ln) }()

	cc, err := Dial(ln.Addr().String(), "vol", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	// Start a write that will still be in the backend when Shutdown runs.
	writeErr := make(chan error, 1)
	go func() {
		writeErr <- cc.Write(context.Background(), 1, 0, []byte("in-flight"), false)
	}()
	<-slow.entered // the request is inside the backend now

	shutErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutErr <- s.Shutdown(ctx)
	}()

	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatalf("in-flight write must complete: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight write did not complete")
	}
	select {
	case err := <-shutErr:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not return after drain")
	}
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("serve: %v", err)
		}
	default:
		t.Fatal("serve did not return after shutdown")
	}
	// New connections after Shutdown must be refused.
	if c, err := net.Dial("tcp", ln.Addr().String()); err == nil {
		c.Close()
		// Connection accepted by the OS backlog is fine; a handshake must fail.
		if _, err := Dial(ln.Addr().String(), "vol", 500*time.Millisecond); err == nil {
			t.Fatal("handshake after shutdown must fail")
		}
	}
}

// TestShutdownClosesIdle: a pooled client parked between requests must
// see the connection closed by the drain promptly, and Get must fail
// (no quorum of servers needed here: the pool is of the dying server).
func TestShutdownClosesIdle(t *testing.T) {
	be := newMemBackend(1 << 20)
	s := &Server{Backend: be}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.ServeListener(ln) }()

	p := NewPool(ln.Addr().String(), "vol")
	defer p.Close()
	ctx := context.Background()
	if err := p.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	// Connection is now idle in the pool.
	start := time.Now()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("idle conn drain too slow: %v", d)
	}
	// After the server is gone, the pool's next op on the stale conn
	// must fail (transport error), not hang.
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := p.Ping(ctx)
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ping on shut-down server must fail")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestShutdownDeadlineForceCloses: a backend request that never finishes
// must not block Shutdown past the context deadline.
func TestShutdownDeadlineForceCloses(t *testing.T) {
	block := &slowBackend{delay: time.Hour, entered: make(chan struct{}), memBackend: newMemBackend(1 << 20)}
	s := &Server{Backend: block}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.ServeListener(ln) }()

	cc, err := Dial(ln.Addr().String(), "vol", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	go func() { _ = cc.Write(context.Background(), 1, 0, []byte("stuck"), false) }()
	<-block.entered

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err = s.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("force-close too slow: %v", d)
	}
}

// slowBackend blocks in WriteAt for delay, signalling entry.
type slowBackend struct {
	delay   time.Duration
	entered chan struct{}
	*memBackend
}

func (b *slowBackend) WriteAt(ctx context.Context, oid, off uint64, p []byte) error {
	select {
	case <-b.entered:
	default:
		close(b.entered)
	}
	select {
	case <-time.After(b.delay):
	case <-ctx.Done():
	}
	return nil
}
