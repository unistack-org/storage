package sdsnet

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// startServerExports boots a Server that routes names to backends.
// Like startServer: no t.* logging from server goroutines (they can
// outlive the test); the Serve error is reported at shutdown.
func startServerExports(t *testing.T, exports map[string]Backend) (string, *Server, func()) {
	t.Helper()
	s := &Server{Exports: exports}
	var serveErr atomic.Pointer[error]
	go func() {
		if err := s.Serve("127.0.0.1:0"); err != nil {
			e := err
			serveErr.Store(&e)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not start")
	}
	return s.Addr(), s, func() {
		s.Close()
		// Close() makes Serve return net.ErrClosed: expected shutdown.
		if p := serveErr.Load(); p != nil && *p != nil && !errors.Is(*p, net.ErrClosed) {
			t.Errorf("serve: %v", *p)
		}
	}
}

func TestUnknownExportRejected(t *testing.T) {
	addr, _, shutdown := startServerExports(t, map[string]Backend{"vol-a": newMemBackend(1 << 20)})
	defer shutdown()

	var re *Error
	_, err := Dial(addr, "vol-b", time.Second)
	if err == nil {
		t.Fatal("dial of unknown export should fail")
	}
	if !errors.As(err, &re) || re.Status != StatusUnknown {
		t.Fatalf("want StatusUnknown, got %v", err)
	}
	// Known export still works.
	cc, err := Dial(addr, "vol-a", time.Second)
	if err != nil {
		t.Fatalf("dial known export: %v", err)
	}
	cc.Close()
}

func TestEpochFencing(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, srv, shutdown := startServerExports(t, map[string]Backend{"v": be})
	defer shutdown()

	ctx := context.Background()
	cc, err := Dial(addr, "v", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	payload := []byte("hello")
	// No fencing configured: writes pass at any epoch.
	if err := cc.Write(ctx, 1, 0, payload, false); err != nil {
		t.Fatal(err)
	}

	srv.SetMinEpoch(7)
	cc.SetEpoch(5)
	err = cc.Write(ctx, 1, 0, payload, false)
	var re *Error
	if !errors.As(err, &re) {
		t.Fatalf("want *Error, got %v", err)
	}
	if re.Status != StatusStale {
		t.Fatalf("want StatusStale, got %v", re.Status)
	}
	if re.StaleEpoch() != 7 {
		t.Fatalf("stale reply should carry demanded epoch 7, got %d", re.StaleEpoch())
	}

	// Reads are fenced too: after a placement change an old target may
	// no longer be authoritative, and the StatusStale reply is what
	// drives the client to refresh its map.
	if _, err := cc.Read(ctx, 1, 0, make([]byte, 5)); err == nil {
		t.Fatal("read at stale epoch should be refused")
	} else if re := (*Error)(nil); !errors.As(err, &re) || re.Status != StatusStale {
		t.Fatalf("read at stale epoch: want StatusStale, got %v", err)
	}
	// Ping too.
	if err := cc.Ping(ctx); err != nil {
		t.Fatalf("ping at stale epoch should pass: %v", err)
	}

	// Advancing the epoch unblocks writes.
	cc.SetEpoch(7)
	if err := cc.Write(ctx, 1, 0, payload, false); err != nil {
		t.Fatalf("write at epoch 7: %v", err)
	}

	// Raising the floor mid-stream re-blocks it (fencing a client that
	// has not refreshed its map yet).
	srv.SetMinEpoch(9)
	if err := cc.Write(ctx, 1, 0, payload, false); err == nil {
		t.Fatal("write at epoch 7 < floor 9 should fail")
	}
}

func TestPoolEpochPropagation(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, srv, shutdown := startServerExports(t, map[string]Backend{"v": be})
	defer shutdown()

	ctx := context.Background()
	pp := NewPool(addr, "v")
	defer pp.Close()

	payload := []byte("zz")
	srv.SetMinEpoch(3)

	// Pool epoch is 0: connections dial at epoch 0, fenced.
	if err := pp.Write(ctx, 1, 0, payload, false); err == nil {
		t.Fatal("pool write at epoch 0 should be fenced")
	}

	pp.SetEpoch(3)
	// Existing (returned) connections are re-stamped at Get.
	if err := pp.Write(ctx, 1, 0, payload, false); err != nil {
		t.Fatalf("pool write after SetEpoch: %v", err)
	}
	// Fresh dials inherit the epoch.
	pp.mu.Lock()
	for _, cc := range pp.idle {
		cc.Close()
	}
	pp.idle = nil
	pp.opened = 0
	pp.mu.Unlock()
	if err := pp.Write(ctx, 1, 0, payload, false); err != nil {
		t.Fatalf("pool write on fresh dial: %v", err)
	}
}
