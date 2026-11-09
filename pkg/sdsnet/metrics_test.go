package sdsnet

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reqSample is one recorded Request hook call.
type reqSample struct {
	op       Op
	status   Status
	bytesIn  uint64
	bytesOut uint64
}

// TestServerMetricsHooks checks the exporter seam: request samples per
// op/status with byte counts in both directions, and the accepted/closed
// connection pair (a connection rejected at handshake fires neither).
func TestServerMetricsHooks(t *testing.T) {
	var (
		mu       sync.Mutex
		samples  []reqSample
		accepted int
		closed   []uint64
	)
	s := &Server{
		Backend: newMemBackend(1 << 20),
		Metrics: &ServerMetrics{
			Request: func(op Op, status Status, bin, bout uint64, dur time.Duration) {
				if dur < 0 {
					t.Errorf("negative duration")
				}
				mu.Lock()
				samples = append(samples, reqSample{op, status, bin, bout})
				mu.Unlock()
			},
			ConnAccepted: func() {
				mu.Lock()
				accepted++
				mu.Unlock()
			},
			ConnClosed: func(ops uint64) {
				mu.Lock()
				closed = append(closed, ops)
				mu.Unlock()
			},
		},
	}
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
	defer func() {
		s.Close()
		if p := serveErr.Load(); p != nil && *p != nil && !errors.Is(*p, net.ErrClosed) {
			t.Errorf("serve: %v", *p)
		}
	}()

	// Handshake rejected (unknown export on a name-routed server is the
	// clean case, but this server is anonymous: any name maps. Use a
	// bad-magic connection instead: raw bytes, no hooks must fire).
	bad, err := net.Dial("tcp", s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	bad.Write([]byte("junk"))
	bad.Close()

	ctx := context.Background()
	cc, err := Dial(s.Addr(), "vol0", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 4096)
	if err := cc.Write(ctx, 1, 0, payload, false); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 4096)
	if _, err := cc.Read(ctx, 1, 0, got); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := cc.Stat(ctx, 42); err != nil || exists {
		t.Fatalf("stat of absent object: exists=%v err=%v", exists, err)
	}
	cc.Close()
	// Give the server goroutine a moment to run its conn-close defer.
	deadline = time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(closed)
		mu.Unlock()
		if n >= 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if accepted != 1 {
		t.Fatalf("accepted = %d, want 1 (bad handshake must not count)", accepted)
	}
	if len(closed) != 1 || closed[0] != 3 {
		t.Fatalf("closed = %v, want [3]", closed)
	}
	want := []reqSample{
		{OpWrite, StatusOK, 4096, 0},
		{OpRead, StatusOK, 0, 4096},
		{OpStat, StatusNotFound, 0, 0},
	}
	if len(samples) != len(want) {
		t.Fatalf("samples = %v, want %v", samples, want)
	}
	for i := range want {
		if samples[i] != want[i] {
			t.Fatalf("sample %d = %+v, want %+v", i, samples[i], want[i])
		}
	}
}
