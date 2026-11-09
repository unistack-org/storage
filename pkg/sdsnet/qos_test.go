package sdsnet

import (
	"context"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/qos"
)

// startQoSServer starts a server whose per-connection limits come from
// the Limits hook (the sdsd -export / test path).
func startQoSServer(t *testing.T, be Backend, limits func(string) *qos.Pair, agg *qos.Pair) (string, func()) {
	t.Helper()
	s := &Server{Backend: be, Limits: limits, Aggregate: agg}
	go func() { _ = s.Serve("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not start")
	}
	return s.Addr(), func() { s.Close() }
}

// TestQoSAggregatePacesWrites proves the server-side aggregate limiter
// throttles ops over the wire: 20 write tokens/s, burst 2, so 4 writes
// cost at least ~2 burst-token refills (~100ms each).
func TestQoSAggregatePacesWrites(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startQoSServer(t, be, nil, qos.NewPair(0, 0, 20, 0))
	defer shutdown()

	cc, err := Dial(addr, "vol0", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := cc.Write(ctx, 1, uint64(i*512), []byte("x"), false); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	// Burst 2 passes free; the 3rd and 4th each wait ~50ms (100ms
	// quantum at 20 IOPS). Tolerant lower bound.
	if elapsed < 80*time.Millisecond {
		t.Fatalf("4 writes at 20 IOPS took %v: aggregate limiter not pacing", elapsed)
	}
}

// TestQoSLimitsPerExport proves the Limits hook gives one export a
// limiter while another export stays unlimited.
func TestQoSLimitsPerExport(t *testing.T) {
	be := newMemBackend(1 << 20)
	pair := qos.NewPair(0, 0, 20, 0)
	addr, shutdown := startQoSServer(t, be, func(export string) *qos.Pair {
		if export == "slow" {
			return pair
		}
		return nil
	}, nil)
	defer shutdown()

	dial := func(export string) *Conn {
		cc, err := Dial(addr, export, 5*time.Second)
		if err != nil {
			t.Fatalf("dial %s: %v", export, err)
		}
		t.Cleanup(func() { cc.Close() })
		return cc
	}
	ctx := context.Background()

	slow := dial("slow")
	start := time.Now()
	for i := 0; i < 4; i++ {
		if err := slow.Write(ctx, 1, uint64(i*512), []byte("x"), false); err != nil {
			t.Fatalf("slow write %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Fatalf("slow export 4 writes took %v: limiter not applied", elapsed)
	}

	fast := dial("vol0")
	start = time.Now()
	for i := 0; i < 4; i++ {
		if err := fast.Write(ctx, 1, uint64(i*512), []byte("x"), false); err != nil {
			t.Fatalf("fast write %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("unlimited export 4 writes took %v: limiter leaked across exports", elapsed)
	}
}

// TestQoSClosedPairRepliesIO proves a limiter closed while a connection
// is live yields a clean StatusIO (never a hang, never a corrupted
// stream).
func TestQoSClosedPairRepliesIO(t *testing.T) {
	be := newMemBackend(1 << 20)
	pair := qos.NewPair(0, 0, 5, 0)
	addr, shutdown := startQoSServer(t, be, func(string) *qos.Pair { return pair }, nil)
	defer shutdown()

	cc, err := Dial(addr, "vol0", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	ctx := context.Background()

	// Exhaust the bucket (5 IOPS => burst 1), so the next write parks
	// in the limiter, then close the pair under it.
	if err := cc.Write(ctx, 1, 0, []byte("x"), false); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cc.Write(ctx, 2, 0, []byte("y"), false) }()
	time.Sleep(30 * time.Millisecond) // let the op enter the bucket
	pair.Close()

	select {
	case err := <-done:
		e, ok := AsError(err)
		if !ok || e.Status != StatusIO {
			t.Fatalf("write after limiter close: got %v, want StatusIO", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("write parked forever after limiter close")
	}

	// The connection stays usable for reads (they pay the read axis,
	// which is unlimited here).
	buf := make([]byte, 512)
	if _, err := cc.Read(ctx, 1, 0, buf); err != nil {
		t.Fatalf("read after limiter close: %v", err)
	}
}
