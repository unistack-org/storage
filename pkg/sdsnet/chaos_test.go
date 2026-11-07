package sdsnet

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// TestPoolReconnectsAfterServerDeath: a server is killed while the pool
// holds idle connections. The next op must fail over the dead conn,
// discard it, dial a fresh one to a restarted server, and succeed.
func TestPoolReconnectsAfterServerDeath(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	pp := NewPool(addr, "vol0")
	defer pp.Close()

	ctx := context.Background()
	payload := []byte("first")
	if err := pp.Write(ctx, 1, 0, payload, false); err != nil {
		t.Fatal(err)
	}

	// Kill the server: the pooled conn goes dead at the peer.
	shutdown()

	// A write over the dead conn must surface a transport error, not
	// hang, and the pool must not keep the dead conn.
	deadCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pp.Write(deadCtx, 1, 0, payload, false); err == nil {
		t.Fatal("expected error writing to dead server")
	}
	if idle, _ := pp.Stats(); idle != 0 {
		t.Fatalf("dead conn returned to pool: idle=%d", idle)
	}

	// Restart a server on the same port with the same backend: writes
	// must recover through a freshly dialed connection.
	restart := &Server{Backend: be}
	go func() { _ = restart.Serve(addr) }()
	deadline := time.Now().Add(2 * time.Second)
	for restart.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if restart.Addr() == "" {
		t.Fatal("server did not restart")
	}

	if err := pp.Write(deadCtx, 2, 0, []byte("second"), false); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	got := make([]byte, 6)
	n, err := pp.Read(deadCtx, 2, 0, got)
	if err != nil || n != 6 || string(got) != "second" {
		t.Fatalf("read after restart: n=%d got=%q err=%v", n, got, err)
	}
	// Data written before the crash survived (same backend).
	head := make([]byte, 5)
	n, err = pp.Read(deadCtx, 1, 0, head)
	if err != nil || n != 5 || string(head) != "first" {
		t.Fatalf("pre-crash data lost: n=%d err=%v", n, err)
	}
	restart.Close()
}

// TestConcurrentPoolUsers: many goroutines share one pool; every write
// must land, no seq desync, connections are reused not leaked.
func TestConcurrentPoolUsers(t *testing.T) {
	be := newMemBackend(1 << 20)
	addr, shutdown := startServer(t, be)
	defer shutdown()
	pp := NewPool(addr, "vol0")
	pp.Limit = 4
	defer pp.Close()

	const workers, writes = 16, 32
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ctx := context.Background()
			buf := []byte{byte(w)}
			for i := 0; i < writes; i++ {
				if err := pp.Write(ctx, uint64(w), uint64(i)*512, buf, false); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent write: %v", err)
	}
	// Verify all writes landed.
	for w := 0; w < workers; w++ {
		got := make([]byte, 1)
		for i := 0; i < writes; i++ {
			n, err := pp.Read(context.Background(), uint64(w), uint64(i)*512, got)
			if err != nil || n != 1 || got[0] != byte(w) {
				t.Fatalf("worker %d write %d: n=%d got=%d err=%v", w, i, n, got[0], err)
			}
		}
	}
	if idle, inUse := pp.Stats(); inUse != 0 || idle > 4 {
		t.Fatalf("pool leaked: idle=%d inUse=%d", idle, inUse)
	}
}

// TestPoolBlocksAtLimit: with Limit=1 and a conn checked out, the next
// Get blocks; it returns as soon as the conn is put back.
func TestPoolBlocksAtLimit(t *testing.T) {
	addr, shutdown := startServer(t, newMemBackend(1<<20))
	defer shutdown()
	pp := NewPool(addr, "vol0")
	pp.Limit = 1
	defer pp.Close()

	cc, err := pp.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *Conn, 1)
	go func() {
		c2, err := pp.Get(context.Background())
		if err != nil {
			t.Errorf("blocked Get: %v", err)
		}
		got <- c2
	}()
	select {
	case <-got:
		t.Fatal("Get returned while pool at limit")
	case <-time.After(50 * time.Millisecond):
	}
	pp.Put(cc)
	select {
	case c2 := <-got:
		if c2 != cc {
			t.Fatal("expected the released conn")
		}
		pp.Put(c2)
	case <-time.After(2 * time.Second):
		t.Fatal("Get did not wake after Put")
	}
	if err := pp.Ping(context.Background()); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

// TestPoolCloseWakesWaiters: Close must unblock a parked Get with ErrClosed.
func TestPoolCloseWakesWaiters(t *testing.T) {
	addr, shutdown := startServer(t, newMemBackend(1<<20))
	defer shutdown()
	pp := NewPool(addr, "vol0")
	pp.Limit = 1
	cc, err := pp.Get(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := pp.Get(context.Background())
		errCh <- err
	}()
	time.Sleep(20 * time.Millisecond)
	pp.Close()
	select {
	case err := <-errCh:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("expected ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake blocked Get")
	}
	pp.Put(cc) // must close, not pool
}

// TestDialTimeoutUnreachable: connecting to a listen socket whose
// accept queue is full (SYNs are answered, nothing is accepted) hangs
// the handshake; Dial must cut it off at the given timeout.
func TestDialTimeoutUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	// Fill the accept queue so new connections are never completed.
	var probe []net.Conn
	for i := 0; i < 128; i++ {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			break
		}
		probe = append(probe, c)
	}
	defer func() {
		for _, c := range probe {
			c.Close()
		}
	}()

	start := time.Now()
	_, err = Dial(ln.Addr().String(), "vol0", 300*time.Millisecond)
	el := time.Since(start)
	if err == nil {
		t.Skip("accept queue not saturated on this platform")
	}
	if el > 3*time.Second {
		t.Fatalf("dial timeout not honoured: %v", el)
	}
	t.Logf("dial err after %v: %v", el, err)
}
