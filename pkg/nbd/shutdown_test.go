package nbd

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// shutdownServer starts a server and returns it (testServer hides the
// *Server, which Shutdown tests need).
func shutdownServer(t *testing.T, size uint64) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := NewFileBackend(filepath.Join(dir, "vol0.img"), size)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	srv := NewServer([]*Export{{Name: "vol0", Backend: b}}, ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close() })
	return srv, ln.Addr().String()
}

// TestServerShutdownGraceful: after Shutdown starts, an existing
// transmission keeps working (the kernel/QEMU guest must not be cut
// mid-IO); once the client disconnects, Shutdown returns nil.
func TestServerShutdownGraceful(t *testing.T) {
	srv, addr := shutdownServer(t, 1<<20)
	cl := newClient(t, addr) // newClient already performs the handshake
	cl.requestStructured(t)
	cl.goExport(t, "vol0")
	if e := cl.write(t, 0, []byte("data")); e != ErrOK {
		t.Fatalf("write: %d", e)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shut := make(chan error, 1)
	go func() { shut <- srv.Shutdown(ctx) }()

	// The connection must stay open during the drain, and the server
	// must answer with ESHUTDOWN (the protocol's "go away" signal),
	// which is how kernel nbd/QEMU learn to stop sending IO.
	time.Sleep(50 * time.Millisecond)
	cl.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, e := cl.read(t, 0, 4)
	if e != ErrESHUTDOWN {
		t.Fatalf("read during drain = %d, want ESHUTDOWN", e)
	}
	select {
	case err := <-shut:
		t.Fatalf("shutdown returned while client is transmitting: %v", err)
	default:
	}

	cl.disconnect(t)
	cl.conn.Close()
	select {
	case err := <-shut:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not return after client disconnect")
	}
}

// TestServerShutdownDeadline: a client that never leaves is force-closed
// at the deadline and Shutdown reports the context error.
func TestServerShutdownDeadline(t *testing.T) {
	srv, addr := shutdownServer(t, 1<<20)
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	// Keep the transmission loop parked in a request read so the
	// handler goroutine is alive when the deadline hits (if no request
	// is pending, the loop is in the 28-byte header read — force-close
	// must kill it too).

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := srv.Shutdown(ctx)
	if err == nil {
		t.Fatal("want context deadline error")
	}
	t.Logf("shutdown err: %v", err)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("force-close too slow: %v", d)
	}
	// The connection was force-closed: the socket must be dead.
	cl.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var b [4]byte
	if _, rerr := cl.conn.Read(b[:]); rerr == nil {
		t.Fatal("read on force-closed conn must fail")
	}
}
