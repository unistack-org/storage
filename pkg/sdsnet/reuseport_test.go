package sdsnet

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"
)

// ServeListeners must accept on every listener, and ListenReusePort
// must fan out on Linux (port 0 resolves once and the rest rebind to
// the same address). Off Linux it degrades to one plain listener.
func TestReusePortServe(t *testing.T) {
	n := 4
	lns, err := ListenReusePort("127.0.0.1:0", n)
	if err != nil {
		t.Fatalf("ListenReusePort: %v", err)
	}
	want := n
	if runtime.GOOS != "linux" {
		want = 1
	}
	if len(lns) != want {
		t.Fatalf("listeners = %d, want %d", len(lns), want)
	}
	addr := lns[0].Addr().String()
	if want > 1 {
		for _, ln := range lns[1:] {
			if ln.Addr().String() != addr {
				t.Fatalf("listener rebinding mismatch: %s != %s", ln.Addr(), addr)
			}
		}
	}

	be := newMemBackend(1 << 20)
	s := &Server{Backend: be}
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.ServeListeners(lns) }()
	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() != addr {
		t.Fatalf("Addr = %q, want %q", s.Addr(), addr)
	}

	ctx := context.Background()
	payload := bytes.Repeat([]byte("abcdefgh"), 512) // 4 KiB
	for i := 0; i < 16; i++ {
		cc, err := Dial(addr, "vol0", 2*time.Second)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		if err := cc.Write(ctx, uint64(i), 0, payload, true); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		got := make([]byte, len(payload))
		n, err := cc.Read(ctx, uint64(i), 0, got)
		if err != nil || n != len(payload) || !bytes.Equal(got, payload) {
			t.Fatalf("read %d: n=%d err=%v", i, n, err)
		}
		cc.Close()
	}

	s.Close()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("ServeListeners returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ServeListeners did not return after Close")
	}
}
