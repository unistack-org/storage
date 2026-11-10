package nbd

import (
	"errors"
	"net"
	"testing"
	"time"
)

// TestExportRuntimeLifecycle covers the admin-facing export surface:
// AddExport/RemoveExport/ListExports and the in-use refusal that keeps
// a control plane from yanking a volume out from under a running VM.
func TestExportRuntimeLifecycle(t *testing.T) {
	dir := t.TempDir()
	b0, err := NewFileBackend(dir+"/vol0.img", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer b0.Close()
	b1, err := NewFileBackend(dir+"/vol1.img", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer b1.Close()

	srv := NewServer([]*Export{{Name: "vol0", Backend: b0}}, ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	defer srv.Close()
	addr := ln.Addr().String()

	// A second export added at runtime is visible to a fresh client and
	// to ListExports.
	srv.AddExport(&Export{Name: "vol1", Backend: b1, ReadOnly: true})
	if got := len(srv.ListExports()); got != 2 {
		t.Fatalf("ListExports: %d exports, want 2", got)
	}
	var info *ExportInfo
	for _, e := range srv.ListExports() {
		if e.Name == "vol1" {
			copied := e
			info = &copied
		}
	}
	if info == nil || !info.ReadOnly || info.Size != 1<<20 {
		t.Fatalf("vol1 info = %+v", info)
	}
	cl := newClient(t, addr)
	cl.goExport(t, "vol1") // negotiation puts us in transmission and takes the ref

	// A live transmission holds a reference: RemoveExport refuses with
	// ErrExportBusy while the client is in transmission. The read
	// confirms the export is live; the conns counter is updated at
	// enterTransmission, before the reply, so a completed read implies
	// the reference is held.
	cl.read(t, 0, 512)
	if err := srv.RemoveExport("vol1"); !errors.Is(err, ErrExportBusy) {
		t.Fatalf("RemoveExport while in use: %v, want ErrExportBusy", err)
	}
	// vol0 (no connections) removes cleanly; the backend stays open for
	// its owner.
	if err := srv.RemoveExport("vol0"); err != nil {
		t.Fatalf("RemoveExport(vol0): %v", err)
	}
	if err := srv.RemoveExport("nope"); err == nil {
		t.Fatal("RemoveExport(nope): want not-found error")
	}

	// Once the client is gone the reference is released and the export
	// becomes removable.
	cl.disconnect(t)
	cl.conn.Close()
	var lastErr error
	for i := 0; i < 500; i++ {
		if lastErr = srv.RemoveExport("vol1"); lastErr == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("RemoveExport(vol1) after disconnect: %v", lastErr)
	}
	if got := len(srv.ListExports()); got != 0 {
		t.Fatalf("ListExports after removals: %d, want 0", got)
	}
}

// TestExportIdleHandshakeNoRef proves the reference is taken when the
// client reaches transmission, not at connect: a client that only
// negotiates options never blocks a removal.
func TestExportIdleHandshakeNoRef(t *testing.T) {
	dir := t.TempDir()
	b, err := NewFileBackend(dir+"/v.img", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	srv := NewServer([]*Export{{Name: "v", Backend: b}}, ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	defer srv.Close()
	cl := newClient(t, ln.Addr().String()) // handshake only, no GO
	_ = cl
	if err := srv.RemoveExport("v"); err != nil {
		t.Fatalf("RemoveExport with an option-phase client: %v", err)
	}
}
