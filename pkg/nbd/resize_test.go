package nbd

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/store"
)

// exportServer starts an NBD server on the given exports for a test.
type exportServer struct{ addr string }

func newTestServer(t *testing.T, es []*Export) *exportServer {
	t.Helper()
	srv := NewServer(es, ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) {
			// server closed
		}
	}()
	t.Cleanup(func() { srv.Close() })
	return &exportServer{addr: ln.Addr().String()}
}

// An export grown under a LIVE transmission must be served without a
// reconnect: the request falling outside the old cached size refreshes
// the cache from the backend (checkRange) and succeeds on the first try.
func TestOnlineGrowServedWithoutReconnect(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "vol0.img")
	b, err := NewFileBackend(file, 1<<20)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	srv := newTestServer(t, []*Export{{Name: "vol0", Backend: b}})

	cl := newClient(t, srv.addr)
	cl.goExport(t, "vol0") // negotiates structured replies

	const oldSize = 1 << 20
	// Straddling the old EOF must fail.
	if e := cl.write(t, oldSize-4096, make([]byte, 8192)); e == ErrOK {
		t.Fatalf("write past old EOF: errno 0, want failure")
	}

	// Grow out-of-band exactly like `truncate -s +1G disk.img` /
	// `unistorctl volume resize` does.
	if err := os.Truncate(file, 2<<20); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// The very next request at the old boundary must now succeed.
	if e := cl.write(t, oldSize, []byte("grown")); e != ErrOK {
		t.Fatalf("write at old EOF after grow: errno %d, want OK", e)
	}
	data, e := cl.read(t, oldSize, 5)
	if e != ErrOK || string(data) != "grown" {
		t.Fatalf("read after grow: errno %d data=%q", e, data)
	}
	// Beyond the new size it must fail again.
	if e := cl.write(t, 2<<20, []byte("x")); e == ErrOK {
		t.Fatalf("write past new EOF: errno 0, want failure")
	}
	cl.disconnect(t)
}

// FileBackend.Size() must follow the file: a resize out-of-band (grow
// the image file) is visible on the next handshake without restarting
// the server.
func TestFileBackendResizeLiveSize(t *testing.T) {
	const size = 1 << 20
	addr, file := testServer(t, size, ServerOptions{})

	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	if cl.size != size {
		t.Fatalf("initial handshake size = %d, want %d", cl.size, size)
	}
	cl.disconnect(t)

	// Grow the image like `truncate -s +1G` would.
	st, err := os.Stat(file)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(file, st.Size()+size); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	cl2 := newClient(t, addr)
	cl2.goExport(t, "vol0")
	if cl2.size != 2*size {
		t.Fatalf("grown handshake size = %d, want %d", cl2.size, 2*size)
	}
	// The grown area must be writable through the new connection.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = 0xA5
	}
	if e := cl2.write(t, size, payload); e != ErrOK {
		t.Fatalf("write into grown area: errno %d", e)
	}
	data, e := cl2.read(t, size, 4096)
	if e != ErrOK {
		t.Fatalf("read grown area: errno %d", e)
	}
	for i := range data {
		if data[i] != 0xA5 {
			t.Fatalf("grown-area data mismatch at %d", i)
		}
	}
	cl2.disconnect(t)
}

// StoreBackend supports online resize via SetSize; new handshakes must
// report the new size and the grown object range must be usable.
func TestStoreBackendResizeLiveSize(t *testing.T) {
	const size = 9 << 20 // 9 MiB: 3 objects
	dir := filepath.Join(t.TempDir(), "objects")
	b, err := NewStoreBackend(dir, size, store.Options{})
	if err != nil {
		t.Fatalf("store backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	if b.Size() != size {
		t.Fatalf("initial size = %d", b.Size())
	}

	srv := newTestServer(t, []*Export{{Name: "vol0", Backend: b}})
	addr := srv.addr

	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	if cl.size != size {
		t.Fatalf("initial handshake size = %d, want %d", cl.size, size)
	}
	cl.disconnect(t)

	b.SetSize(20 << 20)
	if b.Size() != 20<<20 {
		t.Fatalf("SetSize: Size() = %d", b.Size())
	}
	b.SetSize(0) // rejected: size must stay > 0
	if b.Size() != 20<<20 {
		t.Fatalf("SetSize(0) must be ignored, got %d", b.Size())
	}

	cl2 := newClient(t, addr)
	cl2.goExport(t, "vol0")
	if cl2.size != 20<<20 {
		t.Fatalf("grown handshake size = %d, want %d", cl2.size, 20<<20)
	}
	// Write across the old end into the new object range (objects 2..4).
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i)
	}
	if e := cl2.write(t, 18<<20, payload); e != ErrOK {
		t.Fatalf("write at 18MiB: errno %d", e)
	}
	data, e := cl2.read(t, 18<<20, 4096)
	if e != ErrOK {
		t.Fatalf("read at 18MiB: errno %d", e)
	}
	for i := range data {
		if data[i] != byte(i) {
			t.Fatalf("grown data mismatch at %d", i)
		}
	}
	cl2.disconnect(t)
}
