package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/sdsnet"
)

// memBackend duplicates the sdsnet test backend (tests can't import
// test helpers across packages).
type memBackend struct {
	mu    sync.Mutex
	data  map[[2]uint64][]byte
	size  uint64
	sizes map[uint64]uint64 // oid -> high-water mark of writes
}

func newMemBackend(size uint64) *memBackend {
	return &memBackend{data: map[[2]uint64][]byte{}, size: size, sizes: map[uint64]uint64{}}
}

func (m *memBackend) Size() uint64 { return m.size }

func (m *memBackend) ReadAt(_ context.Context, oid, off uint64, p []byte) (int, error) {
	if off >= m.size {
		return 0, io.EOF
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for n < len(p) {
		chunk := off >> 12
		co := int(off & 4095)
		key := [2]uint64{oid, chunk}
		cell := m.data[key]
		if cell == nil {
			cell = make([]byte, 4096)
			m.data[key] = cell
		}
		take := 4096 - co
		if take > len(p)-n {
			take = len(p) - n
		}
		copy(p[n:n+take], cell[co:co+take])
		n += take
		off += uint64(take)
	}
	return n, nil
}

func (m *memBackend) WriteAt(_ context.Context, oid, off uint64, p []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if end := off + uint64(len(p)); end > m.sizes[oid] {
		m.sizes[oid] = end
	}
	n := 0
	for n < len(p) {
		chunk := off >> 12
		co := int(off & 4095)
		key := [2]uint64{oid, chunk}
		cell := m.data[key]
		if cell == nil {
			cell = make([]byte, 4096)
			m.data[key] = cell
		}
		take := 4096 - co
		if take > len(p)-n {
			take = len(p) - n
		}
		copy(cell[co:co+take], p[n:n+take])
		n += take
		off += uint64(take)
	}
	return nil
}

func (m *memBackend) WriteZeroes(ctx context.Context, oid, off, n uint64, _ bool) error {
	return m.WriteAt(ctx, oid, off, make([]byte, n))
}

func (m *memBackend) Sync(context.Context) error { return nil }

func (m *memBackend) Remove(_ context.Context, oid uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.data {
		if k[0] == oid {
			delete(m.data, k)
		}
	}
	delete(m.sizes, oid)
	return nil
}

func (m *memBackend) Stat(_ context.Context, oid uint64) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.sizes[oid]; ok {
		return n, nil
	}
	return 0, os.ErrNotExist
}

// startReplica boots an sdsnet server; returns its address. The
// listener is bound here so the address is known synchronously: the
// old pattern (go Serve(":0") + Addr() poll) raced the Serve goroutine
// against Close under -race when short-lived servers were recycled.
func startReplica(t *testing.T) (*sdsnet.Server, Target) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &sdsnet.Server{Backend: newMemBackend(1 << 20)}
	t.Cleanup(func() { _ = s.Close() })
	go func() { _ = s.ServeListener(ln) }()
	return s, Target{Addr: ln.Addr().String(), Export: "vol0"}
}

func TestQuorumWriteRF3W2(t *testing.T) {
	var targets []Target
	var servers []*sdsnet.Server
	for i := 0; i < 3; i++ {
		s, tg := startReplica(t)
		servers = append(servers, s)
		targets = append(targets, tg)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()

	r := New(targets, 2)
	defer r.Close()
	ctx := context.Background()

	payload := bytes.Repeat([]byte("replicated!"), 400) // ~4.8 KiB
	if err := r.Write(ctx, 7, 1024, payload, false); err != nil {
		t.Fatalf("write: %v", err)
	}

	// All three replicas (quorum + straggler) should have the data.
	for i, tg := range targets {
		cc, err := sdsnet.Dial(tg.Addr, tg.Export, time.Second)
		if err != nil {
			t.Fatalf("replica %d dial: %v", i, err)
		}
		got := make([]byte, len(payload))
		n, err := cc.Read(ctx, 7, 1024, got)
		cc.Close()
		if err != nil {
			t.Fatalf("replica %d read: %v", i, err)
		}
		if n != len(payload) || !bytes.Equal(got, payload) {
			t.Fatalf("replica %d data mismatch (n=%d)", i, n)
		}
	}
}

func TestQuorumSurvivesOneDown(t *testing.T) {
	var targets []Target
	var servers []*sdsnet.Server
	for i := 0; i < 2; i++ {
		s, tg := startReplica(t)
		servers = append(servers, s)
		targets = append(targets, tg)
	}
	// Third target: closed port → connection refused immediately.
	probe, tg3 := startReplica(t)
	probe.Close() // shut it down: target 3 is "down"
	targets = append(targets, tg3)

	r := New(targets, 2)
	defer r.Close()

	done := make(chan error, 1)
	go func() {
		done <- r.Write(context.Background(), 1, 0, bytes.Repeat([]byte("x"), 4096), false)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write with 1 of 3 down: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("write blocked with 1 of 3 down")
	}
}

func TestQuorumLost(t *testing.T) {
	// RF3 W2, two targets down: quorum impossible.
	var targets []Target
	for i := 0; i < 2; i++ {
		p, _ := startReplica(t)
		a := p.Addr()
		p.Close()
		targets = append(targets, Target{Addr: a, Export: "vol0"})
	}
	s, tg := startReplica(t)
	defer s.Close()
	targets = append(targets, tg)

	r := New(targets, 2)
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := r.Write(ctx, 1, 0, make([]byte, 4096), false)
	if !errors.Is(err, ErrQuorumLost) {
		t.Fatalf("expected ErrQuorumLost, got %v", err)
	}
}

func TestWriteZeroesQuorum(t *testing.T) {
	var targets []Target
	var servers []*sdsnet.Server
	for i := 0; i < 3; i++ {
		s, tg := startReplica(t)
		servers = append(servers, s)
		targets = append(targets, tg)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()
	r := New(targets, 2)
	defer r.Close()
	ctx := context.Background()

	if err := r.Write(ctx, 3, 0, bytes.Repeat([]byte("y"), 8192), false); err != nil {
		t.Fatal(err)
	}
	if err := r.WriteZeroes(ctx, 3, 0, 4096, true, false); err != nil {
		t.Fatal(err)
	}
	cc, err := sdsnet.Dial(targets[0].Addr, targets[0].Export, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	got := make([]byte, 8192)
	if _, err := cc.Read(ctx, 3, 0, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:4096], make([]byte, 4096)) {
		t.Fatal("zeroed range not zeroes")
	}
	if bytes.Equal(got[4096:], make([]byte, 4096)) {
		t.Fatal("tail was zeroed too")
	}
}

func TestSyncAll(t *testing.T) {
	var targets []Target
	var servers []*sdsnet.Server
	for i := 0; i < 3; i++ {
		s, tg := startReplica(t)
		servers = append(servers, s)
		targets = append(targets, tg)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()
	r := New(targets, 2)
	defer r.Close()
	if err := r.Sync(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
}
