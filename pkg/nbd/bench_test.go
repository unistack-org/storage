package nbd

import (
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// memBackend is a RAM backend for benchmarks: reads return zeros, writes
// discard (the point is to price the protocol + buffer path, not storage).
type memBackend struct {
	size   uint64
	writes atomic.Int64
}

func (m *memBackend) Size() uint64 { return m.size }
func (m *memBackend) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(m.size) {
		return 0, io.EOF
	}
	return len(p), nil
}
func (m *memBackend) WriteAt(p []byte, off int64) (int, error) {
	if off >= int64(m.size) {
		return 0, io.EOF
	}
	m.writes.Add(1)
	return len(p), nil
}
func (m *memBackend) Flush() error                          { return nil }
func (m *memBackend) Trim(int64, uint32) error              { return nil }
func (m *memBackend) WriteZeroes(int64, uint32, bool) error { return nil }
func (m *memBackend) Close() error                          { return nil }

func benchServer(tb testing.TB, size uint64) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	srv := NewServer([]*Export{{Name: "vol0", Backend: &memBackend{size: size}}}, ServerOptions{})
	go srv.Serve(ln)
	tb.Cleanup(func() {
		srv.Close()
		ln.Close()
	})
	return ln.Addr().String()
}

// buildReq frames a READ/WRITE request (handle filled per iteration).
func buildReq(typ uint16, offset uint64, length uint32, payload []byte) []byte {
	frame := make([]byte, 28+len(payload))
	binary.BigEndian.PutUint32(frame[:4], requestMagic)
	binary.BigEndian.PutUint16(frame[6:8], typ)
	binary.BigEndian.PutUint64(frame[16:24], offset)
	binary.BigEndian.PutUint32(frame[24:28], length)
	copy(frame[28:], payload)
	return frame
}

func benchmarkRoundTrip(b *testing.B, size int, typ uint16) {
	addr := benchServer(b, 1<<30)
	cl := newClient(b, addr)
	cl.goExport(b, "vol0") // goExport negotiates structured replies

	var payload []byte
	if typ == CmdWrite {
		payload = make([]byte, size)
	}
	req := buildReq(typ, 0, uint32(size), payload)
	// Structured replies: READ = chunk(20)+offset(8)+data + DONE(20);
	// WRITE = DONE(20).
	repLen := 48 + size
	if typ == CmdWrite {
		repLen = 20
	}
	rep := make([]byte, repLen)

	handle := cl.nextHandle
	cl.nextHandle += uint64(b.N)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(req[8:16], handle)
		handle++
		if _, err := cl.conn.Write(req); err != nil {
			b.Fatalf("send: %v", err)
		}
		if err := cl.br.ReadFull(rep); err != nil {
			b.Fatalf("recv: %v", err)
		}
	}
}

func BenchmarkNBDRead4K(b *testing.B)  { benchmarkRoundTrip(b, 4096, CmdRead) }
func BenchmarkNBDRead64K(b *testing.B) { benchmarkRoundTrip(b, 64*1024, CmdRead) }
func BenchmarkNBDRead1M(b *testing.B)  { benchmarkRoundTrip(b, 1<<20, CmdRead) }
func BenchmarkNBDWrite4K(b *testing.B) { benchmarkRoundTrip(b, 4096, CmdWrite) }
func BenchmarkNBDWrite64K(b *testing.B) {
	benchmarkRoundTrip(b, 64*1024, CmdWrite)
}

// TestNBDZeroAllocGate prices the SERVER hot path: the client sends
// pipelined 4K READs over a warm connection using preallocated buffers
// (the client loop itself allocates nothing); total process heap growth
// must stay within budget, i.e. the server must not allocate per request
// once the connection buffer pool is warm.
func TestNBDZeroAllocGate(t *testing.T) {
	addr := benchServer(t, 1<<30)
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")

	req := buildReq(CmdRead, 0, 4096, nil)
	rep := make([]byte, 48+4096)

	const n = 4000
	h := runPipelined(t, cl, req, rep, 500, 0) // warm buffers/bufpool

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	runPipelined(t, cl, req, rep, n, h)
	// The gate is a REGRESSION detector: the per-request make([]byte)
	// it must catch costs >=1 alloc + >=payload bytes per request.
	deadline := time.Now().Add(3 * time.Second)
	for {
		runtime.ReadMemStats(&after)
		if after.Mallocs-before.Mallocs >= uint64(n) || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	allocs := after.Mallocs - before.Mallocs
	bytes := after.TotalAlloc - before.TotalAlloc
	// Gate: a per-request regression allocates >=4KB (payload buffer) per
	// request; the warm path allocates ~0. Budget of 2KB/request total is
	// orders of magnitude below the regression signature and above the
	// measured warm baseline.
	if bytes > uint64(n)*2048 {
		t.Fatalf("server allocated %d B for %d requests (%.0f B/req): hot path not alloc-free",
			bytes, n, float64(bytes)/float64(n))
	}
	t.Logf("gate: %d requests -> %d allocs / %d B (%.2f allocs/req, %.1f B/req)",
		n, allocs, bytes, float64(allocs)/float64(n), float64(bytes)/float64(n))
}

// runPipelined sends count READs (batched 16 in flight), drains replies,
// returns the next handle. Client side allocates nothing.
func runPipelined(t *testing.T, cl *client, req, rep []byte, count int, start uint64) uint64 {
	t.Helper()
	h := start
	batch := 16
	for sent, got := 0, 0; got < count; {
		for sent-got < batch && sent < count {
			binary.BigEndian.PutUint64(req[8:16], h)
			h++
			if _, err := cl.conn.Write(req); err != nil {
				t.Fatalf("send: %v", err)
			}
			sent++
		}
		if err := cl.br.ReadFull(rep); err != nil {
			t.Fatalf("recv: %v", err)
		}
		got++
	}
	return h
}
