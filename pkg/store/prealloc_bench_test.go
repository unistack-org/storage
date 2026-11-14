package store

import (
	"path/filepath"
	"runtime/metrics"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// BenchmarkJournal measures the journal hot path end-to-end (WriteAt in
// CommitJournal mode) and reports allocs/op and B/op of the journal glue:
// the pooled encode buffer (js.encodeBuf) eliminates the per-record payload
// allocation from encodeJournal. rec.data is intentionally still a fresh
// copy (it is retained in the overlay and read lock-free), so this bench
// isolates the encode allocation only.
//
// The 4 KiB transfer size matches the NBD client's block granularity.
func BenchmarkJournal(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "s")
	s, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		b.Fatalf("new: %v", err)
	}
	defer s.Close()

	oid := object.OID(0x70)
	const sz = 4 << 10
	const slots = int(object.Size) / sz // object is 4 MiB; wrap to stay in bounds
	wbuf := make([]byte, sz)
	for i := range wbuf {
		wbuf[i] = byte(i)
	}
	// Warm: open the object and let the applier settle.
	if _, err := s.WriteAt(oid, wbuf, 0); err != nil {
		b.Fatalf("warm write: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.WriteAt(oid, wbuf, int64(i%slots)*sz); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
}

// BenchmarkGC measures the GC behaviour of the zero-allocation path over a
// fixed workload: total GC cycles, heap-scanned bytes, and total GC pause
// time. These are the metrics the prealloc migration is meant to improve
// (less heap traffic from per-I/O glue allocations). Reported as user
// metrics, not benchmem.
// gcSample reads the GC metrics that matter for this comparison into a
// map: total GC cycles, bytes scanned, total bytes allocated, live heap,
// and the pause histogram (count + worst-case pause). Metric names and
// kinds were verified against runtime/metrics.All() for this toolchain.
func gcSample() map[string]any {
	var ms []metrics.Sample
	ms = append(ms,
		metrics.Sample{Name: "/gc/cycles/total:gc-cycles"},
		metrics.Sample{Name: "/gc/scan/heap:bytes"},
		metrics.Sample{Name: "/gc/heap/allocs:bytes"},
		metrics.Sample{Name: "/gc/heap/live:bytes"},
		metrics.Sample{Name: "/gc/pauses:seconds"},
	)
	metrics.Read(ms)
	out := map[string]any{}
	for _, s := range ms {
		switch {
		case s.Name == "/gc/pauses:seconds":
			h := s.Value.Float64Histogram()
			var count uint64
			var worst float64
			if h != nil {
				for i, c := range h.Counts {
					count += c
					if c > 0 {
						// This bucket covers [Buckets[i], Buckets[i+1]);
						// the exclusive upper bound is a safe worst-case
						// estimate for a pause that landed here.
						worst = h.Buckets[i+1]
					}
				}
			}
			out["pauses"] = count
			out["pauseMaxSec"] = worst
		default:
			out[s.Name] = s.Value.Uint64()
		}
	}
	return out
}

func BenchmarkGC(b *testing.B) {
	before := gcSample()

	dir := filepath.Join(b.TempDir(), "s")
	s, err := New(dir, Options{CommitMode: CommitJournal, AsyncRing: 16})
	if err != nil {
		b.Fatalf("new: %v", err)
	}
	oid := object.OID(0x70)
	const sz = 4 << 10
	wbuf := make([]byte, sz)
	for i := range wbuf {
		wbuf[i] = byte(i)
	}

	// Fixed workload: 2000 mixed 4 KiB writes.
	for i := 0; i < 2000; i++ {
		if _, err := s.WriteAt(oid, wbuf, int64(i&0xff)*sz); err != nil {
			b.Fatalf("write: %v", err)
		}
	}
	s.Close()

	after := gcSample()
	sub := func(k string) float64 { return float64(after[k].(uint64)) - float64(before[k].(uint64)) }
	b.ReportMetric(sub("/gc/cycles/total:gc-cycles"), "GCcycles")
	b.ReportMetric(sub("/gc/scan/heap:bytes")/1024, "GCscanHeap.KB")
	b.ReportMetric(sub("/gc/heap/allocs:bytes")/1024/1024, "GCheapAlloc.MB")
	b.ReportMetric(sub("/gc/heap/live:bytes")/1024, "GClive.KB")
	b.ReportMetric(float64(after["pauses"].(uint64)-before["pauses"].(uint64)), "GCPauses")
	b.ReportMetric(after["pauseMaxSec"].(float64)*1000, "GCPauseMax.ms")
}
