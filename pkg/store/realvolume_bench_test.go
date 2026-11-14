package store

// Realistic-volume check of the prealloc store (run on the NVMe test VM):
// the full store stack (journal applier + 4 MiB block objects) driven over a
// 1 GiB working set at 256 KiB granularity. A single object is 4 MiB
// (object.Size), so the working set spans 256 objects rather than one
// (oversized) object; per-iteration work is one 256 KiB block write +
// fsync + read-back, which is the journal/apply shape the store is built
// for. This answers the "measure at realistic volume" requirement the
// 4 KiB micro-benches cannot cover. Reported: throughput (MB/s), elapsed,
// GC cycles, heap allocated, RSS, and page-ins over the run.
import (
	"os"
	"path/filepath"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

func benchGB() int {
	gb := 1
	if v := os.Getenv("SDS_BENCH_GB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			gb = n
		}
	}
	return gb
}

// memSample reads the process RSS (KiB) and minor page-in count from /proc.
// Returns 0s on non-Linux so the bench still runs (paging metrics read 0).
func memSample() (rssKiB, pageIn int64) {
	if b, err := os.ReadFile("/proc/self/statm"); err == nil {
		f := strings.Fields(string(b))
		if len(f) > 5 {
			pageIn, _ = strconv.ParseInt(f[5], 10, 64)
		}
	}
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				f := strings.Fields(line)
				if len(f) >= 2 {
					rssKiB, _ = strconv.ParseInt(f[1], 10, 64)
				}
				break
			}
		}
	}
	return
}

func gcCycleCount() uint64 {
	s := metrics.Sample{Name: "/gc/cycles/total:gc-cycles"}
	ms := []metrics.Sample{s}
	metrics.Read(ms)
	return ms[0].Value.Uint64()
}

func heapAllocTotal() uint64 {
	s := metrics.Sample{Name: "/gc/heap/allocs:bytes"}
	ms := []metrics.Sample{s}
	metrics.Read(ms)
	return ms[0].Value.Uint64()
}

func BenchmarkRealVolume(b *testing.B) {
	totalBytes := int64(benchGB()) << 30 // e.g. 1 GiB
	const blk = 256 << 10                // 256 KiB: a realistic block-transfer size
	numObjects := int(totalBytes) / int(object.Size)
	if numObjects < 1 {
		numObjects = 1
	}
	// blocks per 4 MiB object at 256 KiB granularity.
	blkPerObj := int(object.Size) / blk

	dir := filepath.Join(b.TempDir(), "s")
	s, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		b.Fatalf("new: %v", err)
	}
	defer s.Close()

	wbuf := make([]byte, blk)
	var sum uint64
	for i := range wbuf {
		wbuf[i] = byte(i % 251)
		sum += uint64(wbuf[i])
	}

	// Prefault the working set: touch every object once so the
	// steady-state loop measures I/O, not first-touch faults.
	for o := 0; o < numObjects; o++ {
		if _, err := s.WriteAt(object.OID(uint64(o+1)), wbuf, 0); err != nil {
			b.Fatalf("prefault obj %d: %v", o, err)
		}
	}

	gc0 := gcCycleCount()
	alloc0 := heapAllocTotal()
	rss0, pin0 := memSample()
	start := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	rbuf := make([]byte, blk)
	for i := 0; i < b.N; i++ {
		oid := object.OID(uint64(i%numObjects + 1))
		off := int64(i%blkPerObj) * blk
		if _, err := s.WriteAt(oid, wbuf, off); err != nil {
			b.Fatalf("write: %v", err)
		}
		if err := s.Sync(); err != nil {
			b.Fatalf("sync: %v", err)
		}
		if _, err := s.ReadAt(oid, rbuf, off); err != nil {
			b.Fatalf("read: %v", err)
		}
		var v uint64
		for _, x := range rbuf {
			v += uint64(x)
		}
		if v != sum {
			b.Fatalf("data mismatch: got %d want %d", v, sum)
		}
	}
	b.StopTimer()

	elapsed := time.Since(start)
	gc1 := gcCycleCount()
	alloc1 := heapAllocTotal()
	rss1, pin1 := memSample()

	moved := int64(b.N) * blk * 2 // write + read
	mbps := float64(moved) / (elapsed.Seconds() * 1e6)

	b.ReportMetric(mbps, "MB/s")
	b.ReportMetric(elapsed.Seconds(), "elapsed-seconds")
	b.ReportMetric(float64(gc1-gc0), "gc-cycles")
	b.ReportMetric(float64(alloc1-alloc0)/1e6, "heap-alloc-MB")
	b.ReportMetric(float64(rss1-rss0)/1024, "rss-delta-MB")
	b.ReportMetric(float64(pin1-pin0), "page-in")
}
