package store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// BenchmarkRing measures the async-ring hot path end-to-end (WriteAt/
// ReadAt with AsyncRing > 0) and reports allocs/op and B/op of the
// zero-allocation glue (pooled ring completions).
//
// The 4 KiB transfer size matches the NBD client's block granularity —
// the real workload. Subtests isolate the arena dimension:
//
//	Arena 0/1 = no registered-buffer arena / one (fixed 4 KiB slots)
//
// The store is opened once per subtest; the measurement loop only runs
// the data path (the entry is cached in s.files after the first open, so
// open() is steady state).
func BenchmarkRing(b *testing.B) {
	for _, arena := range []bool{false, true} {
		arena := arena
		b.Run(fmt.Sprintf("Arena%d", boolInt(arena)), func(bb *testing.B) {
			dir := filepath.Join(bb.TempDir(), "s")
			opts := Options{AsyncRing: 64}
			if arena {
				opts.RingArenaSlots = 128
				opts.RingArenaSlot = 4 << 10 // 4 KiB slots: the hot-path block size
			}
			s, err := New(dir, opts)
			if err != nil {
				bb.Fatalf("new: %v", err)
			}
			defer s.Close()

			oid := object.OID(0xB0)
			const sz = 4 << 10
			wbuf := make([]byte, sz)
			rbuf := make([]byte, sz)
			for i := range wbuf {
				wbuf[i] = byte(i)
			}
			// Warm: first open registers the fd / allocates the entry.
			if _, err := s.WriteAt(oid, wbuf, 0); err != nil {
				bb.Fatalf("warm write: %v", err)
			}
			if _, err := s.ReadAt(oid, rbuf, 0); err != nil {
				bb.Fatalf("warm read: %v", err)
			}

			bb.ReportAllocs()
			bb.ResetTimer()
			for i := 0; i < bb.N; i++ {
				off := int64((i & 0xf) * sz)
				if _, err := s.WriteAt(oid, wbuf, off); err != nil {
					bb.Fatalf("write: %v", err)
				}
				if _, err := s.ReadAt(oid, rbuf, off); err != nil {
					bb.Fatalf("read: %v", err)
				}
			}
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
