package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// TestStatInfoTracksAllocation guards the vacuum allocation hint: an
// object that holds real (non-zero) data must report allocation > 0
// through StatInfo in every commit mode. A stale hint makes client-side
// vacuum skip written objects (checked:0) and punch holes out from
// under live data.
func TestStatInfoTracksAllocation(t *testing.T) {
	modes := []struct {
		name string
		mode CommitMode
	}{
		{"none", CommitNone},
		{"flush", CommitFlush},
		{"journal", CommitJournal},
	}
	for _, m := range modes {
		for _, n := range []int{int(object.Size) / 2, int(object.Size)} {
			dir := t.TempDir()
			st, err := New(dir, Options{CommitMode: m.mode})
			if err != nil {
				t.Fatal(err)
			}
			oid := object.OID(n)
			data := make([]byte, n)
			for i := range data {
				data[i] = byte(i%251) + 1 // never all-zero: some filesystems do not count zero writes
			}
			if _, err = st.WriteAt(oid, data, 0); err != nil {
				t.Fatal(err)
			}
			if m.mode == CommitJournal {
				// Records reach the data file when the journal is applied.
				if err = st.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			size, alloc, err := st.StatInfo(oid)
			if err != nil {
				t.Fatal(err)
			}
			if size < uint64(n) {
				t.Fatalf("%s n=%d: size=%d want >= %d", m.name, n, size, n)
			}
			if alloc == 0 {
				var sizeOnDisk int64
				if fi, serr := os.Stat(filepath.Join(dir, fmt.Sprintf("%016x", uint64(oid)))); serr == nil {
					sizeOnDisk = fi.Size()
				}
				t.Fatalf("%s n=%d: alloc=0 for object with %d bytes of data (file size %d)",
					m.name, n, n, sizeOnDisk)
			}
			st.Close()
		}
	}
}

// TestJournalStats covers the ENOSPC journal metric: journal mode
// reports the WAL size and pending-record count (and the pair grows
// with acknowledged writes), non-journal modes report ok=false.
func TestJournalStats(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := st.JournalStats(); ok {
		t.Fatal("CommitNone must report ok=false")
	}
	st.Close()

	dir = t.TempDir()
	st, err = New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	lb, pend, ok := st.JournalStats()
	if !ok {
		t.Fatal("journal mode must report ok=true")
	}
	if lb < 0 || pend != 0 {
		t.Fatalf("empty journal: log=%d pending=%d", lb, pend)
	}
	oid := object.OID(42)
	if _, err = st.WriteAt(oid, make([]byte, 64<<10), 0); err != nil {
		t.Fatal(err)
	}
	lb2, pend2, ok := st.JournalStats()
	if !ok {
		t.Fatal("journal mode must report ok=true")
	}
	if lb2 <= lb {
		t.Fatalf("log did not grow: %d -> %d", lb, lb2)
	}
	if pend2 != 1 {
		t.Fatalf("pending=%d want 1 (ack before apply)", pend2)
	}
}
