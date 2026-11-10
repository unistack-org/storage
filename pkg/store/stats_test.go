package store

import (
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// TestWriteStatsCounters checks the write-amplification counters move
// the way the WA formula in stats.go assumes, in both commit modes.
func TestWriteStatsCounters(t *testing.T) {
	for _, mode := range []CommitMode{CommitNone, CommitJournal} {
		t.Run(string(mode), func(t *testing.T) {
			st, err := New(t.TempDir(), Options{CommitMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			base := st.WriteStats()
			if base.ClientBytes != 0 || base.JournalBytes != 0 || base.FlattenBytes != 0 {
				t.Fatalf("fresh store counters not zero: %+v", base)
			}

			oid := object.OID(4242)
			data := make([]byte, 64*1024)
			if _, err := st.WriteAt(oid, data, 0); err != nil {
				t.Fatal(err)
			}
			ws := st.WriteStats()
			if ws.ClientBytes != int64(len(data)) {
				t.Fatalf("ClientBytes = %d, want %d", ws.ClientBytes, len(data))
			}
			if mode == CommitJournal {
				// The write is fsynced into the log before ack; the
				// object file is written by the applier at Sync.
				if ws.JournalBytes <= int64(len(data)) {
					t.Fatalf("JournalBytes = %d, want > %d (record headers included)", ws.JournalBytes, len(data))
				}
				if ws.JournalSyncs == 0 {
					t.Fatal("JournalSyncs = 0 after an acknowledged journal write")
				}
				if err := st.Sync(); err != nil {
					t.Fatal(err)
				}
				ws = st.WriteStats()
				if ws.FlattenBytes != int64(len(data)) {
					t.Fatalf("FlattenBytes = %d, want %d", ws.FlattenBytes, len(data))
				}
				// Journal-mode WA over the full lifecycle: the data
				// hits the device twice (log + object) plus headers.
				wa := float64(ws.JournalBytes+ws.FlattenBytes) / float64(ws.ClientBytes)
				if wa < 2 || wa > 2.1 {
					t.Fatalf("journal WA = %.3f, want ~2 (log + flatten)", wa)
				}
			} else {
				if ws.JournalBytes != 0 || ws.FlattenBytes != 0 {
					t.Fatalf("CommitNone touched journal counters: %+v", ws)
				}
			}

			// Punches count as discard traffic, not as writes.
			if err := st.WriteZeroes(oid, 0, 4096, true); err != nil {
				t.Fatal(err)
			}
			if mode == CommitJournal {
				// The zero record reaches the object at apply time.
				if err := st.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			ws = st.WriteStats()
			if punchSupported && ws.PunchBytes < 4096 {
				t.Fatalf("PunchBytes = %d, want >= 4096", ws.PunchBytes)
			}
		})
	}
}

// TestMetaIndexCounters checks the meta index population API: Load
// indexes pre-existing objects, Remove drops them, and the RAM
// estimate scales with the object count.
func TestMetaIndexCounters(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 10; i++ {
		oid := object.OID(1000 + i)
		if _, err := st.WriteAt(oid, make([]byte, 4096), 0); err != nil {
			t.Fatal(err)
		}
	}
	objs, ram := st.MetaInfo()
	if objs != 10 {
		t.Fatalf("MetaInfo objects = %d, want 10", objs)
	}
	if ram < objs*24 {
		t.Fatalf("MetaInfo ramBytes = %d, want >= %d", ram, objs*24)
	}
	if err := st.Remove(object.OID(1001)); err != nil {
		t.Fatal(err)
	}
	st.Close()

	st2, err := New(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	objs, _ = st2.MetaInfo()
	if objs != 9 {
		t.Fatalf("after reload MetaInfo objects = %d, want 9", objs)
	}
}
