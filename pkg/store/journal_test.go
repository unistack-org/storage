package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// newJournalStore opens a store in journal mode.
func newJournalStore(t *testing.T) *Store {
	t.Helper()
	st, err := New(t.TempDir(), Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestJournalReadYourWrites(t *testing.T) {
	st := newJournalStore(t)
	defer st.Close()
	const oid = object.OID(0xF001)
	// Write without waiting for the applier: the read overlay must see
	// the record immediately (read-your-writes on a fresh object, which
	// does not exist as a file yet).
	payload := bytes.Repeat([]byte("ov"), 500)
	if _, err := st.WriteAt(oid, payload, 100); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len(payload))
	n, err := st.ReadAt(oid, buf, 100)
	if err != nil || n != len(payload) {
		t.Fatalf("read: %d, %v", n, err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("overlay read mismatch")
	}
	// Missing object with no staged records = store contract:
	// ErrNoSuchObject (same as the other commit modes).
	zbuf := make([]byte, 64)
	if _, err := st.ReadAt(object.OID(0x999), zbuf, 0); !errors.Is(err, ErrNoSuchObject) {
		t.Fatalf("missing object read: %v, want ErrNoSuchObject", err)
	}
}

// After Sync (drain) the log must be empty and the objects complete on
// disk by themselves: a reopen sees a truncated journal.
func TestJournalDrainTruncatesLog(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	const oid = object.OID(0xA11)
	payload := bytes.Repeat([]byte("drain"), 300)
	if _, err := st.WriteAt(oid, payload, 0); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteZeroes(oid, 100, 50, true); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if pending, _, truncs := st.journalStats(); pending != 0 || truncs < 1 {
		t.Fatalf("after Sync: pending=%d truncs=%d, want 0/>=1", pending, truncs)
	}
	// Object file on disk holds the flattened data.
	f, err := os.Open(filepath.Join(dir, "0000000000000a11"))
	if err != nil {
		t.Fatal(err)
	}
	disk := make([]byte, len(payload))
	if _, err := f.ReadAt(disk, 0); err != nil {
		t.Fatal(err)
	}
	f.Close()
	want := append([]byte(nil), payload...)
	copy(want[100:150], make([]byte, 50))
	if !bytes.Equal(disk, want) {
		t.Fatal("flattened object mismatch")
	}
	// Journal is truncated: tiny file.
	fi, err := os.Stat(filepath.Join(dir, journalFileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > 1024 {
		t.Fatalf("journal not truncated: %d bytes", fi.Size())
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: everything survives, no replay needed.
	st2, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	buf := make([]byte, len(payload))
	if _, err := st2.ReadAt(oid, buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, want) {
		t.Fatal("reopen after drain mismatch")
	}
}

// Acked writes must survive an unclean shutdown (simulated power loss:
// no Close, the objects lag the log) — replay on reopen rebuilds them.
func TestJournalCrashSurvival(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	const oid = object.OID(0xC05)
	type kv struct {
		off int
		p   []byte
	}
	recs := []kv{
		{0, bytes.Repeat([]byte("A"), 1000)},
		{500, bytes.Repeat([]byte("B"), 700)},
		{4096, []byte("tail")},
	}
	for _, r := range recs {
		if _, err := st.WriteAt(oid, r.p, int64(r.off)); err != nil {
			t.Fatal(err)
		}
	}
	// Overlapping semantics: B overwrote A in [500,1200). The object
	// is sparse: [1000,500) + [1200,4096) stay zeroes.
	want := make([]byte, 4100)
	copy(want[0:], recs[0].p)
	copy(want[500:], recs[1].p)
	copy(want[4096:], recs[2].p)

	// Simulate the crash: abandon the handle without Close, leaving the
	// objects behind the log.
	_ = st

	st2, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	buf := make([]byte, 5000)
	n, err := st2.ReadAt(oid, buf, 0)
	if err != nil || n != 5000 {
		t.Fatalf("read: %d, %v", n, err)
	}
	if !bytes.Equal(buf[:4100], want[:4100]) {
		t.Fatal("acked writes lost across crash")
	}
	// Replay put the records into the overlay: after the applier drains
	// (Sync), the object file alone is complete.
	if err := st2.Sync(); err != nil {
		t.Fatal(err)
	}
	buf2 := make([]byte, 4100) // exact object extent: sparse holes are zeroes
	n2, err := st2.ReadAt(oid, buf2, 0)
	if err != nil || n2 != 4100 {
		t.Fatalf("post-drain read: %d, %v", n2, err)
	}
	if !bytes.Equal(buf2, want) {
		t.Fatal("post-replay read mismatch")
	}
}

// A torn tail (crash mid-append) must be tolerated: the durable prefix
// replays, the garbage tail is dropped.
func TestJournalTornTail(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	const oid = object.OID(0xBEEF)
	payload := bytes.Repeat([]byte("keep"), 400)
	if _, err := st.WriteAt(oid, payload, 0); err != nil {
		t.Fatal(err)
	}
	// Abandon without Close so the log keeps the record.
	_ = st

	// Append a torn header (a partial 16-byte record header) to the log.
	jp := filepath.Join(dir, journalFileName)
	f, err := os.OpenFile(jp, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st2, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatalf("torn tail must be tolerated: %v", err)
	}
	defer st2.Close()
	buf := make([]byte, len(payload))
	if _, err := st2.ReadAt(oid, buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("durable prefix lost")
	}
	// Appends must work after the recovery.
	if _, err := st2.WriteAt(oid, []byte("more"), int64(len(payload))); err != nil {
		t.Fatal(err)
	}
}

// Remove is a checkpoint: after it, the object's history can never be
// replayed over the deletion, across a crash.
func TestJournalRemoveCheckpoint(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	const oid = object.OID(0xDE1)
	if _, err := st.WriteAt(oid, []byte("delete-me"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteAt(oid, []byte("late"), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteAt(object.OID(0xDE2), []byte("keep-me"), 0); err != nil {
		t.Fatal(err)
	}
	if err := st.Remove(oid); err != nil {
		t.Fatal(err)
	}
	_ = st // "crash" right after the checkpoint

	st2, err := New(dir, Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	if _, err := st2.Stat(oid); err == nil {
		t.Fatal("removed object resurrected after crash")
	}
	buf := make([]byte, 7)
	if _, err := st2.ReadAt(object.OID(0xDE2), buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "keep-me" {
		t.Fatalf("other object corrupted: %q", buf)
	}
}

// A write racing a Remove checkpoint must never slip between the drain
// and the delete: the store must stay consistent and usable.
func TestJournalRemoveRace(t *testing.T) {
	st := newJournalStore(t)
	defer st.Close()
	const oid = object.OID(0x5A5A)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if _, err := st.WriteAt(oid, []byte{byte(i)}, int64(i)); err != nil {
				t.Errorf("write: %v", err)
			}
		}(i)
		go func() {
			defer wg.Done()
			if err := st.Remove(oid); err != nil {
				t.Errorf("remove: %v", err)
			}
		}()
	}
	wg.Wait()
	// No invariant on the final state (interleaving is racy by design);
	// the store must stay consistent and usable.
	if _, err := st.WriteAt(oid, []byte("final"), 0); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 5)
	if _, err := st.ReadAt(oid, buf, 0); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "final" {
		t.Fatalf("post-race read = %q", buf)
	}
}

// Background applier: writes settle into the objects without an
// explicit Sync, and the log truncates once drained.
func TestJournalBackgroundApply(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	st := newJournalStore(t)
	defer st.Close()
	const oid = object.OID(0xB0B)
	payload := bytes.Repeat([]byte("bg"), 100)
	for i := 0; i < 50; i++ {
		if _, err := st.WriteAt(oid, payload, int64(i)*200); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if pending, _, truncs := st.journalStats(); pending == 0 && truncs >= 1 {
			break
		}
		if time.Now().After(deadline) {
			p, a, tr := st.journalStats()
			t.Fatalf("applier did not drain: pending=%d applied=%d truncs=%d", p, a, tr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	buf := make([]byte, len(payload))
	if _, err := st.ReadAt(oid, buf, 200); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("background-applied data mismatch")
	}
}

// Concurrency smoke: many writers + readers + a background applier,
// under -race.
func TestJournalConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	st := newJournalStore(t)
	defer st.Close()
	const oid = object.OID(0xC1A)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			p := bytes.Repeat([]byte{byte('a' + w)}, 64)
			for i := 0; i < 200; i++ {
				if _, err := st.WriteAt(oid, p, int64(w*1000+i*64)); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4096)
			for i := 0; i < 100; i++ {
				// A read that races the very first write may legitimately
				// see ErrNoSuchObject (the store contract for a missing
				// object); any other error is a bug.
				if _, err := st.ReadAt(oid, buf, 0); err != nil && !errors.Is(err, ErrNoSuchObject) {
					t.Errorf("read: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	// Every slot must contain data (some writer's byte).
	buf := make([]byte, 8*1000)
	if _, err := st.ReadAt(oid, buf, 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8*1000; i += 64 {
		if buf[i] < 'a' || buf[i] > 'a'+7 {
			t.Fatalf("slot %d = %q, want writer byte", i, buf[i])
		}
	}
}
