package store

import (
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// objectPath is the on-disk name of an object (mirror of Store.path).
func objectPath(dir string, oid object.OID) string {
	return path.Join(dir, fmt.Sprintf("%016x", uint64(oid)))
}

// crash simulates a power loss: the intent journal's buffered tail is
// dropped (flushed records survive), the Store must not be used after.
func crash(t *testing.T, st *Store) {
	t.Helper()
	if st.intent == nil {
		t.Fatal("no intent log to crash")
	}
	if err := st.intent.j.HardClose(); err != nil {
		t.Fatal(err)
	}
}

// TestIntentCleanClose: a flush-mode write acknowledged through the
// store leaves no open intents; a clean Close compacts the log, and the
// reopen checks nothing.
func TestIntentCleanClose(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	oid := object.OID(0xabcdef01)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i)
	}
	if _, err := st.WriteAt(oid, data, 0); err != nil {
		t.Fatal(err)
	}
	if st.IntentStats().Records == 0 {
		t.Fatal("intent records not counted")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	is := st2.IntentStats()
	if is.Checked != 0 || is.Mismatches != 0 {
		t.Fatalf("clean reopen checked %+v, want 0/0", is)
	}
	b := make([]byte, len(data))
	f, err := os.Open(objectPath(dir, oid))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadAt(b, 0); err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if b[i] != data[i] {
			t.Fatalf("object byte %d = %d, want %d", i, b[i], data[i])
		}
	}
}

// TestIntentTornWrite: an acknowledged write whose bytes are later
// clobbered on the object file (the classic torn-write stand-in) is
// reported at recovery: one open intent, one mismatch, a human-readable
// description in IntentMismatches, and the log is truncated so the
// report is one-shot.
func TestIntentTornWrite(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	oid := object.OID(0x4242)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i * 7)
	}
	if _, err := st.WriteAt(oid, data, 1024); err != nil {
		t.Fatal(err)
	}
	crash(t, st) // commit marker lost: the intent stays open

	// The device "lost" the write: rewrite the region with other bytes.
	f, err := os.OpenFile(objectPath(dir, oid), os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(make([]byte, 4096), 1024); err != nil {
		t.Fatal(err)
	}
	f.Close()

	st2, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	is := st2.IntentStats()
	if is.Checked != 1 {
		t.Fatalf("Checked = %d, want 1", is.Checked)
	}
	if is.Mismatches != 1 {
		t.Fatalf("Mismatches = %d, want 1", is.Mismatches)
	}
	msgs := st2.IntentMismatches()
	if len(msgs) != 1 {
		t.Fatalf("IntentMismatches = %v, want one message", msgs)
	}

	// One-shot: the damage was reported and the log truncated, so a
	// second reopen stays quiet.
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
	st3, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	if is := st3.IntentStats(); is.Checked != 0 || is.Mismatches != 0 {
		t.Fatalf("reopen after damage report checked %+v, want 0/0", is)
	}
}

// TestIntentCommitMarkerLost: intent durable + data durable + commit
// marker lost (crash between the data fsync and the marker fsync) must
// re-verify the region, find the CRC intact, and report nothing.
func TestIntentCommitMarkerLost(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	oid := object.OID(0x777)
	data := []byte("intent survives, marker does not")
	if _, err := st.WriteAt(oid, data, 0); err != nil {
		t.Fatal(err)
	}
	crash(t, st) // the buffered commit marker dies with the process

	st2, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	is := st2.IntentStats()
	if is.Checked < 1 {
		t.Fatalf("Checked = %d, want >= 1 (marker lost, region re-verified)", is.Checked)
	}
	if is.Mismatches != 0 {
		t.Fatalf("Mismatches = %d, want 0 (data is intact): %v", is.Mismatches, st2.IntentMismatches())
	}
}

// TestIntentCommitNoneSync: CommitNone makes intents durable with
// Sync() (the commit marker rides the same flush, so a clean Sync leaves
// a fully closed log); a crash right after Sync must reopen clean.
func TestIntentCommitNoneSync(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitNone, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	oid := object.OID(0x11)
	if _, err := st.WriteAt(oid, []byte("synced intent"), 0); err != nil {
		t.Fatal(err)
	}
	if st.IntentStats().Syncs != 0 {
		t.Fatal("CommitNone must not fsync per write")
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if st.IntentStats().Syncs == 0 {
		t.Fatal("Sync did not fsync the intent log")
	}
	crash(t, st)

	st2, err := New(dir, Options{CommitMode: CommitNone, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	is := st2.IntentStats()
	if is.Mismatches != 0 {
		t.Fatalf("Mismatches = %d, want 0: %v", is.Mismatches, st2.IntentMismatches())
	}
	if is.Checked != 0 {
		t.Fatalf("Checked = %d, want 0 (Sync flushed intent and marker together)", is.Checked)
	}
}

// TestIntentJournalModeWins: journal mode already has a write-ahead
// log, so Options.Intent must not create an intent log there.
func TestIntentJournalModeWins(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitJournal, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.WriteAt(object.OID(1), []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	if st.intent != nil {
		t.Fatal("journal mode opened an intent log")
	}
	if _, err := os.Stat(path.Join(dir, intentFileName)); !os.IsNotExist(err) {
		t.Fatalf("intent.log exists in journal mode: %v", err)
	}
}

// TestIntentRemoveFolds: an acknowledged delete drops the object's open
// intents at recovery — no false "missing file" mismatch.
func TestIntentRemoveFolds(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	oid := object.OID(0x99)
	if _, err := st.WriteAt(oid, make([]byte, 8192), 0); err != nil {
		t.Fatal(err)
	}
	if err := st.Remove(oid); err != nil {
		t.Fatal(err)
	}
	crash(t, st) // write commit marker lost; the delete marker is durable

	st2, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	is := st2.IntentStats()
	if is.Mismatches != 0 {
		t.Fatalf("Mismatches = %d, want 0 (delete folds the open intents): %v",
			is.Mismatches, st2.IntentMismatches())
	}
	if _, err := os.Stat(objectPath(dir, oid)); !os.IsNotExist(err) {
		t.Fatalf("object still on disk: %v", err)
	}
}

// TestIntentWriteZeroes: a zeroed region is announced with the zero CRC
// and verifies clean after a marker-losing crash (the punch/zero-fill
// data is durable in flush mode, so the open intent must match).
func TestIntentWriteZeroes(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	oid := object.OID(0x55)
	if _, err := st.WriteAt(oid, make([]byte, 16384), 0); err != nil { // real bytes first
		t.Fatal(err)
	}
	if _, err := st.WriteAt(oid, []byte("scratch data here!"), 4096); err != nil {
		t.Fatal(err)
	}
	if err := st.WriteZeroes(oid, 4096, 4096, true); err != nil {
		t.Fatal(err)
	}
	crash(t, st) // zero-intent survives, its commit marker does not

	st2, err := New(dir, Options{CommitMode: CommitFlush, Intent: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	is := st2.IntentStats()
	if is.Mismatches != 0 {
		t.Fatalf("Mismatches = %d, want 0 (zeroes landed): %v", is.Mismatches, st2.IntentMismatches())
	}
}

// TestZeroCRCLen pins the memoised zero-CRC against a direct checksum.
func TestZeroCRCLen(t *testing.T) {
	for _, n := range []uint64{1, 512, 4096} {
		want := crc32.Checksum(make([]byte, n), crc32cTable)
		if got := zeroCRCLen(n); got != want {
			t.Fatalf("zeroCRCLen(%d) = %08x, want %08x", n, got, want)
		}
	}
}
