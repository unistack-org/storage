package store

import (
	"errors"
	"io/fs"
	"testing"

	"github.com/unistack-org/storage/pkg/object"
)

// TestStatJournalOverlay covers journal-mode metadata visibility while
// the flattener is frozen (un-applied overlay records):
//
//   - an acknowledged write for a brand-new object is reported through
//     ObjectState as present=false, pending=true (reads serve the
//     object from the overlay; its file does not exist yet);
//   - for an object whose file exists, Stat/StatInfo account for
//     un-applied overlay writes: size is the max of the index and the
//     overlay end, alloc a lower bound covering the overlay;
//   - Sync is a checkpoint: it drains the overlay even while the
//     applier is frozen.
func TestStatJournalOverlay(t *testing.T) {
	st, err := New(t.TempDir(), Options{CommitMode: CommitJournal})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	oid := object.OID(31337)
	// Fresh object: nothing on disk, nothing in the overlay.
	if present, pending, err := st.ObjectState(oid); err != nil || present || pending {
		t.Fatalf("ObjectState(missing) = %v/%v, %v", present, pending, err)
	}
	if _, _, err := st.StatInfo(oid); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("StatInfo(missing) err = %v, want not-exist", err)
	}

	// Acknowledge a 128 KiB write, then freeze the applier so the
	// record provably stays in the overlay (not the file).
	data := make([]byte, 128*1024)
	if _, err := st.WriteAt(oid, data, 0); err != nil {
		t.Fatal(err)
	}
	st.pauseApplier()

	if present, pending, err := st.ObjectState(oid); err != nil || present || !pending {
		t.Fatalf("ObjectState over overlay = %v/%v, %v, want present=false pending=true",
			present, pending, err)
	}
	// The file does not exist yet, so the file-level size APIs honestly
	// report it missing; NBD base:allocation uses ObjectState above,
	// and ReadAt serves the bytes from the overlay (journal_test covers
	// read-your-writes).
	if _, err := st.Stat(oid); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Stat over overlay (new object) err = %v, want not-exist", err)
	}

	// Sync drains (a checkpoint bypasses the freeze): the object file
	// now exists (grown to the full object size at creation) and the
	// index entry is authoritative.
	if _, err := st.WriteAt(oid, data[:4096], 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	if sz, err := st.Stat(oid); err != nil || sz != uint64(object.Size) {
		t.Fatalf("Stat after drain = %d, %v, want %d", sz, err, object.Size)
	}
	sz, alloc, err := st.StatInfo(oid)
	if err != nil || sz != uint64(object.Size) {
		t.Fatalf("StatInfo after drain = %d, %v, want size %d", sz, err, object.Size)
	}
	if alloc == 0 {
		t.Fatal("StatInfo after drain: alloc = 0 for an object with data")
	}

	// Extension write while frozen: the object is 4 MiB (grown at
	// creation) so the size is unchanged, but StatInfo's alloc clamps
	// to the un-applied overlay end: those bytes are logically the
	// object's even though its file lags.
	oe := uint64(2<<20 + 4096)
	if _, err := st.WriteAt(oid, data[:4096], 2<<20); err != nil {
		t.Fatal(err)
	}
	if sz, err := st.Stat(oid); err != nil || sz != uint64(object.Size) {
		t.Fatalf("Stat over overlay = %d, %v, want %d", sz, err, object.Size)
	}
	sz, alloc, err = st.StatInfo(oid)
	if err != nil || sz != uint64(object.Size) {
		t.Fatalf("StatInfo over overlay = %d, %v, want size %d", sz, err, object.Size)
	}
	if alloc < oe {
		t.Fatalf("StatInfo overlay alloc = %d, want >= overlay end %d (un-applied bytes count as allocated)", alloc, oe)
	}

	// Remove is a checkpoint: it drains the overlay even while the
	// applier is frozen, then deletes the object.
	st.resumeApplier()
	if err := st.Remove(oid); err != nil {
		t.Fatal(err)
	}
	if present, pending, err := st.ObjectState(oid); err != nil || present || pending {
		t.Fatalf("ObjectState after Remove = %v/%v, %v, want false/false", present, pending, err)
	}
	if _, _, err := st.StatInfo(oid); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("StatInfo after Remove err = %v, want not-exist", err)
	}
}
