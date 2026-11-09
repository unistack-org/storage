package journal

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestJournalFailsOnNoSpace boots a 1 MiB tmpfs (Linux, CAP_SYS_ADMIN —
// skips otherwise), fills it with journal appends past capacity, and
// requires the journal to (a) report ErrNoSpace, (b) poison: every later
// Append refuses with the same cause, (c) leave the durable prefix
// replayable (the guest's data before the full disk is intact).
func TestJournalFailsOnNoSpace(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("needs root to mount tmpfs")
	}
	dir := t.TempDir()
	if err := exec.Command("mount", "-t", "tmpfs", "-o", "size=1m", "tmpfs", dir).Run(); err != nil {
		t.Skipf("cannot mount tmpfs: %v", err)
	}
	t.Cleanup(func() { _ = exec.Command("umount", dir).Run() })

	j, err := Open(dir + "/j.log")
	if err != nil {
		t.Fatal(err)
	}

	// 1 MiB tmpfs: 128 records of 8 KiB = 1 MiB of payload must overrun.
	rec := bytes.Repeat([]byte("x"), 8<<10)
	var appended int
	var spaceErr error
	for i := 0; i < 200; i++ {
		if _, err := j.Append(rec); err != nil {
			spaceErr = err
			break
		}
		appended++
		if err := j.Sync(); err != nil { // force bufio to the file
			spaceErr = err
			break
		}
	}
	if spaceErr == nil {
		j.Close()
		t.Fatalf("filled %d records without ENOSPC", appended)
	}
	if !errors.Is(spaceErr, syscall.ENOSPC) {
		j.Close()
		t.Fatalf("append/sync on full fs: got %v, want ENOSPC", spaceErr)
	}
	if !errors.Is(spaceErr, ErrNoSpace) {
		j.Close()
		t.Fatalf("ENOSPC must surface as ErrNoSpace, got %v", spaceErr)
	}
	if j.Failed() == nil {
		j.Close()
		t.Fatal("journal must be Failed() after ENOSPC")
	}

	// Poisoned: appends refuse with the same cause, not a new one.
	if _, err := j.Append([]byte("late")); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("append after failure: %v", err)
	}
	if err := j.Flush(); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("flush after failure: %v", err)
	}

	// The durable prefix must replay cleanly: a full disk stops new
	// writes, it does not corrupt what was already logged.
	var replayed, good int
	replayed, err = ReplayFile(dir+"/j.log", func(seq uint64, payload []byte) error {
		if len(payload) == len(rec) {
			good++
		}
		return nil
	})
	j.Close()
	if err != nil {
		t.Fatalf("replay after ENOSPC: %v", err)
	}
	if replayed == 0 || replayed != good {
		t.Fatalf("replayed %d records, %d intact", replayed, good)
	}
	if replayed > appended {
		t.Fatalf("replayed %d > appended %d: phantom records", replayed, appended)
	}
}
