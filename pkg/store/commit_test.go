package store

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

func TestParseCommitMode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want CommitMode
		bad  bool
	}{
		{in: "", want: CommitNone},
		{in: "none", want: CommitNone},
		{in: "flush", want: CommitFlush},
		{in: "sync", want: CommitSync},
		{in: "journal", want: CommitJournal},
		{in: "jurnal", bad: true},
	} {
		got, err := ParseCommitMode(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("ParseCommitMode(%q): want error, got %q", tc.in, got)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("ParseCommitMode(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	if _, err := New(t.TempDir(), Options{CommitMode: "sideways"}); err == nil {
		t.Error("New: want error for unknown commit mode")
	}
}

// All three modes must round-trip bytes identically (durability of the
// ack is a crash-time property; behaviour at runtime must be the same).
func TestCommitModesRoundtrip(t *testing.T) {
	for _, mode := range []CommitMode{CommitNone, CommitFlush, CommitSync, CommitJournal} {
		t.Run(string(mode), func(t *testing.T) {
			st, err := New(t.TempDir(), Options{CommitMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			const oid = object.OID(0x70001)
			payload := bytes.Repeat([]byte("flush"), 100)
			if _, err := st.WriteAt(oid, payload, 4096); err != nil {
				t.Fatal(err)
			}
			if err := st.WriteZeroes(oid, 4096+100, 200, true); err != nil {
				t.Fatal(err)
			}
			if err := st.Sync(); err != nil {
				t.Fatal(err)
			}
			buf := make([]byte, len(payload))
			n, err := st.ReadAt(oid, buf, 4096)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(payload) {
				t.Fatalf("read %d bytes, want %d", n, len(payload))
			}
			want := append([]byte(nil), payload...)
			copy(want[100:300], make([]byte, 200))
			if !bytes.Equal(buf, want) {
				t.Fatal("roundtrip mismatch")
			}
			// Committed data must be on disk (a fresh open sees it).
			f, err := os.Open(st.path(oid))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			disk := make([]byte, 4096+len(payload))
			if _, err := f.ReadAt(disk, 0); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(disk[4096:], want) {
				t.Fatal("data not on disk after write")
			}
		})
	}
}

// With FsyncBatch, many concurrent Sync() calls must share (far) fewer
// fsync passes, and every caller must be released with the pass result.
func TestFsyncBatchCoalescing(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	st, err := New(t.TempDir(), Options{CommitMode: CommitNone, FsyncBatch: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const oid = object.OID(0x30001)
	if _, err := st.WriteAt(oid, []byte("data"), 0); err != nil {
		t.Fatal(err)
	}

	const n = 32
	var wg sync.WaitGroup
	var ok atomic.Int64
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := st.Sync(); err != nil {
				t.Errorf("Sync: %v", err)
			}
			ok.Add(1)
		}()
	}
	close(start)
	wg.Wait()
	if int(ok.Load()) != n {
		t.Fatalf("%d syncs returned, want %d", ok.Load(), n)
	}
	passes := st.syncPasses.Load()
	if passes >= n {
		t.Fatalf("%d syncs caused %d fsync passes: no coalescing", n, passes)
	}
	if passes == 0 {
		t.Fatal("no fsync pass ran")
	}
	t.Logf("%d concurrent syncs -> %d fsync passes", n, passes)

	// Writes issued before a Sync are covered by the batch that opens
	// at/after that Sync (ordering guarantee).
	payload := bytes.Repeat([]byte("ordered"), 64)
	if _, err := st.WriteAt(oid, payload, 8192); err != nil {
		t.Fatal(err)
	}
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(st.path(oid))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	buf := make([]byte, len(payload))
	if _, err := f.ReadAt(buf, 8192); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatal("batched Sync did not cover prior write")
	}
}

// Close must release any Sync parked in a batch and reject later ones.
func TestFsyncBatchCloseDrains(t *testing.T) {
	st, err := New(t.TempDir(), Options{FsyncBatch: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	const oid = object.OID(0x10001)
	if _, err := st.WriteAt(oid, []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var syncErr error
	go func() {
		defer wg.Done()
		syncErr = st.Sync() // parks: the batch timer is set to an hour
	}()
	time.Sleep(30 * time.Millisecond) // let the Sync enqueue
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release a parked Sync")
	}
	if syncErr != nil {
		t.Fatalf("parked Sync returned %v, want the drained fsync result (nil)", syncErr)
	}
	if err := st.Sync(); err != ErrClosed {
		t.Fatalf("Sync after Close = %v, want ErrClosed", err)
	}
	// Close's drain fsynced: data is on disk.
	f, err := os.Open(filepath.Join(st.dir, "0000000000010001"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// A volume write with FUA set must reach the backend Sync; without it,
// it must not (server-side semantics live in sdsnet; here we prove the
// store side is usable as the FUA target and stays correct).
func TestCommitFlushPerWrite(t *testing.T) {
	st, err := New(t.TempDir(), Options{CommitMode: CommitFlush})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const oid = object.OID(0x90002)
	for i := 0; i < 8; i++ {
		if _, err := st.WriteAt(oid, []byte{byte(i)}, int64(i)*512); err != nil {
			t.Fatal(err)
		}
	}
	// CommitFlush fsyncs per write: a Sync afterwards should be a no-op
	// pass (it still runs, but the data was durable before each ack).
	if err := st.Sync(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	for i := 0; i < 8; i++ {
		if _, err := st.ReadAt(oid, buf, int64(i)*512); err != nil {
			t.Fatal(err)
		}
		if buf[0] != byte(i) {
			t.Fatalf("byte at %d = %d, want %d", i*512, buf[0], i)
		}
	}
}
