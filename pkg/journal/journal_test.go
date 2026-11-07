package journal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		seq, err := j.Append([]byte{byte(i), byte(i)})
		if err != nil || seq != uint64(i) {
			t.Fatalf("Append %d = %d, %v", i, seq, err)
		}
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	var seen []uint64
	n, err := j.Replay(func(seq uint64, payload []byte) error {
		seen = append(seen, seq)
		return nil
	})
	if err != nil || n != 5 {
		t.Fatalf("Replay = %d, %v", n, err)
	}
	for i, seq := range seen {
		if seq != uint64(i+1) {
			t.Fatalf("seen[%d] = %d", i, seq)
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: seq continues from the durable records.
	j2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	seq, err := j2.Append([]byte("next"))
	if err != nil || seq != 6 {
		t.Fatalf("Append after reopen = %d, %v (want 6)", seq, err)
	}
}

func TestTornTailTolerated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	j.Append([]byte("one"))
	j.Append([]byte("two"))
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-record: a header plus a partial payload.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 5, 'a', 'b'})
	f.Close()
	j.Close()

	var payloads [][]byte
	n, err := ReplayFile(path, func(_ uint64, p []byte) error {
		payloads = append(payloads, p)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayFile: %v", err)
	}
	if n != 2 || string(payloads[1]) != "two" {
		t.Fatalf("n=%d payloads=%q", n, payloads)
	}
	// Open after a torn tail: appends continue from seq 2.
	j2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	seq, err := j2.Append([]byte("three"))
	if err != nil || seq != 3 {
		t.Fatalf("Append = %d, %v (want 3)", seq, err)
	}
}

func TestCRCCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	j.Append([]byte("alpha"))
	j.Sync()
	j.Close()

	// Flip a byte in the payload of the only record.
	b, _ := os.ReadFile(path)
	b[HeaderSize] ^= 0xff
	os.WriteFile(path, b, 0o644)

	_, err = ReplayFile(path, func(uint64, []byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReplayFile: got %v, want ErrCorrupt", err)
	}
}

func TestTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		j.Append([]byte("x"))
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := j.Truncate(4); err != nil {
		t.Fatal(err)
	}
	n, err := j.Replay(func(uint64, []byte) error { return nil })
	if err != nil || n != 0 {
		t.Fatalf("Replay after truncate = %d, %v", n, err)
	}
	seq, err := j.Append([]byte("after"))
	if err != nil || seq != 5 {
		t.Fatalf("Append after truncate = %d, %v (want 5)", seq, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen sees exactly the post-truncate record.
	var got [][]byte
	n, err = ReplayFile(path, func(_ uint64, p []byte) error {
		got = append(got, p)
		return nil
	})
	if err != nil || n != 1 || !bytes.Equal(got[0], []byte("after")) {
		t.Fatalf("n=%d got=%q err=%v", n, got, err)
	}
}

func TestConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.log")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	var wg sync.WaitGroup
	var fail = make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := j.Append([]byte("payload")); err != nil {
				fail <- err
			}
		}()
	}
	wg.Wait()
	close(fail)
	for err := range fail {
		t.Fatal(err)
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	count := 0
	if _, err := j.Replay(func(uint64, []byte) error { count++; return nil }); err != nil {
		t.Fatal(err)
	}
	j.Close()
	if count != n {
		t.Fatalf("replayed %d, want %d", count, n)
	}
}
