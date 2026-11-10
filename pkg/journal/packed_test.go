package journal

import (
	"os"
	"path/filepath"
	"testing"
)

// Packed mode (ideas §2.4): fixed-size items under one outer header.

func TestPackedRoundtrip(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	for i := 0; i < 500; i++ {
		rec := make([]byte, 24)
		rec[0] = byte(i)
		if _, err := j.AppendPacked(rec); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, uint64(i+1))
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	var got []uint64
	if _, err := j.Replay(func(seq uint64, payload []byte) error {
		got = append(got, seq)
		if payload[0] != byte(seq-1) {
			t.Fatalf("payload mismatch at seq %d", seq)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(seqs) {
		t.Fatalf("replayed %d, want %d", len(got), len(seqs))
	}
	for i := range seqs {
		if got[i] != seqs[i] {
			t.Fatalf("seq %d: got %d", i, got[i])
		}
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// A batched log must be far smaller than a per-record one: 500 x 24 B
	// payload + a handful of 16 B headers (+16 B batch body) vs 500 x 40 B.
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if per := float64(fi.Size()) / 500; per > 26 {
		t.Fatalf("packing did not happen: %.1f bytes per 24 B record", per)
	}
}

func TestPackedReopen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil { // seals + persists the partial batch
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j2, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if j2.Seq() != 10 {
		t.Fatalf("seq after reopen: %d, want 10", j2.Seq())
	}
	// Appending after reopen continues the sequence; a replay sees all.
	if _, err := j2.AppendPacked(make([]byte, 24)); err != nil {
		t.Fatal(err)
	}
	if err := j2.Sync(); err != nil {
		t.Fatal(err)
	}
	n, err := j2.Replay(func(seq uint64, _ []byte) error {
		if seq > 11 {
			t.Fatalf("seq ran past 11: %d", seq)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Fatalf("replayed %d, want 11", n)
	}
}

// A crash (HardClose) loses the unsealed batch entirely — buffered
// items never reached the OS. This is the all-or-none batch rule.
func TestPackedCrashLosesUnsealedBatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil { // batch of 100 is durable
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	j.HardClose() // crash: the 7-item partial batch dies in the buffer
	n, err := ReplayFile(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Fatalf("replayed %d, want 100 (unsealed batch must be lost whole)", n)
	}
}

// Flush must seal the partial batch: durability boundaries never leave
// a batch open across them.
func TestPackedFlushSealsPartialBatch(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 3; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Flush(); err != nil {
		t.Fatal(err)
	}
	n, err := ReplayFile(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("after Flush replayed %d, want 3 (Flush must seal the batch)", n)
	}
}

// A torn outer record (truncated file) tolerates the tail like plain
// mode; a corrupted outer payload is detected by CRC.
func TestPackedTornAndCorrupt(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	j.HardClose()

	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	// All 64 items sit in one sealed batch; cutting the tail must drop
	// the whole (single) batch cleanly.
	if err := os.Truncate(p, fi.Size()-13); err != nil {
		t.Fatal(err)
	}
	n, err := ReplayFile(p, nil)
	if err != nil {
		t.Fatalf("torn tail must be tolerated, got %v", err)
	}
	if n != 0 {
		t.Fatalf("torn single batch: replayed %d, want 0", n)
	}

	// Flip a byte inside the outer payload: CRC must catch it.
	j, err = OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	j.HardClose()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	raw[HeaderSize+20] ^= 0xff
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayFile(p, nil); err == nil {
		t.Fatal("corrupted packed payload must be reported")
	}
}

// The scanner checks batch continuity: the outer seq must be the last
// seq before the batch and first must be outer+1. A batch header whose
// seqs skip is corrupt (bytes were lost), not merely odd.
func TestPackedSeqGapIsCorrupt(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	j.HardClose()
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	// Bump firstSeq (outer-body offset 8) from 1 to 9: the batch claims
	// to start at seq 9 while the outer header says its predecessor is 0.
	raw[HeaderSize+8] = 0
	raw[HeaderSize+15] = 9
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayFile(p, nil); err == nil {
		t.Fatal("batch seq gap must be reported corrupt")
	}
}

// Packed items must be exactly itemSize, and packed mode must not be
// mixed with the plain writer.
func TestPackedRejectsBadSize(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if _, err := j.AppendPacked(make([]byte, 23)); err == nil {
		t.Fatal("short payload accepted")
	}
	if _, err := j.AppendPacked(make([]byte, 25)); err == nil {
		t.Fatal("long payload accepted")
	}
	jp, err := Open(filepath.Join(t.TempDir(), "plain.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer jp.Close()
	if _, err := jp.AppendPacked(make([]byte, 24)); err == nil {
		t.Fatal("AppendPacked on plain journal accepted")
	}
	if _, err := jp.Append([]byte("x")); err != nil {
		t.Fatal(err)
	}
}

// The format is self-describing: a packed writer may also issue plain
// Appends (e.g. a future checkpoint record); Append seals the open
// batch first, so replay sees every item in seq order.
func TestPackedMixedPlainAppend(t *testing.T) {
	p := filepath.Join(t.TempDir(), "j.log")
	j, err := OpenPacked(p, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	for i := 0; i < 3; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.Append([]byte("plain")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := j.AppendPacked(make([]byte, 24)); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.Sync(); err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	n, err := j.Replay(func(seq uint64, payload []byte) error {
		seqs = append(seqs, seq)
		_ = payload
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 7 {
		t.Fatalf("replayed %d, want 7", n)
	}
	for i, s := range seqs {
		if s != uint64(i+1) {
			t.Fatalf("seq out of order at %d: %d", i, s)
		}
	}
}
