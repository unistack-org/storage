package store

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// TestSplitDirect pins down the head/middle/tail partitioning for a set of
// representative requests: the middle must always sit on a 512-byte boundary,
// head and tail must be strictly unaligned (0 < x < 512) or absent, and
// head+middle+tail must cover the whole request exactly.
func TestSplitDirect(t *testing.T) {
	cases := []struct {
		name      string
		off, len  int64
		wantHead  int64
		wantMid   int64
		wantTail  int64
	}{
		{"empty", 513, 0, 0, 0, 0},
		{"aligned all", 0, 4096, 0, 4096, 0},
		{"aligned middle 1", 512, 4096, 0, 4096, 0},
		{"head only", 1, 10, 10, 0, 0},
		{"tail only", 4087, 129, 9, 0, 120},
		{"head and tail", 100, 1000, 412, 512, 76},
		{"head tail tiny", 511, 2, 1, 0, 1},
		{"tiny at zero", 0, 1, 0, 0, 1},
		{"exactly one block", 0, 512, 0, 512, 0},
		{"offset on boundary", 2048, 511, 0, 0, 511},
		{"big unaligned", 513, 4 * 1024 * 1024, 511, 4*1024*1024 - 512, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			head, midStart, midLen, tailStart, tailLen := splitDirect(tc.off, tc.len)
			if head != tc.wantHead {
				t.Errorf("head = %d, want %d", head, tc.wantHead)
			}
			if midLen != tc.wantMid {
				t.Errorf("midLen = %d, want %d", midLen, tc.wantMid)
			}
			if tailLen != tc.wantTail {
				t.Errorf("tailLen = %d, want %d", tailLen, tc.wantTail)
			}
			// Invariants: the partition must cover the request exactly,
			// in order.
			if tc.len > 0 && head+midLen+tailLen != tc.len {
				t.Errorf("head+mid+tail = %d, want %d", head+midLen+tailLen, tc.len)
			}
			if head+midLen+tailLen > tc.len {
				t.Fatalf("partition exceeds request")
			}
			// Middle must be 512-aligned in file space.
			if midLen > 0 {
				if (tc.off+midStart)%directAlign != 0 {
					t.Errorf("middle start %d not aligned", tc.off+midStart)
				}
				if midLen%directAlign != 0 {
					t.Errorf("middle len %d not a multiple of %d", midLen, directAlign)
				}
			}
			// Tail starts right after the middle.
			if midLen > 0 {
				if tailStart != head+midLen {
					t.Errorf("tailStart = %d, want head+midLen = %d", tailStart, head+midLen)
				}
			}
		})
	}
}

// TestDirectRMWRoundTrip exercises the read-modify-write helpers against a
// real file: unaligned writes must land exactly, overwriting mid-block must
// not clobber the neighbouring bytes, and an unaligned read must return the
// exact bytes written.
func TestDirectRMWRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const size = 8 * 1024 // 8 KiB of known pattern
	want := make([]byte, size)
	for i := range want {
		want[i] = byte(i * 7)
	}
	// Seed the file with an aligned write (16 blocks, all 512-aligned).
	if _, err := f.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	// Then overwrite with unaligned fragments, head/middle/tail style,
	// using directRangeWrite exactly as the store does.
	frags := []struct {
		off  int64
		data []byte
	}{
		{500, bytes.Repeat([]byte{0xAB}, 32)},            // straddles a block boundary
		{1, bytes.Repeat([]byte{0xCD}, 1000)},            // head-only, then middle, then tail
		{4096 - 3, bytes.Repeat([]byte{0x15}, 7)},        // straddles exactly at 4096
		{size - 500, bytes.Repeat([]byte{0x7F}, 500)},    // tail at EOF
	}
	expected := want // keep mutating the expected image
	for _, fr := range frags {
		if n, err := directRangeWrite(f, fr.data, fr.off); err != nil || int64(n) != int64(len(fr.data)) {
			t.Fatalf("directRangeWrite(off=%d, len=%d) = %d, %v", fr.off, len(fr.data), n, err)
		}
		copy(expected[fr.off:], fr.data)
	}
	got := make([]byte, size)
	if n, err := f.ReadAt(got, 0); err != nil || n != size {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if !bytes.Equal(got, expected) {
		t.Fatal("file content diverged from expected after unaligned writes")
	}

	// Now read the same unaligned ranges back and compare.
	for _, fr := range frags {
		r := make([]byte, len(fr.data))
		if n, err := f.ReadAt(r, fr.off); err != nil || n != len(fr.data) {
			t.Fatalf("ReadAt(off=%d) = %d, %v", fr.off, n, err)
		}
		if !bytes.Equal(r, expected[fr.off:fr.off+int64(len(fr.data))]) {
			t.Fatalf("read-back at off=%d diverged", fr.off)
		}
	}
}

// TestDirectRMWEOF pins the short-block behaviour at end-of-file: the RMW
// write-back is a full block, so a fragment past the file's end zero-fills
// and extends the file to the block boundary (not to the fragment's byte
// count), and a read that runs past EOF returns the present bytes only.
func TestDirectRMWEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "eof")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// File is a single aligned block, all zeros.
	if _, err := f.WriteAt(make([]byte, 512), 0); err != nil {
		t.Fatal(err)
	}
	// Write 100 bytes at offset 500 (spans into the second block, which
	// does not exist yet).
	pay := bytes.Repeat([]byte{0x42}, 100)
	if n, err := directRangeWrite(f, pay, 500); err != nil || n != 100 {
		t.Fatalf("directRangeWrite = %d, %v", n, err)
	}
	// The RMW write-back is the full [512,1024) block (an O_DIRECT write
	// cannot be shorter than a block), so the file is the block-rounded
	// 1024 bytes: the written pattern at 500..600, zeros elsewhere in
	// block 2.
	wantSize := int64(directAlign * 2)
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != wantSize {
		t.Fatalf("file size = %d, want %d (block-rounded)", fi.Size(), wantSize)
	}
	buf := make([]byte, wantSize)
	if n, err := f.ReadAt(buf, 0); err != nil || n != int(wantSize) {
		t.Fatalf("ReadAt = %d, %v", n, err)
	}
	if !bytes.Equal(buf[500:600], pay[:100]) {
		t.Fatal("written pattern not at 500..600")
	}
	for i := 600; i < int(wantSize); i++ {
		if buf[i] != 0 {
			t.Fatalf("byte %d = %#x, want 0 (zero-fill to block end)", i, buf[i])
		}
	}
	// Read exactly up to EOF: the present bytes come back with io.EOF.
	r := make([]byte, 100)
	n, err := f.ReadAt(r, wantSize-24)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadAt at EOF = %v", err)
	}
	if n != 24 {
		t.Fatalf("short read = %d, want 24", n)
	}
	for i := 0; i < n; i++ {
		if r[i] != 0 {
			t.Fatalf("byte %d past 600 = %#x, want 0", i, r[i])
		}
	}
}
