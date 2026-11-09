package nbd

import (
	"io"
	"os"
)

// zeros is the shared zero buffer for zero-fills (1 MiB, allocated at init;
// the hot path reuses it).
var zeros = make([]byte, 1<<20)

// punchAlign is the granularity hole punching requires on every platform we
// support: Linux FALLOC_FL_PUNCH_HOLE demands filesystem-block alignment and
// darwin F_PUNCHHOLE demands a 4096-byte boundary (verified on APFS: an
// unaligned range is rejected with EINVAL even when the page size is 16 KiB,
// while 4096-aligned ranges punch fine on that machine).
const punchAlign = 4096

// alignRange returns the largest aligned sub-range of [off, off+length) as
// (head, tail) byte offsets relative to off: head is where the aligned
// interior starts, tail is where it ends. ok is false when the request
// contains no full aligned block, so there is nothing to punch.
func alignRange(off, length, align int64) (head, tail int64, ok bool) {
	if length <= 0 {
		return 0, 0, false
	}
	start := off
	if rem := start % align; rem != 0 {
		start += align - rem
	}
	end := off + length
	end -= end % align
	if end <= start {
		return 0, 0, false
	}
	return start - off, end - off, true
}

// zeroFillAt writes literal zeroes over [off, off+length) through w, in
// chunks of the shared zeros buffer.
func zeroFillAt(w io.WriterAt, off, length int64) error {
	for length > 0 {
		n := length
		if n > int64(len(zeros)) {
			n = int64(len(zeros))
		}
		if _, err := w.WriteAt(zeros[:n], off); err != nil {
			return err
		}
		off += n
		length -= n
	}
	return nil
}

// punchRange makes [off, off+length) read as zeroes, reclaiming blocks when
// it can: the aligned interior is punched (the file keeps its logical size,
// the blocks return to the filesystem) and the unaligned edges are
// zero-filled. On a platform with no punch primitive it reports
// ErrTrimUnsupported (TRIM stays legally "not supported"); when the punch
// syscall itself fails (a filesystem without holes, EPERM, ...) it degrades
// to a literal zero-fill over the whole range — correctness first, reclaim
// is a bonus.
func punchRange(f *os.File, off, length int64) error {
	if !punchSupported {
		return ErrTrimUnsupported
	}
	head, tail, ok := alignRange(off, length, punchAlign)
	if !ok {
		// Sub-block request: zeros are the best we can do.
		return zeroFillAt(f, off, length)
	}
	if err := punchHole(f, off+head, tail-head); err != nil {
		return zeroFillAt(f, off, length)
	}
	if err := zeroFillAt(f, off, head); err != nil {
		return err
	}
	return zeroFillAt(f, off+tail, length-tail)
}
