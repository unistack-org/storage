package store

import (
	"fmt"
	"io"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The O_DIRECT store must be robust to unaligned offsets, lengths and
// buffers: neither the NBD server (512 B minimum block, but no alignment
// check) nor the client library enforces 512-byte alignment, so a guest
// can drive a read or write at any offset with any length and any caller
// buffer. On an O_DIRECT descriptor the kernel requires offset, length and
// buffer to all be 512-byte aligned, and an unaligned transfer is a hard
// error (EIO on XFS/ext4), never a silent partial transfer. The only
// correct way to serve such a transfer on the O_DIRECT fd is to split it:
//
//   - the unaligned 512-byte head and tail are handled with an aligned
//     read-modify-write on the same fd (read the 512 B block, splice the
//     fragment in, write the block back);
//   - the aligned middle is a plain O_DIRECT transfer, zero-copy when the
//     caller buffer is already 512-aligned, otherwise through a page-aligned
//     bounce buffer.
//
// There is deliberately no "fall back to buffered I/O on the same fd":
// an O_DIRECT fd only does direct I/O. These two helpers are the single
// correctness boundary the rest of the store routes direct I/O through, so
// every synchronous path (ReadAt/WriteAt, zeroFill, the journal applier and
// the RWF_ATOMIC latch fallback) is alignment-safe. The async ring is not
// used in direct mode (a registered arena slot is submitted as a whole at the
// raw caller offset, which O_DIRECT rejects), so the ring path never reaches
// an O_DIRECT fd. They are used only when the store is in direct mode
// (Options.Direct on a platform that opened the fd O_DIRECT); the buffered
// store keeps its original paths byte-for-byte.
const directAlign = 512

// directRead serves a read of len(p) bytes at off from f through the
// alignment-safe direct path. It is a no-op passthrough for the buffered
// store (f.ReadAt), so callers can invoke it unconditionally and it only
// matters when the fd is O_DIRECT.
func (s *Store) directRead(f *os.File, p []byte, off int64) (int, error) {
	if !s.direct {
		return f.ReadAt(p, off)
	}
	var got int
	var derr error
	if len(p) > 0 {
		got, derr = directRangeRead(f, p, off)
		if derr != nil {
			return got, derr
		}
	}
	// Match ReadAt/ReadAt semantics: a short transfer at end-of-file is
	// io.EOF. p is never overfilled because each chunk is sized to the
	// remaining request.
	if got < len(p) {
		return got, io.EOF
	}
	return got, nil
}

// directWrite serves a write of p at off through the alignment-safe direct
// path, mirroring directRead for the buffered store.
func (s *Store) directWrite(f *os.File, p []byte, off int64) (int, error) {
	if !s.direct {
		return f.WriteAt(p, off)
	}
	var put int
	var derr error
	if len(p) > 0 {
		put, derr = directRangeWrite(f, p, off)
		if derr != nil {
			return put, derr
		}
	}
	return put, nil
}

// directRangeRead reads exactly len(dst) bytes at off, splitting the
// request into an unaligned head (RMW), an aligned middle (zero-copy or
// bounce), and an unaligned tail (RMW).
func directRangeRead(f *os.File, dst []byte, off int64) (int, error) {
	head, midStart, midLen, tailStart, tailLen := splitDirect(off, int64(len(dst)))
	var total int

	if head > 0 {
		blkOff := (off / directAlign) * directAlign
		n, err := directRMWRead(f, dst, off, head, blkOff, blkOff+directAlign)
		total += n
		if err != nil {
			return total, err
		}
	}
	if midLen > 0 {
		n, err := directBulkRead(f, dst[head:head+midLen], off+midStart)
		total += n
		if err != nil {
			return total, err
		}
	}
	if tailLen > 0 {
		blkOff := ((off + tailStart) / directAlign) * directAlign
		n, err := directRMWRead(f, dst[head+midLen:], off+tailStart, tailLen, blkOff, blkOff+directAlign)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// directRangeWrite writes len(src) bytes at off with the head/middle/tail
// split of directRangeRead.
func directRangeWrite(f *os.File, src []byte, off int64) (int, error) {
	head, midStart, midLen, tailStart, tailLen := splitDirect(off, int64(len(src)))
	var total int

	if head > 0 {
		blkOff := (off / directAlign) * directAlign
		n, err := directRMWWrite(f, src, off, head, blkOff, blkOff+directAlign)
		total += n
		if err != nil {
			return total, err
		}
	}
	if midLen > 0 {
		n, err := directBulkWrite(f, src[head:head+midLen], off+midStart)
		total += n
		if err != nil {
			return total, err
		}
	}
	if tailLen > 0 {
		blkOff := ((off + tailStart) / directAlign) * directAlign
		n, err := directRMWWrite(f, src[head+midLen:], off+tailStart, tailLen, blkOff, blkOff+directAlign)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// splitDirect partitions a request at off spanning length bytes into its
// unaligned head (head bytes at off), the 512-aligned middle (midLen bytes
// starting midStart bytes into the request), and its unaligned tail
// (tailLen bytes starting tailStart bytes into the request).
func splitDirect(off, length int64) (head, midStart, midLen, tailStart, tailLen int64) {
	if length <= 0 {
		return 0, 0, 0, 0, 0
	}
	end := off + length
	midBegin := (off / directAlign) * directAlign
	if midBegin < off {
		midBegin += directAlign // first 512 boundary at or after off
	}
	midEnd := (end / directAlign) * directAlign
	if midEnd > midBegin {
		midStart = midBegin - off
		midLen = midEnd - midBegin
	}
	// Without a middle, the whole request is RMW: the head must be clamped
	// to the request end, otherwise a short request (no full block inside
	// it) leaves midBegin past end and the tail length goes negative.
	head = midBegin - off
	if head > length {
		head = length
	}
	tailStart = midBegin + midLen - off
	tailLen = end - (midBegin + midLen)
	if tailLen < 0 {
		tailLen = 0
	}
	return head, midStart, midLen, tailStart, tailLen
}

// directBulkRead performs the 512-aligned middle transfer. When the caller
// buffer is already 512-aligned (the ring's registered-buffer arena slots
// always are) it is a zero-copy O_DIRECT read; otherwise it bounces through
// a page-aligned buffer and copies out.
func directBulkRead(f *os.File, dst []byte, off int64) (int, error) {
	if ptrAligned(directAddr(dst), directAlign) {
		return f.ReadAt(dst, off)
	}
	b, berr := directBuf(len(dst))
	if berr != nil {
		return 0, berr
	}
	n, err := f.ReadAt(b, off)
	copy(dst[:n], b[:n])
	putDirectBuf(b)
	return n, err
}

// directBulkWrite is the write counterpart of directBulkRead.
func directBulkWrite(f *os.File, src []byte, off int64) (int, error) {
	if ptrAligned(directAddr(src), directAlign) {
		return f.WriteAt(src, off)
	}
	b, berr := directBuf(len(src))
	if berr != nil {
		return 0, berr
	}
	copy(b, src)
	n, err := f.WriteAt(b, off)
	putDirectBuf(b)
	return n, err
}

// directRMWRead reads the aligned [blkOff, blkOff+blk) block, copies the
// len(fragment) bytes that the caller wants (at request offset off, into
// fragment) from it, and returns. A read is a pure RMW-read: the block is
// only read, never written back.
func directRMWRead(f *os.File, fragment []byte, off, lenFragment int64, blkOff, blkEnd int64) (int, error) {
	b, berr := directBuf(int(blkEnd - blkOff))
	if berr != nil {
		return 0, berr
	}
	n, err := f.ReadAt(b, blkOff)
	if err != nil && err != io.EOF {
		putDirectBuf(b)
		return 0, err
	}
	fragEnd := off + lenFragment
	if int64(n) < fragEnd {
		// Block shorter than the fragment (only possible at a file's
		// end-of-file); copy what is present.
		available := int64(n) - off
		if available > lenFragment {
			available = lenFragment
		}
		if available <= 0 {
			putDirectBuf(b)
			return 0, nil
		}
		copy(fragment, b[:available])
		putDirectBuf(b)
		return int(available), nil
	}
	copy(fragment, b[off-blkOff:fragEnd-blkOff])
	putDirectBuf(b)
	return int(lenFragment), nil
}

// directRMWWrite reads the aligned block, splices the fragment in, and
// writes the block back — the read-modify-write that lets an unaligned
// fragment land on an O_DIRECT fd.
func directRMWWrite(f *os.File, fragment []byte, off, lenFragment int64, blkOff, blkEnd int64) (int, error) {
	b, berr := directBuf(int(blkEnd - blkOff))
	if berr != nil {
		return 0, berr
	}
	n, err := f.ReadAt(b, blkOff)
	if err != nil && err != io.EOF {
		putDirectBuf(b)
		return 0, err
	}
	fragEnd := off + lenFragment
	if int64(n) < fragEnd {
		// The file does not extend to the end of the block yet: zero the
		// rest so the write-back is the full aligned block.
		for i := n; i < int(blkEnd-blkOff); i++ {
			b[i] = 0
		}
	}
	copy(b[off-blkOff:fragEnd-blkOff], fragment)
	if _, err := f.WriteAt(b, blkOff); err != nil {
		putDirectBuf(b)
		return 0, err
	}
	putDirectBuf(b)
	return int(lenFragment), nil
}

// directAddr is the pointer of b's backing array (0 for an empty slice).
func directAddr(b []byte) uintptr {
	if len(b) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&b[0]))
}

// ptrAligned reports whether addr is a multiple of align.
func ptrAligned(addr, align uintptr) bool {
	return addr%align == 0
}

// The bounce pool serves two shapes: 512-byte RMW blocks (head/tail) and
// up to 4 MiB aligned middles (an NBD payload). One page-aligned class
// (4 MiB) covers both; the small RMW case simply uses its first 512 bytes.
// A tiny channel pool (cap 4) mirrors the atomic-scratch pool: pinned
// callers serialize on it, and a contended caller gets a fresh mmap.
// mmap is used directly (portable, page-aligned on both Linux and darwin).
const directBufSize = 4 << 20 // 4 MiB, the O_DIRECT NBD MaxPayload

var directBufPool = make(chan []byte, 4)

// directBuf hands out a page-aligned buffer of at least n bytes (n must be
// a 512-multiple; the mmap base is page-aligned, so the whole region is
// 512-aligned). It errors only if the mmap fails, in which case the direct
// path cannot serve the transfer (an unaligned heap buffer on an O_DIRECT
// fd is a hard EIO, so there is no safe fallback).
func directBuf(n int) ([]byte, error) {
	size := n
	if size < directBufSize {
		size = directBufSize
	}
	select {
	case b := <-directBufPool:
		return b[:n], nil
	default:
	}
	b, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
	if err != nil {
		return nil, fmt.Errorf("store: O_DIRECT bounce buffer: %w", err)
	}
	return b[:n], nil
}

// putDirectBuf returns b to the pool (by its full mapped capacity) or
// unmaps it if the pool is full.
func putDirectBuf(b []byte) {
	full := b[:cap(b)]
	select {
	case directBufPool <- full:
	default:
		_ = unix.Munmap(full)
	}
}
