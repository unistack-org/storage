//go:build linux

package store

import (
	"os"
	"syscall"
)

// punchHole punches a sparse hole with
// fallocate(FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE): the range reads back
// as zeroes and the blocks are returned to the filesystem, while the object
// keeps its full logical size (the object is a fixed-size slot).
//
// The kernel requires both offset and length to be aligned to the
// filesystem block size; punchAligned in store.go guarantees that for the
// ranges the client sends (guest discard granularity is >= 512 B and object
// offsets are 4 MiB aligned at the boundaries), and the caller falls back to
// a zero-fill for any unaligned remainder.
func punchHole(f *os.File, off, length int64) error {
	const (
		flPunchHole = 0x02 // FALLOC_FL_PUNCH_HOLE
		flKeepSize  = 0x01 // FALLOC_FL_KEEP_SIZE
	)
	_, _, errno := syscall.Syscall6(syscall.SYS_FALLOCATE,
		uintptr(f.Fd()),
		uintptr(flPunchHole|flKeepSize),
		uintptr(off),
		uintptr(length),
		0, 0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// punchSupported reports that this platform can punch holes.
const punchSupported = true
