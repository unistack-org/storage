//go:build darwin

package store

import (
	"os"
	"syscall"
	"unsafe"
)

// fpunchhole mirrors the SDK's fpunchhole_t (sys/fcntl.h), the argument
// structure of F_PUNCHHOLE. It is NOT fstore_t (that one is for
// F_PREALLOCATE; passing it to F_PUNCHHOLE is rejected with EINVAL).
type fpunchhole struct {
	FpFlags  uint32
	Reserved uint32
	FpOffset int64
	FpLength int64
}

// punchHole punches a sparse hole with fcntl(F_PUNCHHOLE): the range reads
// back as zeroes and APFS returns the blocks to the filesystem (verified:
// st_blocks drops), while the file keeps its logical size — the APFS
// equivalent of Linux's FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE. It works
// on files opened with F_NOCACHE (the darwin O_DIRECT analog) too.
//
// APFS requires both offset and length to be aligned to a 4096-byte
// boundary (page size on Apple Silicon is 16 KiB, but 4 KiB alignment is
// accepted); punchAligned in store.go guarantees that, and the caller
// zero-fills any unaligned remainder.
func punchHole(f *os.File, off, length int64) error {
	const fPunchHole = 99 // F_PUNCHHOLE
	arg := fpunchhole{FpOffset: off, FpLength: length}
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
		uintptr(f.Fd()),
		fPunchHole,
		uintptr(unsafe.Pointer(&arg)),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// punchSupported reports that this platform can punch holes.
const punchSupported = true
