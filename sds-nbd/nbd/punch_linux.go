//go:build linux

package nbd

import (
	"os"
	"syscall"
)

// punchHole uses Linux fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)
// so TRIM/WRITE_ZEROES create sparse regions without shrinking the file.
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

// errnoToNBDLinux encodes errno as the NBD wire code (negated errno), since
// the NBD protocol defines the error field in terms of Linux errno.
func errnoToNBDLinux(errno syscall.Errno) uint32 {
	return uint32(-int32(errno))
}
