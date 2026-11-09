//go:build darwin

package nbd

import (
	"os"
	"syscall"
	"unsafe"
)

// fpunchhole mirrors the SDK's fpunchhole_t (sys/fcntl.h), the argument
// structure of F_PUNCHHOLE. It is NOT fstore_t (that one belongs to
// F_PREALLOCATE; passing it to F_PUNCHHOLE is rejected with EINVAL).
type fpunchhole struct {
	FpFlags  uint32
	Reserved uint32
	FpOffset int64
	FpLength int64
}

// punchHole punches a sparse hole with fcntl(F_PUNCHHOLE), the APFS
// equivalent of Linux's fallocate(FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE):
// the range reads back as zeroes, the blocks return to the filesystem, and
// the file keeps its logical size. Verified on macOS 27 / Apple Silicon
// (16 KiB pages): 4 KiB-aligned ranges punch fine; unaligned ones are
// rejected with EINVAL, as on Linux.
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

// errnoToNBDLinux maps a (darwin/BSD) syscall errno to the Linux-numbered
// NBD wire code, since the NBD protocol's error field is defined in terms of
// Linux errno. Codes not listed below collapse to EIO.
func errnoToNBDLinux(errno syscall.Errno) uint32 {
	switch errno {
	case syscall.EPERM:
		return ErrEPERM
	case syscall.ENOENT:
		return ErrENOENT
	case syscall.EIO:
		return ErrEIO
	case syscall.ENXIO:
		return ErrENXIO
	case syscall.E2BIG:
		return ErrE2BIG
	case syscall.EINVAL:
		return ErrEINVAL
	case syscall.ENOSPC:
		return ErrENOSPC
	case syscall.ENOTSUP, syscall.EOPNOTSUPP:
		return ErrENOTSUP
	case syscall.ENOTCONN:
		return ErrENOTCONN
	case syscall.ESHUTDOWN:
		return ErrESHUTDOWN
	default:
		return ErrEIO
	}
}
