//go:build !linux

package nbd

import (
	"os"
	"syscall"
)

// punchHole is unsupported on non-Linux systems (macOS has no
// FALLOC_FL_PUNCH_HOLE). The Server maps this to NBD EOPNOTSUPP, and QEMU
// treats TRIM/WRITE_ZEROES as unavailable when the flags are absent, so on
// the darwin dev host these commands are disabled in ServerOptions.
func punchHole(f *os.File, off, length int64) error {
	return ErrTrimUnsupported
}

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
