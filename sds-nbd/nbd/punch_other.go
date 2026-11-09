//go:build !linux && !darwin

package nbd

import (
	"os"
	"syscall"
)

// punchHole is unsupported on platforms with no hole-punch primitive at all
// (Linux has FALLOC_FL_PUNCH_HOLE, darwin/APFS has fcntl(F_PUNCHHOLE)). The
// Server maps this to NBD EOPNOTSUPP, and QEMU treats TRIM/WRITE_ZEROES as
// unavailable when the flags are absent.
func punchHole(f *os.File, off, length int64) error {
	return ErrTrimUnsupported
}

// punchSupported reports that this platform cannot punch holes.
const punchSupported = false

// errnoToNBDLinux maps a (BSD) syscall errno to the Linux-numbered
// NBD wire code, since the NBD protocol's error field is defined in terms of
// Linux errno. Codes not listed below collapse to EIO. Only values that exist
// (and are distinct) on every supported GOOS are listed: windows defines
// ENOTSUP == EOPNOTSUPP, so only one of the pair appears.
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
	case syscall.ENOTSUP:
		return ErrENOTSUP
	case syscall.ENOTCONN:
		return ErrENOTCONN
	case syscall.ESHUTDOWN:
		return ErrESHUTDOWN
	default:
		return ErrEIO
	}
}
