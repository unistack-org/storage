//go:build darwin

package store

import (
	"os"
	"syscall"
)

// setNocache approximates O_DIRECT on darwin with F_NOCACHE, which
// bypasses the buffer cache for reads and writes on this fd.
func setNocache(f *os.File) {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), uintptr(syscall.F_NOCACHE), 1)
	if errno != 0 {
		// Not fatal: buffered I/O is still correct, just cached.
		return
	}
}
