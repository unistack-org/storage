package cpu

import (
	"syscall"
	"unsafe"
)

// The syscall number differs per architecture (203 on x86_64, 122 on
// the asm-generic tables), so it comes from the platform tables.
const sysSchedSetaffinity = syscall.SYS_SCHED_SETAFFINITY

// schedSetAffinity sets the calling thread's CPU mask. The kernel ABI
// takes (pid, len, mask*): pid 0 means "this thread", and the mask is
// a little-endian bitmask over CPU numbers. len=8 covers up to 64
// CPUs, which is the range of the CPU numbers we accept.
func schedSetAffinity(cpumask uint64) error {
	mask := cpumask
	_, _, errno := syscall.RawSyscall(sysSchedSetaffinity, 0, 8,
		uintptr(unsafe.Pointer(&mask)))
	if errno != 0 {
		return errno
	}
	return nil
}
