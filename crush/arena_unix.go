//go:build unix

package crush

import "syscall"

// arenaPageSize is the OS page size used to round arena block sizes.
var arenaPageSize = syscall.Getpagesize()

// arenaMmap maps size bytes of anonymous memory outside the Go heap. The
// returned slice is GC-invisible: the runtime has no span for its data
// pointer, so it is never scanned, moved, or accounted as heap. This is the
// "unsafe package memory allocation" the design calls for, expressed via the
// raw mmap syscall; the []byte header is just a view onto that memory.
func arenaMmap(size int) []byte {
	b, err := syscall.Mmap(-1, 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		panic("crush: arena mmap: " + err.Error())
	}
	return b
}

func arenaMunmap(b []byte) {
	if len(b) == 0 {
		return
	}
	// Best effort: an unmap failure leaks at most one block.
	_ = syscall.Munmap(b)
}
