package mem

import (
	"golang.org/x/sys/unix"
)

// newArena mmaps the arena so its pages are outside the Go heap: the
// GC never scans a registered-buffer region, and MADV_HUGEPAGE makes
// the kernel back it with 2 MiB pages when THP is available (mode
// madvise; the advice is a request, and a kernel that has THP off
// keeps plain pages without failing the mmap). MAP_POPULATE touches
// the pages now, so the first I/O through the arena pays no page
// faults (hugepages: one fault per 2 MiB, not per 4 KiB).
func newArena(n, size int, hugepages bool) (*Arena, error) {
	size = roundPage(size)
	total := n * size
	b, err := unix.Mmap(-1, 0, total, unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_POPULATE)
	if err != nil {
		return nil, err
	}
	huge := false
	if hugepages {
		// MADV_HUGEPAGE cannot fail on a kernel without THP: it is
		// advisory. A refusal (EINVAL: THP compiled out) leaves the
		// arena functional on plain pages.
		if err := unix.Madvise(b, unix.MADV_HUGEPAGE); err == nil {
			huge = true
		}
	}
	return &Arena{Data: b, Huge: huge, size: size, n: n}, nil
}

func roundPage(size int) int {
	if size < unix.Getpagesize() {
		return unix.Getpagesize()
	}
	p := unix.Getpagesize()
	return (size + p - 1) / p * p
}
