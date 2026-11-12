//go:build !linux

package mem

// newArena is the portable variant: ordinary zeroed memory. The
// hugepages hint is accepted and ignored (no MADV_HUGEPAGE off Linux);
// the slot arithmetic and registration semantics are identical, so
// callers behave the same everywhere.
func newArena(n, size int, hugepages bool) (*Arena, error) {
	if size < 4096 {
		size = 4096
	}
	return &Arena{Data: make([]byte, n*size), size: size, n: n}, nil
}
