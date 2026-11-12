//go:build !linux

package mem

// closeArena is a no-op off Linux: the arena is ordinary Go memory
// and the garbage collector reclaims it once the arena is dropped.
func closeArena(b []byte) error { return nil }
