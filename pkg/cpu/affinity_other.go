//go:build !linux

package cpu

// schedSetAffinity exists for build parity; pinCurrent is compiled out
// on this platform, so it is never called.
func schedSetAffinity(uint64) error { return ErrUnsupported }
