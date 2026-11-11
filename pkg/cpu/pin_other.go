//go:build !linux

package cpu

// PinCurrent reports ErrUnsupported: macOS has thread affinity APIs
// (thread_affinity_policy) that Go does not expose, and the feature
// is Linux-only by design.
func PinCurrent(string) error { return ErrUnsupported }
