//go:build linux && !amd64 && !arm64

package store

// pwritev2 numbers are only defined for the two supported data-plane
// architectures; elsewhere the atomic write path stays disabled.
const sysPwritev2 = 0
