//go:build !linux

package store

// directPlatformOK reports whether the platform's direct-open path imposes
// O_DIRECT's hard 512-byte offset/length/buffer-pointer alignment contract.
// On non-Linux platforms (darwin, etc.) the "direct" open is F_NOCACHE or
// similar, which bypasses the page cache but carries no alignment
// constraints, so the store runs the plain buffered I/O paths and the
// alignment-safe helpers in direct.go stay dormant (s.direct is false).
const directPlatformOK = false
