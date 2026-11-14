//go:build linux

package store

// directPlatformOK reports whether the platform's direct-open path imposes
// O_DIRECT's hard 512-byte offset/length/buffer-pointer alignment contract.
// On Linux, store_linux.go's openFile applies O_DIRECT to the data fd, so the
// store must route every data I/O through the alignment-safe helpers in
// direct.go. The bounce pool itself is portable (plain mmap, page-aligned on
// both Linux and darwin), so only this gate is platform-specific.
const directPlatformOK = true
