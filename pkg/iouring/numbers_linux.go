//go:build linux && (amd64 || arm64)

package iouring

// io_uring syscall numbers (identical on amd64 and arm64).
const (
	sysIoUringSetup = 425
	sysIoUringEnter = 426
)
