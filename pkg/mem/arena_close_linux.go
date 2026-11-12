package mem

import "golang.org/x/sys/unix"

func closeArena(b []byte) error { return unix.Munmap(b) }
