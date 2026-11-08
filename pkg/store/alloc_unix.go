//go:build unix

package store

import (
	"os"
	"syscall"
)

// fileAllocated reports the bytes the file actually occupies on disk.
// st_blocks is counted in 512-byte units on every unix we target.
func fileAllocated(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return fi.Size()
}
