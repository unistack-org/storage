//go:build darwin

package client

import (
	"os"
	"syscall"
)

// punchSupported mirrors store.PunchSupported without importing the
// store package from client tests (the client talks to OSDs over the
// wire, never to a store in-process). APFS punches holes via
// fcntl(F_PUNCHHOLE), so darwin behaves like Linux here.
const punchSupported = true

// testAllocated reports the bytes an object file physically occupies
// (st_blocks*512): the honest "is this file free?" metric that
// vacuum's scan uses. APFS allocates lazily, so this is the only
// measure that distinguishes a sparse object from a written one.
func testAllocated(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return fi.Size()
}
