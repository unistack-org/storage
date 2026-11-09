//go:build linux

package client

import (
	"os"
	"syscall"
)

// punchSupported mirrors store.PunchSupported without importing the
// store package from client tests (the client talks to OSDs over the
// wire, never to a store in-process).
const punchSupported = true

// testAllocated reports the bytes an object file physically occupies
// (st_blocks*512): the honest "is this file free?" metric that
// vacuum's scan uses.
func testAllocated(fi os.FileInfo) int64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Blocks * 512
	}
	return fi.Size()
}
