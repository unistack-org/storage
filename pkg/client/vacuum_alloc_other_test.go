//go:build !linux && !darwin

package client

import "os"

const punchSupported = false

func testAllocated(fi os.FileInfo) int64 { return fi.Size() }
