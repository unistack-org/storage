//go:build windows

package store

import "os"

// fileAllocated has no st_blocks on windows; report the visible size.
// Windows is not a supported deployment target — this stub only keeps the
// package compilable for cross-compiled tooling.
func fileAllocated(fi os.FileInfo) int64 { return fi.Size() }
