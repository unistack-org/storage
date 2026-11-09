//go:build windows

package store

import (
	"errors"
	"os"
	"syscall"
)

// Windows is not a supported deployment target (there is no O_DIRECT, no
// hole punching); these stubs only keep the package compilable for
// cross-compiled tooling.
var ErrNoSpace = errors.New("store: no space left on device")

func isNoSpace(err error) bool { return errors.Is(err, syscall.ENOSPC) }

func fileAvail(f *os.File) (int64, bool) { return 0, false }

func storeDirStat(dir string) (*os.File, error) { return os.Open(dir) }

func fileSpace(f *os.File) (avail, total int64, ok bool) { return 0, 0, false }
