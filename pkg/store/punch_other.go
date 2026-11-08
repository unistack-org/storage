//go:build !linux

package store

import (
	"errors"
	"os"
)

// ErrPunchUnsupported means the platform has no FALLOC_FL_PUNCH_HOLE (darwin
// has no equivalent). The dev host is darwin, so the store degrades to a
// plain zero-fill there: guests still see correct data, they just do not get
// their blocks back. The production target (Linux) punches for real.
var ErrPunchUnsupported = errors.New("store: punch hole unsupported on this platform")

func punchHole(f *os.File, off, length int64) error {
	return ErrPunchUnsupported
}

const punchSupported = false
