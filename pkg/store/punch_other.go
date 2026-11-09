//go:build !linux && !darwin

package store

import (
	"errors"
	"os"
)

// ErrPunchUnsupported means the platform has no hole-punch primitive at all
// (Linux has FALLOC_FL_PUNCH_HOLE, darwin/APFS has fcntl(F_PUNCHHOLE)). On
// the remaining ports the store degrades to a plain zero-fill: guests still
// see correct data, they just do not get their blocks back.
var ErrPunchUnsupported = errors.New("store: punch hole unsupported on this platform")

func punchHole(f *os.File, off, length int64) error {
	return ErrPunchUnsupported
}

const punchSupported = false
