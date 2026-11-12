//go:build !linux

package store

import "errors"

var errAtomicUnsupported = errors.New("store: atomic writes unsupported on this platform")

// deviceForPath: no sysfs off Linux; ProbeDisk reports Path only.
// probeDiskImpl: no sysfs off Linux; the report stays Path-only.
func probeDiskImpl(string, *DiskAtomicInfo) error { return nil }

// atomicGeometry: no atomic-write interface off Linux.
func (s *Store) atomicGeometry() (atomicMax, atomicUnit int64) { return 0, 0 }

// atomicWrite is unreachable off Linux (atomicWritePossible is always
// false there); it exists so the portable WriteAt compiles.
func (s *Store) atomicWrite(e *entry, p []byte, off int64) (int, error) {
	return 0, errAtomicUnsupported
}
