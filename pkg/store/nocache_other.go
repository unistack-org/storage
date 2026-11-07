//go:build !darwin

package store

import "os"

// setNocache is a no-op on platforms without F_NOCACHE.
func setNocache(f *os.File) {}
