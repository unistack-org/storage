//go:build !linux

package store

import "os"

// openFile opens an object file. O_DIRECT has no portable equivalent; on
// darwin we approximate it with F_NOCACHE when Direct is requested.
func (s *Store) openFile(p string, create bool) (*os.File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	f, err := os.OpenFile(p, flags, 0o644)
	if err != nil {
		return nil, err
	}
	if s.opts.Direct {
		setNocache(f)
	}
	return f, nil
}

// grow extends the file to size bytes. darwin has no fallocate();
// Preallocate is a no-op there (sparse-by-default apfs/ufs).
func (s *Store) grow(f *os.File, size uint64) error {
	return f.Truncate(int64(size))
}
