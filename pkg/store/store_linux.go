//go:build linux

package store

import (
	"os"
	"syscall"
)

// openFile opens an object file, applying O_DIRECT when requested.
func (s *Store) openFile(p string, create bool) (*os.File, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	if s.opts.Direct {
		flags |= syscall.O_DIRECT
	}
	if s.opts.CommitMode == CommitSync {
		flags |= syscall.O_SYNC
	}
	return os.OpenFile(p, flags, 0o644)
}

// grow preallocates (fallocate) the file to size bytes.
func (s *Store) grow(f *os.File, size uint64) error {
	if !s.opts.Preallocate {
		return f.Truncate(int64(size))
	}
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, int64(size)); err != nil {
		// Filesystems without fallocate (rare): fall back to truncate.
		return f.Truncate(int64(size))
	}
	// fallocate extends the file size to size, keep parity with Stat.
	st, err := f.Stat()
	if err == nil && uint64(st.Size()) < size {
		return f.Truncate(int64(size))
	}
	return nil
}
