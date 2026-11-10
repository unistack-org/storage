//go:build !linux

package nbd

import (
	"io"
	"os"
	"syscall"
)

// Outside Linux the SEEK_DATA/SEEK_HOLE pair is unreliable: on darwin
// lseek(SEEK_DATA/SEEK_HOLE) fails with ENXIO (not implemented) and the
// F_SEARCHDATA/F_SEARCHOLE fcntls are broken on APFS (F_SEARCHOLE returns
// EPERM, F_SEARCHDATA reports every offset as data). macOS hole info is
// therefore omitted: FileBackend answers every range as allocated, which
// keeps the base:allocation context truthful (no false holes).

// FileBackend keeps the StatusReporter interface on all platforms so the
// server code paths (and tests) are platform-independent.
func (b *FileBackend) BlockStatus(off int64, length uint32) ([]Extent, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.f == nil {
		return nil, os.ErrClosed
	}
	size, err := b.f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if off >= size {
		return nil, syscall.EINVAL
	}
	if off+int64(length) > size {
		length = uint32(size - off)
	}
	return []Extent{{Length: length}}, nil
}
