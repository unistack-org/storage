//go:build linux

package nbd

import (
	"errors"
	"io"
	"os"
	"syscall"
)

// FileBackend answers base:allocation from the filesystem's own allocation
// map via lseek(SEEK_DATA/SEEK_HOLE), which Linux has supported since 3.1
// on ext4/xfs/btrfs (and reports a fully-allocated file as one data
// extent). Granularity is the filesystem block size.

// BlockStatus reports hole/data extents of the sparse file.
func (b *FileBackend) BlockStatus(off int64, length uint32) ([]Extent, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.f == nil {
		return nil, os.ErrClosed
	}
	return fileExtents(b.f, off, length)
}

// seekUnsupported reports whether err means "this filesystem implements
// neither SEEK_DATA nor SEEK_HOLE" (ENOTSUP/EOPNOTSUPP, EINVAL, EPERM), in
// which case we conservatively answer "all allocated" instead of failing
// the BLOCK_STATUS command. ENXSYS (88) at offset 0 is the opposite: the
// file has no holes at all, which the caller handles from the seek result.
// Linux lseek(2) whence values (syscall pkg does not export them).
const (
	seekData = 3
	seekHole = 4
)

func seekUnsupported(err error) bool {
	return errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.EPERM)
}

// fileExtents walks [off, off+length) with SEEK_DATA/SEEK_HOLE pairs.
func fileExtents(f *os.File, off int64, length uint32) ([]Extent, error) {
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	if off >= size {
		return nil, syscall.EINVAL
	}
	end := off + int64(length)
	if end > size {
		end = size
	}
	pos := off
	// Data state at pos: start by looking for the first hole at/after pos.
	inData := true
	h, herr := f.Seek(pos, seekHole)
	if herr != nil {
		if seekUnsupported(herr) {
			return []Extent{{Length: uint32(end - off)}}, nil
		}
		// pos is past the last data: the rest is a hole to EOF.
		if errors.Is(herr, syscall.ENXIO) {
			inData = false
			h = size
		} else {
			return nil, herr
		}
	}
	if h >= end {
		if inData {
			return []Extent{{Length: uint32(end - off)}}, nil
		}
		return []Extent{{Length: uint32(end - off), Flags: nbdStateHole | nbdStateZero}}, nil
	}
	// A hole starts at h: everything before it (if any) is data.
	var ext []Extent
	if h > pos {
		ext = append(ext, Extent{Length: uint32(h - pos)})
	}
	pos = h
	for pos < end {
		// pos is a hole start: find where data resumes.
		d, derr := f.Seek(pos, seekData)
		if derr != nil {
			if errors.Is(derr, syscall.ENXIO) {
				d = size // no data left: hole runs to EOF
			} else if seekUnsupported(derr) {
				// Conservative: treat the remainder as allocated.
				if len(ext) > 0 {
					return append(ext, Extent{Length: uint32(end - pos)}), nil
				}
				return []Extent{{Length: uint32(end - off)}}, nil
			} else {
				return nil, derr
			}
		}
		if d > end {
			d = end
		}
		if d > pos {
			ext = append(ext, Extent{Length: uint32(d - pos), Flags: nbdStateHole | nbdStateZero})
		}
		pos = d
		if pos >= end {
			break
		}
		// pos is a data start: find where the next hole begins.
		h, herr = f.Seek(pos, seekHole)
		if herr != nil {
			if errors.Is(herr, syscall.ENXIO) {
				h = size
			} else if seekUnsupported(herr) {
				ext = append(ext, Extent{Length: uint32(end - pos)})
				return ext, nil
			} else {
				return nil, herr
			}
		}
		if h > end {
			h = end
		}
		if h > pos {
			ext = append(ext, Extent{Length: uint32(h - pos)})
		}
		pos = h
	}
	return ext, nil
}
