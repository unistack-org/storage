package nbd

import (
	"errors"
	"io"
	"os"
	"sync"
)

// Backend is the block device abstraction an NBD export sits on top of.
//
// ReadAt/WriteAt must be safe for concurrent use. The hot path must not
// allocate: callers pass pooled buffers and the implementation must forward
// them to the kernel without copying (os.File.Pread/Pwrite qualify).
type Backend interface {
	// Size returns the export size in bytes.
	Size() uint64
	// ReadAt reads len(p) bytes at byte offset off.
	ReadAt(p []byte, off int64) (int, error)
	// WriteAt writes len(p) bytes at byte offset off.
	WriteAt(p []byte, off int64) (int, error)
	// Flush persists all previously written data.
	Flush() error
	// Trim marks [off, off+length) as sparse/unwritten (TRIM command).
	Trim(off int64, length uint32) error
	// WriteZeroes makes [off, off+length) read as zeroes; if punchHole is
	// set the server may punch a sparse hole, otherwise it must write
	// actual zeroes.
	WriteZeroes(off int64, length uint32, punchHole bool) error
	// Close releases resources.
	Close() error
}

// Extent is one entry of an NBD_CMD_BLOCK_STATUS reply: Length bytes
// starting at the queried position carrying Flags (nbdStateHole /
// nbdStateZero bits). Extents are contiguous and their lengths sum to the
// requested range.
type Extent struct {
	Length uint32
	Flags  uint32
}

// StatusReporter is an optional Backend capability: the export can answer
// allocation queries (NBD_CMD_BLOCK_STATUS / base:allocation metadata
// context). The server advertises the context only when the export backend
// implements this interface; backends that answer from a cheap
// presence/absence model (object stores) are good candidates, backends with
// no hole information at all (plain buffers) simply don't implement it.
type StatusReporter interface {
	// BlockStatus reports allocation of [off, off+length) as contiguous
	// extents whose flags use the nbdStateHole / nbdStateZero bits.
	BlockStatus(off int64, length uint32) ([]Extent, error)
}

// FileBackend is a sparse-file Backend backed by a single regular file.
// It is the MVP storage target and the reference implementation for
// higher-level backends (CRUSH-mapped volumes, journal, etc.).
type FileBackend struct {
	mu   sync.RWMutex
	f    *os.File
	size uint64
}

// NewFileBackend opens (creating if needed) a sparse file of the given size.
func NewFileBackend(path string, size uint64) (*FileBackend, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	cur := uint64(st.Size())
	if cur < size {
		if err := f.Truncate(int64(size)); err != nil {
			f.Close()
			return nil, err
		}
		cur = size
	}
	return &FileBackend{f: f, size: cur}, nil
}

// Size reports the live file size. The file can be grown out-of-band
// (e.g. `truncate -s +1G disk.img` to resize a single-node export), and
// the NBD server reads Size() per connection at handshake, so a grown
// file is served with its new capacity after the guest reconnects (or
// rescans) the device. If the stat fails (closed file) the cached size
// is returned.
func (b *FileBackend) Size() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.f == nil {
		return b.size
	}
	if st, err := b.f.Stat(); err == nil && st.Size() > 0 {
		return uint64(st.Size())
	}
	return b.size
}

func (b *FileBackend) ReadAt(p []byte, off int64) (int, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.f.ReadAt(p, off)
}

func (b *FileBackend) WriteAt(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.f.WriteAt(p, off)
}

func (b *FileBackend) Flush() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.f.Sync()
}

func (b *FileBackend) Trim(off int64, length uint32) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.punchHole(off, int64(length))
}

func (b *FileBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if punchHole {
		return b.punchHole(off, int64(length))
	}
	// Zero-write via zero-filled pages: the kernel's /dev/zero read() into
	// the file offset avoids a user-space zero buffer allocation.
	zf, err := os.Open("/dev/zero")
	if err != nil {
		return err
	}
	defer zf.Close()
	var offSrc int64
	_, err = io.Copy(b.newWriterAt(off), io.NewSectionReader(zf, offSrc, int64(length)))
	return err
}

func (b *FileBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.f != nil {
		// Flush the page cache for this file before releasing it so a
		// graceful daemon exit is durable; the error is kept, the
		// close is always attempted.
		serr := b.f.Sync()
		err := b.f.Close()
		b.f = nil
		if serr != nil {
			return serr
		}
		return err
	}
	return nil
}

// punchHole makes the range read as zeroes: the aligned interior is punched
// (Linux fallocate, darwin fcntl(F_PUNCHHOLE)), the edges are zero-filled. On
// platforms with no punch primitive at all it returns ErrTrimUnsupported,
// which the Server maps to NBD's EOPNOTSUPP/ENOTSUP — a legal NBD response.
func (b *FileBackend) punchHole(off, length int64) error {
	return punchRange(b.f, off, length)
}

// ErrTrimUnsupported is returned by platforms lacking hole-punch support.
var ErrTrimUnsupported = errors.New("punch hole unsupported on this platform")

func (b *FileBackend) newWriterAt(off int64) io.Writer {
	return &offsetWriter{f: b.f, off: off}
}

type offsetWriter struct {
	f   *os.File
	off int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}
