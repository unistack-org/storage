// Package store is the single-node local object store: every object is one
// file in a directory, addressed by its 64-bit OID.
//
// Design notes (phase 1 of the roadmap):
//
//   - One file per object, flat layout, 16-digit lowercase hex names. Flat
//     directories are fine up to millions of entries on modern filesystems;
//     Ceph-style sharded prefixes can be added later without changing API.
//   - Hot path (ReadAt/WriteAt) uses cached *os.File handles and
//     File.Pread/Pwrite: no allocations, no copies, fully concurrent.
//   - Objects are created lazily on first write and preallocated to the
//     full object size so thin-provisioning surprises stay visible.
//   - O_DIRECT is requested when the platform supports it (Linux); on
//     other platforms the store transparently falls back to the page cache.
package store

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sync"

	"github.com/unistack-org/storage/pkg/object"
)

// ErrObjectTooSmall is returned when an I/O runs past the object size.
var ErrObjectTooSmall = errors.New("store: io beyond object size")

// ErrNoSuchObject is returned when an object does not exist. It wraps
// fs.ErrNotExist so callers (and the network layer's status mapping) can
// classify it with errors.Is(err, os.ErrNotExist).
var ErrNoSuchObject = fmt.Errorf("store: no such object: %w", fs.ErrNotExist)

// punchAlign is the granularity we punch at. The kernel requires
// FALLOC_FL_PUNCH_HOLE ranges to be aligned to the filesystem block size;
// 4096 is the largest block size we expect to meet (and the common one), so
// we align to it and zero-fill any remainder instead of failing. Guest
// discard requests are at least 512 B aligned and object boundaries are
// 4 MiB, so in practice the aligned region covers the whole request.
const punchAlign = 4096

// Options configures a Store.
type Options struct {
	// Direct requests O_DIRECT (Linux) / F_NOCACHE (darwin) on object
	// files. The store falls back to buffered I/O when unsupported.
	Direct bool
	// Preallocate fallocates each newly created object file to the full
	// object size.
	Preallocate bool
}

// Store is a directory of object files. It is safe for concurrent use.
type Store struct {
	dir  string
	opts Options

	mu    sync.RWMutex
	files map[object.OID]*entry
	usage int64 // sum of file sizes of known objects
}

type entry struct {
	f    *os.File
	size int64
}

// New opens (creating if needed) the store directory.
func New(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir, opts: opts, files: map[object.OID]*entry{}}, nil
}

func (s *Store) path(oid object.OID) string {
	return path.Join(s.dir, fmt.Sprintf("%016x", uint64(oid)))
}

// open returns the cached handle for oid, creating the object on first
// write when create is true.
func (s *Store) open(oid object.OID, create bool) (*entry, error) {
	s.mu.RLock()
	e := s.files[oid]
	s.mu.RUnlock()
	if e != nil {
		return e, nil
	}
	if !create {
		// Check existence on disk without caching.
		if _, err := os.Stat(s.path(oid)); err != nil {
			return nil, ErrNoSuchObject
		}
	}
	return s.openSlow(oid, create)
}

func (s *Store) openSlow(oid object.OID, create bool) (*entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.files[oid]; e != nil { // lost the race, someone cached it
		return e, nil
	}
	p := s.path(oid)
	f, err := s.openFile(p, create)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if create && st.Size() < int64(object.Size) {
		if err := s.grow(f, object.Size); err != nil {
			f.Close()
			return nil, err
		}
		st, err = f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	e := &entry{f: f, size: st.Size()}
	s.files[oid] = e
	s.usage += e.size
	return e, nil
}

// ReadAt reads len(p) bytes from object oid at byte offset off.
// It is allocation-free for cached objects.
func (s *Store) ReadAt(oid object.OID, p []byte, off int64) (int, error) {
	if off+int64(len(p)) > int64(object.Size) {
		return 0, ErrObjectTooSmall
	}
	e, err := s.open(oid, false)
	if err != nil {
		return 0, err
	}
	return e.f.ReadAt(p, off)
}

// WriteAt writes p into object oid at byte offset off, creating the object
// on first write. Writes never leave the object bounds.
func (s *Store) WriteAt(oid object.OID, p []byte, off int64) (int, error) {
	if off+int64(len(p)) > int64(object.Size) {
		return 0, ErrObjectTooSmall
	}
	e, err := s.open(oid, true)
	if err != nil {
		return 0, err
	}
	n, err := e.f.WriteAt(p, off)
	// A write past EOF grew the object.
	s.mu.Lock()
	if grow := off + int64(n) - e.size; grow > 0 {
		e.size += grow
		s.usage += grow
	}
	s.mu.Unlock()
	return n, err
}

// WriteZeroes makes [off, off+n) read as zeroes. When sparse is set and the
// platform supports hole punching, the block-aligned interior of the range
// is punched (fallocate(FALLOC_FL_PUNCH_HOLE|KEEP_SIZE)) so the blocks are
// genuinely returned to the filesystem — this is what makes guest
// TRIM/DISCARD reclaim space on the NVMe. The sub-block head and tail
// (possible when a guest discards at 512 B granularity) are zero-filled, and
// so is the whole range on platforms without punch support (darwin) or when
// the punch syscall fails (a filesystem without FALLOC_FL_PUNCH_HOLE,
// EPERM, ...): the store degrades to a zero-fill, which is always correct.
//
// The object keeps its full logical size in every mode: the file's apparent
// size never shrinks, so a later write into a punched region behaves exactly
// like a write into a hole. Usage accounting stays logical (sum of object
// sizes) and is deliberately not decremented — punched bytes free
// *physical* blocks, which is a filesystem property reported by Free, not a
// change in how much of the volume is addressed.
func (s *Store) WriteZeroes(oid object.OID, off, n uint64, sparse bool) error {
	if off+n > uint64(object.Size) {
		return ErrObjectTooSmall
	}
	if n == 0 {
		return nil
	}
	e, err := s.open(oid, true)
	if err != nil {
		return err
	}
	if sparse && punchSupported {
		if head, tail, ok := alignRange(off, n, punchAlign); ok {
			if perr := punchHole(e.f, int64(off+head), int64(tail-head)); perr == nil {
				// Zero the unaligned head/tail edges around the hole.
				if err := s.zeroFill(e, off, head); err != nil {
					return err
				}
				return s.zeroFill(e, off+tail, n-tail)
			}
			// Punch failed: fall through to a zero-fill. Correctness
			// first, reclaim is a bonus.
		}
	}
	return s.zeroFill(e, off, n)
}

// zeroFill writes n zeroes at off through the cached handle. It never
// touches usage: a zero-fill over existing blocks changes no allocation
// (the object is fixed-size), and over a hole it allocates exactly the
// bytes the kernel zeroes.
func (s *Store) zeroFill(e *entry, off, n uint64) error {
	if n == 0 {
		return nil
	}
	buf := zeroBuf()
	for written := uint64(0); written < n; {
		ch := uint64(len(buf))
		if n-written < ch {
			ch = n - written
		}
		if _, err := e.f.WriteAt(buf[:ch], int64(off+written)); err != nil {
			return err
		}
		written += ch
	}
	return nil
}

// alignRange splits [off, off+n) into the length of the unaligned head and
// the end of the aligned interior (both offsets relative to off); ok=false
// when nothing is block-aligned. The interior is [off+head, off+tail).
func alignRange(off, n, align uint64) (head, tail uint64, ok bool) {
	start := (off + align - 1) &^ (align - 1)
	end := (off + n) &^ (align - 1)
	if start >= end {
		return 0, 0, false
	}
	return start - off, end - off, true
}

var (
	zeroOnce sync.Once
	zeros    []byte
)

func zeroBuf() []byte {
	zeroOnce.Do(func() { zeros = make([]byte, 1<<20) })
	return zeros
}

// Remove deletes an object.
func (s *Store) Remove(oid object.OID) error {
	s.mu.Lock()
	e := s.files[oid]
	delete(s.files, oid)
	s.usage -= e.sizeLookup()
	s.mu.Unlock()
	if e != nil {
		e.f.Close()
	}
	if err := os.Remove(s.path(oid)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// sizeLookup must be called under s.mu; entry size is only mutated under it.
func (e *entry) sizeLookup() int64 {
	if e == nil {
		return 0
	}
	return e.size
}

// Lookup returns the cached handle for an existing object (creating it
// when needed) so callers can perform file-level operations (hole
// punching, fcntl) outside the ReadAt/WriteAt API.
func (s *Store) Lookup(oid object.OID) (*os.File, error) {
	e, err := s.open(oid, true)
	if err != nil {
		return nil, err
	}
	return e.f, nil
}

// Sync flushes every cached object file to stable storage.
func (s *Store) Sync() error {
	s.mu.RLock()
	entries := make([]*entry, 0, len(s.files))
	for _, e := range s.files {
		entries = append(entries, e)
	}
	s.mu.RUnlock()
	for _, e := range entries {
		if err := e.f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Usage returns the total bytes stored across all known objects.
func (s *Store) Usage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usage
}

// Close syncs and closes all cached files.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for oid, e := range s.files {
		if err := e.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := e.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.files, oid)
	}
	return firstErr
}

// Load rescans the directory so Usage reflects pre-existing objects
// without opening them all.
func (s *Store) Load() error {
	des, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, de := range des {
		if de.IsDir() {
			continue
		}
		var oid uint64
		if _, err := fmt.Sscanf(de.Name(), "%016x", &oid); err != nil {
			continue // not an object file
		}
		if _, ok := s.files[object.OID(oid)]; ok {
			continue // already accounted
		}
		st, err := de.Info()
		if err != nil {
			return err
		}
		s.usage += st.Size()
	}
	return nil
}
