package nbd

import (
	"errors"
	"fmt"
	"io"

	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/store"
)

// StoreBackend is a Backend backed by the local object store
// (github.com/unistack-org/storage/pkg/store): the volume is a flat
// range of fixed-size objects, exactly the addressing model the cluster
// data path will use. It is the phase-1 single-node data path behind NBD.
//
// Object OID = object index + 1 (OID 0 is reserved as NilOID).
type StoreBackend struct {
	st   *store.Store
	size uint64
}

// NewStoreBackend opens (creating if needed) the object store directory
// at dir and exposes size bytes as one volume.
func NewStoreBackend(dir string, size uint64, opts store.Options) (*StoreBackend, error) {
	if object.NumObjects(size) == 0 {
		return nil, fmt.Errorf("store backend: volume size must be > 0")
	}
	st, err := store.New(dir, opts)
	if err != nil {
		return nil, err
	}
	if err := st.Load(); err != nil {
		st.Close()
		return nil, err
	}
	return &StoreBackend{st: st, size: size}, nil
}

func (b *StoreBackend) Size() uint64 { return b.size }

// ReadAt reads across object boundaries, forwarding the caller's buffer
// to the store without copying.
func (b *StoreBackend) ReadAt(p []byte, off int64) (int, error) {
	return b.doIO(p, off, false)
}

// WriteAt writes across object boundaries; objects are created lazily.
func (b *StoreBackend) WriteAt(p []byte, off int64) (int, error) {
	return b.doIO(p, off, true)
}

func (b *StoreBackend) doIO(p []byte, off int64, write bool) (int, error) {
	if off < 0 || uint64(off) >= b.size {
		return 0, io.EOF
	}
	if uint64(off)+uint64(len(p)) > b.size {
		p = p[:b.size-uint64(off)] // tail clip; NBD never sends this
	}
	var done int
	for len(p) > 0 {
		idx, inObj := object.Split(uint64(off) + uint64(done))
		n := uint64(len(p))
		if room := object.Size - inObj; n > room {
			n = room
		}
		oid := object.OID(idx + 1)
		var (
			written int
			err     error
		)
		if write {
			written, err = b.st.WriteAt(oid, p[:n], int64(inObj))
		} else {
			written, err = b.st.ReadAt(oid, p[:n], int64(inObj))
			if errors.Is(err, store.ErrNoSuchObject) {
				// A never-written (or trimmed) object is a hole: the
				// volume reads it as zeroes, like a sparse file.
				for i := range p[:n] {
					p[i] = 0
				}
				done += int(n)
				p = p[n:]
				continue
			}
		}
		done += written
		p = p[written:]
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

// Flush persists every cached object file.
func (b *StoreBackend) Flush() error { return b.st.Sync() }

// Trim punches a hole over [off, off+length) object by object. On
// platforms without hole punching it reports ErrTrimUnsupported, which
// the server maps to a legal NBD "not supported" reply.
func (b *StoreBackend) Trim(off int64, length uint32) error {
	return b.punch(off, int64(length))
}

func (b *StoreBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	if punchHole {
		if err := b.punch(off, int64(length)); err == nil {
			return nil
		} else if err != ErrTrimUnsupported {
			return err
		}
		// fall through to zero-writing
	}
	return b.writeZeroes(off, int64(length))
}

// Close releases the store.
func (b *StoreBackend) Close() error { return b.st.Close() }

func (b *StoreBackend) writeZeroes(off, length int64) error {
	if off < 0 || uint64(off) >= b.size {
		return io.EOF
	}
	if uint64(off)+uint64(length) > b.size {
		length = int64(b.size - uint64(off))
	}
	for length > 0 {
		n := length
		if n > int64(len(zeros)) {
			n = int64(len(zeros))
		}
		if _, err := b.doIO(zeros[:n], off, true); err != nil {
			return err
		}
		off += n
		length -= n
	}
	return nil
}

func (b *StoreBackend) punch(off, length int64) error {
	if off < 0 || uint64(off) >= b.size {
		return io.EOF
	}
	if uint64(off)+uint64(length) > b.size {
		length = int64(b.size - uint64(off))
	}
	for length > 0 {
		idx, inObj := object.Split(uint64(off))
		n := length
		if room := int64(object.Size - inObj); n > room {
			n = room
		}
		f, err := b.st.Lookup(object.OID(idx + 1))
		if err != nil {
			return err
		}
		if err := punchRange(f, int64(inObj), n); err != nil {
			return err
		}
		off += n
		length -= n
	}
	return nil
}

var _ Backend = (*StoreBackend)(nil)
