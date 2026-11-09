// Package journal is an append-only write-ahead log for object metadata,
// the seed of Vitastor-style journaling.
//
// Record layout (all big-endian):
//
//	seq   u64   strictly increasing, starts at 1
//	len   u32   payload length
//	crc   u32   CRC-32C (Castagnoli) of payload
//	payload     len bytes
//
// Replay reads records in order and stops at the first torn/corrupt
// record: a torn tail is a normal crash artifact, everything before it
// is durable. A trailing full-16B header with an incomplete payload is
// also treated as torn, never as a record.
package journal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"syscall"
)

// HeaderSize is the per-record header length.
const HeaderSize = 16

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned when a record is present but fails its CRC.
var ErrCorrupt = errors.New("journal: corrupt record")

// ErrNoSpace means the journal's filesystem refused the append. It wraps
// the raw errno so callers classify it with errors.Is(err, syscall.ENOSPC)
// and the OSD maps it onto StatusNoSpace.
//
// A journal has no room to absorb a full disk: unlike an object write,
// where the caller loses one 4 KiB chunk, a journal that fills mid-append
// leaves the log position unknown. So the journal treats ENOSPC as fatal
// for itself — see Failed — and refuses every later append instead of
// appending into a buffer whose flush is guaranteed to fail. That is the
// Vitastor lesson: the journal must never be allowed to fill, and when it
// does, writes must stop loudly, not continue on a corrupt prefix.
var ErrNoSpace = fmt.Errorf("journal: no space left on device: %w", syscall.ENOSPC)

// journalFailed is the sticky cause recorded on an ENOSPC failure; every
// later call returns it, so the reason is not lost after the first one.
var errPoisonedPrefix = errors.New("journal: poisoned by an earlier write failure")

// Journal is a durable append-only log. Append is safe for concurrent
// use; records are serialised in seq order.
type Journal struct {
	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	path string
	seq  uint64
	// failed is set once a write reaches the file and the filesystem
	// refused it (ENOSPC). From then on the buffered content cannot be
	// trusted to be durable, so the journal poisons: every Append and
	// Flush/Sync returns the original cause.
	failed error
}

// Failed reports the fatal journal error (currently ENOSPC), nil while
// healthy. Callers use it to mark the OSD read-only / refuse writes:
// a dead journal means the node must stop taking writes.
func (j *Journal) Failed() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.failed
}

// Open opens (creating if needed) the journal at path and recovers the
// sequence number from the durable records.
func Open(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	j := &Journal{f: f, path: path}
	// Position at the end of the last good record and learn the seq.
	_, lastSeq, end, err := scan(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Seek(int64(end), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	j.seq = lastSeq
	j.w = bufio.NewWriterSize(f, 64<<10)
	return j, nil
}

// Append writes one record and returns its sequence number. The data is
// copied into the journal buffer; Sync (or Flush+Sync) makes it durable.
// Once the journal has failed (Failed != nil) Append refuses immediately:
// the buffer behind it cannot become durable.
func (j *Journal) Append(payload []byte) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return 0, j.failed
	}
	j.seq++
	var hdr [HeaderSize]byte
	binary.BigEndian.PutUint64(hdr[0:8], j.seq)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[12:16], crc32.Checksum(payload, crcTable))
	if _, err := j.w.Write(hdr[:]); err != nil {
		return 0, j.poison(err)
	}
	if _, err := j.w.Write(payload); err != nil {
		return 0, j.poison(err)
	}
	return j.seq, nil
}

// poison records a write failure as the journal's fatal cause. bufio
// reaches the file only when its buffer fills, Flush, or Sync run, so a
// plain Append error is rare; when it happens it is the sticky bufio
// error and equally fatal. ENOSPC is reported as ErrNoSpace.
// Callers hold j.mu.
func (j *Journal) poison(err error) error {
	if j.failed == nil {
		if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
			err = fmt.Errorf("%w (%s)", ErrNoSpace, j.path)
		}
		j.failed = err
	}
	return j.failed
}

// Flush moves buffered records to the OS (visible to Replay after a
// process crash, not to a machine crash). A failed Flush poisons the
// journal: the log position is no longer known, so appends stop.
func (j *Journal) Flush() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if err := j.w.Flush(); err != nil {
		return j.poison(err)
	}
	return nil
}

// Sync flushes and fsyncs: durable to machine crash. Like Flush, a
// failure poisons the journal (ENOSPC surfaces here: bufio only reaches
// the file when its 64 KiB buffer fills).
func (j *Journal) Sync() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return j.failed
	}
	if err := j.w.Flush(); err != nil {
		return j.poison(err)
	}
	if err := j.f.Sync(); err != nil {
		return j.poison(err)
	}
	return nil
}

// Replay feeds every durable record to fn in seq order, stopping at the
// first torn record. It returns the number of records replayed.
func (j *Journal) Replay(fn func(seq uint64, payload []byte) error) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		return 0, err
	}
	n, _, _, err := scanTo(j.f, fn)
	return n, err
}

// scan walks f from the start, invoking fn when provided (nil = count only).
// It returns the record count, the last good seq, and the byte offset just
// past the last good record.
func scan(f *os.File) (int, uint64, uint64, error) { return scanTo(f, nil) }

func scanTo(f *os.File, fn func(seq uint64, payload []byte) error) (int, uint64, uint64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, 0, err
	}
	r := bufio.NewReaderSize(f, 64<<10)
	var (
		count int
		prev  uint64
		off   uint64
	)
	var hdr [HeaderSize]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return count, prev, off, nil // clean EOF or torn header: durable prefix is all
			}
			return count, prev, off, err
		}
		seq := binary.BigEndian.Uint64(hdr[0:8])
		length := binary.BigEndian.Uint32(hdr[8:12])
		want := binary.BigEndian.Uint32(hdr[12:16])
		if length > 16<<20 || (prev != 0 && seq != prev+1) {
			return count, prev, off, nil // implausible: treat as torn tail
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return count, prev, off, nil // torn payload
			}
			return count, prev, off, err
		}
		if crc32.Checksum(payload, crcTable) != want {
			return count, prev, off, ErrCorrupt
		}
		if fn != nil {
			if err := fn(seq, payload); err != nil {
				return count, prev, off, err
			}
		}
		count++
		prev = seq
		off += HeaderSize + uint64(length)
	}
}

// ReplayFile replays the durable records of the journal at path.
func ReplayFile(path string, fn func(seq uint64, payload []byte) error) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, _, _, err := scanTo(f, fn)
	return n, err
}

// Truncate discards all (synced) records and restarts the sequence from
// `kept` — call after the state has been checkpointed and `kept` durable
// elsewhere.
func (j *Journal) Truncate(kept uint64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		return err
	}
	tmp := j.path + ".tmp"
	tf, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := j.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, j.path); err != nil {
		return err
	}
	f, err := os.OpenFile(j.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	j.f = f
	j.w = bufio.NewWriterSize(f, 64<<10)
	j.seq = kept
	return nil
}

// Close flushes and closes.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.w.Flush(); err != nil {
		j.f.Close()
		return err
	}
	return j.f.Close()
}
