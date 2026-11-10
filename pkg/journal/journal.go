// Package journal is an append-only write-ahead log for object metadata,
// the seed of Vitastor-style journaling.
//
// Record layout (all big-endian):
//
//	seq   u64   strictly increasing, starts at 1
//	len   u32   payload length (an outer record; see packing below)
//	crc   u32   CRC-32C (Castagnoli) of payload
//	payload     len bytes
//
// Replay reads records in order and stops at the first torn/corrupt
// record: a torn tail is a normal crash artifact, everything before it
// is durable. A trailing full-16B header with an incomplete payload is
// also treated as torn, never as a record.
//
// Packing (ideas §2.4). A packed writer groups fixed-size records under
// one outer header: seq has bit 63 set, and the outer payload is
//
//	count   u32   items in the batch
//	stride  u32   item byte length (fixed for the journal)
//	first   u64   seq of the first item
//	items         count x payload (stride bytes each)
//
// An item's seq is first+index: AppendPacked assigns seqs in the lock, so
// positions carry the sequence and no per-item header is needed. The
// scanner checks the outer seq (first-1) and the batch length, so a lost
// or reordered batch is caught, while items inside a batch are exactly
// what was appended. One CRC covers the batch, which is sound: a batch is
// fsynced as a unit (Flush/Sync/Close seal any partial batch first), so
// durability is all-of-batch or none-of-it, exactly the torn-tail rule
// the scanner already applies. The win is header density: a 24 B record
// costs 16 B per record unpacked (67% overhead) and 32 B per ~2700 B
// batch packed (~1.2%).
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
	// Packed batches (see the package comment): pack holds the sealed
	// outer header plus the items appended so far, packFirst is the seq
	// of the first item, packCount counts them. A batch is sealed (and
	// written to the buffer) by Append (plain), Flush, Sync, Close and
	// Truncate, so durability is all-of-batch — matching the fsync unit.
	pack      []byte
	packFirst uint64
	packCount int
	itemSize  int // >0 when the journal was opened with OpenPacked
	// failed is set once a write reaches the file and the filesystem
	// refused it (ENOSPC). From then on the buffered content cannot be
	// trusted to be durable, so the journal poisons: every Append and
	// Flush/Sync returns the original cause.
	failed error
}

// packedFlag marks an outer record that groups fixed-size items (see the
// package comment). Real sequence numbers never come near 2^63.
const (
	packedFlag = 1 << 63
	packedHdr  = 16 // count u32 + item_size u32 + first_seq u64, the outer payload prefix
	packTarget = 64 << 10
)

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
func Open(path string) (*Journal, error) { return open(path, 0) }

// OpenPacked opens a journal whose records are packed: every record must
// be exactly itemSize bytes (AppendPacked enforces it), so a batch needs
// no per-item length field beyond the shared one. Small-record logs
// (intent records, meta deltas) get near-zero header overhead.
func OpenPacked(path string, itemSize int) (*Journal, error) {
	if itemSize <= 0 {
		return nil, errors.New("journal: itemSize must be positive")
	}
	return open(path, itemSize)
}

func open(path string, itemSize int) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	j := &Journal{f: f, path: path, itemSize: itemSize}
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
// the buffer behind it cannot become durable. A partial packed batch is
// sealed first: durability (Flush/Sync) is per outer record, so a batch
// must never straddle a durability boundary.
func (j *Journal) Append(payload []byte) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return 0, j.failed
	}
	if err := j.seal(); err != nil {
		return 0, j.poison(err)
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

// AppendPacked writes one fixed-size record into the current packed
// batch (see the package comment) and returns its sequence number. The
// batch is written to the journal buffer under one outer header when it
// reaches packTarget bytes, and always sealed before Flush/Sync/Close,
// so a fsync covers whole batches. Requires OpenPacked; the payload must
// be exactly the item size.
func (j *Journal) AppendPacked(payload []byte) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed != nil {
		return 0, j.failed
	}
	if j.itemSize <= 0 || len(payload) != j.itemSize {
		return 0, fmt.Errorf("journal: AppendPacked: item size %d, got %d", j.itemSize, len(payload))
	}
	stride := j.itemSize
	if j.packCount == 0 {
		// The outer header carries the last pre-batch seq (the scanner
		// continuity-checks it); the batch body carries the first item's
		// seq. This also covers a batch that starts at seq 1 (outer 0).
		j.packFirst = j.seq
		capacity := HeaderSize + packedHdr + ((packTarget+stride-1)/stride)*stride
		j.pack = make([]byte, HeaderSize+packedHdr, capacity)
		binary.BigEndian.PutUint32(j.pack[HeaderSize+4:HeaderSize+8], uint32(j.itemSize))
	}
	j.seq++
	if j.packCount == 0 {
		binary.BigEndian.PutUint64(j.pack[HeaderSize+8:HeaderSize+16], j.seq) // firstSeq
	}
	off := HeaderSize + packedHdr + j.packCount*stride
	j.pack = j.pack[:off+stride] // capacity is reserved per batch above
	copy(j.pack[off:], payload)
	j.packCount++
	if packedHdr+j.packCount*stride >= packTarget {
		if err := j.seal(); err != nil {
			return 0, j.poison(err)
		}
	}
	return j.seq, nil
}

// seal writes the buffered packed batch (if any) to the journal buffer
// as one outer record. Callers hold j.mu.
func (j *Journal) seal() error {
	if j.packCount == 0 {
		return nil
	}
	stride := j.itemSize
	body := j.pack[HeaderSize : HeaderSize+packedHdr+j.packCount*stride]
	binary.BigEndian.PutUint32(j.pack[HeaderSize:HeaderSize+4], uint32(j.packCount))
	binary.BigEndian.PutUint64(j.pack[0:8], j.packFirst|packedFlag)
	binary.BigEndian.PutUint32(j.pack[8:12], uint32(len(body)))
	binary.BigEndian.PutUint32(j.pack[12:16], crc32.Checksum(body, crcTable))
	_, err := j.w.Write(j.pack[:HeaderSize+len(body)])
	j.pack = nil
	j.packCount = 0
	return err
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
	if err := j.seal(); err != nil {
		return err
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
	if err := j.seal(); err != nil {
		return err
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
	if err := j.seal(); err != nil {
		return 0, err
	}
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
		if length > 16<<20 || (prev != 0 && seq != prev+1 && seq&packedFlag == 0) {
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
		if seq&packedFlag != 0 {
			// Packed batch: verify item continuity and fan out. The
			// outer seq is (first item seq - 1); prev==0 with outer
			// seq 0 is the journal's first batch.
			n := binary.BigEndian.Uint32(payload[0:4])
			stride := binary.BigEndian.Uint32(payload[4:8])
			first := binary.BigEndian.Uint64(payload[8:16])
			if stride == 0 || stride > 16<<20 || uint64(n)*uint64(stride)+16 != uint64(length) ||
				(seq&^packedFlag)+1 != first || (prev != 0 && seq&^packedFlag != prev) {
				return count, prev, off, ErrCorrupt
			}
			body := payload[16:]
			for i := uint32(0); i < n; i++ {
				if fn != nil {
					if err := fn(first+uint64(i), body[i*stride:(i+1)*stride]); err != nil {
						return count, prev, off, err
					}
				}
			}
			count += int(n)
			prev = first + uint64(n) - 1
			off += HeaderSize + uint64(length)
			continue
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

// Seq returns the last assigned sequence number.
func (j *Journal) Seq() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.seq
}

// HardClose closes the file without flushing the buffer: a simulated
// crash for tests (the buffered tail is lost exactly as a power loss
// would lose it). The Journal must not be used afterwards.
func (j *Journal) HardClose() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}

// Close flushes and closes.
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.seal(); err != nil {
		return err
	}
	if err := j.w.Flush(); err != nil {
		j.f.Close()
		return err
	}
	return j.f.Close()
}
