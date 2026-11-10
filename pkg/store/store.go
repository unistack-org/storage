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
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unistack-org/storage/pkg/iouring"
	"github.com/unistack-org/storage/pkg/object"
)

// ErrObjectTooSmall is returned when an I/O runs past the object size.
var ErrObjectTooSmall = errors.New("store: io beyond object size")

// ErrClosed is returned by Sync after Close has drained the coalescer.
var ErrClosed = errors.New("store: store closed")

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

// PunchSupported reports whether this platform can punch holes
// (FALLOC_FL_PUNCH_HOLE). Sparse WriteZeroes degrades to a zero-fill when
// it is false.
const PunchSupported = punchSupported

// Commit modes (Options.CommitMode) — the durability/throughput dial,
// the same idea as Vitastor's immediate_commit:
//
//	CommitNone  writes land in the page cache; durability is the caller's
//	            job (an explicit Sync per FLUSH/FUA). Fastest; a machine
//	            crash loses the acked-but-unflushed tail.
//	CommitFlush every write (and hole punch) fsyncs before returning.
//	            Safe; one fsync per operation.
//	CommitSync  files are opened O_SYNC: the kernel fsyncs each write,
//	            cheaper than an fsync syscall per write.
//	CommitJournal every write is appended to a write-ahead log and the
//	            log is fsynced before the write is acknowledged; a
//	            background applier flattens the log into the objects
//	            lazily (see journal.go). Fastest safe mode for small
//	            random writes: sequential log append instead of random
//	            writes + fsyncs into 4 MiB objects.
const (
	CommitNone    CommitMode = "none"
	CommitFlush   CommitMode = "flush"
	CommitSync    CommitMode = "sync"
	CommitJournal CommitMode = "journal"
)

// CommitMode selects how writes reach durable media.
type CommitMode string

// ParseCommitMode maps a CLI string ("none"/"flush"/"sync"/"journal",
// "" = none) to a CommitMode.
func ParseCommitMode(s string) (CommitMode, error) {
	switch s {
	case "", "none":
		return CommitNone, nil
	case "flush":
		return CommitFlush, nil
	case "sync":
		return CommitSync, nil
	case "journal":
		return CommitJournal, nil
	}
	return "", fmt.Errorf("store: unknown commit mode %q (want none, flush, sync or journal)", s)
}

// Options configures a Store.
type Options struct {
	// Direct requests O_DIRECT (Linux) / F_NOCACHE (darwin) on object
	// files. The store falls back to buffered I/O when unsupported.
	Direct bool
	// CommitMode is one of CommitNone/CommitFlush/CommitSync ("" =
	// CommitNone).
	CommitMode CommitMode
	// FsyncBatch > 0 turns Sync() into a coalescer: concurrent Sync
	// callers share one fsync pass started up to FsyncBatch after the
	// first request arrived (the classic writeback coalescing that turns
	// N fsyncs/second into ~1/FsyncBatch). Ordering is kept: every
	// request is served by a pass that starts *after* the caller's
	// writes were issued, so a Sync that returns has all prior writes
	// durable.
	FsyncBatch time.Duration
	// Preallocate fallocates each newly created object file to the full
	// object size.
	Preallocate bool
	// MinFreeBytes refuses to create new objects when the filesystem has
	// less than this many bytes free (0 = no check). It is a cheap
	// guard-rail that fires *before* the kernel ENOSPC so a full disk is
	// reported as "no space" (StatusNoSpace) with the OSD healthy,
	// instead of mid-write corruption of the journal. With Preallocate
	// the reservation is object.Size, so sizing this >= a few objects is
	// enough on a quiet store.
	MinFreeBytes int64
	// AsyncRing > 0 routes ReadAt/WriteAt through the async I/O ring:
	// io_uring on Linux (kernel >= 5.6), a worker-pool pread/pwrite
	// fallback elsewhere. The value is the ring depth (a good starting
	// point is 128-512). Journal mode always uses the plain path; if the
	// ring cannot be created the store logs it and keeps the plain
	// pread/pwrite path, so enabling AsyncRing never breaks a store.
	AsyncRing int
	// Intent turns on the Write Intent Journal (see intent.go): every
	// write is announced by a durable 24-byte intent record (oid/off/
	// len/crc) before its data, and recovery verifies open intents
	// against the objects — a power-loss damage map without double
	// writes. Valid for the non-journal commit modes (journal mode
	// already has a write-ahead log). Cost: ~1% extra device traffic
	// and, in flush/sync modes, one extra fsync of the tiny intent log
	// per write.
	Intent bool
	// Logf receives informational messages (nil = silent).
	Logf func(format string, args ...any)
}

// Store is a directory of object files. It is safe for concurrent use.
type Store struct {
	dir  string
	opts Options

	mu    sync.RWMutex
	files map[object.OID]*entry
	usage int64 // sum of file sizes of known objects

	// meta is the in-RAM object index (see meta.go): existence,
	// apparent size and (when known) allocated bytes for every object
	// the store has seen. It replaces the per-request os.Stat on the
	// read/Stat path; the disk is consulted only at Load and when an
	// allocation measurement is genuinely needed.
	meta map[object.OID]objMeta

	// fsync coalescer (active when opts.FsyncBatch > 0).
	batchMu     sync.Mutex
	batch       []batchWaiter
	batchTimer  *time.Timer
	batchClosed bool

	// jrn is the write-ahead log state (nil unless CommitMode is
	// CommitJournal; see journal.go).
	jrn *journalState

	// intent is the Write Intent Journal (nil unless Options.Intent;
	// see intent.go).
	intent *intentState

	// ring is the async I/O ring (nil = plain pread/pwrite path).
	ring iouring.Ring

	syncPasses atomic.Int64 // fsync passes performed (Sync + batches), for stats/tests

	// Write-activity counters (see stats.go): cumulative, lock-free.
	clientBytes  atomic.Int64 // guest bytes accepted by WriteAt
	journalBytes atomic.Int64 // bytes appended to the WAL (headers incl.)
	flattenBytes atomic.Int64 // bytes the applier flattened into objects
	fillBytes    atomic.Int64 // bytes of zero-fill writes issued
	punchBytes   atomic.Int64 // bytes punched (TRIM), not device writes
}

type batchWaiter struct {
	done chan error
}

type entry struct {
	f    *os.File
	fd   int32 // cached f.Fd() for ring submissions (0 when ring off)
	size int64
	// dirty counts writes issued since the last successful fsync of
	// this file (0 = clean). Sync skips clean files, which keeps a
	// flush latency proportional to the data written in the window
	// rather than to the whole cache of open objects. Atomic: a sync
	// pass snapshots it, fsyncs, and clears it only if an intervening
	// write has not bumped it.
	dirty atomic.Int64
}

// New opens (creating if needed) the store directory.
func New(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	switch opts.CommitMode {
	case "", CommitNone, CommitFlush, CommitSync, CommitJournal:
	default:
		return nil, fmt.Errorf("store: unknown commit mode %q (want none|flush|sync|journal)", opts.CommitMode)
	}
	s := &Store{
		dir: dir, opts: opts, files: map[object.OID]*entry{},
		meta: map[object.OID]objMeta{},
	}
	if opts.CommitMode == CommitJournal {
		js, err := openJournal(dir, opts.Logf)
		if err != nil {
			return nil, err
		}
		s.jrn = js
		s.startApplier(js)
	} else if opts.Intent {
		// The intent log must recover before the meta index and before
		// any write: its check reads object files directly.
		is, err := openIntentLog(dir, opts.Logf)
		if err != nil {
			return nil, err
		}
		s.intent = is
	}
	// Build the meta index from the directory (one readdir, no per-file
	// stat syscalls). The index is the request-path source of truth for
	// existence/size, so every store must have it loaded at open — an
	// OSD that reopened a data directory would otherwise answer
	// "no such object" for objects it owns.
	if err := s.Load(); err != nil {
		if s.jrn != nil {
			s.journalClose()
		}
		return nil, err
	}
	if opts.AsyncRing > 0 && s.jrn == nil {
		ring, err := iouring.New(opts.AsyncRing)
		if err != nil {
			// A missing ring is a performance loss, not a failure:
			// plain pread/pwrite stays in charge.
			opts.Logf("store: async ring unavailable (%v): using plain pread/pwrite", err)
		} else {
			s.ring = ring
		}
	}
	return s, nil
}

// syncPass fsyncs the object files that have writes outstanding. Callers
// that need a stable snapshot take the read lock across the whole pass
// (batch mode); Sync takes it per file when unbuffered.
//
// Only dirty files are flushed and an entry's dirty counter is snapshotted
// under the same lock that the writers take: if a write lands between the
// snapshot and the fsync, the counter has grown, so it stays dirty and the
// next pass (or an explicit Sync) covers it. A pass that starts after a
// caller's writes therefore always flushes them.
func (s *Store) syncPass(underReadLock bool) error {
	s.syncPasses.Add(1)
	var pending []entryWithSeq
	if underReadLock {
		s.mu.RLock()
		defer s.mu.RUnlock()
		pending = s.dirtyEntries()
	} else {
		s.mu.RLock()
		pending = s.dirtyEntries()
		s.mu.RUnlock()
	}
	var firstErr error
	for _, fl := range pending {
		if err := fl.e.f.Sync(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// Clear only if no write landed since the snapshot: a write
		// that raced in bumps the counter and stays for the next pass.
		fl.e.dirty.CompareAndSwap(fl.seq, 0)
	}
	return firstErr
}

// dirtyEntries returns the files with unflushed writes plus the dirty
// counter observed for each.
func (s *Store) dirtyEntries() []entryWithSeq {
	var out []entryWithSeq
	for _, e := range s.files {
		if d := e.dirty.Load(); d > 0 {
			out = append(out, entryWithSeq{e, d})
		}
	}
	return out
}

// entryWithSeq pairs an entry with the dirty counter read under the lock.
type entryWithSeq struct {
	e   *entry
	seq int64
}

// Sync flushes every cached object file to stable storage.
//
// With Options.FsyncBatch > 0 the call joins a coalescing batch: the
// fsync pass starts no earlier than FsyncBatch after the batch opened,
// so concurrent flushes share one pass. Ordering is safe — the batch
// opened after this call, so every write this caller issued is inside
// the pass.
func (s *Store) Sync() error {
	if s.jrn != nil {
		// Journal mode: acknowledged writes are already durable in the
		// log. Sync additionally flattens the log into the objects and
		// truncates it, so after Sync the objects alone are complete
		// (what the guest's FLUSH/FUA semantics promise).
		return s.applyPass(true)
	}
	if s.opts.FsyncBatch <= 0 {
		if s.intent != nil {
			// Intent records go down first: a durable intent in front
			// of durable data is the safe ordering (a crash between the
			// two is exactly what recovery is built to report).
			if err := s.intent.sync(); err != nil {
				return err
			}
		}
		return s.syncPass(false)
	}
	w := batchWaiter{done: make(chan error, 1)}
	s.batchMu.Lock()
	if s.batchClosed {
		s.batchMu.Unlock()
		return ErrClosed
	}
	first := len(s.batch) == 0
	s.batch = append(s.batch, w)
	if first {
		s.batchTimer = time.AfterFunc(s.opts.FsyncBatch, s.runBatch)
	}
	s.batchMu.Unlock()
	return <-w.done
}

// runBatch performs one coalesced fsync pass for all queued waiters.
func (s *Store) runBatch() {
	s.batchMu.Lock()
	if s.batchClosed {
		s.batchMu.Unlock()
		return
	}
	waiters := s.batch
	s.batch = nil
	s.batchMu.Unlock()

	if s.intent != nil {
		_ = s.intent.sync() // markers/intents join this fsync pass (best effort)
	}
	err := s.syncPass(true)
	for _, w := range waiters {
		w.done <- err
	}
}

func (s *Store) path(oid object.OID) string {
	return path.Join(s.dir, fmt.Sprintf("%016x", uint64(oid)))
}

// open returns the cached handle for oid, creating the object on first
// write when create is true. A read of an uncached object consults the
// meta index instead of the disk: the index (filled by Load and kept
// current by every mutation) is the request-path source of truth for
// existence, which is the whole point of keeping meta in RAM.
func (s *Store) open(oid object.OID, create bool) (*entry, error) {
	s.mu.RLock()
	e := s.files[oid]
	_, indexed := s.meta[oid]
	s.mu.RUnlock()
	if e != nil {
		return e, nil
	}
	if !create && !indexed {
		return nil, ErrNoSuchObject
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
	_, statErr := os.Stat(p)
	exists := statErr == nil
	if create && s.opts.MinFreeBytes > 0 && !exists {
		// A brand-new object reserves (at least logically) a full
		// object of space. Refuse before the filesystem does, so the
		// OSD answers "no space" with a healthy process. Existing
		// objects skip the guard: a full disk must never lock a VM
		// out of data it already owns (and the real ENOSPC from the
		// kernel below is the authoritative check for them). The
		// check races with other writers.
		if f, err := storeDirStat(s.dir); err == nil {
			avail, ok := fileAvail(f)
			f.Close()
			if ok && avail < s.opts.MinFreeBytes {
				s.opts.Logf("store: refusing to create %s: %d bytes free, min %d",
					p, avail, s.opts.MinFreeBytes)
				return nil, ErrNoSpace
			}
		}
	}
	f, err := s.openFile(p, create)
	if err != nil {
		if isNoSpace(err) {
			return nil, ErrNoSpace
		}
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if create && st.Size() < int64(object.Size) {
		if err := s.grow(f, object.Size); err != nil {
			if isNoSpace(err) {
				err = ErrNoSpace
			}
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
	// Index the object. Opening is the only moment we see its real
	// allocation for free (the handle's Stat is off the request path,
	// once per object lifetime); punches invalidate the hint later.
	s.meta[oid] = objMeta{size: st.Size(), alloc: fileAllocated(st), allocKnown: true}
	if s.ring != nil {
		// os.File.Fd() dups the descriptor (blocking mode) once per
		// entry: the ring must never call Fd() on the hot path.
		e.fd = int32(f.Fd())
		s.ring.RegisterFile(e.fd, f)
	}
	s.files[oid] = e
	s.usage += e.size
	return e, nil
}

// ReadAt reads len(p) bytes from object oid at byte offset off.
// It is allocation-free for cached objects. In journal mode any
// un-applied log records are overlaid on the file read, so reads always
// see every acknowledged write (read-your-writes).
func (s *Store) ReadAt(oid object.OID, p []byte, off int64) (int, error) {
	if off+int64(len(p)) > int64(object.Size) {
		return 0, ErrObjectTooSmall
	}
	if s.jrn != nil {
		return s.journalReadAt(oid, p, off)
	}
	e, err := s.open(oid, false)
	if err != nil {
		return 0, err
	}
	if s.ring != nil {
		return s.ringIO(e, p, off, false)
	}
	return e.f.ReadAt(p, off)
}

// journalReadAt serves a read in journal mode: overlay the staged
// records over the object file (which may lag the log, not exist yet,
// or be a hole).
func (s *Store) journalReadAt(oid object.OID, p []byte, off int64) (int, error) {
	recs := s.overlayFor(oid)
	if len(recs) == 0 {
		// Nothing staged: plain file read (missing object = hole).
		e, err := s.open(oid, false)
		if err != nil {
			return 0, err
		}
		return e.f.ReadAt(p, off)
	}
	// Start from the file contents (a missing object is all zeroes),
	// then stamp the overlay on top in seq order.
	e, err := s.open(oid, false)
	switch {
	case err == nil:
		if _, err := e.f.ReadAt(p, off); err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
	case errors.Is(err, ErrNoSuchObject):
		for i := range p {
			p[i] = 0
		}
	default:
		return 0, err
	}
	overlayRead(recs, p, off)
	return len(p), nil
}

// WriteAt writes p into object oid at byte offset off, creating the object
// on first write. Writes never leave the object bounds.
func (s *Store) WriteAt(oid object.OID, p []byte, off int64) (int, error) {
	if off+int64(len(p)) > int64(object.Size) {
		return 0, ErrObjectTooSmall
	}
	if s.jrn != nil {
		if err := s.journalWrite(oid, p, off); err != nil {
			return 0, err
		}
		s.clientBytes.Add(int64(len(p)))
		return len(p), nil
	}
	// Write intent: announce the write (durable in flush/sync modes)
	// before its data touches the object file (see intent.go).
	if s.intent != nil {
		crc := crc32.Checksum(p, crc32cTable)
		syncIt := s.opts.CommitMode == CommitFlush || s.opts.CommitMode == CommitSync
		if err := s.intent.appendIntent(oid, off, len(p), crc, syncIt); err != nil {
			return 0, err
		}
	}
	e, err := s.open(oid, true)
	if err != nil {
		return 0, err
	}
	var n int
	if s.ring != nil {
		n, err = s.ringIO(e, p, off, true)
	} else {
		n, err = e.f.WriteAt(p, off)
		if isNoSpace(err) {
			err = ErrNoSpace
		}
	}
	if err == nil && s.intent != nil {
		s.intent.appendCommit(oid, off)
	}
	if err == nil && n > 0 {
		s.clientBytes.Add(int64(n))
	}
	s.mu.Lock()
	// A data write can allocate (fresh file, refill of a punched
	// hole): the allocation hint goes stale on every write, not
	// only on growth.
	if err == nil && n > 0 {
		s.metaInvalidate(oid)
	}
	// A write past EOF grew the object.
	if grow := off + int64(n) - e.size; grow > 0 {
		e.size += grow
		s.usage += grow
		if m, ok := s.meta[oid]; ok {
			m.size = e.size
			s.meta[oid] = m
		}
	}
	// In CommitNone durability is an explicit Sync, so remember that
	// this file needs one. CommitFlush fsyncs below and CommitSync uses
	// O_SYNC: both leave the file clean when WriteAt returns.
	if err == nil && s.opts.CommitMode == CommitNone {
		e.dirty.Add(1)
	}
	s.mu.Unlock()
	if err == nil && s.opts.CommitMode == CommitFlush {
		if serr := e.f.Sync(); serr != nil {
			return n, serr
		}
	}
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
	if s.jrn != nil {
		return s.journalZeroes(oid, off, n, sparse)
	}
	// Intent: the region must read as zeroes after this op, so announce
	// the zero-CRC before the punch/zero-fill and commit only on success
	// (a failed zero-fill leaves the promise open: recovery re-verifies
	// it and honestly reports the region as damaged).
	var intentOn bool
	if s.intent != nil {
		syncIt := s.opts.CommitMode == CommitFlush || s.opts.CommitMode == CommitSync
		if err := s.intent.appendIntent(oid, int64(off), int(n), zeroCRCLen(n), syncIt); err != nil {
			return err
		}
		intentOn = true
	}
	e, err := s.open(oid, true)
	if err != nil {
		return err
	}
	if sparse && punchSupported {
		if head, tail, ok := alignRange(off, n, punchAlign); ok {
			if perr := punchHole(e.f, int64(off+head), int64(tail-head)); perr == nil {
				s.punchBytes.Add(int64(tail - head))
				s.metaInvalidateAlloc(oid)
				// Zero the unaligned head/tail edges around the hole.
				if err := s.zeroFill(oid, e, off, head); err != nil {
					return err
				}
				if err := s.zeroFill(oid, e, off+tail, n-tail); err != nil {
					return err
				}
				if intentOn {
					s.intent.appendCommit(oid, int64(off))
				}
				return s.commitSync(e)
			}
			// Punch failed: fall through to a zero-fill. Correctness
			// first, reclaim is a bonus.
		}
	}
	if err := s.zeroFill(oid, e, off, n); err != nil {
		return err
	}
	if intentOn {
		s.intent.appendCommit(oid, int64(off))
	}
	return s.commitSync(e)
}

// commitSync records (or performs) durability work after a mutating op.
// CommitFlush fsyncs immediately; CommitNone marks the file dirty so an
// explicit Sync covers the punch/zero-fill; CommitSync relies on O_SYNC.
func (s *Store) commitSync(e *entry) error {
	switch s.opts.CommitMode {
	case CommitNone:
		e.dirty.Add(1)
		return nil
	case CommitFlush:
		return e.f.Sync()
	}
	return nil
}

// zeroFill writes n zeroes at off through the cached handle. It never
// touches usage: a zero-fill over existing blocks changes no allocation
// (the object is fixed-size), and over a hole it allocates exactly the
// bytes the kernel zeroes.
func (s *Store) zeroFill(oid object.OID, e *entry, off, n uint64) error {
	if n == 0 {
		return nil
	}
	s.fillBytes.Add(int64(n))
	// Writing zeroes into a hole allocates: the hint is stale.
	s.metaInvalidateAlloc(oid)
	buf := zeroBuf()
	for written := uint64(0); written < n; {
		ch := uint64(len(buf))
		if n-written < ch {
			ch = n - written
		}
		if _, err := e.f.WriteAt(buf[:ch], int64(off+written)); err != nil {
			if isNoSpace(err) {
				err = ErrNoSpace
			}
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

// Remove deletes an object. In journal mode the delete is a checkpoint:
// the log is drained and truncated first (see journalRemove), so the
// object's history can never be replayed over its own deletion.
func (s *Store) Remove(oid object.OID) error {
	if s.jrn != nil {
		return s.journalRemove(oid)
	}
	return s.removeFile(oid)
}

func (s *Store) removeFile(oid object.OID) error {
	if s.intent != nil {
		// The durable delete marker leads the unlink (see appendDelete).
		syncIt := s.opts.CommitMode == CommitFlush || s.opts.CommitMode == CommitSync
		if err := s.intent.appendDelete(oid, syncIt); err != nil {
			return err
		}
	}
	s.mu.Lock()
	e := s.files[oid]
	delete(s.files, oid)
	if m, ok := s.meta[oid]; ok {
		s.usage -= m.size
	} else {
		s.usage -= e.sizeLookup()
	}
	delete(s.meta, oid)
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

// metaInvalidate drops the (now stale) allocation hint. Callers hold
// s.mu (write lock).
func (s *Store) metaInvalidate(oid object.OID) {
	if m, ok := s.meta[oid]; ok && m.allocKnown {
		m.allocKnown = false
		s.meta[oid] = m
	}
}

// metaInvalidateAlloc drops the (now stale) allocation hint after a
// punch: the next StatInfo re-measures.
func (s *Store) metaInvalidateAlloc(oid object.OID) {
	s.mu.Lock()
	s.metaInvalidate(oid)
	s.mu.Unlock()
}

// overlayEnd returns the apparent size implied by un-applied records:
// the highest offset any record reaches. Records are journal writes
// (data) and zeroes; sparse zeroes apply as a hole but still extend
// the file, so they count too.
func overlayEnd(recs []*journalRec) int64 {
	var end int64
	for _, r := range recs {
		e := int64(r.off) + int64(len(r.data)) + int64(r.n)
		if e > end {
			end = e
		}
	}
	return end
}

// Stat reports the size of an existing object without creating it.
// It is served from the meta index: no disk access on the request
// path. It returns fs.ErrNotExist when the object is absent. Unlike
// Lookup it never creates a file. In journal mode the size accounts
// for un-applied overlay records (read-your-writes for stats).
func (s *Store) Stat(oid object.OID) (uint64, error) {
	// Overlay first, index second: a record leaves the overlay only
	// after the applier has updated the index, so an overlay snapshot
	// taken first can only under-report the pop, never miss it
	// (missing a pop is safe: the overlay end is a valid size).
	var oe int64
	if s.jrn != nil {
		oe = overlayEnd(s.overlayFor(oid))
	}
	s.mu.RLock()
	m, ok := s.metaLookup(oid)
	s.mu.RUnlock()
	if !ok {
		return 0, ErrNoSuchObject
	}
	if oe > m.size {
		return uint64(oe), nil
	}
	return uint64(m.size), nil
}

// ObjectState reports, for NBD base:allocation queries, whether an object
// occupies space. present is true when the object file exists on disk.
// pending is true when the journal holds un-applied writes for the object
// (so the object reads as data even though its file is not yet created).
// A thin (never-written) object in writeback mode is neither: it reads as
// zeroes, i.e. a hole. statErr carries the os.Stat error when present is
// false and the stat failed for a reason other than "not exist".
func (s *Store) ObjectState(oid object.OID) (present, pending bool, statErr error) {
	if _, err := s.Stat(oid); err == nil {
		return true, false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, false, err
	}
	if s.jrn != nil {
		for _, r := range s.overlayFor(oid) {
			// A sparse-zeroes record leaves the object a hole when
			// applied; only writes and full zero-fills make it data.
			if r.data != nil || (r.n > 0 && !r.sparse) {
				return false, true, nil
			}
		}
	}
	return false, false, nil
}

// StatInfo reports both the apparent size and the physically allocated
// bytes (st_blocks*512) of an existing object. Vacuum uses the pair to
// tell a punched (freed) object from one that still occupies blocks:
// the apparent size alone cannot, because a punch keeps the size and a
// preallocated thin object reports the full size.
//
// In journal mode un-applied overlay writes are part of the answer:
// they are acknowledged data the object logically owns, and on a
// delayed-allocation filesystem the file's st_blocks lags them (it
// can even read 0 while the applier is between create and write), so
// the size is the max of the index and the overlay, and the alloc a
// lower bound of max(file, overlay bytes). Such a reading is never
// cached: it is not yet the file's stable state.
func (s *Store) StatInfo(oid object.OID) (size, alloc uint64, err error) {
	// Overlay snapshot first (see Stat): a non-empty overlay always
	// wins over the index snapshot, which may predate the pop.
	var oe int64
	if s.jrn != nil {
		oe = overlayEnd(s.overlayFor(oid))
	}
	s.mu.RLock()
	m, ok := s.metaLookup(oid)
	s.mu.RUnlock()
	if !ok {
		return 0, 0, ErrNoSuchObject
	}
	size, alloc = uint64(m.size), uint64(m.alloc)
	if !m.allocKnown && oe == 0 {
		// Allocation unknown (indexed at Load, punched since): one
		// stat, a background/vacuum path cost, and re-measure it now.
		fi, err := os.Stat(s.path(oid))
		if err != nil {
			return 0, 0, err
		}
		if fi.IsDir() {
			return 0, 0, fs.ErrNotExist
		}
		size, alloc = uint64(fi.Size()), uint64(fileAllocated(fi))
		if alloc > 0 {
			s.mu.Lock()
			if cur, ok := s.meta[oid]; ok {
				cur.size, cur.alloc, cur.allocKnown = int64(size), int64(alloc), true
				s.meta[oid] = cur
			}
			s.mu.Unlock()
		}
		return size, alloc, nil
	}
	if oe > 0 {
		// Un-applied writes: their bytes are logically allocated even
		// where the file lags. Never cache a clamped reading.
		if oe > m.size {
			size = uint64(oe)
		}
		if oe > m.alloc {
			alloc = uint64(oe)
		}
	}
	return size, alloc, nil
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

// Usage returns the total bytes stored across all known objects.
func (s *Store) Usage() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.usage
}

// DiskSpace reports the bytes the store directory can still grow into
// (filesystem free space) and the filesystem size. ok=false when the
// filesystem cannot be queried.
func (s *Store) DiskSpace() (free, total int64, ok bool) {
	f, err := storeDirStat(s.dir)
	if err != nil {
		return 0, 0, false
	}
	defer f.Close()
	return fileSpace(f)
}

// Close syncs and closes all cached files. In journal mode the log is
// drained into the objects first (clean shutdown: a reopen sees an
// empty log).
func (s *Store) Close() error {
	// Drain the journal before the object files: the applier writes
	// through open(), which wants the store lock.
	journalErr := s.journalClose()

	// Drain the coalescer before taking the store lock: runBatch takes
	// the read lock, so waiting for it while holding the write lock
	// would deadlock. Waiters that arrive after this point get ErrClosed.
	if s.opts.FsyncBatch > 0 {
		s.batchMu.Lock()
		s.batchClosed = true
		waiters := s.batch
		s.batch = nil
		if s.batchTimer != nil {
			s.batchTimer.Stop()
			s.batchTimer = nil
		}
		s.batchMu.Unlock()
		err := s.syncPass(false)
		for _, w := range waiters {
			w.done <- err
		}
	}

	// Stop the ring before the files: queued completions must not race
	// with closed descriptors. Close fires queued ops with ErrClosed.
	var firstErr error
	if s.ring != nil {
		if err := s.ring.Close(); err != nil {
			firstErr = err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for oid, e := range s.files {
		if err := e.f.Sync(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := e.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.files, oid)
	}
	// Close the intent log after the object fsyncs: every acknowledged
	// write is durable, so no open intents remain to verify and the log
	// can be compacted (a crash before this point keeps the records,
	// and recovery re-verifies regions whose data did not make the
	// fsyncs above).
	if s.intent != nil {
		if err := s.intent.closeClean(); err != nil && journalErr == nil {
			journalErr = err
		}
	}
	if journalErr != nil && firstErr == nil {
		firstErr = journalErr
	}
	return firstErr
}

// Load rescans the directory so Usage and the meta index reflect
// pre-existing objects without opening them all. The directory entry
// carries the file size (readdir metadata), so this costs no per-file
// stat syscalls; allocated bytes are left unknown until an object is
// opened (StatInfo re-measures them on demand).
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
		if de.Name() == journalFileName || de.Name() == intentFileName {
			continue // store log files (hex names would parse as OIDs)
		}
		var oid uint64
		if _, err := fmt.Sscanf(de.Name(), "%016x", &oid); err != nil {
			continue // not an object file
		}
		if _, ok := s.files[object.OID(oid)]; ok {
			continue // already accounted
		}
		if _, ok := s.meta[object.OID(oid)]; ok {
			continue // already indexed
		}
		st, err := de.Info()
		if err != nil {
			return err
		}
		s.usage += st.Size()
		s.meta[object.OID(oid)] = objMeta{size: st.Size()}
	}
	return nil
}
