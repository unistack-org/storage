package store

// Journal commit mode (CommitJournal): every mutating operation is
// appended to a write-ahead log, the log is fsynced, and only then the
// write is acknowledged. The object files are updated lazily by a
// background applier, so an acknowledged write costs one sequential log
// append (plus its fsync) instead of a random write into a 4 MiB object
// (plus, in CommitFlush, an fsync of that object). This is the Vitastor
// "journal" flavour of immediate_commit: a bounded staging log absorbs
// random writes and a background pass flattens them into the objects.
//
// Record payload (after the 16-byte pkg/journal header, all big-endian):
//
//	typ    u8   jrWrite | jrZeroes | jrZeroSpa | jrRemove
//	oid    u64
//	offset u64   (for jrRemove this field is unused and reads back 0)
//	len    u32   data length (jrWrite) / zero count (jrZeroes/jrZeroSpa)
//	data   ...   present only for jrWrite
//
// The only integrity check is the journal header's crc32c: a torn tail
// (a crash mid-append) is tolerated by the replay scanner, which stops
// at the first bad record. A corrupt record in the *middle* of the log
// (valid header checksum, unparseable payload) is a real inconsistency
// and poisons the store at open — refusing to start is the safe answer.
//
// Crash semantics:
//
//   - Acknowledgement happens only after the record is fsynced, so an
//     acknowledged write is never lost: a replay at open reconstructs
//     it into the (re)created object.
//   - Clean Close drains the log into the objects, fsyncs, truncates to
//     empty, and closes; a reopen sees an empty log.
//   - Remove is a checkpoint: it drains and truncates the log first, so
//     no un-applied record for a deleted object can survive to the next
//     open (which would otherwise resurrect the deleted object).
//
// Space: a full disk poisons pkg/journal (sticky ErrNoSpace). journal
// mode maps that to ErrNoSpace, which sdsnet reports as StatusNoSpace;
// reads and the objects already on disk stay intact and served.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unistack-org/storage/pkg/journal"
	"github.com/unistack-org/storage/pkg/object"
)

const (
	jrWrite   uint8 = 1 // raw write: oid, off, len, data
	jrZeroes  uint8 = 2 // zeroes (zero-fill): oid, off, len
	jrZeroSpa uint8 = 3 // zeroes, sparse requested (punch when applied)
	jrRemove  uint8 = 4 // oid deleted (checkpoint marker)
)

// jrFixed is the fixed part of a record payload: typ + oid + off + len.
const jrFixed = 1 + 8 + 8 + 4

// journalFileName is the log file inside the store directory.
const journalFileName = "journal.log"

// journalMaxOverlay bounds the un-applied record set. When the applier
// falls this far behind, writers apply inline (backpressure) so the log
// cannot run ahead of the objects without limit and the journal file
// cannot be allowed to fill (a full journal is poisoned on ENOSPC).
const journalMaxOverlay = 8192

// journalApplyBatch caps records applied in one applier pass so a
// sustained burst truncates the log periodically rather than never.
const journalApplyBatch = 4096

// journalApplyTick is how often the background applier wakes when idle.
const journalApplyTick = 20 * time.Millisecond

// ErrJournalCorrupt means the log contains an unparseable record that
// is not the torn tail: the data the log promised is unusable.
var ErrJournalCorrupt = errors.New("store: journal is corrupt")

// journalRec is one decoded mutating record (jrWrite/jrZeroes variants).
type journalRec struct {
	seq    uint64
	oid    object.OID
	off    uint64
	data   []byte // jrWrite payload (owned copy)
	n      uint64 // zeroes length (jrZeroes)
	sparse bool   // zeroes requested sparseness
}

// encodeJournal builds a record payload from the fixed header + data.
func encodeJournal(typ uint8, oid object.OID, off uint64, length uint32, data []byte) []byte {
	buf := make([]byte, jrFixed+len(data))
	buf[0] = typ
	binary.BigEndian.PutUint64(buf[1:9], uint64(oid))
	binary.BigEndian.PutUint64(buf[9:17], off)
	binary.BigEndian.PutUint32(buf[17:21], length)
	copy(buf[jrFixed:], data)
	return buf
}

// encodeRemove builds the checkpoint marker for a deleted object.
func encodeRemove(oid object.OID) []byte {
	buf := make([]byte, 1+8)
	buf[0] = jrRemove
	binary.BigEndian.PutUint64(buf[1:9], uint64(oid))
	return buf
}

// decodeJournal parses a record payload into rec (for jrWrite/jrZeroes)
// or, for a jrRemove, returns just the oid.
func decodeJournal(seq uint64, p []byte) (rec *journalRec, remOid object.OID, isRemove bool, err error) {
	if len(p) < 1 {
		return nil, 0, false, ErrJournalCorrupt
	}
	switch p[0] {
	case jrRemove:
		if len(p) != 1+8 {
			return nil, 0, false, ErrJournalCorrupt
		}
		return nil, object.OID(binary.BigEndian.Uint64(p[1:9])), true, nil
	case jrWrite, jrZeroes, jrZeroSpa:
		if len(p) < jrFixed {
			return nil, 0, false, ErrJournalCorrupt
		}
		r := &journalRec{seq: seq, off: binary.BigEndian.Uint64(p[9:17])}
		r.oid = object.OID(binary.BigEndian.Uint64(p[1:9]))
		length := binary.BigEndian.Uint32(p[17:21])
		switch p[0] {
		case jrWrite:
			if len(p) != jrFixed+int(length) {
				return nil, 0, false, ErrJournalCorrupt
			}
			r.data = p[jrFixed:]
		default:
			if len(p) != jrFixed {
				return nil, 0, false, ErrJournalCorrupt
			}
			r.n = uint64(length)
			r.sparse = p[0] == jrZeroSpa
		}
		return r, 0, false, nil
	default:
		return nil, 0, false, ErrJournalCorrupt
	}
}

// journalState owns the write-ahead log and the un-applied overlay. It
// is nil on a Store opened in a non-journal commit mode.
type journalState struct {
	j *journal.Journal

	mu sync.Mutex
	// log is the apply-order FIFO (seq ascending). byOID is the same
	// records bucketed per object (seq ascending) for the read overlay.
	// A record appears in exactly one slot of each; popping the FIFO
	// head also pops the head of byOID[oid] (the same pointer).
	log    []*journalRec
	byOID  map[object.OID][]*journalRec
	maxSeq uint64
	// draining parks writers while a Remove/Close checkpoint applies
	// the whole log; drainDone is closed when the drain ends.
	draining bool
	drainDone chan struct{}

	// applyMu guards applying (at most one applier pass at a time);
	// applyCV lets drain callers wait for the in-flight pass to end
	// (a lost single-flight race must retry, not return early).
	applyCV    sync.Cond
	applying   bool

	quit chan struct{} // closes the applier goroutine
	done chan struct{} // closed when the applier goroutine exits

	appliedRecs atomic.Int64 // stats: records flattened into objects
	truncations atomic.Int64 // stats: log truncations
}

// openJournal replays the log at path, returning the surviving records
// (those newer than the last remove for their oid) and the last durable
// seq, then opens the log for appending.
func openJournal(dir string, logf func(string, ...any)) (*journalState, error) {
	p := path.Join(dir, journalFileName)
	var (
		all      []*journalRec
		removals = map[object.OID]uint64{}
	)
	if _, statErr := os.Stat(p); statErr == nil {
		_, err := journal.ReplayFile(p, func(seq uint64, payload []byte) error {
			rec, rem, isRem, err := decodeJournal(seq, payload)
			if err != nil {
				return err
			}
			if isRem {
				if seq > removals[rem] {
					removals[rem] = seq
				}
				return nil
			}
			all = append(all, rec)
			return nil
		})
		if err != nil {
			// A mid-log corruption means acked data is unrecoverable;
			// refuse the store rather than silently dropping records.
			return nil, fmt.Errorf("%w: %w", ErrJournalCorrupt, err)
		}
	}

	js := &journalState{
		byOID:     map[object.OID][]*journalRec{},
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
		drainDone: closedChan(),
	}
	js.applyCV.L = &js.mu
	for _, r := range all {
		if cut, ok := removals[r.oid]; ok && r.seq <= cut {
			continue // superseded by a later delete checkpoint
		}
		js.log = append(js.log, r)
		js.byOID[r.oid] = append(js.byOID[r.oid], r)
		if r.seq > js.maxSeq {
			js.maxSeq = r.seq
		}
	}
	for _, cut := range removals {
		if cut > js.maxSeq {
			js.maxSeq = cut
		}
	}
	if len(all) > 0 && logf != nil {
		logf("store: journal replay: %d records pending for %d objects", len(js.log), len(js.byOID))
	}

	// Reopen for appending: pkg/journal scans the tail and continues the
	// sequence after maxSeq. The replayed records are already in the
	// overlay, so the log is truncated as soon as the applier drains it.
	j, err := journal.Open(p)
	if err != nil {
		return nil, err
	}
	js.j = j
	return js, nil
}

func closedChan() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// startApplier launches the background flattener.
func (s *Store) startApplier(js *journalState) {
	go func() {
		defer close(js.done)
		t := time.NewTicker(journalApplyTick)
		defer t.Stop()
		for {
			select {
			case <-js.quit:
				s.applyPass(true) // final drain
				return
			case <-t.C:
				s.applyPass(false)
			}
		}
	}()
}

// journalAppend stages and durably commits one record. It returns after
// the record is fsynced — that fsync is the acknowledgement point.
func (s *Store) journalAppend(rec *journalRec) error {
	js := s.jrn

	// Backpressure: keep the overlay bounded so the log cannot run
	// ahead of the objects. Callers help by applying inline (outside
	// the journal lock) so the burst amortises into the objects.
	for {
		js.mu.Lock()
		if js.draining {
			w := js.drainDone
			js.mu.Unlock()
			<-w
			continue
		}
		needApply := len(js.log) >= journalMaxOverlay
		js.mu.Unlock()
		if !needApply {
			break
		}
		if err := s.applyPass(true); err != nil {
			return err
		}
	}

	js.mu.Lock()
	if js.draining {
		// A checkpoint started after the check above: park and retry
		// so no record can land between the drain and the delete.
		w := js.drainDone
		js.mu.Unlock()
		<-w
		return s.journalAppend(rec)
	}
	typ := jrWrite
	if rec.n > 0 || len(rec.data) == 0 {
		typ = jrZeroes
		if rec.sparse {
			typ = jrZeroSpa
		}
	}
	var payload []byte
	if typ == jrWrite {
		payload = encodeJournal(typ, rec.oid, rec.off, uint32(len(rec.data)), rec.data)
	} else {
		payload = encodeJournal(typ, rec.oid, rec.off, uint32(rec.n), nil)
	}
	seq, err := js.j.Append(payload)
	if err != nil {
		js.mu.Unlock()
		return mapJournalErr(err)
	}
	rec.seq = seq
	js.maxSeq = seq
	js.log = append(js.log, rec)
	js.byOID[rec.oid] = append(js.byOID[rec.oid], rec)
	js.mu.Unlock()

	// Durable before ack. pkg/journal buffers writes, so Sync pushes
	// this record to the OS and fsyncs the log.
	if err := js.j.Sync(); err != nil {
		return mapJournalErr(err)
	}
	return nil
}

// journalZeroes records a zero-fill (optionally sparse) for the log.
func (s *Store) journalZeroes(oid object.OID, off, n uint64, sparse bool) error {
	return s.journalAppend(&journalRec{oid: oid, off: off, n: n, sparse: sparse})
}

// journalWrite records a raw write. The payload is copied: the caller's
// buffer may be reused before the applier flattens the record.
func (s *Store) journalWrite(oid object.OID, p []byte, off int64) error {
	data := make([]byte, len(p))
	copy(data, p)
	return s.journalAppend(&journalRec{oid: oid, off: uint64(off), data: data})
}

// overlayFor returns a snapshot of the un-applied records for oid.
func (s *Store) overlayFor(oid object.OID) []*journalRec {
	js := s.jrn
	js.mu.Lock()
	defer js.mu.Unlock()
	recs := js.byOID[oid]
	if len(recs) == 0 {
		return nil
	}
	out := make([]*journalRec, len(recs))
	copy(out, recs)
	return out
}

// overlayRead stamps un-applied records over a freshly file-read buffer.
// recs are applied in seq order (so the newest wins); gaps are left as
// the file read filled them (recs is only ever consulted for a live
// object; a missing object is pre-zeroed by the caller).
func overlayRead(recs []*journalRec, p []byte, off int64) {
	if len(recs) == 0 {
		return
	}
	end := off + int64(len(p))
	for _, r := range recs {
		if r.n > 0 { // zeroes
			rs, re := int64(r.off), int64(r.off)+int64(r.n)
			if re <= off || rs >= end {
				continue
			}
			lo, hi := max(rs, off), min(re, end)
			for i := lo - off; i < hi-off; i++ {
				p[i] = 0
			}
			continue
		}
		rs, re := int64(r.off), int64(r.off)+int64(len(r.data))
		if re <= off || rs >= end {
			continue
		}
		lo, hi := max(rs, off), min(re, end)
		copy(p[lo-off:hi-off], r.data[lo-rs:hi-rs])
	}
}

// applyOne flattens a single record into its object file. It runs under
// the store read lock (object file access is serialised by the entry;
// open() takes the write lock internally).
func (s *Store) applyOne(r *journalRec) error {
	e, err := s.open(r.oid, true)
	if err != nil {
		return err
	}
	if r.n > 0 { // zeroes
		if r.sparse && punchSupported {
			if head, tail, ok := alignRange(r.off, r.n, punchAlign); ok {
				if perr := punchHole(e.f, int64(r.off+head), int64(tail-head)); perr == nil {
					if err := s.zeroFill(e, r.off, head); err != nil {
						return err
					}
					if err := s.zeroFill(e, r.off+tail, r.n-tail); err != nil {
						return err
					}
					e.dirty.Add(1)
					return nil
				}
			}
		}
		if err := s.zeroFill(e, r.off, r.n); err != nil {
			return err
		}
		e.dirty.Add(1)
		return nil
	}
	if _, err := e.f.WriteAt(r.data, int64(r.off)); err != nil {
		if isNoSpace(err) {
			return ErrNoSpace
		}
		return err
	}
	s.mu.Lock()
	if grow := int64(r.off) + int64(len(r.data)) - e.size; grow > 0 {
		e.size += grow
		s.usage += grow
	}
	s.mu.Unlock()
	e.dirty.Add(1)
	return nil
}

// applyPass flattens up to journalApplyBatch staged records (all of
// them when drain is set), then fsyncs the touched objects and, if the
// overlay is fully drained, truncates the log. Concurrency is
// single-flight: the first caller does the work; a drain caller whose
// pass was lost to a concurrent applier re-checks the overlay and
// retries until it is empty (writers are parked during Remove
// checkpoints, so a drain always terminates).
func (s *Store) applyPass(drain bool) error {
	js := s.jrn
	if js == nil {
		return nil
	}
	for {
		js.applyCV.L.Lock()
		for js.applying {
			js.applyCV.Wait()
		}
		js.applying = true
		js.applyCV.L.Unlock()

		err := s.applyLocked(drain)

		js.applyCV.L.Lock()
		js.applying = false
		js.applyCV.Broadcast()
		empty := js.peekLenLocked() == 0
		js.applyCV.L.Unlock()

		if err != nil || !drain || empty {
			return err
		}
	}
}

func (s *Store) applyLocked(drain bool) error {
	js := s.jrn
	limit := journalApplyBatch
	if drain {
		limit = int(^uint(0) >> 1) // max int: apply everything
	}
	var firstErr error
	for applied := 0; applied < limit; applied++ {
		rec, ok := js.peekOldest()
		if !ok {
			break
		}
		// Apply to the file *before* dropping from the overlay: a
		// reader that misses the record in the overlay is guaranteed
		// to find its bytes in the object file (no visibility window).
		if err := s.applyOne(rec); err != nil {
			firstErr = err
			break // head stays in the overlay; retried next pass
		}
		js.popHead(rec)
		js.appliedRecs.Add(1)
	}

	// Make the flattened objects durable before truncating the log that
	// described them.
	if serr := s.syncPass(false); serr != nil && firstErr == nil {
		firstErr = serr
	}

	js.mu.Lock()
	drained := len(js.log) == 0
	js.mu.Unlock()
	if drained {
		if terr := js.j.Truncate(0); terr != nil {
			if firstErr == nil {
				firstErr = mapJournalErr(terr)
			}
		} else {
			js.truncations.Add(1)
		}
	}
	return firstErr
}

// peekOldest returns the FIFO head without removing it.
func (js *journalState) peekOldest() (*journalRec, bool) {
	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.log) == 0 {
		return nil, false
	}
	return js.log[0], true
}

// peekLenLocked reports the overlay depth. The caller must hold js.mu
// (applyPass checks it under the applyCV's lock).
func (js *journalState) peekLenLocked() int { return len(js.log) }

// popHead removes rec from both views. It must be the FIFO head (the
// applier applies strictly in order), so it is also the head of
// byOID[rec.oid].
func (js *journalState) popHead(rec *journalRec) {
	js.mu.Lock()
	defer js.mu.Unlock()
	if len(js.log) > 0 && js.log[0] == rec {
		js.log = js.log[1:]
	}
	lst := js.byOID[rec.oid]
	if len(lst) > 0 && lst[0] == rec {
		if len(lst) == 1 {
			delete(js.byOID, rec.oid)
		} else {
			js.byOID[rec.oid] = lst[1:]
		}
	}
}

// journalRemove deletes an object as a checkpoint: park writers, drain
// and truncate the log (so no record for the oid can survive to the next
// open), then remove the file, then unpark.
func (s *Store) journalRemove(oid object.OID) error {
	js := s.jrn
	if js == nil {
		return s.removeFile(oid)
	}
	js.mu.Lock()
	for js.draining {
		w := js.drainDone
		js.mu.Unlock()
		<-w
		js.mu.Lock()
	}
	js.draining = true
	js.drainDone = make(chan struct{})
	js.mu.Unlock()

	drainErr := s.applyPass(true)

	// The log is drained and truncated; the delete is now a plain file
	// op with no WAL interaction.
	removeErr := s.removeFile(oid)

	js.mu.Lock()
	js.draining = false
	close(js.drainDone)
	js.mu.Unlock()

	if drainErr != nil {
		return drainErr
	}
	return removeErr
}

// journalClose drains, stops the applier, and closes the log (clean
// shutdown: the reopen sees an empty log).
func (s *Store) journalClose() error {
	js := s.jrn
	if js == nil {
		return nil
	}
	close(js.quit)
	<-js.done // final drain ran
	err := js.j.Close()
	return err
}

// journalStats reports overlay depth (tests / metrics).
func (s *Store) journalStats() (pending int, applied, trunc int64) {
	js := s.jrn
	if js == nil {
		return 0, 0, 0
	}
	js.mu.Lock()
	p := len(js.log)
	js.mu.Unlock()
	return p, js.appliedRecs.Load(), js.truncations.Load()
}

// JournalMode reports whether the store runs with a write-ahead log.
// Callers that touch object files directly (e.g. hole punching through
// Lookup) must route through WriteZeroes when this is true so the log
// and the objects never disagree.
func (s *Store) JournalMode() bool { return s.jrn != nil }

func mapJournalErr(err error) error {
	if errors.Is(err, journal.ErrNoSpace) {
		return ErrNoSpace
	}
	return err
}
