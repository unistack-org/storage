// Write Intent Journal (ideas §2.1, the Vitastor Highload'2025 answer to
// torn writes): before data reaches an object file, a small intent
// record — "object O, offset X, length L will hold CRC=Y" — is appended
// to a durable log. After the data write completes, a commit marker for
// the same (oid, off) is appended (buffered: it is an optimisation, not
// a durability point).
//
// Recovery replays the log. An intent whose commit marker is durable
// needs no check. An open intent is verified against the object file:
// the region reads back with the recorded CRC ⇒ the write completed;
// anything else (torn content, a hole) means the acknowledged write did
// not fully land. That is the damage report a power loss leaves behind,
// visible without copying the disk.
//
// Semantics per commit mode (the intent log is a non-journal feature —
// journal mode already has a write-ahead log that rebuilds the objects):
//
//	CommitFlush  intent fsynced before the data write, data fsynced
//	             before the ack. An open intent failing its CRC is a
//	             torn write of acknowledged data: loud, counted.
//	CommitSync   same, with O_SYNC data writes.
//	CommitNone   intents are appended to the page cache and made durable
//	             with Sync(); a plain process crash keeps them (and the
//	             issued data writes), a machine crash loses both. Check
//	             results are advisory here: an intent may describe a
//	             write that was never acknowledged.
//
// Cost: one small (24 B) record per write plus, in flush/sync modes, one
// extra fsync of a tiny hot log file — no double data write. The commit
// marker's only durability requirement comes from the data fsync path:
// when data is durable and the marker is lost (power loss between the
// two), recovery re-verifies the region, it matches, and the marker's
// absence costs nothing.
package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/unistack-org/storage/pkg/journal"
	"github.com/unistack-org/storage/pkg/object"
)

// intentFileName is the intent log inside the store directory.
const intentFileName = "intent.log"

// intentRecSize is the fixed intent-record payload (big-endian):
//
//	flags  u8    bit 0 = commit marker for (oid, off)
//	_      u8[3] reserved (keeps oid 8-byte aligned)
//	oid    u64
//	off    u32   byte offset inside the object
//	len    u32   byte length of the intended write (0 on a commit marker)
//	crc    u32   CRC-32C (Castagnoli) of the data to be written
//
// 24 B per record plus the 16 B journal header. A 4 KiB randwrite adds
// ~1% device traffic — the price of a power-loss damage map.
const intentRecSize = 24

const intentFlagCommit = 1

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// intentKey identifies an open intent; the commit marker carries the
// same key fields. A repeated (oid,off) collapses to the newest intent:
// an older open intent for the same region is superseded by the newer
// CRC whenever the newer record is durable (records replay in seq
// order), which is exactly the Vitastor rule.
type intentKey struct {
	oid object.OID
	off uint64
}

// intentRec is a decoded intent record.
type intentRec struct {
	key intentKey
	len uint32
	crc uint32
}

// IntentStats counts intent-log traffic (observability of §2.1).
type IntentStats struct {
	// Records is the number of intent records appended.
	Records int64 `json:"records"`
	// Syncs is the number of intent-log fsyncs (the extra fsync a
	// flush/sync-mode write pays; CommitNone batches them into Sync).
	Syncs int64 `json:"syncs"`
	// Checked is the number of open intents verified at recovery
	// (cumulative over reopens; 0 = the log was clean).
	Checked int64 `json:"checked"`
	// Mismatches is the number of open intents whose region does not
	// match the recorded CRC: writes that did not fully land (torn) or
	// are gone (hole).
	Mismatches int64 `json:"mismatches"`
}

// intentState owns the intent log of a non-journal store.
type intentState struct {
	j *journal.Journal

	mu       sync.Mutex
	recent   []string // last few mismatch descriptions (admin status)
	records  atomic.Int64
	syncs    atomic.Int64
	checked  atomic.Int64
	misatch  atomic.Int64
	recorded sync.Mutex // serialises Append(+Sync) pairs on the shared log
}

// openIntentLog opens (creating) the intent log in dir and runs recovery:
// surviving records are folded into the open set, open intents are
// verified against the object files, and the log is truncated (the check
// is a one-shot damage report; keeping records grows the log forever).
func openIntentLog(dir string, logf func(string, ...any)) (*intentState, error) {
	p := path.Join(dir, intentFileName)
	is := &intentState{}
	if _, statErr := os.Stat(p); statErr == nil {
		var (
			records int
			open    = map[intentKey]intentRec{}
		)
		_, err := journal.ReplayFile(p, func(_ uint64, payload []byte) error {
			records++
			if len(payload) < intentRecSize {
				return nil
			}
			k := intentKey{
				oid: object.OID(binary.BigEndian.Uint64(payload[4:12])),
				off: uint32(binary.BigEndian.Uint32(payload[12:16])),
			}
			if payload[0]&intentFlagCommit != 0 {
				delete(open, k)
				return nil
			}
			open[k] = intentRec{
				key: k,
				len: binary.BigEndian.Uint32(payload[16:20]),
				crc: binary.BigEndian.Uint32(payload[20:24]),
			}
			return nil
		})
		if err != nil {
			// A corrupt mid-log intent is not user data: log and
			// treat the durable prefix as the whole truth.
			logf("store: intent log %s: %v (durable prefix trusted)", p, err)
		}
		is.records.Store(int64(records))
		if len(open) > 0 {
			is.verify(dir, open, logf)
			// Damage reported; the log starts fresh (the sequence
			// continues) — the check must not fire again on the
			// next reopen.
			if err := is.j0Truncate(p); err != nil {
				return nil, err
			}
		}
	}
	j, err := journal.Open(p)
	if err != nil {
		return nil, err
	}
	is.j = j
	return is, nil
}

// j0Truncate truncates the (pre-open) log via a temporary journal
// handle; kept separate so the recovery path is obvious.
func (is *intentState) j0Truncate(p string) error {
	j, err := journal.Open(p)
	if err != nil {
		return err
	}
	defer j.Close()
	return j.Truncate(j.Seq())
}

// verify checks every open intent against its object file. A missing
// object, a region shorter than intended, a hole, or a CRC mismatch all
// count as a mismatch: the intended write is not (fully) on disk.
func (is *intentState) verify(dir string, open map[intentKey]intentRec, logf func(string, ...any)) {
	var mismatches int64
	for _, rec := range open {
		ok, err := regionMatches(path.Join(dir, fmt.Sprintf("%016x", uint64(rec.key.oid))), int64(rec.key.off), rec.len, rec.crc)
		if err != nil {
			logf("store: intent check: %v", err)
			continue
		}
		if ok {
			continue
		}
		mismatches++
		msg := fmt.Sprintf("object %016x: acknowledged write at %d+%d is not durable (crc %08x)",
			uint64(rec.key.oid), rec.key.off, rec.len, rec.crc)
		logf("store: intent check FAILED: %s", msg)
		is.mu.Lock()
		if len(is.recent) < 8 {
			is.recent = append(is.recent, msg)
		}
		is.mu.Unlock()
	}
	is.checked.Add(int64(len(open)))
	is.misatch.Add(mismatches)
}

// regionMatches reads [off, off+len) of the object file and compares its
// CRC-32C with want. A missing file is a mismatch, not an error.
func regionMatches(p string, off int64, length uint32, want uint32) (bool, error) {
	if length == 0 {
		return true, nil
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, off)
	if n < int(length) {
		return false, nil // short/absent region: the write is not there
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	return crc32.Checksum(buf, crc32cTable) == want, nil
}

// appendIntent records the intent to write len(p) bytes with the given
// CRC at oid/off. syncIt makes the record durable immediately (flush /
// sync modes); in CommitNone the caller's Sync() covers it.
func (is *intentState) appendIntent(oid object.OID, off int64, length int, want uint32, syncIt bool) error {
	rec := make([]byte, intentRecSize)
	rec[0] = 0
	binary.BigEndian.PutUint64(rec[4:12], uint64(oid))
	binary.BigEndian.PutUint32(rec[12:16], uint32(off))
	binary.BigEndian.PutUint32(rec[16:20], uint32(length))
	binary.BigEndian.PutUint32(rec[20:24], want)

	is.recorded.Lock()
	_, err := is.j.Append(rec)
	if err == nil && syncIt {
		err = is.j.Sync()
	}
	is.recorded.Unlock()
	if err != nil {
		return err
	}
	is.records.Add(1)
	if syncIt {
		is.syncs.Add(1)
	}
	return nil
}

// appendCommit marks the intent for (oid,off) as completed. Buffered:
// durability of the marker is not required (recovery re-verifies an
// uncommitted intent and a CRC match is free).
func (is *intentState) appendCommit(oid object.OID, off int64) {
	rec := make([]byte, intentRecSize)
	rec[0] = intentFlagCommit
	binary.BigEndian.PutUint64(rec[4:12], uint64(oid))
	binary.BigEndian.PutUint32(rec[12:16], uint32(off))
	is.recorded.Lock()
	_, _ = is.j.Append(rec) // best effort by design
	is.recorded.Unlock()
	is.records.Add(1)
}

// sync makes buffered intent records durable (CommitNone path).
func (is *intentState) sync() error { return is.j.Sync() }

// intentStats returns a snapshot of the intent counters.
func (is *intentState) intentStats() IntentStats {
	return IntentStats{
		Records:    is.records.Load(),
		Syncs:      is.syncs.Load(),
		Checked:    is.checked.Load(),
		Mismatches: is.misatch.Load(),
	}
}

// recentMismatches returns the last mismatch descriptions from recovery
// (admin status warnings); empty when the last reopen was clean.
func (is *intentState) recentMismatches() []string {
	is.mu.Lock()
	defer is.mu.Unlock()
	out := make([]string, len(is.recent))
	copy(out, is.recent)
	return out
}

// close flushes and closes the log (clean shutdown: buffered commit
// markers reach disk, the reopen finds an empty open set cheaply).
func (is *intentState) close() error { return is.j.Close() }
