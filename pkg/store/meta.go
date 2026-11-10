// In-memory object meta index (roadmap Фаза 5 / ideas §2.5).
//
// The Vitastor lesson: an OSD must never read object metadata from disk
// on the request path. Before this index, every read of an uncached
// object and every Stat/StatInfo/ObjectState ran os.Stat against the
// object file — one disk metadata lookup per request, exactly what
// caps randread IOPS and adds latency (the Vitastor numbers: 129K+
// randread IOPS rest on the meta living in RAM).
//
// The index lives in Store.meta (guarded by Store.mu, which every
// mutation of the object set already takes):
//
//   - Load() fills it from one directory scan ( readdir + dir-entry
//     stat info, no per-file os.Stat syscalls on the request path).
//   - openSlow() adds the entry when an object is created/opened.
//   - WriteAt/applyOne update the entry's apparent size when a write
//     grows the object; punches clear the (stale) alloc hint.
//   - removeFile() deletes the entry.
//
// INVARIANT — one object, one meta entry. An object's entire metadata
// (exists, apparent size, alloc hint) is the single 24-byte objMeta
// record: there is no secondary structure that can disagree with it.
// The disk is authoritative only at Load; after that the index is the
// source of truth for this process, and any mutation path that changes
// the file updates the index under the same lock. Cost: ~24 B of map
// payload + ~48 B of Go map overhead per object, i.e. roughly
// 72 B / 4 MiB = ~18 MB per stored TiB — three orders of magnitude
// below Vitastor's 152–663 MB/TB because our meta is one word per
// object (their index is per 4 KiB block).
//
// Allocation (st_blocks) is deliberately a *hint* kept only for
// objects whose files we opened in this process: vacuum's StatInfo
// still pays one os.Stat for indexed objects it never opened, which
// is a background path, not the request path.

package store

import (
	"unsafe"

	"github.com/unistack-org/storage/pkg/object"
)

// objMeta is the index entry for one object.
type objMeta struct {
	size int64 // apparent file size
	// alloc is the physically allocated bytes (st_blocks*512), valid
	// only when allocKnown; a punch invalidates it (set allocKnown to
	// false so the next StatInfo re-measures).
	alloc      int64
	allocKnown bool
}

// metaLookup returns the index entry for oid. Callers hold s.mu
// (read lock is enough).
func (s *Store) metaLookup(oid object.OID) (objMeta, bool) {
	m, ok := s.meta[oid]
	return m, ok
}

// metaSet records/overwrites the entry for oid. Callers hold s.mu
// (write lock).
func (s *Store) metaSet(oid object.OID, m objMeta) {
	s.meta[oid] = m
}

// MetaInfo reports the index population: indexed objects and the RAM
// they cost (entry payload + per-object map overhead estimate).
func (s *Store) MetaInfo() (objects, ramBytes int64) {
	s.mu.RLock()
	n := int64(len(s.meta))
	s.mu.RUnlock()
	return n, n * (int64(unsafe.Sizeof(objMeta{})) + metaMapOverhead)
}

// metaMapOverhead is the approximate per-entry cost of the Go map
// itself (bucket slots, keys, load factor). objMeta is 24 B (key
// uint64 + value 24 B = 32 B, one entry per bucket slot at the
// default load factor 6.5): the map adds roughly as much as the
// payload, plus 8 B of header/slice amortised over the bucket.
const metaMapOverhead = 32
