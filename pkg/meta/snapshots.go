package meta

// Snapshots (plan M3, todos/adr-snapshots.md).
//
// A snapshot is a read-only generation of a volume. The scheme never
// moves object data: every generation owns a private, freshly
// reserved OID range, and the live addressing (OIDBase/OIDCount/
// Extents + the live allocation bitmap) always describes ONLY the
// writes made after the last generation swap. A read that misses on
// the live range (object missing on every replica) falls back through
// the frozen generations newest-to-oldest; nothing there means the
// block is zeros. This keeps the sdsnet protocol unchanged (no
// rename, no new ops) and a volume that never snapshots pays exactly
// what it paid before.
//
// Consistency model: the volume record is the single truth. Every
// snapshot operation is one read-modify-write under CAS
// (mutateVolumeChecked), so racing snapshot operations serialize -
// the parallel-delete chain-tear the plan warns about cannot happen.
// An interrupted operation leaks allocator ids at worst (the
// allocator is monotonic), never data.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// MaxSnapshots caps the snapshot chain of one volume. The cap bounds
// read-fallback depth (a block written in no generation walks the
// whole chain) and the volume record size; the uint32 Gen counter is
// 64-bit-safe far beyond it (the plan's 8-bit overflow pitfall is a
// non-issue here, and the tests run 300 generations).
const MaxSnapshots = 64

// GenRecord is one frozen snapshot generation: the addressing the
// volume used at snapshot time plus a copy of the allocation bitmap,
// so a reader knows exactly which objects of the frozen range hold
// data without touching the (live, mutated) allocmap.
type GenRecord struct {
	Gen       uint32      `json:"gen"`
	CreatedAt int64       `json:"created_at"` // unix seconds
	Size      uint64      `json:"size"`       // logical size at snapshot time
	OIDBase   uint64      `json:"oid_base"`
	OIDShift  uint32      `json:"oid_shift"`
	OIDCount  uint64      `json:"oid_count"`
	Extents   []OIDExtent `json:"extents,omitempty"`
	// Bits is the allocation bitmap (bit i = logical object index i
	// allocated) copied from the live allocmap at snapshot time.
	Bits []byte `json:"bits"`
}

// Lookup maps a logical object index to the object id inside this
// frozen generation (same base+extents walk as VolumeSpec.Lookup).
func (g GenRecord) Lookup(index uint64) (object.OID, bool) {
	if index < g.OIDCount {
		return object.OID(g.OIDBase + index + 1), true
	}
	index -= g.OIDCount
	for _, e := range g.Extents {
		if index < e.Count {
			return object.OID(e.Base + index + 1), true
		}
		index -= e.Count
	}
	return object.NilOID, false
}

// NumObjects is the object count the frozen range covers.
func (g GenRecord) NumObjects() uint64 {
	n := g.OIDCount
	for _, e := range g.Extents {
		n += e.Count
	}
	return n
}

// Ranges lists every private OID range of the generation in the
// vacuum-segment form ([Base+1 .. Base+Count] swept whole): the base
// range plus each extent. Vacuuming a generation never consults the
// allocmap - its whole reserved range is garbage by construction.
func (g GenRecord) Ranges() []OIDExtent {
	out := make([]OIDExtent, 0, 1+len(g.Extents))
	out = append(out, OIDExtent{Base: g.OIDBase, Count: g.OIDCount})
	out = append(out, g.Extents...)
	return out
}

// GenBit reports whether logical object index was allocated in this
// generation (length-tolerant: a short record means unallocated,
// matching GetAllocMap's zero-extension convention).
func (g GenRecord) GenBit(index uint64) bool {
	i := int(index)
	return i >= 0 && i < len(g.Bits)*8 && g.Bits[i/8]&(1<<(i%8)) != 0
}

// ErrTooManySnapshots is returned when a volume already carries
// MaxSnapshots generations.
var ErrTooManySnapshots = errors.New("meta: snapshot limit reached")

// ErrSnapshotNotFound is returned by RollbackVolume/DeleteSnapshot
// for a generation the volume does not have.
var ErrSnapshotNotFound = errors.New("meta: snapshot generation not found")

// ErrSnapshotsUnsupported marks operations that cannot run while a
// volume carries snapshots (v1): resize (the chain freezes the
// geometry).
var ErrSnapshotsUnsupported = errors.New("meta: operation not supported while snapshots exist")

// GenRange is one private OID range to vacuum (a whole generation is
// garbage once it stops being live).
type GenRange struct {
	Base  uint64 `json:"base"`
	Count uint64 `json:"count"`
}

// SnapshotInfo is the public summary of one generation (for ls/API).
type SnapshotInfo struct {
	Gen       uint32 `json:"gen"`
	CreatedAt int64  `json:"created_at"`
	Size      uint64 `json:"size"`
	Objects   int    `json:"objects"` // allocated objects in the generation
}

// CreateSnapshot freezes the volume's current data as read-only
// generation spec.Gen+1 and gives the live volume a brand-new OID
// range. One CAS on the volume record makes the swap atomic: a
// writer racing the flip lands in exactly one range (the old one,
// which the snapshot froze - so a racing write is lost exactly like
// a write racing a real snapshot, never duplicated). Steps inside the
// CAS loop: copy the live bitmap, reserve a fresh range (monotonic
// allocator; an interrupted attempt leaks ids at worst), swap live
// addressing, append the frozen record. Returns the committed spec.
func CreateSnapshot(ctx context.Context, s Store, name string) (VolumeSpec, error) {
	for {
		b, rev, err := s.Get(ctx, PrefixVolumes, name)
		if err != nil {
			return VolumeSpec{}, err
		}
		var spec VolumeSpec
		if err := json.Unmarshal(b, &spec); err != nil {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s: %w", name, err)
		}
		if len(spec.Snapshots) >= MaxSnapshots {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s: %w", name, ErrTooManySnapshots)
		}
		// Generation numbers never rewind: LastGen is the high-water
		// mark, so after a rollback (which sets Gen back) the next
		// snapshot is numbered above every generation that ever
		// existed, and "rollback to N" stays unambiguous.
		nextGen := spec.Gen + 1
		if spec.LastGen+1 > nextGen {
			nextGen = spec.LastGen + 1
		}
		if n := len(spec.Snapshots); n > 0 {
			if g := spec.Snapshots[n-1].Gen + 1; g > nextGen {
				nextGen = g
			}
		}
		// Freeze what the volume is right now.
		gen := GenRecord{
			Gen:       nextGen,
			CreatedAt: time.Now().Unix(),
			Size:      spec.Size,
			OIDBase:   spec.OIDBase,
			OIDShift:  spec.OIDShift,
			OIDCount:  spec.BaseObjs(),
			Extents:   append([]OIDExtent(nil), spec.Extents...),
		}
		rec, err := GetAllocMap(ctx, s, name, int(object.NumObjects(spec.Size)))
		if err != nil {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s snapshot: bitmap: %w", name, err)
		}
		gen.Bits = rec.Bits
		// Reserve the live volume's next private range before the
		// CAS: a CAS retry re-reserves (leaks at worst), a crash
		// between reserve and CAS leaks too - never overlaps.
		newBase, err := NextOIDBase(ctx, s, gen.OIDCount)
		if err != nil {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s snapshot: oid reserve: %w", name, err)
		}
		spec.Snapshots = append(spec.Snapshots, gen)
		spec.Gen = gen.Gen
		spec.LastGen = gen.Gen
		spec.OIDBase = newBase
		spec.OIDCount = gen.OIDCount
		spec.Extents = nil
		nb, err := json.Marshal(spec)
		if err != nil {
			return VolumeSpec{}, err
		}
		if _, err := s.Put(ctx, PrefixVolumes, name, nb, rev); err != nil {
			if errors.Is(err, ErrConflict) {
				continue // racing editor: re-read, re-reserve, retry
			}
			return VolumeSpec{}, err
		}
		return spec, nil
	}
}

// ListSnapshots summarizes the volume's frozen generations (oldest
// first) without shipping their bitmaps.
func ListSnapshots(ctx context.Context, s Store, name string) ([]SnapshotInfo, error) {
	spec, err := GetVolume(ctx, s, name)
	if err != nil {
		return nil, err
	}
	out := make([]SnapshotInfo, 0, len(spec.Snapshots))
	for _, g := range spec.Snapshots {
		out = append(out, SnapshotInfo{
			Gen:       g.Gen,
			CreatedAt: g.CreatedAt,
			Size:      g.Size,
			Objects:   AllocMapPopcount(g.Bits),
		})
	}
	return out, nil
}

// DeleteSnapshot drops one frozen generation; its ranges become
// garbage. Chain reads skip the removed record, so no data the
// volume still references is lost: the live bitmap and the surviving
// generations are untouched. Ranges shared with clones (genrefs,
// clones.go) are released, and only the ranges no other volume
// references are returned for the vacuum.
func DeleteSnapshot(ctx context.Context, s Store, name string, gen uint32) ([]GenRange, error) {
	var garbage []GenRange
	err := mutateVolumeChecked(ctx, s, name, func(spec *VolumeSpec) (bool, error) {
		idx := -1
		for i, g := range spec.Snapshots {
			if g.Gen == gen {
				idx = i
				break
			}
		}
		if idx < 0 {
			return false, fmt.Errorf("meta: volume %s snapshot %d: %w", name, gen, ErrSnapshotNotFound)
		}
		for _, r := range spec.Snapshots[idx].Ranges() {
			garbage = append(garbage, GenRange(r))
		}
		spec.Snapshots = append(spec.Snapshots[:idx], spec.Snapshots[idx+1:]...)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	// Release our references after the record CAS: clones that
	// pre-shared keep the ranges alive (see clones.go protocol).
	return DropGenRanges(ctx, s, garbage)
}

// RollbackVolume makes snapshot gen the live generation again: its
// frozen addressing AND its frozen bitmap become live (that is what
// rollback means: the volume sees exactly the snapshot's allocation),
// the previously live range becomes garbage, and every newer
// snapshot is dropped - rolling back discards the future. Returns
// the committed spec and every range now to vacuum.
//
// The volume record and the allocmap cannot flip in one CAS, so the
// order is fixed: write the generation's bitmap into the live
// allocmap key FIRST, then CAS the volume record. Until the volume
// CAS lands, readers address objects through the OLD live record and
// the bitmap is addressing-private (each range is owned by exactly
// one live addressing), so the premature bitmap write is invisible to
// every reader. Any crash in between leaves a consistent
// (record, bitmap) pair; clients re-read both after every spec
// change, so they converge.
//
// A write that raced into the old live range is discarded with it -
// it lands in no generation, exactly as on a real LUN reset.
func RollbackVolume(ctx context.Context, s Store, name string, gen uint32) (VolumeSpec, []GenRange, error) {
	var (
		out     VolumeSpec
		garbage []GenRange
	)
	for {
		b, rev, err := s.Get(ctx, PrefixVolumes, name)
		if err != nil {
			return VolumeSpec{}, nil, err
		}
		var spec VolumeSpec
		if err := json.Unmarshal(b, &spec); err != nil {
			return VolumeSpec{}, nil, fmt.Errorf("meta: volume %s: %w", name, err)
		}
		idx := -1
		for i, g := range spec.Snapshots {
			if g.Gen == gen {
				idx = i
				break
			}
		}
		if idx < 0 {
			return VolumeSpec{}, nil, fmt.Errorf("meta: volume %s rollback to gen %d: %w", name, gen, ErrSnapshotNotFound)
		}
		chosen := spec.Snapshots[idx]
		// Garbage: the discarded live range and every dropped newer
		// generation. The chosen range becomes LIVE (reads of it are
		// addressed through the live record now), so it is NOT
		// garbage - vacuuming it would erase the rollback image.
		garbage = garbage[:0]
		garbage = append(garbage, GenRange{Base: spec.OIDBase, Count: spec.BaseObjs()})
		for _, e := range spec.Extents {
			garbage = append(garbage, GenRange(e))
		}
		for _, g := range spec.Snapshots[idx+1:] {
			for _, r := range g.Ranges() {
				garbage = append(garbage, GenRange(r))
			}
		}
		// Step 1: bitmap first (see the ordering comment above). The
		// live allocmap key always holds the LIVE bitmap (snapshot
		// copies live inside GenRecord), so rollback overwrites it
		// with the chosen generation's copy. Unconditional put (Rev
		// -1): this write is invisible until the volume CAS lands,
		// and it must not fail on a racing quota edit's rev.
		bits := make([]byte, len(chosen.Bits))
		copy(bits, chosen.Bits)
		if _, err := PutAllocMap(ctx, s, name, AllocMapRecord{Bits: bits, Rev: -1}); err != nil {
			return VolumeSpec{}, nil, fmt.Errorf("meta: volume %s rollback: bitmap: %w", name, err)
		}
		// Step 2: flip the volume record. Size stays at the live
		// Size (grow is one-directional, shrink is never supported):
		// objects beyond the frozen bitmap read as zeros, the
		// correct rollback image for blocks grown after the snapshot.
		out = spec
		out.OIDBase = chosen.OIDBase
		out.OIDCount = chosen.OIDCount
		out.Extents = append([]OIDExtent(nil), chosen.Extents...)
		out.Gen = chosen.Gen
		// LastGen is deliberately left at the high-water mark: a
		// snapshot created after this rollback must be numbered above
		// every generation that ever existed (monotonic ids).
		// The chosen generation becomes live and every newer one is
		// discarded (they are the future); only older snapshots
		// remain in the chain.
		out.Snapshots = append([]GenRecord(nil), spec.Snapshots[:idx]...)
		nb, err := json.Marshal(out)
		if err != nil {
			return VolumeSpec{}, nil, err
		}
		if _, err := s.Put(ctx, PrefixVolumes, name, nb, rev); err != nil {
			if errors.Is(err, ErrConflict) {
				continue // racing editor: bitmap rewrite is harmless, redo
			}
			return VolumeSpec{}, nil, err
		}
		// Release the discarded ranges' clone references. The chosen
		// generation is NOT released: its ranges become this volume's
		// live ranges (it stays a holder; siblings keep theirs).
		garbage, err = DropGenRanges(ctx, s, garbage)
		if err != nil {
			return out, nil, err
		}
		return out, garbage, nil
	}
}

// VolumeGarbage lists every private range that stops being reachable
// when the volume record disappears (volume delete): the live range
// plus all generations. The delete path writes one tombstone covering
// all of them so no snapshot range leaks.
func VolumeGarbage(spec VolumeSpec) []GenRange {
	out := make([]GenRange, 0, 1+len(spec.Extents)+len(spec.Snapshots))
	out = append(out, GenRange{Base: spec.OIDBase, Count: spec.BaseObjs()})
	for _, e := range spec.Extents {
		out = append(out, GenRange(e))
	}
	for _, g := range spec.Snapshots {
		for _, r := range g.Ranges() {
			out = append(out, GenRange(r))
		}
	}
	return out
}

// mutateVolumeChecked is MutateVolume with an error-returning
// mutate.
func mutateVolumeChecked(ctx context.Context, s Store, name string, mutate func(*VolumeSpec) (bool, error)) error {
	for {
		b, rev, err := s.Get(ctx, PrefixVolumes, name)
		if err != nil {
			return err
		}
		var spec VolumeSpec
		if err := json.Unmarshal(b, &spec); err != nil {
			return fmt.Errorf("meta: volume %s: %w", name, err)
		}
		change, err := mutate(&spec)
		if err != nil {
			return err
		}
		if !change {
			return nil
		}
		nb, err := json.Marshal(spec)
		if err != nil {
			return err
		}
		if _, err := s.Put(ctx, PrefixVolumes, name, nb, rev); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return err
		}
		return nil
	}
}
