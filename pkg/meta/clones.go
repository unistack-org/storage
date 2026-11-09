package meta

// Volume cloning (plan M3, todos/adr-snapshots.md).
//
// A clone is a new volume record whose live OID range is freshly
// reserved and whose snapshot chain is a COPY of the source chain
// (the source's current state is frozen as one more generation
// first). No object data moves: the clone reads the shared frozen
// generations through the standard chain-read path, and its own
// writes land in its private live range. Cloning a 64 GiB image
// costs a handful of meta writes, and a "party of identical VMs"
// pays for a base image once.
//
// Sharing safety. A frozen generation's OID ranges are referenced by
// every volume whose record carries the generation, so the delete
// paths (DeleteSnapshot, Rollback, DeleteVolume) must not vacuum a
// range a sibling still reads. Reference counts live under the
// genrefs prefix, keyed by the range's OIDBase (ranges never overlap,
// so the base identifies the range). The invariant: a genrefs record
// exists iff Refs == the number of live volume records referencing
// the range. Volumes that were never cloned own their generations
// implicitly (no record; deleting the generation vacuums it).
//
// Clone protocol: (1) SHARE the source's current chain ranges and
// live range (absent record -> 2: the owner plus the coming clone;
// present -> +1), (2) CreateSnapshot the source under CAS, (3) create
// the destination record (create-only CAS, never clobbers). Steps 2
// and 3 re-read the source under CAS, so the destination's chain is
// exactly the source chain as of step 2 - never a range a concurrent
// DeleteSnapshot removed.
//
// Delete protocol: AFTER the volume-record CAS removes the
// generation, DROP the ranges (decrement; the writer that reaches 0
// deletes the record and owns the vacuum). A range whose record is
// absent was never shared: vacuum.
//
// Crash/abort leaks refs (records left behind keep ranges alive
// forever); it never destroys a referenced range. The ordering above
// is what makes the interleavings leak-safe: a clone pre-shares
// before its CAS, so any deletion that commits afterwards sees the
// record; a deletion that decided "absent" before the clone's CAS
// committed races only with clones whose (later) CAS re-read the
// chain WITHOUT the range.
//
// Generation numbering per volume stays independent (ids are unique
// within a record); the clone starts at Gen 0 with its own counter.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/unistack-org/storage/pkg/object"
)

// PrefixGenRefs holds per-range reference counts for shared snapshot
// generations (clones). Key: zero-padded OIDBase; value: GenRefRecord.
const PrefixGenRefs = "genrefs:"

// ErrVolumeExists is returned by CloneVolume when the destination
// name is taken.
var ErrVolumeExists = errors.New("meta: volume already exists")

// GenRefRecord counts the volume records that reference one private
// OID range (a snapshot generation's base range or extent).
type GenRefRecord struct {
	Base uint64 `json:"base"`
	Refs int    `json:"refs"`
}

func genRefKey(base uint64) string { return fmt.Sprintf("%020d", base) }

// ShareGenRanges records that one more volume will reference each
// range: absent -> 2 (the creating owner plus the coming clone),
// present -> +1. CAS loop; idempotent per call.
func ShareGenRanges(ctx context.Context, s Store, ranges []GenRange) error {
	for _, r := range ranges {
		key := genRefKey(r.Base)
		for {
			b, rev, err := s.Get(ctx, PrefixGenRefs, key)
			if err == ErrNotFound {
				nb, err := json.Marshal(GenRefRecord{Base: r.Base, Refs: 2})
				if err != nil {
					return err
				}
				if _, err := s.Put(ctx, PrefixGenRefs, key, nb, 0); err != nil {
					if errors.Is(err, ErrConflict) {
						continue // racing creator: re-read and increment
					}
					return err
				}
				break
			}
			if err != nil {
				return err
			}
			var rec GenRefRecord
			if err := json.Unmarshal(b, &rec); err != nil {
				return fmt.Errorf("meta: genrefs %d: %w", r.Base, err)
			}
			rec.Refs++
			nb, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if _, err := s.Put(ctx, PrefixGenRefs, key, nb, rev); err != nil {
				if errors.Is(err, ErrConflict) {
					continue
				}
				return err
			}
			break
		}
	}
	return nil
}

// DropGenRanges releases one volume's references. It returns exactly
// the ranges the caller must vacuum: ranges whose record was absent
// (never shared) or whose count reached 0 (this was the last
// holder). Ranges with remaining holders are left alone.
func DropGenRanges(ctx context.Context, s Store, ranges []GenRange) ([]GenRange, error) {
	var vacuum []GenRange
	for _, r := range ranges {
		key := genRefKey(r.Base)
		for {
			b, rev, err := s.Get(ctx, PrefixGenRefs, key)
			if err == ErrNotFound {
				vacuum = append(vacuum, r) // never shared: plain vacuum
				break
			}
			if err != nil {
				return nil, err
			}
			var rec GenRefRecord
			if err := json.Unmarshal(b, &rec); err != nil {
				return nil, fmt.Errorf("meta: genrefs %d: %w", r.Base, err)
			}
			if rec.Refs > 1 {
				rec.Refs--
				nb, err := json.Marshal(rec)
				if err != nil {
					return nil, err
				}
				if _, err := s.Put(ctx, PrefixGenRefs, key, nb, rev); err != nil {
					if errors.Is(err, ErrConflict) {
						continue
					}
					return nil, err
				}
				break // still referenced: the objects stay
			}
			// Last holder: remove the record; we own the vacuum.
			if err := s.Delete(ctx, PrefixGenRefs, key, rev); err != nil {
				if errors.Is(err, ErrConflict) {
					continue
				}
				if err == ErrNotFound {
					// A racing Share pre-claimed: the clone that
					// shared this range re-read the source chain
					// AFTER our volume-record CAS removed the
					// generation, so it does not reference it; the
					// fresh record is a leak, and vacuum proceeds.
					vacuum = append(vacuum, r)
					break
				}
				return nil, err
			}
			vacuum = append(vacuum, r)
			break
		}
	}
	return vacuum, nil
}

// LiveRanges lists the volume's live private ranges (base range plus
// resize extents) in vacuum-segment form.
func LiveRanges(spec VolumeSpec) []GenRange {
	out := make([]GenRange, 0, 1+len(spec.Extents))
	out = append(out, GenRange{Base: spec.OIDBase, Count: spec.BaseObjs()})
	for _, e := range spec.Extents {
		out = append(out, GenRange(e))
	}
	return out
}

// AllFrozenRanges lists every OID range of every frozen generation.
func AllFrozenRanges(spec VolumeSpec) []GenRange {
	var out []GenRange
	for _, g := range spec.Snapshots {
		for _, r := range g.Ranges() {
			out = append(out, GenRange(r))
		}
	}
	return out
}

// CloneVolume creates volume dstName as a clone of srcName: the
// source's current state is frozen as one more snapshot generation
// (the source pays the range swap, exactly like a manual snapshot),
// and dst starts with a fresh live range on top of a copy of the
// source's full chain. Geometry (size, placement, replication) is
// inherited verbatim - chain reads address the shared objects with
// the source's layout, so it must match. Quota 0 inherits the
// source's quota; QoS settings are inherited verbatim (the caller
// adjusts the clone afterwards).
func CloneVolume(ctx context.Context, s Store, srcName, dstName string, quota uint64) (VolumeSpec, error) {
	if srcName == "" || dstName == "" {
		return VolumeSpec{}, errors.New("meta: clone needs source and destination names")
	}
	if srcName == dstName {
		return VolumeSpec{}, errors.New("meta: clone destination equals source")
	}
	// Step 0: validate (a snapshot of the source's view; the CAS in
	// step 2 re-reads and re-checks the limits that matter).
	src, err := GetVolume(ctx, s, srcName)
	if err != nil {
		return VolumeSpec{}, err
	}
	if _, err := GetVolume(ctx, s, dstName); !errors.Is(err, ErrNotFound) {
		if err == nil {
			return VolumeSpec{}, fmt.Errorf("meta: clone destination %s: %w", dstName, ErrVolumeExists)
		}
		return VolumeSpec{}, err
	}
	if quota == 0 {
		quota = src.Quota
	}
	if quota > src.Size {
		return VolumeSpec{}, fmt.Errorf("meta: clone %s: quota %d exceeds size %d", dstName, quota, src.Size)
	}
	// The clone's chain = the source's chain + the freeze of its
	// current live state. Bound it by the same depth limit.
	if len(src.Snapshots)+1 > MaxSnapshots {
		return VolumeSpec{}, fmt.Errorf("meta: clone %s: source chain at %d generations: %w",
			dstName, len(src.Snapshots), ErrTooManySnapshots)
	}
	// Step 1: pre-share the source's ranges BEFORE the CAS, so any
	// deletion that commits after our CAS sees the refcount record
	// (see the protocol comment at the top). The live range is
	// shared too: after CreateSnapshot it becomes the newest frozen
	// generation the clone's chain carries.
	if err := ShareGenRanges(ctx, s, append(AllFrozenRanges(src), LiveRanges(src)...)); err != nil {
		return VolumeSpec{}, fmt.Errorf("meta: clone %s of %s: share: %w", dstName, srcName, err)
	}
	// Step 2: freeze the source's current state (and learn the exact
	// chain the clone will carry).
	src, err = CreateSnapshot(ctx, s, srcName)
	if err != nil {
		return VolumeSpec{}, fmt.Errorf("meta: clone %s of %s: snapshot source: %w", dstName, srcName, err)
	}
	// Step 3: the clone's private live range.
	nobjs := object.NumObjects(src.Size)
	base, err := NextOIDBase(ctx, s, nobjs+1)
	if err != nil {
		return VolumeSpec{}, fmt.Errorf("meta: clone %s: oid reserve: %w", dstName, err)
	}
	dst := VolumeSpec{
		Name:     dstName,
		Size:     src.Size,
		Quota:    quota,
		OIDBase:  base,
		OIDShift: src.OIDShift,
		OIDCount: nobjs,
		Replicas: src.Replicas,
		MinSize:  src.MinSize,
		Rule:     src.Rule,
		PGNum:    src.PGNum,
		Epoch:    src.Epoch,
		// QoS travels with the image: a cloned VM is a normal tenant.
		QoSTemplate: src.QoSTemplate,
		ReadIOPS:    src.ReadIOPS,
		ReadBPS:     src.ReadBPS,
		WriteIOPS:   src.WriteIOPS,
		WriteBPS:    src.WriteBPS,
		// The clone starts its own generation counter; the source's
		// frozen chain (including the freeze we just took) is the
		// clone's chain.
		Gen:       0,
		LastGen:   0,
		Snapshots: append([]GenRecord(nil), src.Snapshots...),
	}
	// Create-only CAS: a racing creator of the same name wins and we
	// leak ids/refs (a leak, not a clobber).
	b, err := json.Marshal(dst)
	if err != nil {
		return VolumeSpec{}, err
	}
	if _, err := s.Put(ctx, PrefixVolumes, dstName, b, 0); err != nil {
		if errors.Is(err, ErrConflict) {
			return VolumeSpec{}, fmt.Errorf("meta: clone destination %s: %w", dstName, ErrVolumeExists)
		}
		return VolumeSpec{}, err
	}
	return dst, nil
}
