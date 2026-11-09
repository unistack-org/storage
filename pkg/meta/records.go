package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/unistack-org/storage/pkg/object"
)

// Canonical key prefixes.
const (
	PrefixOSDs    = "osds"    // key: OSD id decimal, value: OSDInfo
	PrefixVolumes = "volumes" // key: volume name, value: VolumeSpec
	PrefixMaps    = "maps"    // key: epoch decimal, value: MapRecord
	// PrefixAlloc holds cluster-wide allocators; the "oids" key is the
	// next free object id (see NextOIDBase).
	PrefixAlloc = "alloc"
	// PrefixQoSClasses holds named IOPS/bandwidth classes referenced by
	// volume specs (per-user tiers).
	PrefixQoSClasses = "qos"
)

// AllocOIDs is the allocator key for volume object-id ranges.
const AllocOIDs = "oids"

// OSDInfo is one OSD's registration.
type OSDInfo struct {
	ID       int32   `json:"id"`
	Addr     string  `json:"addr"`      // host:port of the sdsnet server
	UUID     string  `json:"uuid"`      // stable per-device identity
	DataPath string  `json:"data_path"` // human info only
	Weight   float64 `json:"weight"`    // crush weight, 0 = out
	BootedAt int64   `json:"booted_at"`
}

// VolumeSpec describes a volume (thin-provisioned object-backed image).
type VolumeSpec struct {
	Name string `json:"name"`
	Size uint64 `json:"size"` // logical bytes
	// Quota caps the bytes the volume may actually allocate on the
	// OSDs (thin provisioning, plan M0.4): a write that first touches
	// a new object-granularity block beyond the cap fails with ENOSPC,
	// a sparse trim that frees whole blocks gives room back. 0 means
	// the quota equals Size (the volume can never hold more than its
	// logical size). The counter is the per-volume allocation bitmap
	// (allocmap prefix), not a per-OSD byte figure.
	Quota uint64 `json:"quota,omitempty"`
	// OIDBase is the first object id of the volume's private OID range.
	// Objects of a volume are OIDBase+1 .. OIDBase+NumObjects(Size); OID
	// OIDBase itself is reserved (mirrors the NilOID = 0 convention). The
	// base comes from the cluster-wide allocator (NextOIDBase), so two
	// volumes never address the same object on a shared OSD.
	OIDBase  uint64 `json:"oid_base"`
	OIDShift uint32 `json:"oid_shift"` // object size = 1<<OIDShift
	// OIDCount is the object count the base range was reserved for at
	// create time. Online grow (ResizeVolume) cannot extend a contiguous
	// reservation (other volumes allocate from the top meanwhile), so a
	// grown volume gains extra Extents; Lookup maps a logical object
	// index across the base range plus the extents. 0 on a legacy record
	// means "created at Size, never grown" (BaseObjs derives it).
	OIDCount uint64      `json:"oid_count,omitempty"`
	Extents  []OIDExtent `json:"extents,omitempty"`
	Replicas int         `json:"replicas"`
	// MinSize is the minimum number of live replicas a volume stays
	// writable with (quorum width). 0 means the Ceph-style default
	// Replicas/2+1. Below it, writes fail fast instead of hanging:
	// the volume is degraded and must be recovered (Vitastor
	// pg_minsize equivalent).
	MinSize int    `json:"min_size,omitempty"`
	Rule    string `json:"rule"` // crush rule name
	PGNum   int    `json:"pg_num"`
	Epoch   uint32 `json:"epoch"` // map epoch the volume was created/last rebalanced at
	// QoS: sustained ceilings enforced by every client that opens the
	// volume (so the limit travels with the volume, whoever touches it)
	// and, for exports, by the OSD serving them. 0 on an axis =
	// unlimited. An all-zero QoS is the admin class: operators,
	// backup/restore, and the admin HTTP API are never throttled.
	// QoSTemplate names a QoSClassRecord (the per-user tier): when set
	// and the class exists, the class rates override the inline fields
	// (OpenVolume resolves it).
	QoSTemplate string `json:"qos_template,omitempty"`
	ReadIOPS    uint64 `json:"read_iops,omitempty"`
	ReadBPS     uint64 `json:"read_bps,omitempty"`
	WriteIOPS   uint64 `json:"write_iops,omitempty"`
	WriteBPS    uint64 `json:"write_bps,omitempty"`

	// Gen is the current snapshot generation: 0 on a volume that
	// never had a snapshot; CreateSnapshot increments it. The live
	// addressing (OIDBase/OIDCount/Extents) plus the live allocmap
	// always describe ONLY writes made in the current generation; a
	// read that misses on the live range falls back through
	// Snapshots newest-to-oldest (see snapshots.go).
	Gen uint32 `json:"gen,omitempty"`

	// LastGen is the generation high-water mark: the largest generation
	// number this volume ever used. Generation ids are monotonic and
	// never rewound: a rollback sets Gen back to the chosen generation
	// and drops the newer ones, but LastGen stays, so the next
	// CreateSnapshot numbers above every generation that ever existed
	// (a stale client holding snapshot gen 5 can never be confused with
	// a newly created gen 5).
	LastGen uint32 `json:"last_gen,omitempty"`

	// Snapshots are the frozen read-only generations, oldest first.
	// The live volume is generation Gen and is not listed here.
	Snapshots []GenRecord `json:"snapshots,omitempty"`

	// ReadOnly marks the volume for read-only serving: an export (NBD
	// or VDUSE) created for it advertises the device read-only
	// (NBD_FLAG_READ_ONLY / VIRTIO_BLK_F_RO) and refuses every write,
	// and the flag lives here so the setting survives a daemon
	// restart that re-creates the export from meta.
	ReadOnly bool `json:"read_only,omitempty"`
	// Shared advertises that the volume may be exported to several
	// clients at once (NBD_FLAG_SEND_FLUSH semantics aside, it is the
	// caller's contract that the guest uses a shared-capable
	// filesystem). VDUSE devices are inherently single-attach.
	Shared bool `json:"shared,omitempty"`
	// Owner is free-form bookkeeping (the control plane that created
	// the volume, e.g. "cell:<id>"); the daemon never interprets it.
	Owner string `json:"owner,omitempty"`
}

// QoSed reports whether the spec carries any limit.
func (v VolumeSpec) QoSed() bool {
	return v.ReadIOPS != 0 || v.ReadBPS != 0 || v.WriteIOPS != 0 || v.WriteBPS != 0
}

// OIDExtent is an extra object-id range a volume received by growing
// online. Like the base range, Base itself is unused: objects are
// Base+1 .. Base+Count, so an extent never collides with the id its
// predecessor's last object uses (the allocator hands out contiguous,
// non-overlapping ranges).
type OIDExtent struct {
	Base  uint64 `json:"base"`
	Count uint64 `json:"count"`
}

// BaseObjs is the object count the base range covers: recorded on
// resize, derived from Size for records created before growth existed
// (a grown legacy record has Extents, and its OIDCount was written
// with it).
func (v VolumeSpec) BaseObjs() uint64 {
	if v.OIDCount != 0 {
		return v.OIDCount
	}
	return object.NumObjects(v.Size)
}

// Lookup maps a logical object index (0-based within the volume) to its
// object id, walking the base range and then the extents in order. It
// reports false for an index at or beyond the reserved object count
// (a caller must never address it: the ids belong to other volumes).
func (v VolumeSpec) Lookup(index uint64) (object.OID, bool) {
	if base := v.BaseObjs(); index < base {
		return object.OID(v.OIDBase + index + 1), true
	}
	index -= v.BaseObjs()
	for _, e := range v.Extents {
		if index < e.Count {
			return object.OID(e.Base + index + 1), true
		}
		index -= e.Count
	}
	return object.NilOID, false
}

// ReservedObjs is the total object count the volume may address (base
// plus extents). After a resize it is >= NumObjects(Size): grow only
// appends whole/partial ranges as needed.
func (v VolumeSpec) ReservedObjs() uint64 {
	n := v.BaseObjs()
	for _, e := range v.Extents {
		n += e.Count
	}
	return n
}

// MapRecord stores one crush map generation.
type MapRecord struct {
	Epoch  uint32 `json:"epoch"`
	Format string `json:"format"` // "text" | "binary" | "json"
	Map    []byte `json:"map"`
}

// PutOSD registers (or refreshes) an OSD record.
func PutOSD(ctx context.Context, s Store, info OSDInfo) (int64, error) {
	b, err := json.Marshal(info)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixOSDs, strconv.FormatInt(int64(info.ID), 10), b, -1)
}

// GetOSD reads one OSD record.
func GetOSD(ctx context.Context, s Store, id int32) (OSDInfo, error) {
	var info OSDInfo
	b, _, err := s.Get(ctx, PrefixOSDs, strconv.FormatInt(int64(id), 10))
	if err != nil {
		return info, err
	}
	return info, json.Unmarshal(b, &info)
}

// SetOSDWeight changes one OSD's CRUSH weight (0 = out of placement).
func SetOSDWeight(ctx context.Context, s Store, id int32, w float64) error {
	info, err := GetOSD(ctx, s, id)
	if err != nil {
		return err
	}
	info.Weight = w
	_, err = PutOSD(ctx, s, info)
	return err
}

// ListOSDs reads every registered OSD, sorted by id.
func ListOSDs(ctx context.Context, s Store) ([]OSDInfo, error) {
	keys, err := s.List(ctx, PrefixOSDs)
	if err != nil {
		return nil, err
	}
	out := make([]OSDInfo, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixOSDs, k)
		if err != nil {
			return nil, fmt.Errorf("meta: osd %s: %w", k, err)
		}
		var info OSDInfo
		if err := json.Unmarshal(b, &info); err != nil {
			return nil, fmt.Errorf("meta: osd %s: %w", k, err)
		}
		out = append(out, info)
	}
	return out, nil
}

// NextOIDBase reserves the next count object ids from the cluster-wide
// allocator and returns the first one. It compare-and-swaps the allocator
// record, so concurrent creators never hand out overlapping ranges; the
// reservation is durable (a restarted cluster continues after the
// highest base ever handed out). Zero is never returned: the reserved 0
// value is the first free id and it is skipped, keeping the NilOID
// convention intact for the first volume's first object.
func NextOIDBase(ctx context.Context, s Store, count uint64) (uint64, error) {
	if count == 0 {
		return 0, fmt.Errorf("meta: oid reservation for zero objects")
	}
	for {
		cur, rev, err := s.Get(ctx, PrefixAlloc, AllocOIDs)
		var next uint64
		have := false
		if err == nil {
			if err := json.Unmarshal(cur, &next); err != nil {
				return 0, fmt.Errorf("meta: corrupt alloc record: %w", err)
			}
			have = true
		} else if err != ErrNotFound {
			return 0, err
		}
		base := next
		if base == 0 {
			base = 1 // NilOID is reserved; ranges start at 1
		}
		if base > ^uint64(0)-count {
			return 0, fmt.Errorf("meta: oid space exhausted")
		}
		b, err := json.Marshal(base + count)
		if err != nil {
			return 0, err
		}
		cas := int64(-1)
		if have {
			cas = rev
		}
		if _, err := s.Put(ctx, PrefixAlloc, AllocOIDs, b, cas); err != nil {
			if errors.Is(err, ErrConflict) {
				continue // racing creator: re-read and retry
			}
			return 0, err
		}
		return base, nil
	}
}

// ResizeVolume grows a volume to newSize (grow-only: shrinking would
// strand the data in the removed objects, and the guest's filesystem
// has no idea its disk got smaller). Growing a volume is a two-phase
// meta change and it is crash-safe in this order:
//
//  1. Reserve the extra object ids from the cluster allocator. The
//     allocator is monotonic, so an interrupted grow leaks ids at worst
//     (harmless) and can never make the grown range overlap a range a
//     later volume receives.
//  2. CAS-write the new spec (Size grows; OIDBase, replicas and the
//     rest are untouched). A concurrent editor makes the CAS fail, so
//     two racing resizes cannot both "win" with a lost update, and a
//     concurrent QoS/quota edit forces a retry against the fresh spec.
//
// The allocation bitmap needs no touch: GetAllocMap zero-extends a
// bitmap written before the grow (the new objects are, truthfully,
// unallocated), and clients refresh their cached size from the volume
// watch. Shrinking is refused (ErrShrinkUnsupported). Returns the
// committed spec.
func ResizeVolume(ctx context.Context, s Store, name string, newSize uint64) (VolumeSpec, error) {
	for {
		b, rev, err := s.Get(ctx, PrefixVolumes, name)
		if err != nil {
			return VolumeSpec{}, err
		}
		var spec VolumeSpec
		if err := json.Unmarshal(b, &spec); err != nil {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s: %w", name, err)
		}
		if newSize < spec.Size {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s: shrink %d < %d: %w",
				name, newSize, spec.Size, ErrShrinkUnsupported)
		}
		if newSize == spec.Size {
			return spec, nil // idempotent no-op
		}
		if len(spec.Snapshots) > 0 {
			// Snapshots freeze the geometry: the chain fallback walks
			// frozen ranges sized for the old capacity, and a grown
			// live range would need per-generation extent tracking.
			// v1 refuses (delete the snapshots, grow, re-snapshot).
			return VolumeSpec{}, fmt.Errorf("meta: volume %s: resize with %d snapshot(s): %w",
				name, len(spec.Snapshots), ErrSnapshotsUnsupported)
		}
		// The volume addresses indices [0, NumObjects(newSize)). The
		// base plus existing extents cover ReservedObjs; the delta is
		// the new range to reserve. Extents (not a widened base) are
		// used because the allocator hands ranges out from the top:
		// the ids directly above a volume's base may belong to a
		// volume created after it.
		want := object.NumObjects(newSize)
		if have := spec.ReservedObjs(); want > have {
			base, err := NextOIDBase(ctx, s, want-have)
			if err != nil {
				return VolumeSpec{}, fmt.Errorf("meta: volume %s resize: oid reserve: %w", name, err)
			}
			// Pin the base count so BaseObjs stays stable across
			// further grows (the first resize freezes what create
			// reserved).
			if spec.OIDCount == 0 {
				spec.OIDCount = object.NumObjects(spec.Size)
			}
			spec.Extents = append(spec.Extents, OIDExtent{Base: base, Count: want - have})
		}
		spec.Size = newSize
		// Quota deliberately stays where the operator put it: growing
		// the volume never raises the allocation cap.
		nb, err := json.Marshal(spec)
		if err != nil {
			return VolumeSpec{}, err
		}
		if _, err := s.Put(ctx, PrefixVolumes, name, nb, rev); err != nil {
			if errors.Is(err, ErrConflict) {
				continue // racing editor: re-read and re-check
			}
			return VolumeSpec{}, err
		}
		return spec, nil
	}
}

// ErrShrinkUnsupported is returned by ResizeVolume for a shrink.
var ErrShrinkUnsupported = errors.New("meta: volume shrink is not supported")

// PutVolume stores a volume spec.
func PutVolume(ctx context.Context, s Store, spec VolumeSpec) (int64, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixVolumes, spec.Name, b, -1)
}

// ListVolumes returns all registered volume specs, sorted by name.
func ListVolumes(ctx context.Context, s Store) ([]VolumeSpec, error) {
	keys, err := s.List(ctx, PrefixVolumes)
	if err != nil {
		return nil, err
	}
	out := make([]VolumeSpec, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixVolumes, k)
		if err != nil {
			if err == ErrNotFound {
				continue // deleted concurrently
			}
			return nil, fmt.Errorf("meta: volume %s: %w", k, err)
		}
		var spec VolumeSpec
		if err := json.Unmarshal(b, &spec); err != nil {
			return nil, fmt.Errorf("meta: volume %s: %w", k, err)
		}
		out = append(out, spec)
	}
	return out, nil
}

// DeleteVolume removes a volume record.
func DeleteVolume(ctx context.Context, s Store, name string) error {
	return s.Delete(ctx, PrefixVolumes, name, -1)
}

// GetVolume reads a volume spec.
func GetVolume(ctx context.Context, s Store, name string) (VolumeSpec, error) {
	var spec VolumeSpec
	b, _, err := s.Get(ctx, PrefixVolumes, name)
	if err != nil {
		return spec, err
	}
	return spec, json.Unmarshal(b, &spec)
}

// MutateVolume read-modify-writes a volume spec under CAS: mutate
// receives the current record and returns false to leave it untouched.
// A racing editor (a resize appending extents, another QoS edit) makes
// the CAS fail and the pair is re-read and re-merged, so concurrent
// edits of different fields never overwrite each other.
func MutateVolume(ctx context.Context, s Store, name string, mutate func(*VolumeSpec) bool) (VolumeSpec, error) {
	for {
		b, rev, err := s.Get(ctx, PrefixVolumes, name)
		if err != nil {
			return VolumeSpec{}, err
		}
		var spec VolumeSpec
		if err := json.Unmarshal(b, &spec); err != nil {
			return VolumeSpec{}, fmt.Errorf("meta: volume %s: %w", name, err)
		}
		if !mutate(&spec) {
			return spec, nil
		}
		nb, err := json.Marshal(spec)
		if err != nil {
			return VolumeSpec{}, err
		}
		if _, err := s.Put(ctx, PrefixVolumes, name, nb, rev); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return VolumeSpec{}, err
		}
		return spec, nil
	}
}

// PutMap stores a crush map generation and returns its revision.
func PutMap(ctx context.Context, s Store, rec MapRecord) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixMaps, strconv.FormatUint(uint64(rec.Epoch), 10), b, -1)
}

// LatestMap returns the highest-epoch map record, or ErrNotFound.
func LatestMap(ctx context.Context, s Store) (MapRecord, error) {
	keys, err := s.List(ctx, PrefixMaps)
	if err != nil {
		return MapRecord{}, err
	}
	if len(keys) == 0 {
		return MapRecord{}, ErrNotFound
	}
	best := keys[0]
	for _, k := range keys[1:] {
		a, _ := strconv.ParseUint(k, 10, 32)
		b, _ := strconv.ParseUint(best, 10, 32)
		if a > b {
			best = k
		}
	}
	b, _, err := s.Get(ctx, PrefixMaps, best)
	if err != nil {
		return MapRecord{}, err
	}
	var rec MapRecord
	return rec, json.Unmarshal(b, &rec)
}

// QoSClassRecord is a named IOPS/bandwidth class (the "user tier" of
// per-user limits): volumes reference classes by name via
// VolumeSpec.QoSTemplate. 0 on an axis = unlimited there; an all-zero
// class (or a missing record) is the admin class: no limits.
type QoSClassRecord struct {
	Name      string `json:"name"`
	ReadIOPS  uint64 `json:"read_iops,omitempty"`
	ReadBPS   uint64 `json:"read_bps,omitempty"`
	WriteIOPS uint64 `json:"write_iops,omitempty"`
	WriteBPS  uint64 `json:"write_bps,omitempty"`
}

// Pair converts the record into a qos-free limiter spec: the four rates.
// (The meta package must not import qos; call sites build the pair.)
func (r QoSClassRecord) Rates() (readIOPS, readBPS, writeIOPS, writeBPS uint64) {
	return r.ReadIOPS, r.ReadBPS, r.WriteIOPS, r.WriteBPS
}

// PutQoSClass stores a class record.
func PutQoSClass(ctx context.Context, s Store, rec QoSClassRecord) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixQoSClasses, rec.Name, b, -1)
}

// GetQoSClass reads one class record (ErrNotFound when absent: callers
// treat a missing class as unlimited, never as an error at open time).
func GetQoSClass(ctx context.Context, s Store, name string) (QoSClassRecord, error) {
	var rec QoSClassRecord
	b, _, err := s.Get(ctx, PrefixQoSClasses, name)
	if err != nil {
		return rec, err
	}
	err = json.Unmarshal(b, &rec)
	return rec, err
}

// ListQoSClasses returns every class record.
func ListQoSClasses(ctx context.Context, s Store) ([]QoSClassRecord, error) {
	keys, err := s.List(ctx, PrefixQoSClasses)
	if err != nil {
		return nil, err
	}
	out := make([]QoSClassRecord, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixQoSClasses, k)
		if err != nil {
			return nil, err
		}
		var rec QoSClassRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("meta: qos class %q: %w", k, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// DeleteQoSClass removes a class record.
func DeleteQoSClass(ctx context.Context, s Store, name string) error {
	return s.Delete(ctx, PrefixQoSClasses, name, -1)
}
