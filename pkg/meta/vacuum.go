package meta

import (
	"context"
	"encoding/json"
	"fmt"
)

// PrefixVacuum holds deletion tombstones: one record per volume whose
// objects still have to be vacuumed from the OSDs (plan M1.4). The
// key is the volume name, so a name that is created again while an
// older vacuum is pending needs no extra disambiguation: the tombstone
// carries the private OID range of the *deleted* incarnation, and the
// fresh volume addresses a disjoint range (the OID allocator never
// reuses ranges).
//
// DeleteVolume writes the tombstone *before* removing the volume
// record, so a crash between the two meta writes can only leak a
// tombstone (harmless: the vacuum removes objects that exist and skips
// the rest), never orphan a range nobody knows about.
const PrefixVacuum = "vacuums"

// VacuumRecord is one tombstone: the object range to vacuum.
type VacuumRecord struct {
	Name      string `json:"name"`
	OIDBase   uint64 `json:"oid_base"`
	NumObjs   uint64 `json:"num_objects"` // count; objects are Base+1..Base+NumObjs
	DeletedAt int64  `json:"deleted_at"`  // unix seconds, informational
}

// PutVacuum records a tombstone (overwrite is fine: a repeated delete
// of the same name describes the same range only if the spec did not
// change; the latest write wins and both ranges get swept because the
// old record is consumed by the vacuumer first).
func PutVacuum(ctx context.Context, s Store, rec VacuumRecord) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixVacuum, rec.Name, b, -1)
}

// GetVacuum reads one tombstone (ErrNotFound when absent).
func GetVacuum(ctx context.Context, s Store, name string) (VacuumRecord, error) {
	var rec VacuumRecord
	b, _, err := s.Get(ctx, PrefixVacuum, name)
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(b, &rec); err != nil {
		return rec, fmt.Errorf("meta: vacuum %s: %w", name, err)
	}
	return rec, nil
}

// ListVacuum returns all pending tombstones, sorted by key.
func ListVacuum(ctx context.Context, s Store) ([]VacuumRecord, error) {
	keys, err := s.List(ctx, PrefixVacuum)
	if err != nil {
		return nil, err
	}
	out := make([]VacuumRecord, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixVacuum, k)
		if err != nil {
			if err == ErrNotFound {
				continue // swept concurrently
			}
			return nil, fmt.Errorf("meta: vacuum %s: %w", k, err)
		}
		var rec VacuumRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("meta: vacuum %s: %w", k, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// DeleteVacuum removes a tombstone once its range is fully vacuumed.
// A missing tombstone is not an error (vacuums are replayable).
func DeleteVacuum(ctx context.Context, s Store, name string) error {
	err := s.Delete(ctx, PrefixVacuum, name, -1)
	if err == ErrNotFound {
		return nil
	}
	return err
}
