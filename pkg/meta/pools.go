package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
)

// ErrInvalidPool marks a pool policy the placement layer cannot honour
// (bad name, replica/min_size/pg_num combinations). It wraps into
// CreatePool errors so HTTP callers can answer 400.
var ErrInvalidPool = errors.New("meta: invalid pool")

// ErrPoolInUse marks a pool delete refused while volumes still
// reference the pool.
var ErrPoolInUse = errors.New("meta: pool in use")

// PoolRecord is a cluster-wide pool: a namespace carrying the shared
// placement policy (replica count, min_size, PG count, CRUSH rule, QoS
// template). Volumes are created inside a pool (like in Ceph); per-volume
// flags override the pool's values. Pools live under PrefixPools keyed
// by name.
type PoolRecord struct {
	Name        string `json:"name"`
	Replicas    int    `json:"replicas"`
	MinSize     int    `json:"min_size"`
	PGNum       int    `json:"pg_num"`
	Rule        string `json:"rule,omitempty"`
	QoSTemplate string `json:"qos_template,omitempty"`
}

// ValidName reports whether s is usable as a pool, volume, class or
// export name: 1..64 bytes of [a-zA-Z0-9_.-], and not "." / "..".
// Hand-rolled to avoid a regexp dependency in this package.
func ValidName(s string) bool {
	if len(s) == 0 || len(s) > 64 || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '_' || c == '.' || c == '-':
		default:
			return false
		}
	}
	return true
}

// Validate rejects a pool the placement layer cannot honour: replica
// count below one, min_size outside 1..replicas, a PG count that is not
// a power of two (the PG-to-OSD mapping uses a bit mask), and a pool
// whose PG count exceeds the cluster default (a volume may never carry
// more PGs than the client snapshot offers, so such a pool could never
// place a volume). clusterPGNum <= 0 skips the last check. Failures
// wrap ErrInvalidPool so HTTP callers can answer 400.
func (p *PoolRecord) Validate(clusterPGNum int) error {
	if !ValidName(p.Name) {
		return fmt.Errorf("%w: name %q must match [a-zA-Z0-9_.-]{1,64}", ErrInvalidPool, p.Name)
	}
	if p.Replicas < 1 {
		return fmt.Errorf("%w: pool %s: replicas must be >= 1", ErrInvalidPool, p.Name)
	}
	if p.MinSize < 1 || p.MinSize > p.Replicas {
		return fmt.Errorf("%w: pool %s: min_size %d must be in 1..replicas(%d)", ErrInvalidPool, p.Name, p.MinSize, p.Replicas)
	}
	if p.PGNum < 1 || p.PGNum&(p.PGNum-1) != 0 {
		return fmt.Errorf("%w: pool %s: pg_num %d must be a power of two", ErrInvalidPool, p.Name, p.PGNum)
	}
	if clusterPGNum > 0 && p.PGNum > clusterPGNum {
		return fmt.Errorf("%w: pool %s: pg_num %d exceeds cluster pg_num %d", ErrInvalidPool, p.Name, p.PGNum, clusterPGNum)
	}
	return nil
}

// PutPool stores a pool record. Overwriting an existing pool is refused
// with ErrConflict (CAS at revision 0 = key must not exist): placement
// decisions already made against the old policy stay in effect, and
// silently changing a live pool is how data ends up in the wrong place.
// Delete and re-create to change a policy.
func PutPool(ctx context.Context, s Store, rec PoolRecord, clusterPGNum int) (int64, error) {
	if err := rec.Validate(clusterPGNum); err != nil {
		return 0, err
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixPools, rec.Name, b, 0)
}

// GetPool reads one pool record (ErrNotFound when absent).
func GetPool(ctx context.Context, s Store, name string) (PoolRecord, error) {
	var rec PoolRecord
	b, _, err := s.Get(ctx, PrefixPools, name)
	if err != nil {
		return rec, err
	}
	err = json.Unmarshal(b, &rec)
	return rec, err
}

// ListPools returns every pool record (s.List returns sorted keys, so
// the result is ordered by name).
func ListPools(ctx context.Context, s Store) ([]PoolRecord, error) {
	keys, err := s.List(ctx, PrefixPools)
	if err != nil {
		return nil, err
	}
	out := make([]PoolRecord, 0, len(keys))
	for _, k := range keys {
		b, _, err := s.Get(ctx, PrefixPools, k)
		if err != nil {
			if err == ErrNotFound {
				continue // deleted concurrently
			}
			return nil, err
		}
		var rec PoolRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return nil, fmt.Errorf("meta: pool %q: %w", k, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// DeletePool removes a pool record (ErrNotFound when absent). The caller
// (client/admin) is responsible for refusing the delete while volumes
// still reference the pool.
func DeletePool(ctx context.Context, s Store, name string) error {
	return s.Delete(ctx, PrefixPools, name, -1)
}

// CountPoolVolumes counts volumes whose spec names the given pool.
func CountPoolVolumes(ctx context.Context, s Store, pool string) (int, error) {
	vols, err := ListVolumes(ctx, s)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, v := range vols {
		if v.Pool == pool {
			n++
		}
	}
	return n, nil
}

// Pow2Ceil rounds n up to the next power of two. Values needing more
// than 62 bits clamp at 1<<62: the power-of-two checks elsewhere in the
// project reject anything that large, and clamping keeps the shift safe.
func Pow2Ceil(n int) int {
	if n <= 1 {
		return 1
	}
	if bits.Len(uint(n-1)) >= 63 {
		return 1 << 62
	}
	return 1 << bits.Len(uint(n-1))
}
