package meta

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

// Canonical key prefixes.
const (
	PrefixOSDs    = "osds"    // key: OSD id decimal, value: OSDInfo
	PrefixVolumes = "volumes" // key: volume name, value: VolumeSpec
	PrefixMaps    = "maps"    // key: epoch decimal, value: MapRecord
)

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
	Name     string `json:"name"`
	Size     uint64 `json:"size"`      // logical bytes
	OIDShift uint32 `json:"oid_shift"` // object size = 1<<OIDShift
	Replicas int    `json:"replicas"`
	// MinSize is the minimum number of live replicas a volume stays
	// writable with (quorum width). 0 means the Ceph-style default
	// Replicas/2+1. Below it, writes fail fast instead of hanging:
	// the volume is degraded and must be recovered (Vitastor
	// pg_minsize equivalent).
	MinSize int    `json:"min_size,omitempty"`
	Rule    string `json:"rule"` // crush rule name
	PGNum   int    `json:"pg_num"`
	Epoch   uint32 `json:"epoch"` // map epoch the volume was created/last rebalanced at
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

// PutVolume stores a volume spec.
func PutVolume(ctx context.Context, s Store, spec VolumeSpec) (int64, error) {
	b, err := json.Marshal(spec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixVolumes, spec.Name, b, -1)
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
