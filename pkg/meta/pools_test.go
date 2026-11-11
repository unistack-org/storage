package meta

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func newPoolStore(t *testing.T) Store {
	t.Helper()
	s, err := OpenDir(filepath.Join(t.TempDir(), "meta"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPoolCRUD(t *testing.T) {
	ctx := context.Background()
	s := newPoolStore(t)

	rec := PoolRecord{Name: "fast", Replicas: 3, MinSize: 2, PGNum: 1024, Rule: "replicated", QoSTemplate: "std"}
	if _, err := PutPool(ctx, s, rec, 1024); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Create-if-absent: a second put is a conflict, not an overwrite.
	if _, err := PutPool(ctx, s, PoolRecord{Name: "fast", Replicas: 5, MinSize: 3, PGNum: 1024}, 1024); !errors.Is(err, ErrConflict) {
		t.Fatalf("re-put: want ErrConflict, got %v", err)
	}
	got, err := GetPool(ctx, s, "fast")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Replicas != 3 || got.MinSize != 2 || got.PGNum != 1024 || got.Rule != "replicated" || got.QoSTemplate != "std" {
		t.Fatalf("get: record round-trip lost data: %+v", got)
	}
	// The original survived the rejected re-put.
	if got.Replicas != 3 {
		t.Fatalf("re-put overwrote the pool: %+v", got)
	}

	if _, err := PutPool(ctx, s, PoolRecord{Name: "ssd", Replicas: 2, MinSize: 2, PGNum: 64}, 1024); err != nil {
		t.Fatalf("put ssd: %v", err)
	}
	all, err := ListPools(ctx, s)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 || all[0].Name != "fast" || all[1].Name != "ssd" {
		t.Fatalf("list: want [fast ssd] sorted, got %+v", all)
	}

	if err := DeletePool(ctx, s, "ssd"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := GetPool(ctx, s, "ssd"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete: want ErrNotFound, got %v", err)
	}
	if err := DeletePool(ctx, s, "ssd"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("double delete: want ErrNotFound, got %v", err)
	}
}

func TestPoolValidate(t *testing.T) {
	ok := PoolRecord{Name: "p1", Replicas: 3, MinSize: 2, PGNum: 1024}
	if err := ok.Validate(1024); err != nil {
		t.Fatalf("valid pool rejected: %v", err)
	}
	bad := []struct {
		title   string
		rec     PoolRecord
		cluster int
	}{
		{"empty name", PoolRecord{Name: "", Replicas: 3, MinSize: 2, PGNum: 64}, 0},
		{"path-y name", PoolRecord{Name: "a/b", Replicas: 3, MinSize: 2, PGNum: 64}, 0},
		{"replicas zero", PoolRecord{Name: "p", Replicas: 0, MinSize: 1, PGNum: 64}, 0},
		{"min_size over replicas", PoolRecord{Name: "p", Replicas: 3, MinSize: 4, PGNum: 64}, 0},
		{"pg_num not pow2", PoolRecord{Name: "p", Replicas: 3, MinSize: 2, PGNum: 100}, 0},
		{"pg_num over cluster", PoolRecord{Name: "p", Replicas: 3, MinSize: 2, PGNum: 2048}, 1024},
	}
	for _, b := range bad {
		err := b.rec.Validate(b.cluster)
		if err == nil {
			t.Fatalf("%s: accepted", b.title)
		}
		if !errors.Is(err, ErrInvalidPool) {
			t.Fatalf("%s: want ErrInvalidPool, got %v", b.title, err)
		}
	}
}

func TestCountPoolVolumes(t *testing.T) {
	ctx := context.Background()
	s := newPoolStore(t)
	for _, v := range []VolumeSpec{
		{Name: "a", Pool: "fast"},
		{Name: "b", Pool: "fast"},
		{Name: "c", Pool: "slow"},
		{Name: "d"},
	} {
		if _, err := PutVolume(ctx, s, v); err != nil {
			t.Fatalf("put %s: %v", v.Name, err)
		}
	}
	if n, err := CountPoolVolumes(ctx, s, "fast"); err != nil || n != 2 {
		t.Fatalf("count fast: got %d, %v; want 2, nil", n, err)
	}
	if n, err := CountPoolVolumes(ctx, s, "none"); err != nil || n != 0 {
		t.Fatalf("count none: got %d, %v; want 0, nil", n, err)
	}
}

func TestValidName(t *testing.T) {
	for _, s := range []string{"a", "fast-1", "fast_1", "fast.1", "A9"} {
		if !ValidName(s) {
			t.Fatalf("ValidName(%q) = false", s)
		}
	}
	for _, s := range []string{"", ".", "..", "a/b", "a b", "a\x00", string(make([]byte, 65))} {
		if ValidName(s) {
			t.Fatalf("ValidName(%q) = true", s)
		}
	}
}

func TestPow2Ceil(t *testing.T) {
	for _, c := range [][2]int{{0, 1}, {1, 1}, {2, 2}, {3, 4}, {100, 128}, {1024, 1024}, {1025, 2048}} {
		if got := Pow2Ceil(c[0]); got != c[1] {
			t.Fatalf("Pow2Ceil(%d) = %d, want %d", c[0], got, c[1])
		}
	}
	if got := Pow2Ceil(1 << 62); got != 1<<62 {
		t.Fatalf("Pow2Ceil(1<<62) = %d, want clamp at 1<<62", got)
	}
}
