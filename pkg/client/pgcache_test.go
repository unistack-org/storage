package client

import (
	"errors"
	"testing"

	"github.com/unistack-org/storage/pkg/placement"
)

func compileMap3(t testing.TB, epoch uint32, reg placement.Registry) *placement.Snapshot {
	t.Helper()
	s, err := placement.Compile(epoch, []byte(map3), reg, placement.Options{PGNum: 64, Replicas: 3})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return s
}

func reg3() placement.Registry {
	return placement.Registry{
		0: {OSD: 0, Addr: "127.0.0.1:1", Export: "data"},
		1: {OSD: 1, Addr: "127.0.0.1:2", Export: "data"},
		2: {OSD: 2, Addr: "127.0.0.1:3", Export: "data"},
	}
}

// The cache answers from the cached row while the snapshot pointer is
// unchanged (one compute per pg per epoch), copies into scratch, and
// drops every entry when the epoch swaps.
func TestPGCache(t *testing.T) {
	var c pgCache
	snap := compileMap3(t, 1, reg3())
	calls := map[uint32]int{}
	compute := func(pg uint32) func() ([]placement.Ref, error) {
		return func() ([]placement.Ref, error) {
			calls[pg]++
			var out []placement.Ref
			for _, id := range snap.Row(pg) {
				if id >= 0 {
					if r, ok := snap.LookupRef(id); ok {
						out = append(out, r)
					}
				}
			}
			return out, nil
		}
	}
	var scratch [3]placement.Ref
	for rep := 0; rep < 5; rep++ {
		for pg := uint32(0); pg < 64; pg++ {
			refs, err := c.resolve(snap, pg, scratch[:], compute(pg))
			if err != nil {
				t.Fatalf("resolve pg %d: %v", pg, err)
			}
			if len(refs) != 3 {
				t.Fatalf("pg %d: %d refs", pg, len(refs))
			}
			for i, r := range refs {
				want := snap.Row(pg)[i]
				if r.OSD != want {
					t.Fatalf("pg %d ref %d: %d != %d", pg, i, r.OSD, want)
				}
			}
		}
	}
	for pg, n := range calls {
		if n != 1 {
			t.Fatalf("pg %d computed %d times", pg, n)
		}
	}

	// Epoch swap (new snapshot pointer) must invalidate everything.
	snap2 := compileMap3(t, 2, reg3())
	refs, err := c.resolve(snap2, 7, scratch[:], compute(7))
	if err != nil || len(refs) != 3 {
		t.Fatalf("after swap: %v %v", len(refs), err)
	}
	if calls[7] != 2 {
		t.Fatalf("epoch swap did not invalidate: calls=%d", calls[7])
	}
	// The old snapshot is not current any more; its entries are gone.
	c.resolve(snap2, 8, scratch[:], compute(8))
	if calls[8] != 2 { // once on snap, once on snap2 (cache was cleared)
		t.Fatalf("pg 8 computed %d times", calls[8])
	}
	c.resolve(snap2, 8, scratch[:], compute(8))
	if calls[8] != 2 {
		t.Fatalf("pg 8 recomputed on snap2 hit: %d", calls[8])
	}
}

// The targets() verdict (ErrDegraded on a thin row) is cached too:
// callers keep seeing it on every hit, and the row is not recomputed
// per I/O while degraded.
func TestPGCacheDegradedVerdict(t *testing.T) {
	var c pgCache
	thin := placement.Registry{0: {OSD: 0, Addr: "127.0.0.1:1", Export: "data"}} // one OSD for a rep3 map
	snap := compileMap3(t, 1, thin)
	calls := 0
	fn := func() ([]placement.Ref, error) {
		calls++
		var out []placement.Ref
		for _, id := range snap.Row(3) {
			if id >= 0 {
				if r, ok := snap.LookupRef(id); ok {
					out = append(out, r)
				}
			}
		}
		if len(out) < 3 {
			return out, ErrDegraded
		}
		return out, nil
	}
	var scratch [3]placement.Ref
	for i := 0; i < 4; i++ {
		refs, err := c.resolve(snap, 3, scratch[:], fn)
		if !errors.Is(err, ErrDegraded) {
			t.Fatalf("i=%d: err=%v", i, err)
		}
		_ = refs
	}
	if calls != 1 {
		t.Fatalf("degraded verdict recomputed %d times", calls)
	}
}
