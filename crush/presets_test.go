package crush_test

import (
	"testing"

	"github.com/unistack-org/storage/crush"
)

// Type ids for the DC-shaped test map: root -> datacenter -> rack -> host -> osd.
const (
	presetTypRoot = int32(1)
	presetTypDC   = int32(2)
	presetTypRack = int32(3)
	presetTypHost = int32(4)
)

// presetBuild assembles one root over dcs DCs, each with racks racks of
// hosts hosts of uniform straw2 buckets over osdsPerHost OSDs, and freezes
// the map with rules returned by mkRules (called once bucket ids are known).
func presetBuild(dcs, racks, hosts, osdsPerHost int, mkRules func(rootID int32, dcIDs []int32) []*crush.Rule) (*crush.Map, int32, []int32) {
	mb := crush.NewMapBuilder()
	total := dcs * racks * hosts * osdsPerHost
	weights := make([]uint32, total)
	for i := range weights {
		weights[i] = crush.WeightOne
	}
	id := int32(-1)
	next := func() int32 { v := id; id--; return v }
	rootID := next() // root is allocated first, so it is always -1

	var dcIDs []int32
	dev := 0
	for d := 0; d < dcs; d++ {
		var rackItems []crush.ItemSpec
		for r := 0; r < racks; r++ {
			var hostItems []crush.ItemSpec
			for h := 0; h < hosts; h++ {
				devItems := make([]crush.ItemSpec, osdsPerHost)
				for i := range devItems {
					devItems[i] = crush.ItemSpec{ID: int32(dev)}
					dev++
				}
				hid := next()
				if err := mb.AddStraw2(hid, presetTypHost, crush.HashRjenkins1, devItems); err != nil {
					panic(err)
				}
				hostItems = append(hostItems, crush.ItemSpec{ID: hid, Weight: crush.WeightOne})
			}
			rid := next()
			if err := mb.AddStraw2(rid, presetTypRack, crush.HashRjenkins1, hostItems); err != nil {
				panic(err)
			}
			rackItems = append(rackItems, crush.ItemSpec{ID: rid, Weight: crush.WeightOne})
		}
		dcid := next()
		if err := mb.AddStraw2(dcid, presetTypDC, crush.HashRjenkins1, rackItems); err != nil {
			panic(err)
		}
		dcIDs = append(dcIDs, dcid)
	}
	var dcItems []crush.ItemSpec
	for _, id := range dcIDs {
		dcItems = append(dcItems, crush.ItemSpec{ID: id, Weight: crush.WeightOne})
	}
	if err := mb.AddStraw2(rootID, presetTypRoot, crush.HashRjenkins1, dcItems); err != nil {
		panic(err)
	}

	rules := mkRules(rootID, dcIDs)
	m := mb.Build(rules, int32(total), weights)
	return m, rootID, dcIDs
}

// ancestorsOfType returns bucket ids of the given type on the path from
// root down to device dev (inclusive of buckets that contain dev).
func ancestorsOfType(m *crush.Map, rootID, dev, typ int32) []int32 {
	var out []int32
	var walk func(bid int32) bool
	walk = func(bid int32) bool {
		b := m.Buckets[-1-bid]
		if b == nil {
			return false
		}
		for _, it := range b.Items {
			if it == dev {
				if b.Type == typ {
					out = append(out, bid)
				}
				return true
			}
			if it < 0 && walk(it) {
				if b.Type == typ {
					out = append(out, bid)
				}
				return true
			}
		}
		return false
	}
	walk(rootID)
	return out
}

// placeMany runs rule ruleno for many x values, asserting exactly n
// placements per x, and calls check with the placed devices.
func placeMany(t *testing.T, m *crush.Map, ruleno int32, n int, iters int, check func(x int32, placed []int32)) {
	t.Helper()
	w := crush.NewWork(m, 10, nil)
	result := make([]int32, 10)
	for i := 0; i < iters; i++ {
		x := int32(uint32(i) * 2654435761)
		got := m.DoRule(w, ruleno, x, result[:n], m.Weight)
		if got != n {
			t.Fatalf("x=%d: placed %d, want %d", x, got, n)
		}
		check(x, result[:n])
	}
}

func TestPresetRackAwareRep3DistinctRacks(t *testing.T) {
	// 2 DCs x 3 racks x 2 hosts x 4 osds = 48 osds, 6 racks.
	m, rootID, _ := presetBuild(2, 3, 2, 4, func(root int32, _ []int32) []*crush.Rule {
		return []*crush.Rule{crush.RackAwareRep3(root, presetTypRack)}
	})
	placeMany(t, m, 0, 3, 2000, func(x int32, placed []int32) {
		racks := map[int32]bool{}
		for _, dev := range placed {
			if dev < 0 {
				t.Fatalf("x=%d: no device placed: %v", x, placed)
			}
			anc := ancestorsOfType(m, rootID, dev, presetTypRack)
			if len(anc) != 1 {
				t.Fatalf("device %d has %d rack ancestors", dev, len(anc))
			}
			racks[anc[0]] = true
		}
		if len(racks) != 3 {
			t.Fatalf("x=%d: %v not in 3 distinct racks: %v", x, placed, racks)
		}
	})
}

func TestPresetRackAwareRep3DistinctDCs(t *testing.T) {
	// 6 DCs of 2 racks each. Passing the DC type as the failure domain
	// makes the datacenter the rack: three replicas land in three
	// distinct DCs. (With failure_domain=rack, CRUSH only guarantees
	// distinct racks; replicas may share a DC above them.)
	m, rootID, _ := presetBuild(6, 2, 2, 2, func(root int32, _ []int32) []*crush.Rule {
		return []*crush.Rule{crush.RackAwareRep3(root, presetTypDC)}
	})
	placeMany(t, m, 0, 3, 1000, func(x int32, placed []int32) {
		dcs := map[int32]bool{}
		for _, dev := range placed {
			anc := ancestorsOfType(m, rootID, dev, presetTypDC)
			if len(anc) != 1 {
				t.Fatalf("device %d has %d DC ancestors", dev, len(anc))
			}
			dcs[anc[0]] = true
		}
		if len(dcs) != 3 {
			t.Fatalf("x=%d: %v not in 3 distinct DCs: %v", x, placed, dcs)
		}
	})
}

func TestPresetRackAwareRep3PoolSizeVaried(t *testing.T) {
	// numrep=0: the same rule serves any pool size (1..10); the placement
	// time result length picks the count, like Ceph's stock rules.
	m, _, _ := presetBuild(2, 5, 2, 2, func(root int32, _ []int32) []*crush.Rule {
		return []*crush.Rule{crush.RackAwareRep3(root, presetTypRack)}
	})
	w := crush.NewWork(m, 10, nil)
	result := make([]int32, 10)
	for size := 1; size <= 5; size++ {
		x := int32(uint32(size) * 2654435761)
		if n := m.DoRule(w, 0, x, result[:size], m.Weight); n != size {
			t.Fatalf("size=%d: placed %d, want %d", size, n, size)
		}
	}
}

func TestPresetEC2Plus2AcrossDCs(t *testing.T) {
	// 4 DCs; a 2+2 pool must spread its 4 shards one per distinct DC.
	m, rootID, dcIDs := presetBuild(4, 2, 2, 2, func(root int32, _ []int32) []*crush.Rule {
		return []*crush.Rule{crush.EC2Plus2AcrossDCs(root, presetTypDC)}
	})
	dcSet := map[int32]bool{}
	for _, id := range dcIDs {
		dcSet[id] = true
	}
	placeMany(t, m, 0, 4, 1000, func(x int32, placed []int32) {
		dcs := map[int32]bool{}
		for _, dev := range placed {
			if dev < 0 {
				t.Fatalf("x=%d: shard missing: %v", x, placed)
			}
			anc := ancestorsOfType(m, rootID, dev, presetTypDC)
			if len(anc) != 1 || !dcSet[anc[0]] {
				t.Fatalf("device %d not under a DC: %v", dev, anc)
			}
			dcs[anc[0]] = true
		}
		if len(dcs) != 4 {
			t.Fatalf("x=%d: shards %v not in 4 distinct DCs: %v", x, placed, dcs)
		}
	})
}

func TestPresetDCPreferredPinnedInDC(t *testing.T) {
	// 3 DCs; placement must stay inside the preferred DC, spread over racks.
	// The DCPreferred rule is at slice index 0, so DoRule ruleno is 0; the
	// Ruleset field is metadata that callers use to route a volume's PGs.
	const dc = 1
	m, rootID, dcIDs := presetBuild(3, 3, 2, 2, func(_ int32, ids []int32) []*crush.Rule {
		return []*crush.Rule{crush.DCPreferred(1, ids[dc], presetTypRack)}
	})
	placeMany(t, m, 0, 3, 500, func(x int32, placed []int32) {
		racks := map[int32]bool{}
		for _, dev := range placed {
			if dev < 0 {
				t.Fatalf("x=%d: device missing: %v", x, placed)
			}
			anc := ancestorsOfType(m, rootID, dev, presetTypDC)
			if len(anc) != 1 || anc[0] != dcIDs[dc] {
				t.Fatalf("x=%d: device %d outside preferred DC: %v", x, dev, anc)
			}
			racks[ancestorsOfType(m, rootID, dev, presetTypRack)[0]] = true
		}
		if len(racks) != 3 {
			t.Fatalf("x=%d: %v not in 3 distinct racks", x, placed)
		}
	})
}
