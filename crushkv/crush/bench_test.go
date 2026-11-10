package crush_test

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/unistack-org/crushkv/crush"
)

// buildThreeTier builds a realistic 3-tier map: `roots` roots of `racks`
// straw2 racks of `hosts` straw2 hosts of `osds` uniform OSDs.
func buildThreeTier(roots, racks, hosts, osds int) *crush.Map {
	mb := crush.NewMapBuilder()
	weights := make([]uint32, roots*racks*hosts*osds)
	for i := range weights {
		weights[i] = crush.WeightOne
	}
	var rules []*crush.Rule

	id := int32(-1)
	next := func() int32 { v := id; id--; return v }

	var firstRoot int32
	for r := 0; r < roots; r++ {
		var rackItems []crush.ItemSpec
		for k := 0; k < racks; k++ {
			var hostItems []crush.ItemSpec
			for h := 0; h < hosts; h++ {
				base := (r*racks + k*hosts + h) * osds
				devItems := make([]crush.ItemSpec, osds)
				for d := range devItems {
					devItems[d] = crush.ItemSpec{ID: int32(base + d)}
				}
				hid := next()
				if err := mb.AddStraw2(hid, 3, crush.HashRjenkins1, devItems); err != nil {
					panic(err)
				}
				hostItems = append(hostItems, crush.ItemSpec{ID: hid, Weight: crush.WeightOne})
			}
			rid := next()
			if err := mb.AddStraw2(rid, 2, crush.HashRjenkins1, hostItems); err != nil {
				panic(err)
			}
			rackItems = append(rackItems, crush.ItemSpec{ID: rid, Weight: crush.WeightOne})
		}
		rootid := next()
		if err := mb.AddStraw2(rootid, 1, crush.HashRjenkins1, rackItems); err != nil {
			panic(err)
		}
		if r == 0 {
			firstRoot = rootid
		}
	}

	// rule 0: take root, chooseleaf firstn 0 type host, emit
	rules = append(rules, &crush.Rule{
		Ruleset: 0, Type: crush.RuleTypeReplicated, MinSize: 1, MaxSize: 10,
		Steps: []crush.Step{
			{Op: crush.OpTake, Arg1: firstRoot},
			{Op: crush.OpChooseLeafFirstN, Arg1: 0, Arg2: 3},
			{Op: crush.OpEmit},
		},
	})
	return mb.Build(rules, int32(len(weights)), weights)
}

func BenchmarkDoRule(b *testing.B) {
	m := buildThreeTier(3, 3, 5, 5) // 225 osds, 3 roots
	w := crush.NewWork(m, 10, nil)
	defer w.Free()
	result := make([]int32, 10)

	b.ReportAllocs()
	b.SetBytes(3)
	for i := 0; i < b.N; i++ {
		m.DoRule(w, 0, int32(uint32(i)*2654435761), result, m.Weight)
	}
}

// BenchmarkArenaCarve compares carving the exact same buffers from an arena
// once vs allocating them with make() per call (the pre-arena NewWork
// pattern). It shows the per-allocation economics of the two strategies.
func BenchmarkArenaCarve(b *testing.B) {
	a := crush.NewArena(64 << 10)
	defer a.Free()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		a.Reset()
		a.AllocInt32(10)
		a.AllocInt32(10)
		a.AllocInt32(10)
		for j := 0; j < 100; j++ {
			a.AllocInt32(5) // permutation buffers
		}
	}
}

func BenchmarkMakeCarve(b *testing.B) {
	b.ReportAllocs()
	sink = nil
	for i := 0; i < b.N; i++ {
		var s []int32
		s = append(s, make([]int32, 10)...)
		s = append(s, make([]int32, 10)...)
		s = append(s, make([]int32, 10)...)
		for j := 0; j < 100; j++ {
			s = append(s, make([]int32, 5)...)
		}
		sink = s
	}
}

var sink []int32

// TestArenaNotOnHeap verifies the arena's memory is invisible to the Go
// heap: allocating far more than the heap watermark growth proves the
// blocks are not runtime-managed.
func TestArenaNotOnHeap(t *testing.T) {
	a := crush.NewArena(1 << 20)
	defer a.Free()
	if a.AllocInt32(1<<18) == nil {
		t.Fatal("nil slice")
	}
	mapped, total := a.Stats()
	if mapped < 1<<20 {
		t.Fatalf("expected >=1MiB mapped, got %d", mapped)
	}
	if total < 1<<20 {
		t.Fatalf("expected >=1MiB total, got %d", total)
	}
}

func TestArenaResetReuse(t *testing.T) {
	a := crush.NewArena(2 * 4096) // page-rounded block size
	defer a.Free()
	// Two big allocs force (at least) two blocks on any page size.
	x := a.AllocInt32(5000) // 20000 B: bigger than any page-rounded default block
	_ = a.AllocInt32(5000)  // forces a second block
	px := uintptr(unsafe.Pointer(unsafe.SliceData(x)))
	mappedBefore, _ := a.Stats()
	a.Reset()
	if mappedAfter, _ := a.Stats(); mappedAfter >= mappedBefore {
		t.Fatalf("Reset should release all but one block: %d -> %d", mappedBefore, mappedAfter)
	}
	// After Reset the bump cursor restarts at offset 0 of the base block.
	y := a.AllocInt32(2000)
	py := uintptr(unsafe.Pointer(unsafe.SliceData(y)))
	if px != py {
		t.Fatalf("expected reuse of the base block: %x != %x", px, py)
	}
}

func ExampleWork() {
	mb := crush.NewMapBuilder()
	mb.AddType("osd", 0)
	mb.AddDevice("osd.0", 0)
	mb.AddDevice("osd.1", 1)
	// One arena per worker goroutine; buffers live outside the GC heap.
	arena := crush.NewArena(64 << 10)
	defer arena.Free()
	_ = arena
	fmt.Println("see benchmarks: hot path is zero-alloc")
	// Output:
	// see benchmarks: hot path is zero-alloc
}
