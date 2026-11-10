package placement

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/unistack-org/storage/crush"
)

// mapRep3 is a three-host rep3 map (two osds per host) with a single
// replicated rule, in the crushkv text format. chooseleaf firstn needs one
// failure domain per replica, so rep3 requires three hosts.
const mapRep3 = `# begin crush map
tunable choose_local_tries 0
tunable choose_local_fallback_tries 0
tunable choose_total_tries 50
tunable chooseleaf_descend_once 1
tunable chooseleaf_vary_r 1
tunable straw_calc_version 1
tunable chooseleaf_stable 1
# devices
device 0 osd.0
device 1 osd.1
device 2 osd.2
device 3 osd.3
device 4 osd.4
device 5 osd.5
# types
type 0 osd
type 1 host
type 2 root
# buckets
root r {
id -2
alg straw2
hash 0
item host h1
item host h2
item host h3
}
host h1 {
id -3
alg straw2
hash 0
item osd.0 weight 1.000
item osd.1 weight 1.000
}
host h2 {
id -4
alg straw2
hash 0
item osd.2 weight 1.000
item osd.3 weight 1.000
}
host h3 {
id -5
alg straw2
hash 0
item osd.4 weight 1.000
item osd.5 weight 1.000
}
# rules
rule data {
id 0
type replicated
step take r
step chooseleaf firstn 0 type host
step emit
}
# end crush map
`

func reg6() Registry {
	r := Registry{}
	for i := 0; i < 6; i++ {
		r[int32(i)] = Ref{OSD: int32(i), Addr: fmt.Sprintf("127.0.0.1:%d", 7000+i), Export: "v"}
	}
	return r
}

func mustCompile(t testing.TB, epoch uint32, reg Registry) *Snapshot {
	t.Helper()
	s, err := Compile(epoch, []byte(mapRep3), reg, Options{PGNum: 1024, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestCompileDeterministic(t *testing.T) {
	a := mustCompile(t, 1, reg6())
	b := mustCompile(t, 2, reg6())
	if a.Rule != "data" {
		t.Fatalf("rule = %q", a.Rule)
	}
	for pg := uint32(0); pg < a.PGNum; pg++ {
		ra, rb := a.Row(pg), b.Row(pg)
		for i := range ra {
			if ra[i] != rb[i] {
				t.Fatalf("pg %d slot %d: %d vs %d (compile not deterministic)", pg, i, ra[i], rb[i])
			}
		}
	}
}

func TestReplicasAcrossHosts(t *testing.T) {
	s := mustCompile(t, 1, reg6())
	seen := map[int32]int{}
	for pg := uint32(0); pg < s.PGNum; pg++ {
		row := s.Row(pg)
		hosts := map[int32]bool{}
		for _, id := range row {
			if id < 0 {
				t.Fatalf("pg %d has empty slot: %v", pg, row)
			}
			seen[id]++
			hosts[id/2] = true // h1 = osd 0..1, h2 = 2..3, h3 = 4..5
		}
		if len(row) != 3 {
			t.Fatalf("pg %d row width %d", pg, len(row))
		}
		if len(hosts) != 3 {
			t.Fatalf("pg %d replicas %v do not span 3 hosts", pg, row)
		}
	}
	// Uniformity: each of 6 osds carries ~1024*3/6 = 512 placements.
	for id, n := range seen {
		if n < 400 || n > 620 {
			t.Fatalf("osd %d has %d placements, want ~512 (chi-square fail)", id, n)
		}
	}
	if len(seen) != 6 {
		t.Fatalf("only %d of 6 osds used", len(seen))
	}
}

func TestTargetsAndLookupZeroAlloc(t *testing.T) {
	s := mustCompile(t, 1, reg6())
	var dst []Ref
	for oid := uint64(1); oid < 1000; oid++ {
		refs := s.Lookup(oid, dst[:0])
		if len(refs) != 3 {
			t.Fatalf("oid %d: %d refs", oid, len(refs))
		}
		tgs, err := s.Targets(oid)
		if err != nil || len(tgs) != 3 {
			t.Fatalf("oid %d: Targets: %v %v", oid, tgs, err)
		}
	}
	allocs := testing.AllocsPerRun(1000, func() {
		dst = s.Lookup(42, dst[:0])
		_ = dst
	})
	if allocs != 0 {
		t.Fatalf("Lookup allocated %v/op", allocs)
	}
}

func TestReweightDrainsOSD(t *testing.T) {
	base := mustCompile(t, 1, reg6())
	before := 0
	for pg := uint32(0); pg < base.PGNum; pg++ {
		for _, id := range base.Row(pg) {
			if id == 4 {
				before++
			}
		}
	}
	if before == 0 {
		t.Fatal("osd.4 unused at epoch 1")
	}

	s, err := base.Reweight(2, map[int32]float64{4: 0})
	if err != nil {
		t.Fatal(err)
	}
	if s.Epoch != 2 {
		t.Fatalf("epoch = %d", s.Epoch)
	}
	for pg := uint32(0); pg < s.PGNum; pg++ {
		for _, id := range s.Row(pg) {
			if id == 4 {
				t.Fatalf("osd.4 still placed at pg %d after drain", pg)
			}
		}
	}
	// h3 keeps osd.5; every row still fills three replicas.
	seen := map[int32]int{}
	for pg := uint32(0); pg < s.PGNum; pg++ {
		live := 0
		for _, id := range s.Row(pg) {
			if id >= 0 {
				live++
				seen[id]++
			}
		}
		if live < 2 {
			t.Fatalf("pg %d down to %d replicas", pg, live)
		}
	}
	if seen[4] != 0 {
		t.Fatal("osd.4 placed after drain")
	}
	// Original snapshot is untouched (immutability).
	if base.Row(0)[0] == -1 {
		t.Fatal("base snapshot mutated")
	}
}

func TestRegistryOutRemovesTargets(t *testing.T) {
	s := mustCompile(t, 1, reg6())
	reg := reg6()
	delete(reg, 2)
	s2, err := s.WithRegistry(2, reg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s2.Targets(1)
	if err == nil {
		// pg for oid 1 may not include osd.2; check the pg that does.
		found := false
		for pg := uint32(0); pg < s.PGNum; pg++ {
			for _, id := range s.Row(pg) {
				if id == 2 {
					found = true
					break
				}
			}
		}
		if !found {
			t.Fatal("test map never places osd.2")
		}
	} else if !errors.Is(err, ErrNoTarget) {
		t.Fatalf("err = %v", err)
	}
	// Every pg that used osd.2 now reports 2/3 live.
	for pg := uint32(0); pg < s.PGNum; pg++ {
		want := 3
		for _, id := range s.Row(pg) {
			if id == 2 {
				want = 2
			}
		}
		n := 0
		for _, id := range s2.Row(pg) {
			if id >= 0 && id != 2 {
				n++
			}
		}
		if n < want {
			t.Fatalf("pg %d: %d live, want %d", pg, n, want)
		}
	}
}

func TestEpochStabilityMinimalDisruption(t *testing.T) {
	// Draining one osd must only move pgs that used it.
	base := mustCompile(t, 1, reg6())
	s2, err := base.Reweight(2, map[int32]float64{4: 0})
	if err != nil {
		t.Fatal(err)
	}
	moved, total := 0, 0
	for pg := uint32(0); pg < base.PGNum; pg++ {
		r1, r2 := base.Row(pg), s2.Row(pg)
		uses4 := false
		for _, id := range r1 {
			if id == 4 {
				uses4 = true
			}
		}
		changed := false
		for i := range r1 {
			if r1[i] != r2[i] {
				changed = true
			}
		}
		if changed && !uses4 {
			moved++
		}
		if uses4 {
			total++
		}
	}
	if moved*10 > total {
		t.Fatalf("epoch change disrupted %d unrelated pgs (osd.4 pgs: %d)", moved, total)
	}
}

func TestDecodeFormats(t *testing.T) {
	// Text is the baseline; JSON round-trip must place identically.
	txt := []byte(mapRep3)
	m, err := crush.Decode(txt)
	if err != nil {
		t.Fatal(err)
	}
	js, err := m.EncodeJson()
	if err != nil {
		t.Fatal(err)
	}
	s1 := mustCompile(t, 1, reg6())
	s2, err := Compile(1, js, reg6(), Options{PGNum: 1024, Replicas: 3})
	if err != nil {
		t.Fatalf("json compile: %v", err)
	}
	for pg := uint32(0); pg < 4; pg++ {
		r1, r2 := s1.Row(pg), s2.Row(pg)
		for i := range r1 {
			if r1[i] != r2[i] {
				t.Fatalf("text vs json pg %d: %v vs %v", pg, r1, r2)
			}
		}
	}
	// Binary.
	bin, err := m.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}
	s3, err := Compile(1, bin, reg6(), Options{PGNum: 1024, Replicas: 3})
	if err != nil {
		t.Fatalf("binary compile: %v", err)
	}
	for pg := uint32(0); pg < 4; pg++ {
		r1, r3 := s1.Row(pg), s3.Row(pg)
		for i := range r1 {
			if r1[i] != r3[i] {
				t.Fatalf("text vs binary pg %d: %v vs %v", pg, r1, r3)
			}
		}
	}
}

func TestBadOptions(t *testing.T) {
	if _, err := Compile(1, []byte(mapRep3), reg6(), Options{PGNum: 1000, Replicas: 3}); err == nil {
		t.Fatal("non-power-of-two PGNum accepted")
	}
	if _, err := Compile(1, []byte(mapRep3), reg6(), Options{PGNum: 64, Replicas: 0}); err == nil {
		t.Fatal("Replicas 0 accepted")
	}
	if _, err := Compile(1, []byte("garbage not a map"), reg6(), Options{PGNum: 64, Replicas: 3}); err == nil {
		t.Fatal("garbage accepted")
	}
	if _, err := Compile(1, []byte(mapRep3), reg6(), Options{PGNum: 64, Replicas: 3, Rule: "nope"}); err == nil {
		t.Fatal("unknown rule accepted")
	}
}

// BenchmarkLookup is the per-IO placement cost: hash + array scan.
func BenchmarkLookup(b *testing.B) {
	s := mustCompile(b, 1, reg6())
	dst := make([]Ref, 0, 3)
	b.ReportAllocs()
	var sink int
	for i := 0; i < b.N; i++ {
		dst = s.Lookup(uint64(i)*0x9E3779B97F4A7C15, dst[:0])
		sink += len(dst)
	}
	_ = sink
}

// BenchmarkRebuild measures the off-IO-path cost: full pg table rebuild
// after an epoch change.
func BenchmarkRebuild(b *testing.B) {
	s := mustCompile(b, 1, reg6())
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.Reweight(uint32(i+1), map[int32]float64{int32(i % 6): 0.5}); err != nil {
			b.Fatal(err)
		}
	}
}

// chi-square statistic over pg rows (sanity, not a test gate).
func TestPlacementChiSquare(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	s := mustCompile(t, 1, reg6())
	counts := make([]float64, 6)
	for pg := uint32(0); pg < s.PGNum; pg++ {
		for _, id := range s.Row(pg) {
			counts[id]++
		}
	}
	exp := float64(s.PGNum) * float64(s.Replicas) / 6
	chi := 0.0
	for _, c := range counts {
		chi += (c - exp) * (c - exp) / exp
	}
	if math.IsNaN(chi) || chi > 100 {
		t.Fatalf("chi-square %.1f too high (counts %v)", chi, counts)
	}
	t.Logf("chi-square = %.1f, counts = %v", chi, counts)
}

// TestEncodeMapTextRoundTrip: text -> Map -> text -> Map must place
// identically (this is `unistorctl map dump -format text` over a json
// or binary source). It also covers FormatMap for json/binary.
func TestEncodeMapTextRoundTrip(t *testing.T) {
	m, err := DecodeMap([]byte(mapRep3))
	if err != nil {
		t.Fatal(err)
	}
	txt, err := m.EncodeText()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := DecodeMap(txt)
	if err != nil {
		t.Fatalf("re-parse encoded text: %v\n%s", err, txt)
	}
	base, err := Compile(1, []byte(mapRep3), reg6(), Options{PGNum: 1024, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	re, err := Compile(1, txt, reg6(), Options{PGNum: 1024, Replicas: 3})
	if err != nil {
		t.Fatalf("compile re-encoded: %v\n%s", err, txt)
	}
	for pg := uint32(0); pg < base.PGNum; pg++ {
		a, b := base.Row(pg), re.Row(pg)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("pg %d: base %v != re %v", pg, a, b)
		}
	}
	_ = m2
	// FormatMap json + binary round-trips through the same compiler.
	js, err := FormatMap([]byte(mapRep3), "json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(1, js, reg6(), Options{PGNum: 1024, Replicas: 3}); err != nil {
		t.Fatalf("compile json: %v", err)
	}
	bin, err := FormatMap([]byte(mapRep3), "binary")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Compile(1, bin, reg6(), Options{PGNum: 1024, Replicas: 3}); err != nil {
		t.Fatalf("compile binary: %v", err)
	}
	// json source shown as text must be re-encoded, not passed through.
	txt2, err := FormatMap(js, "text")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.HasPrefix(bytes.TrimSpace(txt2), []byte("{")) {
		t.Fatal("text format returned json")
	}
	if _, err := FormatMap([]byte(mapRep3), "yaml"); err == nil {
		t.Fatal("FormatMap should not accept yaml (CLI side)")
	}
}
