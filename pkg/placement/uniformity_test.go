package placement

import (
	"fmt"
	"math"
	"testing"
)

// mapBig is an H-host map with O OSDs per host (H*O devices) and one
// replicated rule. Used by the uniformity and failure-emulation checks.
func mapBig(hosts, perHost int) string {
	dev, typ, buck, rules := "", "type 0 osd\ntype 1 host\ntype 2 root\n", "", ""
	root := "root r {\nid -2\nalg straw2\nhash 0\n"
	for h := 0; h < hosts; h++ {
		root += fmt.Sprintf("item host h%d\n", h)
		b := fmt.Sprintf("host h%d {\nid %d\nalg straw2\nhash 0\n", h, -3-h)
		for o := 0; o < perHost; o++ {
			id := h*perHost + o
			dev += fmt.Sprintf("device %d osd.%d\n", id, id)
			b += fmt.Sprintf("item osd.%d weight 1.000\n", id)
		}
		b += "}\n"
		buck += b
	}
	root += "}\n"
	rules = "rule data {\nid 0\ntype replicated\nstep take r\nstep chooseleaf firstn 0 type host\nstep emit\n}\n"
	return "# begin crush map\ntunable choose_local_tries 0\n" +
		"tunable choose_local_fallback_tries 0\ntunable choose_total_tries 50\n" +
		"tunable chooseleaf_descend_once 1\ntunable chooseleaf_vary_r 1\n" +
		"tunable straw_calc_version 1\ntunable chooseleaf_stable 1\n" +
		"# devices\n" + dev + "# types\n" + typ + "# buckets\n" + root + buck +
		"# rules\n" + rules + "# end crush map\n"
}

func regN(n int) Registry {
	r := Registry{}
	for i := 0; i < n; i++ {
		r[int32(i)] = Ref{OSD: int32(i), Addr: fmt.Sprintf("127.0.0.1:%d", 7000+i), Export: "v"}
	}
	return r
}

// chi2 computes Pearson's chi-square statistic over counts versus the
// uniform expectation.
func chi2(counts []int) float64 {
	total := 0
	for _, c := range counts {
		total += c
	}
	exp := float64(total) / float64(len(counts))
	x := 0.0
	for _, c := range counts {
		d := float64(c) - exp
		x += d * d / exp
	}
	return x
}

func x2min(a []int) int {
	m := a[0]
	for _, v := range a[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func x2max(a []int) int {
	m := a[0]
	for _, v := range a[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

// TestPlacementUniformity: straw2 over a 10x3 map (30 OSDs) should put
// roughly equal shares of a 65536-pg grid on every OSD. The pg grid is
// the load unit, so on grids this large a correct CRUSH lands far below
// the chi-square critical values.
func TestPlacementUniformity(t *testing.T) {
	const hosts, perHost, pgNum = 10, 3, 65536
	s, err := Compile(1, []byte(mapBig(hosts, perHost)), regN(hosts*perHost),
		Options{PGNum: pgNum, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	osdCount := make([]int, hosts*perHost)
	hostCount := make([]int, hosts)
	for pg := uint32(0); pg < pgNum; pg++ {
		row := s.Row(pg)
		seenHost := map[int]bool{}
		for _, id := range row {
			if id < 0 {
				t.Fatalf("pg %d has empty slot", pg)
			}
			osdCount[id]++
			seenHost[int(id)/perHost] = true
		}
		if len(seenHost) != 3 {
			t.Fatalf("pg %d spans %d hosts, want 3", pg, len(seenHost))
		}
		for h := range seenHost {
			hostCount[h]++
		}
	}
	// Each OSD carries 3*pgNum/(hosts*perHost) placements on average.
	if x := chi2(osdCount); x > 200 {
		t.Errorf("osd placement chi2 = %.1f, want <= 200 (30 osds, %d placements each)", x, 3*pgNum/(hosts*perHost))
	}
	// Hosts carry 3*pgNum/hosts placements on average; the chooseleaf
	// constraint makes host selection the visible skew surface.
	if x := chi2(hostCount); x > 200 {
		t.Errorf("host placement chi2 = %.1f, want <= 200 (10 hosts, %d placements each)", x, 3*pgNum/hosts)
	}
	t.Logf("chi2 osd=%.1f host=%.1f (min/max osd %d/%d)",
		chi2(osdCount), chi2(hostCount), x2min(osdCount), x2max(osdCount))
}

// TestFailureMovement: taking one OSD out (registry drop) must move ONLY
// the pgs that referenced it (CRUSH minimality), and the surviving grid
// must stay uniform.
func TestFailureMovement(t *testing.T) {
	const hosts, perHost, pgNum = 10, 3, 16384
	src := mapBig(hosts, perHost)
	full, err := Compile(1, []byte(src), regN(hosts*perHost), Options{PGNum: pgNum, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	const dead int32 = 7
	reg := regN(hosts * perHost)
	delete(reg, dead)
	after, err := Compile(2, []byte(src), reg, Options{PGNum: pgNum, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	moved, affected := 0, 0
	for pg := uint32(0); pg < pgNum; pg++ {
		row0, row1 := full.Row(pg), after.Row(pg)
		had := false
		for _, id := range row0 {
			if id == dead {
				had = true
			}
		}
		same := true
		for i := range row0 {
			if row0[i] != row1[i] {
				same = false
			}
		}
		if had {
			affected++
			if same {
				t.Fatalf("pg %d kept identical row after losing osd %d", pg, dead)
			}
			for _, id := range row1 {
				if id == dead {
					t.Fatalf("pg %d still references dead osd", pg)
				}
			}
			for _, id := range row0 {
				if id == dead {
					continue
				}
				kept := false
				for _, j := range row1 {
					if j == id {
						kept = true
					}
				}
				if !kept {
					t.Fatalf("pg %d lost live osd %d (movement beyond minimum)", pg, id)
				}
			}
		} else {
			if !same {
				moved++
			}
		}
	}
	if moved != 0 {
		t.Errorf("%d pgs moved that did not reference the dead osd (CRUSH minimality violated)", moved)
	}
	// One OSD of 30 carries ~1/30 of rows; with rep3 ~3/30 of pgs.
	share := float64(affected) / float64(pgNum)
	if math.Abs(share-3.0/30.0) > 0.02 {
		t.Errorf("affected share = %.3f, want ~%.3f", share, 3.0/30.0)
	}
	// Surviving grid stays uniform across the remaining 29 OSDs.
	counts := make([]int, 0, hosts*perHost-1)
	idx := map[int32]int{}
	for id := range reg {
		idx[id] = len(counts)
		counts = append(counts, 0)
	}
	for pg := uint32(0); pg < pgNum; pg++ {
		for _, id := range after.Row(pg) {
			counts[idx[id]]++
		}
	}
	if x := chi2(counts); x > 250 {
		t.Errorf("post-failure chi2 = %.1f, want <= 250", x)
	}
}

// TestReweightScaleInvariance: scaling ALL weights by the same factor
// must move almost nothing. straw2 draws are fixed-point integer
// divisions, so uniform scaling is only approximately invariant (Ceph
// measures the same effect in strawscale.py); the tolerance here matches
// that reality. Note: only weights >= 1.0 are pure "capacity" weights —
// below 1.0 the CRUSH is_out() probabilistic drain kicks in (Ceph
// semantics: reweight < 1.0 means the device is partially out), so the
// invariant is exercised by doubling, not halving.
func TestReweightScaleInvariance(t *testing.T) {
	const hosts, perHost, pgNum = 6, 2, 4096
	src := mapBig(hosts, perHost)
	s, err := Compile(1, []byte(src), regN(hosts*perHost), Options{PGNum: pgNum, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	w := map[int32]float64{}
	for id := range regN(hosts * perHost) {
		w[id] = 2.0
	}
	s2, err := s.Reweight(2, w)
	if err != nil {
		t.Fatal(err)
	}
	changed, slots := 0, pgNum*3
	for pg := uint32(0); pg < pgNum; pg++ {
		r0, r1 := s.Row(pg), s2.Row(pg)
		for i := range r0 {
			if r0[i] != r1[i] {
				changed++
			}
		}
	}
	if float64(changed)/float64(slots) > 0.02 {
		t.Errorf("uniform reweight moved %d/%d slots (>2%%): fixed-point straw2 badly broken?", changed, slots)
	}
	t.Logf("uniform doubling moved %d/%d slots (%.2f%%)", changed, slots, 100*float64(changed)/float64(slots))
}

// TestReweightZeroIsOut: reweighting one OSD to 0 must behave exactly
// like removing it from the registry (same rows).
func TestReweightZeroIsOut(t *testing.T) {
	const hosts, perHost, pgNum = 6, 2, 4096
	src := mapBig(hosts, perHost)
	s, err := Compile(1, []byte(src), regN(hosts*perHost), Options{PGNum: pgNum, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	const dead int32 = 5
	w := map[int32]float64{dead: 0}
	reweighted, err := s.Reweight(2, w)
	if err != nil {
		t.Fatal(err)
	}
	reg := regN(hosts * perHost)
	delete(reg, dead)
	dropped, err := Compile(3, []byte(src), reg, Options{PGNum: pgNum, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	for pg := uint32(0); pg < pgNum; pg++ {
		r0, r1 := reweighted.Row(pg), dropped.Row(pg)
		for i := range r0 {
			if r0[i] != r1[i] {
				t.Fatalf("pg %d slot %d: reweight-0 gives %d, registry-drop gives %d", pg, i, r0[i], r1[i])
			}
		}
	}
}
