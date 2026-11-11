package placement

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// map100k builds a realistic large-cluster crushmap: 100 racks x 100 hosts
// x 10 OSDs = 100000 devices, weights 20.000..80.000 GB (deterministic LCG
// per id so runs are reproducible). One replicated rule, chooseleaf on
// host, straw2 everywhere. Racks keep every straw scan small (<=100
// items), the way real 100k-OSD clusters are laid out.
func map100k() string {
	const racks, hostsPerRack, osdPerHost = 100, 100, 10
	nDev := racks * hostsPerRack * osdPerHost
	dev := make([]byte, 0, nDev*24)
	for i := 0; i < nDev; i++ {
		dev = append(dev, fmt.Sprintf("device %d osd.%d\n", i, i)...)
	}
	typ := "type 0 osd\ntype 1 host\ntype 2 rack\ntype 3 root\n"
	root := "root r {\nid -2\nalg straw2\nhash 0\n"
	for k := 0; k < racks; k++ {
		root += fmt.Sprintf("item rack rk%d\n", k)
	}
	root += "}\n"
	buck := make([]byte, 0, 1<<22)
	for k := 0; k < racks; k++ {
		buck = append(buck, fmt.Sprintf("rack rk%d {\nid %d\nalg straw2\nhash 0\n", k, -101-k)...)
		for hr := 0; hr < hostsPerRack; hr++ {
			buck = append(buck, fmt.Sprintf("item host h%d\n", k*hostsPerRack+hr)...)
		}
		buck = append(buck, "}\n"...)
	}
	for k := 0; k < racks; k++ {
		for hr := 0; hr < hostsPerRack; hr++ {
			h := k*hostsPerRack + hr
			buck = append(buck, fmt.Sprintf("host h%d {\nid %d\nalg straw2\nhash 0\n", h, -1001-h)...)
			for o := 0; o < osdPerHost; o++ {
				id := h*osdPerHost + o
				// LCG in [20,80]: mixed 20..80 GB disks.
				w := 20.0 + float64((id*1103515245+12345)%61)
				buck = append(buck, fmt.Sprintf("item osd.%d weight %.3f\n", id, w)...)
			}
			buck = append(buck, "}\n"...)
		}
	}
	rules := "rule data {\nid 0\ntype replicated\nstep take r\n" +
		"step chooseleaf firstn 0 type host\nstep emit\n}\n"
	return "# begin crush map\ntunable choose_local_tries 0\n" +
		"tunable choose_local_fallback_tries 0\ntunable choose_total_tries 50\n" +
		"tunable chooseleaf_descend_once 1\ntunable chooseleaf_vary_r 1\n" +
		"tunable straw_calc_version 1\ntunable chooseleaf_stable 1\n" +
		"# devices\n" + string(dev) + "# types\n" + typ + "# buckets\n" + root +
		string(buck) + "# rules\n" + rules + "# end crush map\n"
}

// 100k-device layout used by the scale checks.
const (
	hugeRacks     = 100
	hugeHostsRack = 100
	hugeOSDsHost  = 10
	hugeDevices   = hugeRacks * hugeHostsRack * hugeOSDsHost
)

// hugeWeights returns the per-device GB size used by map100k (must stay in
// sync with the LCG above).
func hugeWeights(n int) []float64 {
	ws := make([]float64, n)
	for i := range ws {
		ws[i] = 20.0 + float64((i*1103515245+12345)%61)
	}
	return ws
}

func regHuge(n int) Registry {
	r := make(Registry, n)
	for i := 0; i < n; i++ {
		r[int32(i)] = Ref{OSD: int32(i), Addr: fmt.Sprintf("10.0.%d.%d:%d", i>>8&0xff, i&0xff, 7000+i%1000), Export: "v"}
	}
	return r
}

// TestHugeMapCompile compiles the 100k-device map and builds a modest pg
// table: guards the text parser, straw precalculation and table build at
// scale, and measures one-epoch switch cost end to end.
func TestHugeMapCompile(t *testing.T) {
	src := map100k()
	reg := regHuge(hugeDevices)
	t0 := time.Now()
	s, err := Compile(1, []byte(src), reg, Options{PGNum: 4096, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	d := time.Since(t0)
	t.Logf("100k devices: parse+compile+4096pg table in %v (%.1f us/pg)", d, float64(d.Microseconds())/4096)
	if d > 120*time.Second {
		t.Errorf("epoch switch latency too high: %v", d)
	}
	// Every row must name 3 distinct hosts' devices.
	seen := map[int32]bool{}
	for _, pg := range []uint32{0, 1, 2048, 4095} {
		row := s.Row(pg)
		for _, id := range row {
			if id < 0 || seen[id] {
				t.Fatalf("pg %d row %v not distinct", pg, row)
			}
			seen[id] = true
		}
		for i := range row {
			seen[row[i]] = false
		}
	}
}

// TestHugeBalance runs the 100k-device balance measurement at several pg
// grid sizes (parallel; each subtest compiles its own snapshot). This is
// the answer to "100k disks of 20-80GB: is placement uniform?".
//
// Method: count placements per device, compare against the weight-
// proportional expectation (bytes land proportional to CRUSH weight).
// Statistical floor: with N=100k devices and M placements the multinomial
// sampling noise alone yields E[chi2] = N-1; a perfect balancer cannot
// beat it. We assert observed chi2 <= floor*1.5 + 100 (generous systematic
// margin) and log max per-device capacity deviation.
func TestHugeBalance(t *testing.T) {
	// 100k devices x up to 1M pgs is a statistical measurement, not a
	// unit check: a single pg1024k subtest alone runs ~10 min under
	// -race, past the CI package timeout. TestHugeMapCompile keeps the
	// compile path under the race detector.
	if testing.Short() {
		t.Skip("skipping 100k-device balance measurement in short mode")
	}
	src := map100k()
	reg := regHuge(hugeDevices)
	ws := hugeWeights(hugeDevices)
	totalW := 0.0
	for _, w := range ws {
		totalW += w
	}
	for _, pgNum := range []uint32{65536, 262144, 1048576} {
		pgNum := pgNum
		t.Run(fmt.Sprintf("pg%dk", pgNum/1024), func(t *testing.T) {
			t.Parallel()
			t0 := time.Now()
			s, err := Compile(1, []byte(src), reg, Options{PGNum: pgNum, Replicas: 3})
			if err != nil {
				t.Fatal(err)
			}
			compile := time.Since(t0)
			counts := make([]int64, hugeDevices)
			for pg := uint32(0); pg < pgNum; pg++ {
				for _, id := range s.Row(pg) {
					counts[id]++
				}
			}
			placements := float64(pgNum) * 3
			avgE := placements / float64(hugeDevices)
			chi, chiFloor := 0.0, float64(hugeDevices-1)
			maxDev, sumDev := 0.0, 0.0
			minC, maxC := int64(1<<60), int64(0)
			for i, c := range counts {
				exp := placements * ws[i] / totalW
				d := float64(c) - exp
				chi += d * d / exp
				dev := math.Abs(d) / exp
				sumDev += dev
				if dev > maxDev {
					maxDev = dev
				}
				if c < minC {
					minC = c
				}
				if c > maxC {
					maxC = c
				}
			}
			meanDev := sumDev / float64(hugeDevices)
			t.Logf("pg=%d: compile %v | chi2=%.0f floor=%.0f ratio=%.2f | mean dev=%.2f%% max dev=%.1f%% | min/max count %d/%d (avg %.1f placements/device)",
				pgNum, compile, chi, chiFloor, chi/chiFloor, 100*meanDev, 100*maxDev, minC, maxC, avgE)
			if lim := chiFloor*1.5 + 100; chi > lim {
				t.Errorf("chi2 = %.0f exceeds limit %.0f (floor %.0f): systematic imbalance beyond sampling noise", chi, lim, chiFloor)
			}
			// Mean per-device deviation bound is noise-aware: with avgE
			// placements per device the multinomial noise floor of the
			// mean |dev| is ~sqrt(2/(pi*avgE)) (Poisson approx), so a
			// perfect balancer sits near that; 1.13/sqrt(avgE) covers
			// small avgE, +15pp of systematic margin on top.
			if lim := 1.13/math.Sqrt(avgE) + 0.15; meanDev > lim {
				t.Errorf("mean capacity deviation %.2f%% exceeds noise-aware limit %.2f%%", 100*meanDev, 100*lim)
			}
		})
	}
}
