package rebalance

import (
	"context"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
)

func infos(ids ...int32) []meta.OSDInfo {
	out := make([]meta.OSDInfo, 0, len(ids))
	for _, id := range ids {
		out = append(out, meta.OSDInfo{ID: id, Weight: 1})
	}
	return out
}

func TestPlanProportionalToFree(t *testing.T) {
	infos := []meta.OSDInfo{
		{ID: 0, Weight: 1}, {ID: 1, Weight: 1}, {ID: 2, Weight: 1},
	}
	disk := map[int32]client.DiskUsage{
		0: {Free: 1000}, // most free: keeps the top weight
		1: {Free: 500},  // half free: target 0.5, a material change
		2: {Free: 980},  // 2% under the leader: under threshold
	}
	current := map[int32]float64{0: 1, 1: 1, 2: 1}
	rep := Plan(infos, disk, current, Options{})
	if len(rep.Changes) != 1 {
		t.Fatalf("want 1 change, got %+v", rep.Changes)
	}
	c := rep.Changes[0]
	if c.OSD != 1 || c.Target < 0.499 || c.Target > 0.501 {
		t.Fatalf("bad change: %+v", c)
	}
	if rep.Eligible != 3 || rep.ProbeFail != 0 {
		t.Fatalf("report counters: %+v", rep)
	}
}

func TestPlanSkipsOutAndUnprobed(t *testing.T) {
	infos := []meta.OSDInfo{
		{ID: 0, Weight: 1}, {ID: 1, Weight: 1}, {ID: 2, Weight: 0},
	}
	disk := map[int32]client.DiskUsage{0: {Free: 1000}, 1: {Free: 10}}
	current := map[int32]float64{0: 1, 1: 1, 2: 1}
	rep := Plan(infos, disk, current, Options{})
	if rep.ProbeFail != 0 || rep.Eligible != 2 { // osd 2 is out: skipped, not a probe failure
		t.Fatalf("report: %+v", rep)
	}
	// osd 1 sits at the floor (0.05 * 1): a huge drop is material.
	if len(rep.Changes) != 1 || rep.Changes[0].OSD != 1 || rep.Changes[0].Target != 0.05 {
		t.Fatalf("want osd 1 floored at 0.05, got %+v", rep.Changes)
	}
}

func TestPlanHysteresis(t *testing.T) {
	infos := []meta.OSDInfo{{ID: 0, Weight: 1}, {ID: 1, Weight: 1}}
	disk := map[int32]client.DiskUsage{0: {Free: 1000}, 1: {Free: 960}}
	current := map[int32]float64{0: 1, 1: 1}
	if rep := Plan(infos, disk, current, Options{Threshold: 0.05}); len(rep.Changes) != 0 {
		t.Fatalf("4%% under threshold must be silent: %+v", rep.Changes)
	}
	if rep := Plan(infos, disk, current, Options{Threshold: 0.01}); len(rep.Changes) != 1 {
		t.Fatalf("4%% over threshold must change: %+v", rep.Changes)
	}
}

func TestReweightMapRoundTrip(t *testing.T) {
	src, err := readTestdata(t)
	if err != nil {
		t.Fatal(err)
	}
	w, err := DeviceWeights(src)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[int32]float64{0: 0.5, 1: 0.5, 2: 0.5, 3: 0.058, 4: 0.058, 5: 0.058} {
		if got, ok := w[id]; !ok || got < want-0.001 || got > want+0.001 {
			t.Fatalf("device %d weight %v, want %v (all: %v)", id, got, want, w)
		}
	}
	out, err := ReweightMap(src, map[int32]float64{1: 0.25})
	if err != nil {
		t.Fatal(err)
	}
	w2, err := DeviceWeights(out)
	if err != nil {
		t.Fatal(err)
	}
	if w2[1] != 0.25 {
		t.Fatalf("reweighted device 1: %v", w2[1])
	}
	if w2[0] != w[0] || w2[5] != w[5] {
		t.Fatalf("untouched devices changed: %v vs %v", w2, w)
	}
}

func TestDaemonOncePublishes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := meta.OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	src, err := readTestdata(t)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := meta.PutMap(ctx, m, meta.MapRecord{Epoch: 7, Format: "text", Map: src}); err != nil {
		t.Fatal(err)
	}
	for _, in := range infos(0, 1, 2) {
		in.Addr = "127.0.0.1:1"
		if _, err := meta.PutOSD(ctx, m, in); err != nil {
			t.Fatal(err)
		}
	}
	cl := client.NewCluster(m)
	defer cl.Close()
	d := New(cl, Options{})
	// osd 1 is nearly full: its weight must drop and epoch 8 appear.
	d.Probe = func(ctx context.Context, timeout time.Duration) ([]meta.OSDInfo, map[int32]error, map[int32]client.DiskUsage, error) {
		ls, err := meta.ListOSDs(ctx, m)
		return ls, nil, map[int32]client.DiskUsage{
			0: {Free: 1000}, 1: {Free: 10}, 2: {Free: 1000},
		}, err
	}
	rep, err := d.Once(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Epoch != 7 || rep.NewEpoch != 8 {
		t.Fatalf("epochs: %+v", rep)
	}
	if len(rep.Changes) != 1 || rep.Changes[0].OSD != 1 {
		t.Fatalf("changes: %+v", rep.Changes)
	}
	// The published map carries the new weight; the registry mirrors it.
	rec, err := meta.LatestMap(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Epoch != 8 || rec.Format != "text" {
		t.Fatalf("published record: %+v", rec)
	}
	w, err := DeviceWeights(rec.Map)
	if err != nil {
		t.Fatal(err)
	}
	if w[1] > 0.06 {
		t.Fatalf("published map weight for osd 1: %v", w[1])
	}
	info, err := meta.GetOSD(ctx, m, 1)
	if err != nil {
		t.Fatal(err)
	}
	// The map round-trips weights through float32, so compare loosely.
	if d := info.Weight - w[1]; d > 1e-6 || d < -1e-6 {
		t.Fatalf("registry weight %v != map weight %v", info.Weight, w[1])
	}
	// A second pass with the same usage is a no-op (no epoch burned).
	rep2, err := d.Once(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep2.Changes) != 0 || rep2.NewEpoch != 0 {
		t.Fatalf("second pass: %+v", rep2)
	}
}

func TestDaemonDryRunWritesNothing(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	m, err := meta.OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	src, err := readTestdata(t)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := meta.PutMap(ctx, m, meta.MapRecord{Epoch: 3, Format: "text", Map: src}); err != nil {
		t.Fatal(err)
	}
	for _, in := range infos(0, 1) {
		in.Addr = "127.0.0.1:1"
		if _, err := meta.PutOSD(ctx, m, in); err != nil {
			t.Fatal(err)
		}
	}
	cl := client.NewCluster(m)
	defer cl.Close()
	d := New(cl, Options{DryRun: true})
	d.Probe = func(ctx context.Context, timeout time.Duration) ([]meta.OSDInfo, map[int32]error, map[int32]client.DiskUsage, error) {
		ls, err := meta.ListOSDs(ctx, m)
		return ls, nil, map[int32]client.DiskUsage{0: {Free: 1000}, 1: {Free: 10}}, err
	}
	rep, err := d.Once(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Changes) != 1 || rep.NewEpoch != 0 {
		t.Fatalf("dry run report: %+v", rep)
	}
	rec, err := meta.LatestMap(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Epoch != 3 {
		t.Fatalf("dry run published epoch %d", rec.Epoch)
	}
	info, err := meta.GetOSD(ctx, m, 1)
	if err != nil {
		t.Fatal(err)
	}
	if info.Weight != 1 {
		t.Fatalf("dry run touched registry: %v", info.Weight)
	}
}
