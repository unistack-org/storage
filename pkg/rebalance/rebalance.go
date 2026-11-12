// Package rebalance is the cluster free-space rebalancer: a stateless
// loop that reads OSD disk usage over the control plane, computes
// CRUSH device weights proportional to free space, and publishes a
// new crush map epoch (plus mirrored registry weights) when the plan
// changes materially. OSDs never talk to it — it only writes meta.
//
// This is the lp_solve analogue from the roadmap: CRUSH minimizes
// movement by construction, so the rebalancer only needs smooth
// weights, not a min-movement ILP. Hysteresis keeps a noisy usage
// signal from publishing epoch storms.
package rebalance

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/unistack-org/storage/crush"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
)

// Options configures a rebalancer pass.
type Options struct {
	// Threshold is the minimum relative weight change that makes an
	// OSD "materially" rebalanced: |target-current|/current must reach
	// it before the plan emits a change (hysteresis). Default 0.05.
	Threshold float64
	// MinWeightFloor is the smallest weight an eligible OSD may be
	// given, as a fraction of the cluster's largest current weight:
	// a nearly-full OSD is slowed down, not drained out of placement
	// (weight 0 is an operator's decision, not the rebalancer's).
	// Default 0.05.
	MinWeightFloor float64
	// ProbeTimeout bounds one disk-usage probe round. Default 5s.
	ProbeTimeout time.Duration
	// DryRun computes and reports the plan without writing meta.
	DryRun bool
}

func (o *Options) fill() {
	if o.Threshold <= 0 {
		o.Threshold = 0.05
	}
	if o.MinWeightFloor <= 0 {
		o.MinWeightFloor = 0.05
	}
	if o.ProbeTimeout <= 0 {
		o.ProbeTimeout = 5 * time.Second
	}
}

// Change is one OSD the plan wants to reweight.
type Change struct {
	OSD     int32
	Current float64
	Target  float64
	Free    uint64 // bytes free the target was computed from
}

// Report is the outcome of one rebalance pass.
type Report struct {
	Changes   []Change // material changes, sorted by osd id
	Epoch     uint32   // epoch the current weights were read from
	NewEpoch  uint32   // epoch published (0 when nothing changed / dry run)
	Eligible  int      // OSDs with weight>0 and fresh disk data
	ProbeFail int      // OSDs that did not answer the disk probe
}

// Plan computes free-space-proportional device weights. current are
// the crush weights the map places with; only OSDs registered with
// weight > 0 that answered the disk probe participate. Targets are
// normalized so the most-free OSD keeps the largest current weight
// (a pure rescale never triggers a global data movement):
// target[i] = free[i]/maxFree * maxCurrent, floored at
// MinWeightFloor*maxCurrent. Changes below Threshold relative
// deviation are dropped.
func Plan(infos []meta.OSDInfo, disk map[int32]client.DiskUsage, current map[int32]float64, opts Options) Report {
	opts.fill()
	type elig struct {
		id   int32
		free uint64
		cur  float64
	}
	var es []elig
	var maxFree uint64
	var maxCur float64
	rep := Report{}
	for _, in := range infos {
		if in.Weight <= 0 {
			continue // out of service: the operator drained it
		}
		d, ok := disk[in.ID]
		if !ok {
			rep.ProbeFail++
			continue
		}
		cur, ok := current[in.ID]
		if !ok || cur <= 0 {
			continue // not in the crush map (or weight 0): not ours to steer
		}
		es = append(es, elig{in.ID, d.Free, cur})
		if d.Free > maxFree {
			maxFree = d.Free
		}
		if cur > maxCur {
			maxCur = cur
		}
	}
	rep.Eligible = len(es)
	if len(es) < 2 || maxFree == 0 || maxCur == 0 {
		return rep
	}
	floor := opts.MinWeightFloor * maxCur
	for _, e := range es {
		t := float64(e.free) / float64(maxFree) * maxCur
		if t < floor {
			t = floor
		}
		if math.Abs(t-e.cur)/e.cur < opts.Threshold {
			continue
		}
		rep.Changes = append(rep.Changes, Change{OSD: e.id, Current: e.cur, Target: t, Free: e.free})
	}
	sort.Slice(rep.Changes, func(i, j int) bool { return rep.Changes[i].OSD < rep.Changes[j].OSD })
	return rep
}

// DeviceWeights extracts the device weights the crush map places with
// (the weights on the bucket items referencing each device; devices
// no bucket references are out, weight 0, and are omitted).
func DeviceWeights(src []byte) (map[int32]float64, error) {
	m, err := crush.Decode(src)
	if err != nil {
		return nil, fmt.Errorf("rebalance: decode crush map: %w", err)
	}
	w := make(map[int32]float64, len(m.Devices))
	for _, b := range m.Buckets {
		for _, it := range b.Items {
			if it.ID < 0 {
				continue // bucket reference
			}
			w[it.ID] = float64(it.Weight)
		}
	}
	return w, nil
}

// ReweightMap returns the map as text with the given device weights
// substituted on the bucket items that reference them. Bucket weights
// are recomputed by the compiler from items, so only item weights
// matter. Devices not named in weights keep their weight.
func ReweightMap(src []byte, weights map[int32]float64) ([]byte, error) {
	m, err := crush.Decode(src)
	if err != nil {
		return nil, fmt.Errorf("rebalance: decode crush map: %w", err)
	}
	for _, b := range m.Buckets {
		for _, it := range b.Items {
			if it.ID < 0 {
				continue
			}
			if w, ok := weights[it.ID]; ok {
				it.Weight = float32(w)
			}
		}
	}
	return m.EncodeText()
}

// Daemon is the rebalancer loop bound to one cluster's meta.
type Daemon struct {
	Cl   *client.Cluster
	Opts Options
	// Probe supplies disk usage; nil uses the cluster's control-plane
	// probe. Tests inject it.
	Probe func(ctx context.Context, timeout time.Duration) ([]meta.OSDInfo, map[int32]error, map[int32]client.DiskUsage, error)
}

// New returns a daemon for cl.
func New(cl *client.Cluster, opts Options) *Daemon {
	return &Daemon{Cl: cl, Opts: opts}
}

// Once runs one rebalance pass: probe, plan, publish (unless dry run).
// A pass with no material changes is a no-op (no epoch is burned).
func (d *Daemon) Once(ctx context.Context) (Report, error) {
	d.Opts.fill()
	probe := d.Probe
	if probe == nil {
		probe = d.Cl.ProbeOSDsDisk
	}
	rec, err := meta.LatestMap(ctx, d.Cl.Meta())
	if err != nil {
		return Report{}, fmt.Errorf("rebalance: no crush map in meta: %w", err)
	}
	current, err := DeviceWeights(rec.Map)
	if err != nil {
		return Report{}, err
	}
	infos, probes, disk, err := probe(ctx, d.Opts.ProbeTimeout)
	if err != nil {
		return Report{}, fmt.Errorf("rebalance: list osds: %w", err)
	}
	_ = probes // probe failures surface as missing disk entries
	rep, err := d.apply(ctx, rec, current, infos, disk)
	return rep, err
}

// apply is the publish half of a pass, split out so tests can drive
// probe data through the same code path.
func (d *Daemon) apply(ctx context.Context, rec meta.MapRecord, current map[int32]float64, infos []meta.OSDInfo, disk map[int32]client.DiskUsage) (Report, error) {
	rep := Plan(infos, disk, current, d.Opts)
	rep.Epoch = rec.Epoch
	if len(rep.Changes) == 0 || d.Opts.DryRun {
		return rep, nil
	}
	targets := make(map[int32]float64, len(rep.Changes))
	for _, c := range rep.Changes {
		targets[c.OSD] = c.Target
	}
	newMap, err := ReweightMap(rec.Map, targets)
	if err != nil {
		return rep, err
	}
	if err := d.Cl.MapEpoch(ctx, rec.Epoch+1, newMap); err != nil {
		return rep, fmt.Errorf("rebalance: publish epoch %d: %w", rec.Epoch+1, err)
	}
	rep.NewEpoch = rec.Epoch + 1
	// Mirror the weights onto the OSD registry records (the operator
	// view and the in/out flag). Registry weight semantics: >0 = in
	// service, which the floor guarantees for every change we publish.
	ms := d.Cl.Meta()
	for _, c := range rep.Changes {
		if err := meta.SetOSDWeight(ctx, ms, c.OSD, c.Target); err != nil {
			return rep, fmt.Errorf("rebalance: osd %d registry weight: %w", c.OSD, err)
		}
	}
	return rep, nil
}

// Run loops Once every interval until ctx is cancelled. Pass errors
// are reported through logf and the loop continues: a rebalancer that
// dies on a transient meta hiccup stops balancing silently.
func (d *Daemon) Run(ctx context.Context, interval time.Duration, logf func(format string, args ...any)) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
		rep, err := d.Once(ctx)
		if err != nil {
			if logf != nil {
				logf("rebalance: %v", err)
			}
			continue
		}
		if logf != nil && len(rep.Changes) > 0 {
			for _, c := range rep.Changes {
				logf("rebalance: osd %d weight %.4f -> %.4f (free %d)", c.OSD, c.Current, c.Target, c.Free)
			}
			if rep.NewEpoch > 0 {
				logf("rebalance: published epoch %d (%d change(s), %d eligible, %d probe fail)", rep.NewEpoch, len(rep.Changes), rep.Eligible, rep.ProbeFail)
			} else {
				logf("rebalance: dry run: %d change(s), %d eligible, %d probe fail", len(rep.Changes), rep.Eligible, rep.ProbeFail)
			}
		}
	}
}
