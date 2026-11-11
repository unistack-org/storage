// Metrics for unistord, exported through the micro-meter
// VictoriaMetrics exporter (go.unistack.org/micro-meter-victoriametrics/v5):
// GET /metrics on the standalone -metrics address only — the admin
// REST API (-http) never serves them. This is the single metrics
// export path of the daemon: there is no prometheus/client_golang
// registry and no duplicated exposition anywhere.
//
// The meter runs in Prometheus-compat mode
// (victoriametrics.PrometheusCompat(true)), so latency histograms
// expose the standard *_bucket{le=...}/_sum/_count triples and
// VictoriaMetrics scrapes the usual Prometheus text format. The
// mapping mirrors the admin JSON surface (/v1/status) plus the
// hot-path counters; the registry lives here, in the cmd module,
// while the core pkg/ modules stay dependency-free and only fire the
// sdsnet hook callbacks (sdsnet.ServerMetrics) this file adapts into
// meter series.
//
// Metric families (all prefixed unistor_):
//
//	cluster state:  unistor_cluster_osds / _osds_live / _healthy /
//	                _map_epoch / _volumes / _raft_has_leader /
//	                _raft_state{state}
//	per node/OSD:   unistor_osd_up{osd,addr}, unistor_osd_weight{osd},
//	                unistor_osd_disk_stored|free|total_bytes{osd},
//	                unistor_osd_connections{osd},
//	                unistor_osd_pool_connections{osd,state},
//	                unistor_osd_write_bytes{osd,name} (write-amp
//	                counters, local OSDs), unistor_osd_meta{osd,name}
//	                (meta index, local OSDs)
//	per-OSD speed:  unistor_osd_requests_total{osd,op,status},
//	                unistor_osd_bytes_total{osd,op,direction},
//	                unistor_osd_request_duration_seconds{osd,op}
//	volume state:   unistor_volume_writable|degraded|live_targets|
//	                min_size|size_bytes|quota_bytes|usage_bytes{name}
//	export speed:   unistor_export_requests_total{export,op,error},
//	                unistor_export_bytes_total{export,op,direction},
//	                unistor_export_request_duration_seconds{export,op},
//	                unistor_export_qos_*{export} (configured limits) and
//	                unistor_export_qos_sustained_*{export} (limiter
//	                throughput)
//
// All averaged figures (latency, throughput) come out of counters and
// histogram sum/count pairs, so rate()/histogram_quantile() in the
// dashboard compute true window averages, never sums-of-averages.

package main

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	victoriametrics "go.unistack.org/micro-meter-victoriametrics/v5"
	"go.unistack.org/micro/v5/meter"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// opBuckets covers the latency span of the block path: sub-100µs NVMe
// dispatch through multi-second queue waits, geometric so each bucket
// doubles the last (histogram_quantile resolution stays even).
var opBuckets = metrics.ExponentialBuckets(0.000125, 2, 17) // 0.125ms .. 8.2s

// labelKey joins label values into one cache key; I/O identifiers
// (ids, ops, addresses) never contain NUL, so the join is injective.
func labelKey(labels []string) string { return strings.Join(labels, "\x00") }

// pairLabels zips a family's fixed label names with one tuple's values
// into the flat name1,value1,name2,value2... slice the meter API takes.
func pairLabels(names, values []string) []string {
	out := make([]string, 0, 2*len(names))
	for i, n := range names {
		out = append(out, n, values[i])
	}
	return out
}

// counterVec is the micro-meter analogue of a labelled counter family:
// series are get-or-create on the meter, so the vector caches the
// created counter per label tuple to keep hot-path calls a map hit.
// Callers pass label VALUES; names are fixed at construction.
type counterVec struct {
	m     meter.Meter
	name  string
	names []string
	cache sync.Map // labelKey -> meter.Counter
}

func newCounterVec(m meter.Meter, name string, names ...string) *counterVec {
	return &counterVec{m: m, name: name, names: names}
}

func (c *counterVec) get(values ...string) meter.Counter {
	key := labelKey(values)
	if v, ok := c.cache.Load(key); ok {
		return v.(meter.Counter)
	}
	ctr := c.m.Counter(c.name, pairLabels(c.names, values)...)
	actual, _ := c.cache.LoadOrStore(key, ctr)
	return actual.(meter.Counter)
}

// histogramVec caches meter.Histogram series per label tuple; created
// via HistogramExt with the explicit opBuckets, which in
// Prometheus-compat mode emits *_bucket{le=...}/_sum/_count.
type histogramVec struct {
	m       meter.Meter
	name    string
	names   []string
	buckets []float64
	cache   sync.Map // labelKey -> meter.Histogram
}

func newHistogramVec(m meter.Meter, name string, buckets []float64, names ...string) *histogramVec {
	return &histogramVec{m: m, name: name, names: names, buckets: buckets}
}

func (h *histogramVec) get(values ...string) meter.Histogram {
	key := labelKey(values)
	if v, ok := h.cache.Load(key); ok {
		return v.(meter.Histogram)
	}
	hist := h.m.HistogramExt(h.name, h.buckets, pairLabels(h.names, values)...)
	actual, _ := h.cache.LoadOrStore(key, hist)
	return actual.(meter.Histogram)
}

// gaugeVec caches settable gauges per label tuple. Snapshot families
// also track their live series so collect() can purge them per scrape
// (the meter has no per-family Reset: Unregister removes one series).
type gaugeVec struct {
	m     meter.Meter
	name  string
	names []string
	mu    sync.Mutex
	live  map[string][]string // labelKey -> label values (series registered on the meter)
}

func newGaugeVec(m meter.Meter, name string, names ...string) *gaugeVec {
	return &gaugeVec{m: m, name: name, names: names}
}

// gauge returns the (settable) gauge for the label tuple, tracking it
// as a live series of the family. Callers pass label VALUES.
func (g *gaugeVec) gauge(values ...string) meter.Gauge {
	key := labelKey(values)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.live == nil {
		g.live = map[string][]string{}
	}
	if _, ok := g.live[key]; !ok {
		g.live[key] = append([]string(nil), values...)
	}
	return g.m.Gauge(g.name, nil, pairLabels(g.names, values)...)
}

func (g *gaugeVec) set(v float64, values ...string) { g.gauge(values...).Set(v) }

// purge unregisters every tracked series of the family (snapshot
// refresh: values are re-set after the purge, vanished series stay
// gone).
func (g *gaugeVec) purge() {
	g.mu.Lock()
	live := g.live
	g.live = nil
	g.mu.Unlock()
	for _, values := range live {
		g.m.Unregister(g.name, pairLabels(g.names, values)...)
	}
}

// clusterMetrics owns the process-wide meter. A nil *clusterMetrics is
// valid everywhere (metrics disabled): the daemon runs identically
// when /metrics is not configured.
type clusterMetrics struct {
	mtr meter.Meter

	// Data-plane server counters, filled from sdsnet.ServerMetrics hooks
	// (one hook set per hosted OSD, id-bound).
	osdRequests *counterVec
	osdBytes    *counterVec
	osdLatency  *histogramVec
	osdConns    *gaugeVec

	// Export (guest NBD/VDUSE) side, filled by the Backend wrapper.
	exportRequests  *counterVec
	exportBytes     *counterVec
	exportLatency   *histogramVec
	exportSustained *gaugeVec // {export,rate}

	// Snapshot gauges, refreshed on every scrape by collect().
	gCluster    *gaugeVec // {name}
	gRaftState  *gaugeVec // {state}
	gRaftMember *gaugeVec // {id,addr}
	gOsd        *gaugeVec // {osd,addr,name} name=up|weight
	gDisk       *gaugeVec // {osd,name} name=stored|free|total
	gWrite      *gaugeVec // {osd,name} write-amp counters (local OSDs)
	gJournal    *gaugeVec // {osd,name} journal staging footprint (local journal-mode OSDs)
	gMeta       *gaugeVec // {osd,name} meta index (local OSDs)
	gIntent     *gaugeVec // {osd,name} intent log (local intent-mode OSDs)
	gPool       *gaugeVec // {osd,state}
	gVolume     *gaugeVec // {name,metric} metric=writable|degraded|...
	gQoSLimit   *gaugeVec // {export,rate} configured limits
	gVacuum     *gaugeVec // {name} vacuum backlog + autovacuum config

	// Recovery traffic, filled by the RecoveryOpts.Record hook (runs
	// started through this process).
	recoveryCopies meter.Counter
	recoveryBytes  meter.Counter

	// tracked exports: name -> volume handle, for the sustained-rate gauges
	exports map[string]*client.Volume

	// collectMu serialises snapshot refreshes: with both -http and
	// -metrics enabled, two handlers scrape one meter.
	collectMu sync.Mutex
}

// newClusterMetrics builds the meter and all series. The VictoriaMetrics
// exporter writes the go_* and process_* runtime series alongside the
// unistor_* families on every scrape (WriteProcessMetrics/WriteFDMetrics).
func newClusterMetrics() *clusterMetrics {
	mtr := victoriametrics.NewMeter(victoriametrics.PrometheusCompat(true))
	m := &clusterMetrics{
		mtr:     mtr,
		exports: map[string]*client.Volume{},

		osdRequests: newCounterVec(mtr, "unistor_osd_requests_total", "osd", "op", "status"),
		osdBytes:    newCounterVec(mtr, "unistor_osd_bytes_total", "osd", "op", "direction"),
		osdLatency:  newHistogramVec(mtr, "unistor_osd_request_duration_seconds", opBuckets, "osd", "op"),
		osdConns:    newGaugeVec(mtr, "unistor_osd_connections", "osd"),

		exportRequests:  newCounterVec(mtr, "unistor_export_requests_total", "export", "op", "error"),
		exportBytes:     newCounterVec(mtr, "unistor_export_bytes_total", "export", "op", "direction"),
		exportLatency:   newHistogramVec(mtr, "unistor_export_request_duration_seconds", opBuckets, "export", "op"),
		exportSustained: newGaugeVec(mtr, "unistor_export_qos_sustained", "export", "rate"),

		gCluster:    newGaugeVec(mtr, "unistor_cluster", "name"),
		gRaftState:  newGaugeVec(mtr, "unistor_raft_state", "state"),
		gRaftMember: newGaugeVec(mtr, "unistor_raft_member", "id", "addr"),
		gOsd:        newGaugeVec(mtr, "unistor_osd", "osd", "addr", "name"),
		gDisk:       newGaugeVec(mtr, "unistor_osd_disk_bytes", "osd", "name"),
		gWrite:      newGaugeVec(mtr, "unistor_osd_write_bytes", "osd", "name"),
		gJournal:    newGaugeVec(mtr, "unistor_osd_journal", "osd", "name"),
		gMeta:       newGaugeVec(mtr, "unistor_osd_meta", "osd", "name"),
		gIntent:     newGaugeVec(mtr, "unistor_osd_intent", "osd", "name"),
		gPool:       newGaugeVec(mtr, "unistor_osd_pool_connections", "osd", "state"),
		gVolume:     newGaugeVec(mtr, "unistor_volume", "name", "metric"),
		gQoSLimit:   newGaugeVec(mtr, "unistor_export_qos_limit", "export", "rate"),
		gVacuum:     newGaugeVec(mtr, "unistor_vacuum", "name"),

		recoveryCopies: mtr.Counter("unistor_recovery_copies_total"),
		recoveryBytes:  mtr.Counter("unistor_recovery_bytes_total"),
	}
	return m
}

// metricsSource is the snapshot data source the gauges read at scrape
// time. It is decoupled from adminAPI so the metrics endpoint can run
// on its own address (-metrics) without the admin API (-http) being
// enabled: the server builds a source over the shared meta store, its
// own client.Cluster, the raft handle and the hosted-disk registry.
type metricsSource struct {
	cl    *client.Cluster
	ms    meta.Store
	raft  *meta.RaftStore
	disks *diskRegistry

	// vacuumInterval is the -vacuum-interval the autovacuum loop runs
	// with in this process (0 = autovacuum off); exported as
	// unistor_vacuum{name="interval_seconds"} so a dashboard can
	// compute "time until the backlog is cleaned" against it.
	vacuumInterval int
}

// volumeUsage reads a volume's allocation (bitmap popcount x object
// size); mirrors adminAPI.allocUsage without the admin dependency.
func (s metricsSource) volumeUsage(ctx context.Context, name string, nbits int) (uint64, error) {
	rec, err := meta.GetAllocMap(ctx, s.ms, name, nbits)
	if err != nil {
		return 0, err
	}
	return uint64(meta.AllocMapPopcount(rec.Bits)) * object.Size, nil
}

// labelOSD renders an OSD id for a label.
func labelOSD(id int32) string { return strconv.Itoa(int(id)) }

// labelStatus sanitizes a sdsnet status for a label value ("not
// supported" -> "not_supported").
func labelStatus(s sdsnet.Status) string {
	return strings.ReplaceAll(s.String(), " ", "_")
}

// osdServerMetrics returns the sdsnet hook set for one hosted OSD; the
// callbacks fire on the connection goroutines, so they are O(1)
// increments only (the meter series are get-or-create-cached per
// label tuple).
func (m *clusterMetrics) osdServerMetrics(id int32) *sdsnet.ServerMetrics {
	if m == nil {
		return nil
	}
	idL := labelOSD(id)
	return &sdsnet.ServerMetrics{
		Request: func(op sdsnet.Op, status sdsnet.Status, bin, bout uint64, dur time.Duration) {
			opL := op.String()
			m.osdRequests.get(idL, opL, labelStatus(status)).Inc()
			if bin > 0 {
				m.osdBytes.get(idL, opL, "in").Add(int(bin))
			}
			if bout > 0 {
				m.osdBytes.get(idL, opL, "out").Add(int(bout))
			}
			m.osdLatency.get(idL, opL).Update(dur.Seconds())
		},
		ConnAccepted: func() { m.osdConns.gauge(idL).Inc() },
		ConnClosed:   func(uint64) { m.osdConns.gauge(idL).Dec() },
	}
}

// trackExport wraps a volume backend so guest I/O is counted, and
// registers the volume for the per-scrape QoS gauges. Returns b
// unchanged when metrics are disabled.
func (m *clusterMetrics) trackExport(name string, vol *client.Volume, b nbd.Backend) nbd.Backend {
	if m == nil {
		return b
	}
	if m.exports == nil {
		m.exports = map[string]*client.Volume{}
	}
	m.exports[name] = vol
	if _, ok := b.(nbd.StatusReporter); ok {
		return &metricsStatus{metricsBackend{b: b, m: m, name: name}}
	}
	return &metricsBackend{b: b, m: m, name: name}
}

// metricsStatus mirrors the wrapped backend's StatusReporter capability
// (the NBD server detects base:allocation via type assertion, which an
// explicit delegating wrapper hides). Allocation queries are metadata:
// they pass through without per-request counters.
type metricsStatus struct {
	metricsBackend
}

func (ms *metricsStatus) BlockStatus(off int64, length uint32) ([]nbd.Extent, error) {
	return ms.b.(nbd.StatusReporter).BlockStatus(off, length)
}

// metricsBackend is the instrumented nbd.Backend wrapper: one label
// lookup per call on the hot path, no allocations beyond the
// counter-pair lookups.
type metricsBackend struct {
	b    nbd.Backend
	m    *clusterMetrics
	name string
}

func (mb *metricsBackend) Size() uint64 { return mb.b.Size() }
func (mb *metricsBackend) Close() error { return mb.b.Close() }

func (mb *metricsBackend) count(op string, err error) {
	e := "false"
	if err != nil {
		e = "true"
	}
	mb.m.exportRequests.get(mb.name, op, e).Inc()
}

func (mb *metricsBackend) ReadAt(p []byte, off int64) (int, error) {
	start := time.Now()
	n, err := mb.b.ReadAt(p, off)
	mb.count("read", err)
	if n > 0 {
		mb.m.exportBytes.get(mb.name, "read", "out").Add(n)
	}
	mb.m.exportLatency.get(mb.name, "read").Update(time.Since(start).Seconds())
	return n, err
}

func (mb *metricsBackend) WriteAt(p []byte, off int64) (int, error) {
	start := time.Now()
	n, err := mb.b.WriteAt(p, off)
	mb.count("write", err)
	if n > 0 {
		mb.m.exportBytes.get(mb.name, "write", "in").Add(n)
	}
	mb.m.exportLatency.get(mb.name, "write").Update(time.Since(start).Seconds())
	return n, err
}

func (mb *metricsBackend) Flush() error {
	start := time.Now()
	err := mb.b.Flush()
	mb.count("flush", err)
	mb.m.exportLatency.get(mb.name, "flush").Update(time.Since(start).Seconds())
	return err
}

func (mb *metricsBackend) Trim(off int64, length uint32) error {
	start := time.Now()
	err := mb.b.Trim(off, length)
	mb.count("trim", err)
	mb.m.exportLatency.get(mb.name, "trim").Update(time.Since(start).Seconds())
	return err
}

func (mb *metricsBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	start := time.Now()
	err := mb.b.WriteZeroes(off, length, punchHole)
	mb.count("write_zeroes", err)
	if err == nil && length > 0 {
		mb.m.exportBytes.get(mb.name, "write_zeroes", "in").Add(int(length))
	}
	mb.m.exportLatency.get(mb.name, "write_zeroes").Update(time.Since(start).Seconds())
	return err
}

// collect refreshes every snapshot gauge from meta + live probes. It
// runs per scrape and mirrors handleStatus's fan-out (probes bounded by
// statusProbeTimeout); a partial failure degrades the affected family,
// it never fails the whole exposition.
func (m *clusterMetrics) collect(ctx context.Context, s metricsSource) {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	// Purge the snapshot families so vanished OSDs/volumes disappear
	// between scrapes (counters/histograms keep accumulating).
	m.gCluster.purge()
	m.gRaftState.purge()
	m.gRaftMember.purge()
	m.gOsd.purge()
	m.gDisk.purge()
	m.gWrite.purge()
	m.gJournal.purge()
	m.gMeta.purge()
	m.gIntent.purge()
	m.gPool.purge()
	m.gVolume.purge()
	m.gQoSLimit.purge()
	m.gVacuum.purge()

	vols, err := s.cl.ListVolumes(ctx)
	if err == nil {
		m.gCluster.set(float64(len(vols)), "volumes")
	}
	if rec, err := meta.LatestMap(ctx, s.ms); err == nil {
		m.gCluster.set(float64(rec.Epoch), "map_epoch")
	}

	// healthy mirrors /v1/status: a warning condition zeroes it.
	healthy := 1.0

	// Raw capacity seen by the probe round; the volume loop below turns
	// it into per-volume usable-capacity gauges.
	var totalRaw, freeRaw, usedRaw float64
	var rawReported int

	// One probe round per scrape: ping + StatFS every registered OSD, so
	// disk accounting covers the whole cluster, not just local OSDs.
	infos, probes, disk, metaErr := s.cl.ProbeOSDsDisk(ctx, statusProbeTimeout)
	if metaErr != nil {
		healthy = 0
	} else {
		live := 0
		m.gCluster.set(float64(len(infos)), "osds")
		for _, info := range infos {
			idL := labelOSD(info.ID)
			up := probes[info.ID] == nil
			if up {
				live++
			}
			if !up && info.Weight > 0 {
				healthy = 0 // a dead OSD that was not evicted
			}
			if info.Weight <= 0 {
				healthy = 0 // evicted: data needs recovery
			}
			m.gOsd.set(b2f(up), idL, info.Addr, "up")
			m.gOsd.set(info.Weight, idL, info.Addr, "weight")
			// StatFS answers arrive for answering OSDs (older servers
			// omit the op); fall back to in-process figures for a local
			// OSD whose probe predates StatFS support.
			d, ok := disk[info.ID]
			if !ok && s.disks != nil {
				if ld := s.disks.disk(info.ID); ld != nil {
					if usage, free, total, lok := ld.DiskStats(); lok {
						d = client.DiskUsage{Usage: uint64(max0(usage)), Free: uint64(max0(free)), Total: uint64(max0(total))}
						ok = true
					}
				}
			}
			if ok {
				m.gDisk.set(float64(d.Usage), idL, "stored")
				m.gDisk.set(float64(d.Free), idL, "free")
				m.gDisk.set(float64(d.Total), idL, "total")
				totalRaw += float64(d.Total)
				freeRaw += float64(d.Free)
				usedRaw += float64(d.Usage)
				rawReported++
			}
			// Write-amp counters and meta index exist only for OSDs
			// hosted in this process (in-process store counters).
			if s.disks != nil {
				if ld := s.disks.disk(info.ID); ld != nil {
					ws := ld.WriteStats()
					m.gWrite.set(float64(ws.ClientBytes), idL, "client")
					m.gWrite.set(float64(ws.JournalBytes), idL, "journal")
					m.gWrite.set(float64(ws.FlattenBytes), idL, "flatten")
					m.gWrite.set(float64(ws.FillBytes), idL, "fill")
					m.gWrite.set(float64(ws.PunchBytes), idL, "punch")
					m.gWrite.set(float64(ws.JournalSyncs), idL, "journal_syncs")
					if lb, pend, jfree, jtotal, jok := ld.JournalStats(); jok {
						m.gJournal.set(float64(lb), idL, "log_bytes")
						m.gJournal.set(float64(pend), idL, "pending_records")
						m.gJournal.set(float64(jfree), idL, "free_bytes")
						m.gJournal.set(float64(jtotal), idL, "total_bytes")
					}
					objs, ram := ld.MetaInfo()
					m.gMeta.set(float64(objs), idL, "objects")
					m.gMeta.set(float64(ram), idL, "ram_bytes")
					if isv, _ := ld.IntentStats(); isv.Records > 0 || isv.Checked > 0 || isv.Mismatches > 0 {
						m.gIntent.set(float64(isv.Records), idL, "records")
						m.gIntent.set(float64(isv.Syncs), idL, "syncs")
						m.gIntent.set(float64(isv.Checked), idL, "checked")
						m.gIntent.set(float64(isv.Mismatches), idL, "mismatches")
					}
				}
			}
		}
		m.gCluster.set(float64(live), "osds_live")
		if rawReported > 0 {
			m.gCluster.set(totalRaw, "total_raw_bytes")
			m.gCluster.set(freeRaw, "free_raw_bytes")
			m.gCluster.set(usedRaw, "used_raw_bytes")
		}
	}

	if s.raft != nil {
		state := s.raft.RaftState()
		m.gRaftState.set(1, strings.ToLower(state))
		m.gCluster.set(b2f(state == "Leader" || state == "Follower"), "raft_has_leader")
		if state != "Leader" && state != "Follower" {
			healthy = 0 // control plane has no quorum
		}
		for _, mem := range s.raft.RaftMembers() {
			v := 1.0
			if mem.Leader {
				v = 2
			}
			m.gRaftMember.set(v, strconv.FormatUint(mem.ID, 10), mem.Addr)
		}
	}

	// Vacuum backlog: the garbage tombstones that the autovacuum loop
	// has yet to unlink, with the configured interval so a dashboard
	// can answer "how much is left / how long until autovacuum cleans
	// it" (bytes, oldest age vs interval).
	vctx, vcancel := context.WithTimeout(ctx, statusProbeTimeout)
	vb, vbErr := s.cl.VacuumBacklog(vctx)
	vcancel()
	if vbErr == nil {
		m.gVacuum.set(float64(vb.Tombstones), "tombstones")
		m.gVacuum.set(float64(vb.Objects), "objects")
		m.gVacuum.set(float64(vb.Bytes), "bytes")
		m.gVacuum.set(vb.Oldest, "oldest_seconds")
		m.gVacuum.set(float64(s.vacuumInterval), "interval_seconds")
	}

	for id, st := range s.cl.PoolStats() {
		idL := labelOSD(id)
		m.gPool.set(float64(st[0]), idL, "idle")
		m.gPool.set(float64(st[1]), idL, "in_use")
	}

	for _, spec := range vols {
		name := spec.Name
		h, err := s.cl.VolumeHealth(ctx, spec)
		if err == nil {
			m.gVolume.set(float64(h.LiveTargets), name, "live_targets")
			m.gVolume.set(float64(h.MinSize), name, "min_size")
			m.gVolume.set(b2f(h.Writable), name, "writable")
			m.gVolume.set(b2f(!h.Writable), name, "degraded")
			// PG census (mirrors vitastor_pg_count): every PG is ok
			// (full replicas), degraded (min_size..replicas-1) or down
			// (below min_size).
			m.gVolume.set(float64(h.PGs), name, "pgs")
			m.gVolume.set(float64(h.PGsOK), name, "pgs_ok")
			m.gVolume.set(float64(h.PGsDegraded), name, "pgs_degraded")
			m.gVolume.set(float64(h.PGsDown), name, "pgs_down")
			if !h.Writable {
				healthy = 0
			}
		}
		m.gVolume.set(float64(spec.Size), name, "size_bytes")
		m.gVolume.set(float64(spec.Quota), name, "quota_bytes")
		nits := (spec.Size + object.Mask) >> object.SizeShift
		if usage, err := s.volumeUsage(ctx, name, int(nits)); err == nil {
			m.gVolume.set(float64(usage), name, "usage_bytes")
		}
		// Usable capacity the probed raw total affords at this volume's
		// replica count (vitastor_pool_raw_to_usable); space efficiency
		// is usable/raw = 1/replicas (vitastor_pool_space_efficiency).
		rep := float64(spec.Replicas)
		if rep < 1 {
			rep = 1
		}
		if rawReported > 0 {
			m.gVolume.set(totalRaw/rep, name, "raw_to_usable_bytes")
			m.gVolume.set(1/rep, name, "space_efficiency")
		}
		m.gQoSLimit.set(float64(spec.ReadIOPS), name, "read_iops")
		m.gQoSLimit.set(float64(spec.WriteIOPS), name, "write_iops")
		m.gQoSLimit.set(float64(spec.ReadBPS), name, "read_bps")
		m.gQoSLimit.set(float64(spec.WriteBPS), name, "write_bps")
	}

	// QoS sustained rates from the limiter of every volume opened by
	// this process (the exports it serves).
	for name, vol := range m.exports {
		riops, rbps, wiops, wbps := vol.QoSSustained()
		m.exportSustained.set(float64(riops), name, "read_iops")
		m.exportSustained.set(float64(rbps), name, "read_bps")
		m.exportSustained.set(float64(wiops), name, "write_iops")
		m.exportSustained.set(float64(wbps), name, "write_bps")
	}

	m.gCluster.set(healthy, "healthy")
}

// b2f renders a bool as the 1/0 gauge value.
func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// max0 clamps a disk figure (int64) to a non-negative uint64.
func max0(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// recoveryRecorder returns the RecoveryOpts.Record hook that feeds the
// recovery counters; nil when metrics are disabled.
func (m *clusterMetrics) recoveryRecorder() func(copies int, bytes uint64) {
	if m == nil {
		return nil
	}
	return func(copies int, bytes uint64) {
		m.recoveryCopies.Add(copies)
		m.recoveryBytes.Add(int(bytes))
	}
}

// handler serves GET /metrics: refresh snapshots, then write the
// standard Prometheus text exposition through the micro-meter
// VictoriaMetrics exporter (the VictoriaMetrics scrape target), plus
// the go_*/process_* runtime series.
func (m *clusterMetrics) handler(s metricsSource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.collect(r.Context(), s)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		if err := m.mtr.Write(w, meter.WriteProcessMetrics(true), meter.WriteFDMetrics(true)); err != nil {
			log.Printf("metrics write: %v", err)
		}
	}
}

// serveMetrics runs a standalone exposition endpoint on addr (the
// -metrics flag: "host:port", exposition path /metrics), independent of
// the admin API, until ctx is cancelled. This is the exporter address
// VictoriaMetrics scrapes, e.g. -metrics 0.0.0.0:8080 -> /metrics.
func (m *clusterMetrics) serveMetrics(ctx context.Context, addr string, s metricsSource, logger *log.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return m.serveMetricsListener(ctx, ln, s, logger)
}

// serveMetricsListener serves the exposition on an already-bound
// listener (tests bind an ephemeral port and scrape the real server).
func (m *clusterMetrics) serveMetricsListener(ctx context.Context, ln net.Listener, s metricsSource, logger *log.Logger) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", m.handler(s))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/metrics", http.StatusFound)
			return
		}
		http.NotFound(w, r)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	logger.Printf("metrics (micro-meter-victoriametrics) on http://%s/metrics", ln.Addr())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
