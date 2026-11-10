// Prometheus metrics for unistord (VictoriaMetrics-compatible
// exposition): GET /metrics on the standalone -metrics address only —
// the admin REST API (-http) never serves them.
//
// The mapping mirrors the admin JSON surface (/v1/status) plus the
// hot-path counters: VictoriaMetrics scrapes the standard Prometheus
// text format, so this is the "micrometer-style" export for a Go
// daemon — the registry lives here, in the cmd module, while the core
// pkg/ modules stay dependency-free and only fire the sdsnet hook
// callbacks (sdsnet.ServerMetrics) this file adapts into collectors.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// opBuckets covers the latency span of the block path: sub-100µs NVMe
// dispatch through multi-second queue waits, geometric so each bucket
// doubles the last (histogram_quantile resolution stays even).
var opBuckets = prometheus.ExponentialBuckets(0.000125, 2, 17) // 0.125ms .. 8.2s

// clusterMetrics owns the process-wide Prometheus registry. A nil
// *clusterMetrics is valid everywhere (metrics disabled): the daemon
// runs identically when /metrics is not configured.
type clusterMetrics struct {
	reg *prometheus.Registry

	// Data-plane server counters, filled from sdsnet.ServerMetrics hooks
	// (one hook set per hosted OSD, id-bound).
	osdRequests *prometheus.CounterVec
	osdBytes    *prometheus.CounterVec
	osdLatency  *prometheus.HistogramVec
	osdConns    *prometheus.GaugeVec

	// Export (guest NBD/VDUSE) side, filled by the Backend wrapper.
	exportRequests  *prometheus.CounterVec
	exportBytes     *prometheus.CounterVec
	exportLatency   *prometheus.HistogramVec
	exportSustained *prometheus.GaugeVec // {export,rate}

	// Snapshot gauges, refreshed on every scrape by collect().
	gCluster    *prometheus.GaugeVec // {name}
	gRaftState  *prometheus.GaugeVec // {state}
	gRaftMember *prometheus.GaugeVec // {id,addr}
	gOsd        *prometheus.GaugeVec // {osd,addr,name} name=up|weight
	gDisk       *prometheus.GaugeVec // {osd,name} name=stored|free|total
	gWrite      *prometheus.GaugeVec // {osd,name} write-amp counters (local OSDs)
	gJournal    *prometheus.GaugeVec // {osd,name} journal staging footprint (local journal-mode OSDs)
	gMeta       *prometheus.GaugeVec // {osd,name} meta index (local OSDs)
	gIntent     *prometheus.GaugeVec // {osd,name} intent log (local intent-mode OSDs)
	gPool       *prometheus.GaugeVec // {osd,state}
	gVolume     *prometheus.GaugeVec // {name} name=writable|degraded|...
	gQoSLimit   *prometheus.GaugeVec // {export,rate} configured limits
	gVacuum     *prometheus.GaugeVec // {name} vacuum backlog + autovacuum config

	// Recovery traffic, filled by the RecoveryOpts.Record hook (runs
	// started through this process).
	recoveryCopies prometheus.Counter
	recoveryBytes  prometheus.Counter

	// tracked exports: name -> volume handle, for the sustained-rate gauges
	exports map[string]*client.Volume

	// collectMu serialises snapshot refreshes: with both -http and
	// -metrics enabled, two handlers scrape one registry.
	collectMu sync.Mutex
}

// newClusterMetrics builds the registry and all collectors. The Go and
// process collectors ship standard process/go runtime series alongside
// the unistor_* families.
func newClusterMetrics() *clusterMetrics {
	m := &clusterMetrics{
		reg:     prometheus.NewRegistry(),
		exports: map[string]*client.Volume{},
	}
	m.osdRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "unistor_osd_requests_total",
		Help: "Requests served by each hosted OSD, by op and status.",
	}, []string{"osd", "op", "status"})
	m.osdBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "unistor_osd_bytes_total",
		Help: "Payload bytes moved by each hosted OSD, by op and direction (in = client-to-OSD, out = OSD-to-client).",
	}, []string{"osd", "op", "direction"})
	m.osdLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "unistor_osd_request_duration_seconds",
		Help:    "Time from request header to dispatch completion at each hosted OSD (queue/throttle time included).",
		Buckets: opBuckets,
	}, []string{"osd", "op"})
	m.osdConns = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_connections",
		Help: "Live client connections per hosted OSD.",
	}, []string{"osd"})
	m.exportRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "unistor_export_requests_total",
		Help: "Guest I/O per export (NBD/VDUSE), by op and error.",
	}, []string{"export", "op", "error"})
	m.exportBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "unistor_export_bytes_total",
		Help: "Guest payload bytes per export, by op and direction (in = write path, out = read path).",
	}, []string{"export", "op", "direction"})
	m.exportLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "unistor_export_request_duration_seconds",
		Help:    "Guest I/O latency per export, by op (QoS queue wait included).",
		Buckets: opBuckets,
	}, []string{"export", "op"})
	m.exportSustained = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_export_qos_sustained",
		Help: "Sustained (long-window) throughput the volume's QoS limiter admitted, per export and rate (iops/bps).",
	}, []string{"export", "rate"})
	m.gCluster = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_cluster",
		Help: "Cluster-level snapshot at scrape time: osds, osds_live, healthy, volumes, map_epoch, raft_has_leader.",
	}, []string{"name"})
	m.gRaftState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_raft_state",
		Help: "Meta raft state of this node: the active state is 1, the others 0.",
	}, []string{"state"})
	m.gRaftMember = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_raft_member",
		Help: "Meta raft configuration members known to this node: value 1 = voter, 2 = leader (the leader is also a voter).",
	}, []string{"id", "addr"})
	m.gOsd = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd",
		Help: "Per-OSD snapshot at scrape time: up (answers a ping), weight (0 = evicted).",
	}, []string{"osd", "addr", "name"})
	m.gDisk = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_disk_bytes",
		Help: "Disk accounting per OSD (stored/free/total bytes), probed cluster-wide over StatFS; a remote OSD answering an older protocol is omitted.",
	}, []string{"osd", "name"})
	m.gWrite = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_write_bytes",
		Help: "Lifetime write traffic of locally hosted OSDs (client/journal/flatten/fill/punch bytes, journal_syncs count): write amplification = (journal+flatten+fill)/client. Counter semantics: monotonically growing since process start.",
	}, []string{"osd", "name"})
	m.gJournal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_journal",
		Help: "Journal staging footprint of locally hosted journal-mode OSDs: log_bytes (WAL file size on disk), pending_records (acked, not yet flattened), free_bytes/total_bytes (filesystem the journal grows into). Alert when free_bytes drops toward the journal's growth headroom.",
	}, []string{"osd", "name"})
	m.gMeta = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_meta",
		Help: "In-memory object meta index of locally hosted OSDs: objects (indexed entries), ram_bytes (estimated RAM the index occupies).",
	}, []string{"osd", "name"})
	m.gIntent = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_intent",
		Help: "Write intent log of locally hosted OSDs (only exported when the intent log is on): records (appended), syncs (log fsyncs), checked (open intents verified at recovery), mismatches (acknowledged writes not durable on disk — data damage).",
	}, []string{"osd", "name"})
	m.gPool = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_osd_pool_connections",
		Help: "Connections in this process's client pool per OSD, by state (idle|in_use).",
	}, []string{"osd", "state"})
	m.gVolume = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_volume",
		Help: "Per-volume snapshot: writable, degraded, live_targets, min_size, size_bytes, quota_bytes, usage_bytes.",
	}, []string{"name", "metric"})
	m.gQoSLimit = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_export_qos_limit",
		Help: "Configured QoS limits per export (0 = unlimited), by rate (read_iops|write_iops|read_bps|write_bps).",
	}, []string{"export", "rate"})
	m.recoveryCopies = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "unistor_recovery_copies_total",
		Help: "Object copies replicated by recovery runs started through this process (admin POST /v1/volumes/{name}/recover).",
	})
	m.recoveryBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "unistor_recovery_bytes_total",
		Help: "Payload bytes moved by recovery runs started through this process.",
	})

	m.gVacuum = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "unistor_vacuum",
		Help: "Vacuum backlog and autovacuum configuration: tombstones (pending records), objects (OIDs to unlink per OSD), bytes (garbage occupying disk = objects x 4MiB), oldest_seconds (age of the oldest tombstone), interval_seconds (autovacuum period; 0 = autovacuum off). bytes/oldest_seconds together answer how much work is left until autovacuum cleans the garbage.",
	}, []string{"name"})

	for _, c := range []prometheus.Collector{
		m.osdRequests, m.osdBytes, m.osdLatency, m.osdConns,
		m.exportRequests, m.exportBytes, m.exportLatency, m.exportSustained,
		m.gCluster, m.gRaftState, m.gRaftMember, m.gOsd, m.gDisk, m.gWrite, m.gJournal, m.gMeta, m.gIntent, m.gPool, m.gVolume, m.gQoSLimit, m.gVacuum,
		m.recoveryCopies, m.recoveryBytes,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		m.reg.MustRegister(c)
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
// increments only (the prometheus vectors are lock-sharded).
func (m *clusterMetrics) osdServerMetrics(id int32) *sdsnet.ServerMetrics {
	if m == nil {
		return nil
	}
	idL := labelOSD(id)
	return &sdsnet.ServerMetrics{
		Request: func(op sdsnet.Op, status sdsnet.Status, bin, bout uint64, dur time.Duration) {
			opL := op.String()
			m.osdRequests.WithLabelValues(idL, opL, labelStatus(status)).Inc()
			if bin > 0 {
				m.osdBytes.WithLabelValues(idL, opL, "in").Add(float64(bin))
			}
			if bout > 0 {
				m.osdBytes.WithLabelValues(idL, opL, "out").Add(float64(bout))
			}
			m.osdLatency.WithLabelValues(idL, opL).Observe(dur.Seconds())
		},
		ConnAccepted: func() { m.osdConns.WithLabelValues(idL).Inc() },
		ConnClosed:   func(uint64) { m.osdConns.WithLabelValues(idL).Dec() },
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
	mb.m.exportRequests.WithLabelValues(mb.name, op, e).Inc()
}

func (mb *metricsBackend) ReadAt(p []byte, off int64) (int, error) {
	start := time.Now()
	n, err := mb.b.ReadAt(p, off)
	mb.count("read", err)
	if n > 0 {
		mb.m.exportBytes.WithLabelValues(mb.name, "read", "out").Add(float64(n))
	}
	mb.m.exportLatency.WithLabelValues(mb.name, "read").Observe(time.Since(start).Seconds())
	return n, err
}

func (mb *metricsBackend) WriteAt(p []byte, off int64) (int, error) {
	start := time.Now()
	n, err := mb.b.WriteAt(p, off)
	mb.count("write", err)
	if n > 0 {
		mb.m.exportBytes.WithLabelValues(mb.name, "write", "in").Add(float64(n))
	}
	mb.m.exportLatency.WithLabelValues(mb.name, "write").Observe(time.Since(start).Seconds())
	return n, err
}

func (mb *metricsBackend) Flush() error {
	start := time.Now()
	err := mb.b.Flush()
	mb.count("flush", err)
	mb.m.exportLatency.WithLabelValues(mb.name, "flush").Observe(time.Since(start).Seconds())
	return err
}

func (mb *metricsBackend) Trim(off int64, length uint32) error {
	start := time.Now()
	err := mb.b.Trim(off, length)
	mb.count("trim", err)
	mb.m.exportLatency.WithLabelValues(mb.name, "trim").Observe(time.Since(start).Seconds())
	return err
}

func (mb *metricsBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	start := time.Now()
	err := mb.b.WriteZeroes(off, length, punchHole)
	mb.count("write_zeroes", err)
	if err == nil && length > 0 {
		mb.m.exportBytes.WithLabelValues(mb.name, "write_zeroes", "in").Add(float64(length))
	}
	mb.m.exportLatency.WithLabelValues(mb.name, "write_zeroes").Observe(time.Since(start).Seconds())
	return err
}

// collect refreshes every snapshot gauge from meta + live probes. It
// runs per scrape and mirrors handleStatus's fan-out (probes bounded by
// statusProbeTimeout); a partial failure degrades the affected family,
// it never fails the whole exposition.
func (m *clusterMetrics) collect(ctx context.Context, s metricsSource) {
	m.collectMu.Lock()
	defer m.collectMu.Unlock()
	// Reset the snapshot families so vanished OSDs/volumes disappear
	// between scrapes (counters/histograms keep accumulating).
	m.gCluster.Reset()
	m.gRaftState.Reset()
	m.gRaftMember.Reset()
	m.gOsd.Reset()
	m.gDisk.Reset()
	m.gWrite.Reset()
	m.gJournal.Reset()
	m.gMeta.Reset()
	m.gIntent.Reset()
	m.gPool.Reset()
	m.gVolume.Reset()
	m.gQoSLimit.Reset()
	m.gVacuum.Reset()

	vols, err := s.cl.ListVolumes(ctx)
	if err == nil {
		m.gCluster.WithLabelValues("volumes").Set(float64(len(vols)))
	}
	if rec, err := meta.LatestMap(ctx, s.ms); err == nil {
		m.gCluster.WithLabelValues("map_epoch").Set(float64(rec.Epoch))
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
		m.gCluster.WithLabelValues("osds").Set(float64(len(infos)))
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
			m.gOsd.WithLabelValues(idL, info.Addr, "up").Set(b2f(up))
			m.gOsd.WithLabelValues(idL, info.Addr, "weight").Set(info.Weight)
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
				m.gDisk.WithLabelValues(idL, "stored").Set(float64(d.Usage))
				m.gDisk.WithLabelValues(idL, "free").Set(float64(d.Free))
				m.gDisk.WithLabelValues(idL, "total").Set(float64(d.Total))
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
					m.gWrite.WithLabelValues(idL, "client").Set(float64(ws.ClientBytes))
					m.gWrite.WithLabelValues(idL, "journal").Set(float64(ws.JournalBytes))
					m.gWrite.WithLabelValues(idL, "flatten").Set(float64(ws.FlattenBytes))
					m.gWrite.WithLabelValues(idL, "fill").Set(float64(ws.FillBytes))
					m.gWrite.WithLabelValues(idL, "punch").Set(float64(ws.PunchBytes))
					m.gWrite.WithLabelValues(idL, "journal_syncs").Set(float64(ws.JournalSyncs))
					if lb, pend, jfree, jtotal, jok := ld.JournalStats(); jok {
						m.gJournal.WithLabelValues(idL, "log_bytes").Set(float64(lb))
						m.gJournal.WithLabelValues(idL, "pending_records").Set(float64(pend))
						m.gJournal.WithLabelValues(idL, "free_bytes").Set(float64(jfree))
						m.gJournal.WithLabelValues(idL, "total_bytes").Set(float64(jtotal))
					}
					objs, ram := ld.MetaInfo()
					m.gMeta.WithLabelValues(idL, "objects").Set(float64(objs))
					m.gMeta.WithLabelValues(idL, "ram_bytes").Set(float64(ram))
					if isv, _ := ld.IntentStats(); isv.Records > 0 || isv.Checked > 0 || isv.Mismatches > 0 {
						m.gIntent.WithLabelValues(idL, "records").Set(float64(isv.Records))
						m.gIntent.WithLabelValues(idL, "syncs").Set(float64(isv.Syncs))
						m.gIntent.WithLabelValues(idL, "checked").Set(float64(isv.Checked))
						m.gIntent.WithLabelValues(idL, "mismatches").Set(float64(isv.Mismatches))
					}
				}
			}
		}
		m.gCluster.WithLabelValues("osds_live").Set(float64(live))
		if rawReported > 0 {
			m.gCluster.WithLabelValues("total_raw_bytes").Set(totalRaw)
			m.gCluster.WithLabelValues("free_raw_bytes").Set(freeRaw)
			m.gCluster.WithLabelValues("used_raw_bytes").Set(usedRaw)
		}
	}

	if s.raft != nil {
		state := s.raft.RaftState()
		m.gRaftState.WithLabelValues(strings.ToLower(state)).Set(1)
		m.gCluster.WithLabelValues("raft_has_leader").Set(b2f(state == "Leader" || state == "Follower"))
		if state != "Leader" && state != "Follower" {
			healthy = 0 // control plane has no quorum
		}
		for _, mem := range s.raft.RaftMembers() {
			v := 1.0
			if mem.Leader {
				v = 2
			}
			m.gRaftMember.WithLabelValues(strconv.FormatUint(mem.ID, 10), mem.Addr).Set(v)
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
		m.gVacuum.WithLabelValues("tombstones").Set(float64(vb.Tombstones))
		m.gVacuum.WithLabelValues("objects").Set(float64(vb.Objects))
		m.gVacuum.WithLabelValues("bytes").Set(float64(vb.Bytes))
		m.gVacuum.WithLabelValues("oldest_seconds").Set(vb.Oldest)
		m.gVacuum.WithLabelValues("interval_seconds").Set(float64(s.vacuumInterval))
	}

	for id, st := range s.cl.PoolStats() {
		idL := labelOSD(id)
		m.gPool.WithLabelValues(idL, "idle").Set(float64(st[0]))
		m.gPool.WithLabelValues(idL, "in_use").Set(float64(st[1]))
	}

	for _, spec := range vols {
		name := spec.Name
		h, err := s.cl.VolumeHealth(ctx, spec)
		if err == nil {
			m.gVolume.WithLabelValues(name, "live_targets").Set(float64(h.LiveTargets))
			m.gVolume.WithLabelValues(name, "min_size").Set(float64(h.MinSize))
			m.gVolume.WithLabelValues(name, "writable").Set(b2f(h.Writable))
			m.gVolume.WithLabelValues(name, "degraded").Set(b2f(!h.Writable))
			// PG census (mirrors vitastor_pg_count): every PG is ok
			// (full replicas), degraded (min_size..replicas-1) or down
			// (below min_size).
			m.gVolume.WithLabelValues(name, "pgs").Set(float64(h.PGs))
			m.gVolume.WithLabelValues(name, "pgs_ok").Set(float64(h.PGsOK))
			m.gVolume.WithLabelValues(name, "pgs_degraded").Set(float64(h.PGsDegraded))
			m.gVolume.WithLabelValues(name, "pgs_down").Set(float64(h.PGsDown))
			if !h.Writable {
				healthy = 0
			}
		}
		m.gVolume.WithLabelValues(name, "size_bytes").Set(float64(spec.Size))
		m.gVolume.WithLabelValues(name, "quota_bytes").Set(float64(spec.Quota))
		nits := (spec.Size + object.Mask) >> object.SizeShift
		if usage, err := s.volumeUsage(ctx, name, int(nits)); err == nil {
			m.gVolume.WithLabelValues(name, "usage_bytes").Set(float64(usage))
		}
		// Usable capacity the probed raw total affords at this volume's
		// replica count (vitastor_pool_raw_to_usable); space efficiency
		// is usable/raw = 1/replicas (vitastor_pool_space_efficiency).
		rep := float64(spec.Replicas)
		if rep < 1 {
			rep = 1
		}
		if rawReported > 0 {
			m.gVolume.WithLabelValues(name, "raw_to_usable_bytes").Set(totalRaw / rep)
			m.gVolume.WithLabelValues(name, "space_efficiency").Set(1 / rep)
		}
		m.gQoSLimit.WithLabelValues(name, "read_iops").Set(float64(spec.ReadIOPS))
		m.gQoSLimit.WithLabelValues(name, "write_iops").Set(float64(spec.WriteIOPS))
		m.gQoSLimit.WithLabelValues(name, "read_bps").Set(float64(spec.ReadBPS))
		m.gQoSLimit.WithLabelValues(name, "write_bps").Set(float64(spec.WriteBPS))
	}

	// QoS sustained rates from the limiter of every volume opened by
	// this process (the exports it serves).
	for name, vol := range m.exports {
		riops, rbps, wiops, wbps := vol.QoSSustained()
		m.exportSustained.WithLabelValues(name, "read_iops").Set(float64(riops))
		m.exportSustained.WithLabelValues(name, "read_bps").Set(float64(rbps))
		m.exportSustained.WithLabelValues(name, "write_iops").Set(float64(wiops))
		m.exportSustained.WithLabelValues(name, "write_bps").Set(float64(wbps))
	}

	m.gCluster.WithLabelValues("healthy").Set(healthy)
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
		m.recoveryCopies.Add(float64(copies))
		m.recoveryBytes.Add(float64(bytes))
	}
}

// handler serves GET /metrics: refresh snapshots, then write the
// standard Prometheus text exposition (VictoriaMetrics scrape target).
func (m *clusterMetrics) handler(s metricsSource) http.HandlerFunc {
	gather := promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
	return func(w http.ResponseWriter, r *http.Request) {
		m.collect(r.Context(), s)
		gather.ServeHTTP(w, r)
	}
}

// serveMetrics runs a standalone Prometheus endpoint on addr (the
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
	logger.Printf("metrics (prometheus) on http://%s/metrics", ln.Addr())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
