package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/nbd"
)

// gatherText scrapes the meter through the handler and returns the
// Prometheus text exposition.
func gatherText(t *testing.T, m *clusterMetrics, s metricsSource) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.handler(s).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics: %d", rec.Code)
	}
	return rec.Body.String()
}

// metricValue refreshes the snapshot (as a scrape would) and finds a
// sample of family whose labels include all of want.
func metricValue(t *testing.T, m *clusterMetrics, s metricsSource, family string, labels map[string]string) (float64, bool) {
	t.Helper()
	return findSample(gatherText(t, m, s), family, labels)
}

// findSample scans a Prometheus text exposition for a
// `family{k="v",...} value` line whose labels include all of want and
// returns its value. The exporter preserves creation label order (no
// alphabetical sorting), so label-order-sensitive assertions go through
// this parser rather than raw substrings.
func findSample(body, family string, want map[string]string) (float64, bool) {
	for _, line := range strings.Split(body, "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		i := strings.IndexAny(line, " {")
		if i < 0 || line[:i] != family {
			continue
		}
		rest := line[i:]
		if rest[0] == '{' {
			rbr := strings.LastIndexByte(rest, '}')
			if rbr < 0 {
				continue
			}
			got := map[string]string{}
			for _, kv := range strings.Split(rest[1:rbr], ",") {
				k, v, _ := strings.Cut(kv, "=\"")
				got[k] = strings.TrimSuffix(v, `"`)
			}
			matched := true
			for k, v := range want {
				if got[k] != v {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			rest = rest[rbr+1:]
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(fields[0], 64)
		if err != nil {
			continue
		}
		return v, true
	}
	return 0, false
}

// TestMetricsEndpoint: a live cluster scraped through the metrics
// handler (the same one the admin /metrics route serves). Checks the
// cluster/node/volume families and that a killed OSD is reflected.
func TestMetricsEndpoint(t *testing.T) {
	a, cl, daems := startStatusCluster(t)
	mreg := newClusterMetrics()
	src := metricsSource{cl: cl, ms: a.ms, raft: nil, disks: a.disks}

	body := gatherText(t, mreg, src)
	for _, want := range []string{
		`unistor_cluster{name="osds"} 3`,
		`unistor_cluster{name="osds_live"} 3`,
		`unistor_cluster{name="healthy"} 1`,
		`unistor_cluster{name="volumes"} 1`,
		`unistor_cluster{name="map_epoch"} 1`,
		`unistor_volume{name="vm0",metric="writable"} 1`,
		`unistor_volume{name="vm0",metric="size_bytes"} 134217728`,
		`unistor_volume{name="vm0",metric="min_size"} 2`,
		`unistor_volume{name="vm0",metric="live_targets"} 3`,
		// vitastor-gap families: full PG census, cluster capacity from
		// the cluster-wide StatFS probe, per-volume raw-to-usable.
		`unistor_volume{name="vm0",metric="pgs"} 64`,
		`unistor_volume{name="vm0",metric="pgs_ok"} 64`,
		`unistor_volume{name="vm0",metric="pgs_degraded"} 0`,
		`unistor_volume{name="vm0",metric="pgs_down"} 0`,
		`unistor_cluster{name="total_raw_bytes"}`,
		`unistor_cluster{name="free_raw_bytes"}`,
		`unistor_cluster{name="used_raw_bytes"}`,
		`unistor_volume{name="vm0",metric="raw_to_usable_bytes"}`,
		`unistor_volume{name="vm0",metric="space_efficiency"} 0.3333333333333333`,
		`unistor_osd_disk_bytes{osd="0",name="total"}`,
		`unistor_osd_disk_bytes{osd="2",name="total"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition missing %q\n%s", want, body)
		}
	}

	if v, ok := findSample(body, "unistor_osd", map[string]string{"osd": "0", "name": "up"}); !ok || v != 1 {
		t.Fatalf("osd 0 up: %v %v\n%s", v, ok, body)
	}
	if !strings.Contains(body, "unistor_osd{osd=\"0\",addr=") {
		t.Fatalf("exposition missing osd series with addr label\n%s", body)
	}

	// Kill one OSD: it stays registered (osds=3) but is no longer live.
	if err := daems[2].Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if v, ok := metricValue(t, mreg, src, "unistor_cluster", map[string]string{"name": "osds_live"}); ok && v == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("osds_live never dropped to 2:\n%s", gatherText(t, mreg, src))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_cluster", map[string]string{"name": "osds"}); !ok || v != 3 {
		t.Fatalf("osds: %v %v", v, ok)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_cluster", map[string]string{"name": "healthy"}); !ok || v != 0 {
		t.Fatalf("healthy after kill: %v %v", v, ok)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_osd", map[string]string{"osd": "2", "name": "up"}); !ok || v != 0 {
		t.Fatalf("dead osd up: %v %v", v, ok)
	}
}

// TestMetricsStandaloneServer drives serveMetrics over a real socket on
// an ephemeral port (the -metrics path).
func TestMetricsStandaloneServer(t *testing.T) {
	a, cl, _ := startStatusCluster(t)
	mreg := newClusterMetrics()
	src := metricsSource{cl: cl, ms: a.ms, raft: nil, disks: nil}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- mreg.serveMetricsListener(ctx, ln, src, log.New(io.Discard, "", 0)) }()

	url := "http://" + ln.Addr().String() + "/metrics"
	var body string
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body = string(b)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("scrape %s: no 200", url)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(body, `unistor_cluster{name="osds"} 3`) {
		t.Fatalf("standalone exposition:\n%s", body)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Fatal("go collector missing")
	}

	// "/" redirects to /metrics (no redirect following: assert 302).
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noRedirect.Get("http://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("root redirect: %d", resp.StatusCode)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveMetrics: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveMetrics did not shut down")
	}
}

// nopBackend is the minimal nbd.Backend for the nil-registry test.
type nopBackend struct{}

func (nopBackend) Size() uint64                                       { return 0 }
func (nopBackend) ReadAt(p []byte, off int64) (int, error)            { return 0, nil }
func (nopBackend) WriteAt(p []byte, off int64) (int, error)           { return 0, nil }
func (nopBackend) Flush() error                                       { return nil }
func (nopBackend) Trim(off int64, length uint32) error                { return nil }
func (nopBackend) WriteZeroes(off int64, length uint32, _ bool) error { return nil }
func (nopBackend) Close() error                                       { return nil }

// TestMetricsNilSafe: a disabled registry (nil) is inert everywhere.
func TestMetricsNilSafe(t *testing.T) {
	var m *clusterMetrics
	if m.osdServerMetrics(1) != nil {
		t.Fatal("osdServerMetrics on nil must be nil")
	}
	var b nbd.Backend = nopBackend{}
	if got := m.trackExport("x", nil, b); got != b {
		t.Fatal("trackExport on nil must return the backend unchanged")
	}
	if m.recoveryRecorder() != nil {
		t.Fatal("recoveryRecorder on nil must be nil")
	}
}

// TestRecoveryRecorder checks the RecoveryOpts.Record seam: copies and
// bytes land on the counters, and a nil record hook (unistorctl path)
// is simply not wired.
func TestRecoveryRecorder(t *testing.T) {
	m := newClusterMetrics()
	rec := m.recoveryRecorder()
	if rec == nil {
		t.Fatal("recoveryRecorder on a live registry must not be nil")
	}
	rec(3, 1<<20)
	rec(1, 4096)
	if got := m.recoveryCopies.Get(); got != 4 {
		t.Fatalf("unistor_recovery_copies_total = %v, want 4", got)
	}
	if got := m.recoveryBytes.Get(); got != 1<<20+4096 {
		t.Fatalf("unistor_recovery_bytes_total = %v, want %v", got, uint64(1<<20+4096))
	}
	// The counters must reach the exposition, too (written straight
	// through the exporter: no metricsSource, so no snapshot collect).
	var buf bytes.Buffer
	if err := m.mtr.Write(&buf); err != nil {
		t.Fatal(err)
	}
	body := buf.String()
	for _, want := range []string{
		"unistor_recovery_copies_total 4",
		"unistor_recovery_bytes_total 1052672",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("exposition missing %q\n%s", want, body)
		}
	}
}

// TestVacuumBacklogMetrics checks the autovacuum-backlog family: the
// clean cluster reports an empty queue (oldest -1), deleting a volume
// queues its objects, and the configured interval rides along so a
// dashboard can compute "how long until autovacuum cleans it".
func TestVacuumBacklogMetrics(t *testing.T) {
	a, cl, _ := startStatusCluster(t)
	mreg := newClusterMetrics()
	src := metricsSource{cl: cl, ms: a.ms, raft: nil, disks: a.disks, vacuumInterval: 300}

	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "tombstones"}); !ok || v != 0 {
		t.Fatalf("tombstones = %v (ok=%v), want 0", v, ok)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "oldest_seconds"}); !ok || v != -1 {
		t.Fatalf("oldest_seconds = %v (ok=%v), want -1", v, ok)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "interval_seconds"}); !ok || v != 300 {
		t.Fatalf("interval_seconds = %v (ok=%v), want 300", v, ok)
	}

	// vm0 is 128 MiB at 4 MiB objects: its tombstone queues 32 OIDs.
	if err := cl.DeleteVolume(context.Background(), "vm0"); err != nil {
		t.Fatal(err)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "tombstones"}); !ok || v != 1 {
		t.Fatalf("tombstones = %v (ok=%v), want 1", v, ok)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "objects"}); !ok || v != 32 {
		t.Fatalf("objects = %v (ok=%v), want 32", v, ok)
	}
	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "bytes"}); !ok || v != 128<<20 {
		t.Fatalf("bytes = %v (ok=%v), want %v", v, ok, float64(128<<20))
	}
	if v, ok := metricValue(t, mreg, src, "unistor_vacuum", map[string]string{"name": "oldest_seconds"}); !ok || v < 0 {
		t.Fatalf("oldest_seconds = %v (ok=%v), want >= 0", v, ok)
	}
}
