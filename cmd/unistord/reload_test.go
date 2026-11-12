package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/tlsconf"
)

// startReloadState builds a reload coordinator over a dir-backed meta
// store with one cluster client: the same shape main.go gives a
// non-witness daemon.
func startReloadState(t *testing.T, spec declarativeSpec) (*reloadState, meta.Store, *client.Cluster) {
	t.Helper()
	ms, err := meta.OpenDir(filepath.Join(t.TempDir(), "meta"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	cl := client.NewCluster(ms)
	t.Cleanup(func() { cl.Close() })
	r := newReloadState(nil, 0, log.New(io.Discard, "", 0))
	r.enableCluster(spec, cl, ms)
	return r, ms, cl
}

func section(t *testing.T, rep reloadReport, name string) sectionResult {
	t.Helper()
	for _, s := range rep.Sections {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("report has no %q section: %s", name, rep)
	return sectionResult{}
}

// TestReloadCrushmapPublishesOnlyOnContentChange pins the reload rule
// that makes "edit the map file, hit /v1/reload" work: a changed file
// becomes latest+1, an unchanged file is a no-op (so a periodic reload
// never floods epochs), and a broken file fails its own section without
// touching the live map.
func TestReloadCrushmapPublishesOnlyOnContentChange(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "map.txt")
	if err := os.WriteFile(mapPath, []byte(map3Hosts), 0o600); err != nil {
		t.Fatal(err)
	}
	r, ms, cl := startReloadState(t, declarativeSpec{crushmap: mapPath, pgNum: 64, replicas: 3, rule: "data"})

	// Boot semantics first: on a fresh cluster epoch 0 means 1.
	if _, _, err := applyCrushmap(ctx, cl, ms, r.spec, r.retryWriter(ctx), func(string, ...any) {}, publishBoot, nil); err != nil {
		t.Fatal(err)
	}
	rec, err := meta.LatestMap(ctx, ms)
	if err != nil || rec.Epoch != 1 {
		t.Fatalf("boot publish: epoch %v err %v", rec.Epoch, err)
	}

	// Reload with the same bytes: nothing to publish.
	rep := r.reloadAll(ctx, "test")
	s := section(t, rep, sectionCrush)
	if s.Applied || s.Error != "" || !strings.Contains(s.Detail, "unchanged") {
		t.Fatalf("unchanged map must be a no-op, got %+v", s)
	}
	if rec, _ := meta.LatestMap(ctx, ms); rec.Epoch != 1 {
		t.Fatalf("unchanged map must not bump the epoch, got %d", rec.Epoch)
	}

	// Reload with a changed file: latest+1.
	changed := strings.Replace(map3Hosts, "item osd.2 weight 1.000", "item osd.2 weight 0.500", 1)
	if err := os.WriteFile(mapPath, []byte(changed), 0o600); err != nil {
		t.Fatal(err)
	}
	rep = r.reloadAll(ctx, "test")
	s = section(t, rep, sectionCrush)
	if !s.Applied || !strings.Contains(s.Detail, "published epoch 2") {
		t.Fatalf("changed map must publish epoch 2, got %+v", s)
	}
	if rec, _ := meta.LatestMap(ctx, ms); rec.Epoch != 2 {
		t.Fatalf("latest epoch after change: %d", rec.Epoch)
	}

	// A missing file fails the section and leaves the live map alone.
	if err := os.Remove(mapPath); err != nil {
		t.Fatal(err)
	}
	rep = r.reloadAll(ctx, "test")
	s = section(t, rep, sectionCrush)
	if s.Error == "" || !strings.Contains(s.Error, "map.txt") {
		t.Fatalf("a missing map file must fail its section, got %+v", s)
	}
	if rec, _ := meta.LatestMap(ctx, ms); rec.Epoch != 2 {
		t.Fatalf("a failed publish must keep the live map, got epoch %d", rec.Epoch)
	}
	if rep.OK {
		t.Fatal("a failed section must make the report not-OK")
	}
	if v := r.view(); v.Reloads != 3 || v.LastOK {
		t.Fatalf("view after three reloads: %+v", v)
	}
}

// TestReloadQoSAndCreatesAreIdempotent covers the other two declarative
// sections: re-registering a class refreshes it (the live-change path for
// an already-open volume), and a declared volume that already exists is
// kept as is - a reload never resizes, re-places or deletes.
func TestReloadQoSAndCreatesAreIdempotent(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "map.txt")
	if err := os.WriteFile(mapPath, []byte(map3Hosts), 0o600); err != nil {
		t.Fatal(err)
	}
	spec := declarativeSpec{
		crushmap:   mapPath,
		pgNum:      64,
		replicas:   3,
		rule:       "data",
		qosClasses: []string{"gold=1000:100M:1000:100M"},
		creates:    []string{"vm0:128M"},
	}
	r, ms, cl := startReloadState(t, spec)
	writeMeta := r.retryWriter(ctx)
	logf := func(string, ...any) {}
	if _, _, err := applyCrushmap(ctx, cl, ms, r.spec, writeMeta, logf, publishBoot, nil); err != nil {
		t.Fatal(err)
	}

	rep := r.reloadAll(ctx, "test")
	if !rep.OK {
		t.Fatalf("first reload: %s", rep)
	}
	if s := section(t, rep, sectionQoS); !strings.Contains(s.Detail, "gold") {
		t.Fatalf("qos section: %+v", s)
	}
	if s := section(t, rep, sectionVolumes); !strings.Contains(s.Detail, "1 new") {
		t.Fatalf("volumes section: %+v", s)
	}
	rec, err := meta.GetQoSClass(ctx, ms, "gold")
	if err != nil || rec.ReadIOPS != 1000 {
		t.Fatalf("class record: %+v err %v", rec, err)
	}

	// An edited rate is picked up by re-registering the class.
	r.spec.qosClasses = []string{"gold=2000:100M:1000:100M"}
	rep = r.reloadAll(ctx, "test")
	if !rep.OK {
		t.Fatalf("second reload: %s", rep)
	}
	if rec, _ := meta.GetQoSClass(ctx, ms, "gold"); rec.ReadIOPS != 2000 {
		t.Fatalf("class rate not refreshed: %+v", rec)
	}

	// The declared volume now exists: the section reports it as kept and
	// leaves the record alone.
	vol, err := meta.GetVolume(ctx, ms, "vm0")
	if err != nil {
		t.Fatal(err)
	}
	rep = r.reloadAll(ctx, "test")
	if s := section(t, rep, sectionVolumes); !strings.Contains(s.Detail, "kept vm0") || strings.Contains(s.Detail, "1 new") {
		t.Fatalf("existing volume must be kept, got %+v", s)
	}
	if after, _ := meta.GetVolume(ctx, ms, "vm0"); after.Size != vol.Size || after.Replicas != vol.Replicas || after.Pool != vol.Pool {
		t.Fatalf("reload must not touch an existing volume: %+v -> %+v", vol, after)
	}
}

// TestReloadSectionsAreIndependent is the keep-last-good guarantee: one
// broken section (a crushmap file that cannot be read) must not stop the
// certificates from being re-read.
func TestReloadSectionsAreIndependent(t *testing.T) {
	ctx := context.Background()
	cert, key, ca := writeTLSMaterial(t, "node-a", time.Now().Add(time.Hour))
	planeList, err := tlsconf.ParsePlanes("data,admin")
	if err != nil {
		t.Fatal(err)
	}
	ts, err := setupTLS(tlsconf.Config{CertFile: cert, KeyFile: key, CAFile: ca}, "",
		tlsconf.NewPlaneSet(planeList...), log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	r, _, _ := startReloadState(t, declarativeSpec{crushmap: filepath.Join(t.TempDir(), "absent.txt")})
	r.tls = ts

	rep := r.reloadAll(ctx, "test")
	if s := section(t, rep, sectionCrush); s.Error == "" {
		t.Fatalf("the missing map must fail, got %+v", s)
	}
	s := section(t, rep, sectionTLS)
	if !s.Applied || s.Error != "" || !strings.Contains(s.Detail, "node-a") {
		t.Fatalf("the certificate section must still apply, got %+v", s)
	}

	// Rotate the leaf in place: the failing crushmap section must not
	// keep the old certificate in effect.
	rotated, rotatedKey, _ := writeTLSMaterial(t, "node-b", time.Now().Add(72*time.Hour))
	if err := os.WriteFile(cert, mustRead(t, rotated), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, mustRead(t, rotatedKey), 0o600); err != nil {
		t.Fatal(err)
	}
	rep = r.reloadAll(ctx, "test")
	if s := section(t, rep, sectionTLS); !strings.Contains(s.Detail, "node-b") {
		t.Fatalf("rotated leaf not picked up: %+v", s)
	}
}

// TestReloadStandaloneReportsRestartRequired pins the no-cluster shape:
// a daemon without -meta-dir reloads the certificates and the GC knob
// only, and says so.
func TestReloadStandaloneReportsRestartRequired(t *testing.T) {
	r := newReloadState(nil, 0, log.New(io.Discard, "", 0))
	rep := r.reloadAll(context.Background(), "test")
	if s := section(t, rep, sectionTLS); s.Applied || !strings.Contains(s.Detail, "-tls-cert") {
		t.Fatalf("TLS off must be reported as skipped, got %+v", s)
	}
	if len(rep.RestartRequired) == 0 {
		t.Fatal("a standalone daemon must say the declarative sections are not applied here")
	}
	if !rep.OK {
		t.Fatalf("nothing failed, the report must be OK: %s", rep)
	}
}

// TestAdminReloadEndpoint covers the HTTP surface: the bearer gate, the
// per-section report, and 500 (with the report) when a section fails.
func TestAdminReloadEndpoint(t *testing.T) {
	ms, err := meta.OpenDir(filepath.Join(t.TempDir(), "meta"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	cl := client.NewCluster(ms)
	t.Cleanup(func() { cl.Close() })
	mapPath := filepath.Join(t.TempDir(), "map.txt")
	if err := os.WriteFile(mapPath, []byte(map3Hosts), 0o644); err != nil {
		t.Fatal(err)
	}
	r := newReloadState(nil, 0, log.New(io.Discard, "", 0))
	r.enableCluster(declarativeSpec{crushmap: mapPath}, cl, ms)
	a, err := newAdminServer(ms, "127.0.0.1:0", log.New(io.Discard, "", 0), nil, newDiskRegistry(), nil, nil, nil, r, "s3cr3t", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Control: the endpoint is behind the token, like every write.
	rec := httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/reload", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/reload must be token-gated, got %d", rec.Code)
	}

	post := func() (int, reloadReport) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/reload", nil)
		req.Header.Set("Authorization", "Bearer s3cr3t")
		rec := httptest.NewRecorder()
		a.srv.Handler.ServeHTTP(rec, req)
		var rep reloadReport
		if err := json.Unmarshal(rec.Body.Bytes(), &rep); err != nil {
			t.Fatalf("reply is not a reload report: %d %s", rec.Code, rec.Body.String())
		}
		return rec.Code, rep
	}

	code, rep := post()
	if code != http.StatusOK || !rep.OK {
		t.Fatalf("reload over HTTP: %d %+v", code, rep)
	}
	if rep.Reason != "admin API" || len(rep.Sections) != 5 {
		t.Fatalf("report must list every section: %+v", rep)
	}
	if len(rep.RestartRequired) == 0 {
		t.Fatal("the report must tell the operator what a reload cannot adopt")
	}
	// /v1/status carries the reload state so healthcheck sees a failed
	// reload.
	st := getStatusAuth(t, a.srv.Handler, "s3cr3t")
	rv, ok := st["reload"].(map[string]any)
	if !ok || rv["reloads"].(float64) != 1 {
		t.Fatalf("/v1/status must report the reload: %+v", st["reload"])
	}

	// A section that fails answers 500 and still reports the rest.
	r.spec.crushmap = filepath.Join(t.TempDir(), "absent.txt")
	code, rep = post()
	if code != http.StatusInternalServerError || rep.OK {
		t.Fatalf("a failed section must answer 500, got %d %+v", code, rep)
	}
	if section(t, rep, sectionTLS).Error != "" {
		t.Fatal("the TLS section had nothing to fail")
	}
	if v := getStatusAuth(t, a.srv.Handler, "s3cr3t"); !hasReloadWarning(v["warnings"]) {
		t.Fatalf("a failed reload must warn in /v1/status: %+v", v["warnings"])
	}

	// The operator fixes the file: the same endpoint now succeeds and
	// the crushmap section reports the published epoch.
	if err := os.WriteFile(mapPath, []byte(strings.Replace(map3Hosts, "item osd.2 weight 1.000", "item osd.2 weight 0.500", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	r.spec.crushmap = mapPath
	code, rep = post()
	if code != http.StatusOK || !rep.OK {
		t.Fatalf("after the fix: %d %+v", code, rep)
	}
	if s := section(t, rep, sectionCrush); !s.Applied || !strings.Contains(s.Detail, "published epoch 2") {
		t.Fatalf("the crushmap section must publish the fixed map: %+v", s)
	}
	if s := section(t, rep, sectionVolumes); s.Applied || s.Error != "" || s.Detail == "" {
		t.Fatalf("a section with nothing to do must explain itself: %+v", s)
	}
}

// getStatusAuth reads /v1/status through the bearer gate.
func getStatusAuth(t *testing.T, h http.Handler, token string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/status: %d %s", rec.Code, rec.Body.String())
	}
	var st map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	return st
}

func hasReloadWarning(v any) bool {
	list, ok := v.([]any)
	if !ok {
		return false
	}
	for _, e := range list {
		if s, _ := e.(string); strings.Contains(s, "reload:") {
			return true
		}
	}
	return false
}

// TestReloadCrushmapRejectsBrokenMap pins the safety net a reload needs and
// boot gets for free from its placement check: a file that is not a usable
// crush map must fail the crushmap section, and it must never reach meta.
// Publishing a broken epoch is cluster-wide damage - every writer adopts it.
func TestReloadCrushmapRejectsBrokenMap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "map.txt")
	if err := os.WriteFile(mapPath, []byte(map3Hosts), 0o600); err != nil {
		t.Fatal(err)
	}
	r, ms, cl := startReloadState(t, declarativeSpec{crushmap: mapPath, pgNum: 64, replicas: 3, rule: "data"})
	if _, _, err := applyCrushmap(ctx, cl, ms, r.spec, r.retryWriter(ctx), func(string, ...any) {}, publishBoot, nil); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{
		"this is not a crush map at all\n",
		"crushmap { }\n", // decodes, but has no devices and no rules
	} {
		if err := os.WriteFile(mapPath, []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		rep := r.reloadAll(ctx, "test")
		s := section(t, rep, sectionCrush)
		if s.Error == "" {
			t.Fatalf("a broken map must fail its section, got %+v", s)
		}
		if rec, err := meta.LatestMap(ctx, ms); err != nil || rec.Epoch != 1 {
			t.Fatalf("a broken map must not be published (epoch %v err %v)", rec.Epoch, err)
		}
	}

	// The operator fixes the file and reloads again: the same section
	// recovers, and the good map lands on the next epoch.
	if err := os.WriteFile(mapPath, []byte(strings.Replace(map3Hosts, "item osd.2 weight 1.000", "item osd.2 weight 0.500", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	rep := r.reloadAll(ctx, "test")
	if s := section(t, rep, sectionCrush); !s.Applied || !strings.Contains(s.Detail, "published epoch 2") {
		t.Fatalf("the fixed map must publish epoch 2, got %+v", s)
	}
}

// TestReloadCrushmapCompilesAgainstRegistry covers the second half of the
// reload gate: a map can be structurally valid and still be useless for
// this cluster (an unknown rule, a pg width the map cannot fill). That is
// exactly what the boot placement check catches, so reload compiles the
// candidate against the live OSD registry before publishing it.
func TestReloadCrushmapCompilesAgainstRegistry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	mapPath := filepath.Join(dir, "map.txt")
	if err := os.WriteFile(mapPath, []byte(map3Hosts), 0o600); err != nil {
		t.Fatal(err)
	}
	r, ms, _ := startReloadState(t, declarativeSpec{crushmap: mapPath, pgNum: 64, replicas: 3, rule: "data"})
	for id := int32(0); id < 3; id++ {
		if _, err := meta.PutOSD(ctx, ms, meta.OSDInfo{ID: id, Addr: fmt.Sprintf("127.0.0.1:%d", 9000+id), UUID: fmt.Sprintf("u%d", id), Weight: 1}); err != nil {
			t.Fatal(err)
		}
	}
	rep := r.reloadAll(ctx, "test") // boot-style first reload publishes epoch 1
	if s := section(t, rep, sectionCrush); !s.Applied {
		t.Fatalf("first reload must publish the map: %+v (%s)", s, rep)
	}

	// Structurally fine, but the daemon asks for a rule the map lacks.
	if err := os.WriteFile(mapPath, []byte(strings.Replace(map3Hosts, "rule data {", "rule other {", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	rep = r.reloadAll(ctx, "test")
	s := section(t, rep, sectionCrush)
	if s.Error == "" || !strings.Contains(s.Error, `rule "data"`) {
		t.Fatalf("a map the cluster cannot use must fail with the rule name, got %+v", s)
	}
	if rec, _ := meta.LatestMap(ctx, ms); rec.Epoch != 1 {
		t.Fatalf("the unusable map must not be published, got epoch %d", rec.Epoch)
	}
}
