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
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/osd"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// vacuumEnv is the same three-OSD harness as startStatusCluster, plus
// the data directory and meta store (the vacuum/recovery tests inspect
// object files on disk and restart daemons in place).
type vacuumEnv struct {
	admin *adminAPI
	cl    *client.Cluster
	daems []*osd.Daemon
	dir   string
	ms    meta.Store
}

func startVacuumEnv(t *testing.T) *vacuumEnv {
	t.Helper()
	dir := t.TempDir()
	ms, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	env := &vacuumEnv{dir: dir, ms: ms}
	for i := 0; i < 3; i++ {
		d, err := osd.New(osd.Options{
			ID:      int32(i),
			Addr:    "127.0.0.1:0",
			DataDir: filepath.Join(dir, fmt.Sprintf("osd%d", i)),
			Meta:    ms,
		})
		if err != nil {
			t.Fatal(err)
		}
		env.daems = append(env.daems, d)
		go d.Run(ctx)
		deadline := time.Now().Add(5 * time.Second)
		for {
			infos, err := meta.ListOSDs(ctx, ms)
			if err == nil && len(infos) >= i+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("osd %d did not register: %v", i, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cl := client.NewCluster(ms)
	t.Cleanup(func() { cl.Close() })
	env.cl = cl
	if err := cl.MapEpoch(ctx, 1, []byte(map3Hosts)); err != nil {
		t.Fatal(err)
	}
	// Pull the epoch into the client: restarted OSDs fence requests
	// that carry an older map epoch, and only a Refresh updates it.
	if _, err := cl.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := newAdminServer(ms, "127.0.0.1:0", log.New(io.Discard, "", 0), nil, newDiskRegistry())
	if err != nil {
		t.Fatal(err)
	}
	env.admin = a
	return env
}

// restartOSDInPlace stops OSD id, deletes nothing, relaunches the daemon
// on the same address (a crashed-and-restarted node). The old daemon is
// closed first so its listener is free and no pooled connection keeps
// serving a deleted file.
func (e *vacuumEnv) restartOSDInPlace(t *testing.T, id int32) {
	t.Helper()
	addr := e.daems[id].Addr()
	if err := e.daems[id].Close(); err != nil {
		t.Fatalf("close osd %d: %v", id, err)
	}
	d, err := osd.New(osd.Options{
		ID: id, Addr: addr,
		DataDir: filepath.Join(e.dir, fmt.Sprintf("osd%d", id)),
		Meta:    e.ms,
	})
	if err != nil {
		t.Fatalf("osd %d restart: %v", id, err)
	}
	runErr := make(chan error, 1)
	rctx, rcancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		rcancel()
		select {
		case <-runErr:
		case <-time.After(5 * time.Second):
		}
	})
	go func() { runErr <- d.Run(rctx) }()
	pctx, pcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pcancel()
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-runErr:
			t.Fatalf("osd %d restart died: %v", id, err)
		default:
		}
		// The replacement daemon is ready only when it completes the
		// SDS handshake (a plain TCP connect succeeds while the
		// listener merely queues the connection in the backlog).
		// Pooled client connections redial on their own: a transport
		// error poisons them.
		cc, err := sdsnet.Dial(addr, "vol", time.Second)
		if err == nil {
			err = cc.Ping(pctx)
			cc.Close()
		}
		if err == nil {
			e.daems[id] = d
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("osd %d did not come back on %s: %v", id, addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func objFile(dir string, osdID int32, oid uint64) string {
	return filepath.Join(dir, fmt.Sprintf("osd%d", osdID), fmt.Sprintf("%016x", oid))
}

// doAdminJSON runs one admin request and decodes the JSON reply.
func doAdminJSON(t *testing.T, h http.Handler, method, target string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var v map[string]any
	body := rec.Body.String()
	if body != "" && rec.Code < 400 {
		if err := json.Unmarshal([]byte(body), &v); err != nil {
			t.Fatalf("%s %s: json: %v\n%s", method, target, err, body)
		}
	}
	return rec.Code, v
}

func TestAdminVacuumEndpoints(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler

	code, v := doAdminJSON(t, h, http.MethodGet, "/v1/vacuum")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/vacuum: %d", code)
	}
	if p, _ := v["pending"].([]any); len(p) != 0 {
		t.Fatalf("pending at start: %+v", v)
	}

	if _, err := env.cl.CreateVolume(context.Background(), "gone", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	code, _ = doAdminJSON(t, h, http.MethodDelete, "/v1/volumes/gone")
	if code != http.StatusNoContent {
		t.Fatalf("DELETE gone: %d", code)
	}
	code, v = doAdminJSON(t, h, http.MethodGet, "/v1/vacuum")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/vacuum: %d", code)
	}
	p, _ := v["pending"].([]any)
	if len(p) != 1 {
		t.Fatalf("pending after delete: %+v", v)
	}
	if rec := p[0].(map[string]any); rec["name"] != "gone" {
		t.Fatalf("pending record: %+v", rec)
	}

	code, v = doAdminJSON(t, h, http.MethodPost, "/v1/vacuum")
	if code != http.StatusOK {
		t.Fatalf("POST /v1/vacuum: %d", code)
	}
	if v["vacuums"] != float64(1) || v["deleted"] != float64(1) {
		t.Fatalf("vacuum result: %+v", v)
	}
	if _, v = doAdminJSON(t, h, http.MethodGet, "/v1/vacuum"); true {
		if p, _ := v["pending"].([]any); len(p) != 0 {
			t.Fatalf("pending after vacuum: %+v", v)
		}
	}
	// Idempotent second pass: no tombstones, no work.
	if code, v = doAdminJSON(t, h, http.MethodPost, "/v1/vacuum"); code != http.StatusOK || v["vacuums"] != float64(0) {
		t.Fatalf("second pass: %d %+v", code, v)
	}
}

// TestAdminVacuumVolumeEndpoint proves POST /v1/vacuum/{name}: a live
// volume is vacuumed in live mode (its all-zero object is punched, its
// data object survives), a deleted volume is vacuumed by tombstone,
// and an unknown name is 404.
func TestAdminVacuumVolumeEndpoint(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := env.cl.CreateVolume(ctx, "vm", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	v, err := env.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, object.Size)
	for i := range data {
		data[i] = byte(i ^ 0x5a)
	}
	if _, err := v.WriteAt(ctx, data, 0); err != nil { // obj0: real data
		t.Fatal(err)
	}
	if _, err := v.WriteAt(ctx, make([]byte, object.Size), object.Size); err != nil { // obj1: zeroes
		t.Fatal(err)
	}
	// Wait for the zero object to replicate to every OSD so the
	// vacuum pass sees it everywhere (stragglers are async).
	for id := int32(0); id < 3; id++ {
		oid := uint64(1) + 1 // OIDBase 1 (first volume), index 1, +1
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(objFile(env.dir, id, oid)); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("osd %d zero-object oid %x never appeared", id, oid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	v.Close()

	code, res := doAdminJSON(t, h, http.MethodPost, "/v1/vacuum/vm")
	if code != http.StatusOK {
		t.Fatalf("POST /v1/vacuum/vm: %d", code)
	}
	if res["mode"] != "live" {
		t.Fatalf("live mode expected: %+v", res)
	}
	if res["zeroed"].(float64) < 1 || res["freed"].(float64) < float64(object.Size) {
		t.Fatalf("live vacuum punched nothing: %+v", res)
	}

	// A deleted volume answers in tombstone mode.
	if _, err := env.cl.CreateVolume(ctx, "gone", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	if err := env.cl.DeleteVolume(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	code, res = doAdminJSON(t, h, http.MethodPost, "/v1/vacuum/gone")
	if code != http.StatusOK {
		t.Fatalf("POST /v1/vacuum/gone: %d", code)
	}
	if res["mode"] != "tombstone" || res["deleted"].(float64) != 1 {
		t.Fatalf("tombstone vacuum: %+v", res)
	}

	// Unknown name: nothing to vacuum.
	if code, _ = doAdminJSON(t, h, http.MethodPost, "/v1/vacuum/ghost"); code != http.StatusNotFound {
		t.Fatalf("POST /v1/vacuum/ghost: %d, want 404", code)
	}
}

// TestAdminRecoveryEndpoints proves plan M6.3 over HTTP: the scan
// reports a volume with objects missing on one replica, the per-volume
// check lists the copies to make, and the repair pass rewrites them
// byte-exactly; afterwards the scan is green again.
func TestAdminRecoveryEndpoints(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	spec, err := env.cl.CreateVolume(ctx, "rec", 8<<20, 0, 3, 0, 64, "data")
	if err != nil {
		t.Fatal(err)
	}
	v, err := env.cl.OpenVolume(ctx, "rec")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, object.Size)
	for i := range buf {
		buf[i] = byte(i)
	}
	const nobjs = 2
	for idx := 0; idx < nobjs; idx++ {
		if _, err := v.WriteAt(ctx, buf, uint64(idx)*object.Size); err != nil {
			t.Fatal(err)
		}
		for id := int32(0); id < 3; id++ {
			oid := spec.OIDBase + uint64(idx) + 1
			deadline := time.Now().Add(10 * time.Second)
			for {
				if _, err := os.Stat(objFile(env.dir, id, oid)); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("osd %d oid %x never appeared", id, oid)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	v.Close()

	// Healthy before the sabotage.
	code, sv := doAdminJSON(t, h, http.MethodGet, "/v1/recovery")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/recovery: %d", code)
	}
	if sv["healthy"] != true {
		t.Fatalf("expected healthy scan, got %+v", sv)
	}

	// Crash osd 2: stop it, delete its objects, restart it in place.
	const sabotaged int32 = 2
	env.restartOSDInPlace(t, sabotaged)
	for idx := 0; idx < nobjs; idx++ {
		p := objFile(env.dir, sabotaged, spec.OIDBase+uint64(idx)+1)
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	env.restartOSDInPlace(t, sabotaged)

	// Scan: the volume is unhealthy with nobjs repairs pending.
	code, sv = doAdminJSON(t, h, http.MethodGet, "/v1/recovery")
	if code != http.StatusOK {
		t.Fatalf("scan: %d", code)
	}
	if sv["healthy"] != false {
		t.Fatalf("expected unhealthy scan: %+v", sv)
	}
	var rec map[string]any
	for _, x := range sv["volumes"].([]any) {
		m := x.(map[string]any)
		if m["volume"] == "rec" {
			rec = m
		}
	}
	if rec == nil || rec["to_repair"] != float64(nobjs) {
		t.Fatalf("rec row: %+v", sv)
	}

	// Per-volume check: full item list, every copy targets osd 2.
	code, cv := doAdminJSON(t, h, http.MethodGet, "/v1/recovery/rec")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/recovery/rec: %d", code)
	}
	items, _ := cv["items"].([]any)
	if len(items) != nobjs {
		t.Fatalf("items: %+v", cv)
	}
	for _, it := range items {
		m := it.(map[string]any)
		if m["osd"] != float64(2) || m["source"] == float64(2) {
			t.Fatalf("bad item: %+v", m)
		}
	}

	// Repair pass (unthrottled: bps=0, iops high).
	code, rv := doAdminJSON(t, h, http.MethodPost, "/v1/recovery/rec?iops=1000&bps=0")
	if code != http.StatusOK {
		t.Fatalf("POST repair: %d", code)
	}
	if rv["copied"] != float64(nobjs) || rv["failed"] != float64(0) {
		t.Fatalf("repair stats: %+v", rv)
	}
	for idx := 0; idx < nobjs; idx++ {
		b, err := os.ReadFile(objFile(env.dir, sabotaged, spec.OIDBase+uint64(idx)+1))
		if err != nil {
			t.Fatalf("repaired object missing: %v", err)
		}
		if len(b) != int(object.Size) || b[0] != 0 || b[1] != 1 {
			t.Fatalf("repaired object content wrong: len=%d", len(b))
		}
	}

	// Scan is green again.
	if code, sv = doAdminJSON(t, h, http.MethodGet, "/v1/recovery"); code != http.StatusOK || sv["healthy"] != true {
		t.Fatalf("final scan: %d %+v", code, sv)
	}
}
