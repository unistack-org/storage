package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// doAdminBody is doAdminJSON with a request body (rollback posts
// {"gen":N}).
func doAdminBody(t *testing.T, h http.Handler, method, target, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var v map[string]any
	if out := rec.Body.String(); out != "" && rec.Code < 400 {
		if err := json.Unmarshal([]byte(out), &v); err != nil {
			t.Fatalf("%s %s: json: %v\n%s", method, target, err, out)
		}
	}
	return rec.Code, v
}

// TestAdminSnapshotEndpoints walks the M3 HTTP surface end to end:
// create -> list -> rollback (404 on a dropped gen, 404 on an unknown
// snapshot rm) and proves the rollback restored the image through the
// data plane.
func TestAdminSnapshotEndpoints(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := env.cl.CreateVolume(ctx, "vm", 16<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	v, err := env.cl.OpenVolume(ctx, "vm")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()

	data := make([]byte, object.Size)
	for i := range data {
		data[i] = byte(i ^ 0xa5)
	}
	if _, err := v.WriteAt(ctx, data, 0); err != nil {
		t.Fatal(err)
	}

	// POST snapshot -> 201 with gen 1.
	code, sp := doAdminJSON(t, h, http.MethodPost, "/v1/volumes/vm/snapshot")
	if code != http.StatusCreated {
		t.Fatalf("POST snapshot: %d", code)
	}
	if sp["gen"] != float64(1) || sp["objects"] != float64(1) {
		t.Fatalf("snapshot view: %+v", sp)
	}

	// Overwrite after the snapshot: the live volume diverges.
	data2 := make([]byte, object.Size)
	for i := range data2 {
		data2[i] = 0x77
	}
	if _, err := v.WriteAt(ctx, data2, 0); err != nil {
		t.Fatal(err)
	}

	// GET snapshots -> one entry.
	code, lst := doAdminJSON(t, h, http.MethodGet, "/v1/volumes/vm/snapshots")
	if code != http.StatusOK {
		t.Fatalf("GET snapshots: %d", code)
	}
	arr, _ := lst["snapshots"].([]any)
	if len(arr) != 1 {
		t.Fatalf("snapshots list: %+v", lst)
	}

	// POST rollback to gen 1 -> 200, live data restored.
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/vm/rollback", `{"gen":1}`); code != http.StatusOK {
		t.Fatalf("POST rollback: %d", code)
	}
	got := make([]byte, object.Size)
	if _, err := v.ReadAt(ctx, got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("rollback through HTTP did not restore the image")
	}

	// The rolled-back generation is gone: rollback 404s, rm 404s.
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/vm/rollback", `{"gen":1}`); code != http.StatusNotFound {
		t.Fatalf("rollback to dropped gen: %d, want 404", code)
	}
	if code, _ = doAdminJSON(t, h, http.MethodDelete, "/v1/volumes/vm/snapshot/1"); code != http.StatusNotFound {
		t.Fatalf("rm dropped snapshot: %d, want 404", code)
	}
	// Unknown volume: 404.
	if code, _ = doAdminJSON(t, h, http.MethodPost, "/v1/volumes/nope/snapshot"); code != http.StatusNotFound {
		t.Fatalf("snapshot on unknown volume: %d, want 404", code)
	}

	// Create + rm round trip. Only gen 1 ever existed (the rollback to
	// it discarded nothing newer), so the next snapshot is numbered 2:
	// above every generation that ever existed (monotonic ids).
	code, sp = doAdminJSON(t, h, http.MethodPost, "/v1/volumes/vm/snapshot")
	if code != http.StatusCreated {
		t.Fatalf("POST snapshot after rollback: %d", code)
	}
	gen := uint32(sp["gen"].(float64))
	if gen != 2 {
		t.Fatalf("gen after rollback = %d, want 2 (monotonic)", gen)
	}
	if code, _ = doAdminJSON(t, h, http.MethodDelete, "/v1/volumes/vm/snapshot/2"); code != http.StatusOK {
		t.Fatalf("DELETE snapshot: %d", code)
	}
	if _, lst = doAdminJSON(t, h, http.MethodGet, "/v1/volumes/vm/snapshots"); true {
		if arr, _ := lst["snapshots"].([]any); len(arr) != 0 {
			t.Fatalf("snapshots after rm: %+v", lst)
		}
	}
	// Deleting a snapshot sweeps its frozen range eagerly (the client
	// reuses the tombstone engine inline), so nothing is left pending.
	if code, v := doAdminJSON(t, h, http.MethodGet, "/v1/vacuum"); code != http.StatusOK {
		t.Fatalf("GET /v1/vacuum: %d", code)
	} else if p, _ := v["pending"].([]any); len(p) != 0 {
		t.Fatalf("pending vacuum after snapshot rm: %+v", v)
	}
}
