package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// doAdminRaw is doAdminBody for endpoints whose success body is a JSON
// array (pool listing): it returns the status and the raw body.
func doAdminRaw(t *testing.T, h http.Handler, method, target, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// TestAdminPools walks the pool HTTP surface: create (with cluster
// default fill), duplicate 409, bad name 400, a volume created into the
// pool inheriting its policy, the delete guard, and the listings.
func TestAdminPools(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler

	// POST -> 201 with the resolved record (omitted fields filled from
	// the compiled cluster defaults).
	code, body := doAdminBody(t, h, http.MethodPost, "/v1/pools", `{"name":"gold","replicas":3,"min_size":2,"pg_num":64}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /v1/pools: %d (%v)", code, body)
	}
	if body["name"] != "gold" || body["pg_num"] != float64(64) || body["rule"] == "" {
		t.Fatalf("POST /v1/pools body: %v", body)
	}

	// Duplicate: 409. Bad name: 400.
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/pools", `{"name":"gold","replicas":3,"min_size":2,"pg_num":64}`); code != http.StatusConflict {
		t.Fatalf("POST duplicate pool: %d, want 409", code)
	}
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/pools", `{"name":"bad/name","replicas":3,"min_size":2,"pg_num":64}`); code != http.StatusBadRequest {
		t.Fatalf("POST bad pool name: %d, want 400", code)
	}

	// A volume created into the pool carries the pool's policy.
	code, vol := doAdminBody(t, h, http.MethodPost, "/v1/volumes", `{"name":"vm0","size":"1G","pool":"gold"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /v1/volumes with pool: %d (%v)", code, vol)
	}
	if vol["pool"] != "gold" || vol["pg_num"] != float64(64) || vol["replicas"] != float64(3) {
		t.Fatalf("volume did not inherit the pool policy: %v", vol)
	}
	// Volume into a missing pool: 404.
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes", `{"name":"vm1","size":"1G","pool":"nope"}`); code != http.StatusNotFound {
		t.Fatalf("POST volume in missing pool: %d, want 404", code)
	}

	// GET by name and the bare-array listing.
	if code, body = doAdminBody(t, h, http.MethodGet, "/v1/pools/gold", ""); code != http.StatusOK || body["name"] != "gold" {
		t.Fatalf("GET /v1/pools/gold: %d (%v)", code, body)
	}
	code, raw := doAdminRaw(t, h, http.MethodGet, "/v1/pools", "")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/pools: %d (%s)", code, raw)
	}
	var pools []map[string]any
	if err := json.Unmarshal([]byte(raw), &pools); err != nil {
		t.Fatalf("GET /v1/pools json: %v\n%s", err, raw)
	}
	if len(pools) != 1 || pools[0]["name"] != "gold" {
		t.Fatalf("GET /v1/pools: want [gold], got %s", raw)
	}

	// Deleting a pool that has volumes: 409; once the volume is gone: 204.
	if code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/pools/gold", ""); code != http.StatusConflict {
		t.Fatalf("DELETE pool in use: %d, want 409", code)
	}
	if code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/volumes/vm0", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE volume: %d, want 204", code)
	}
	if code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/pools/gold", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE pool: %d, want 204", code)
	}
	if code, _ = doAdminBody(t, h, http.MethodGet, "/v1/pools/gold", ""); code != http.StatusNotFound {
		t.Fatalf("GET deleted pool: %d, want 404", code)
	}
}
