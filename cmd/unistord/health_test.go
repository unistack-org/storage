package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/unistack-org/storage/pkg/meta"
)

// healthReply runs GET /healthcheck against the admin handler and
// decodes the verdict envelope.
func healthReply(t *testing.T, h http.Handler) (int, string, map[string]string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/healthcheck", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var body struct {
		Status string `json:"status"`
		Checks []struct {
			Name    string `json:"name"`
			State   string `json:"state"`
			Message string `json:"message"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthcheck json: %v\n%s", err, rec.Body.String())
	}
	states := map[string]string{}
	for _, c := range body.Checks {
		states[c.Name] = c.State
	}
	return rec.Code, body.Status, states
}

func TestHealthcheckHealthy(t *testing.T) {
	a, _, _ := startStatusCluster(t)
	code, status, states := healthReply(t, a.srv.Handler)
	if code != http.StatusOK || status != checkOK {
		t.Fatalf("want 200/ok, got %d/%s", code, status)
	}
	for _, name := range []string{"meta", "raft", "map", "osds", "volumes"} {
		if states[name] != checkOK {
			t.Fatalf("check %s: state %q, want ok (all: %v)", name, states[name], states)
		}
	}
}

func TestHealthcheckFlagsDownOSD(t *testing.T) {
	a, _, daems := startStatusCluster(t)
	if err := daems[2].Close(); err != nil {
		t.Fatal(err)
	}
	code, status, states := healthReply(t, a.srv.Handler)
	if code != http.StatusServiceUnavailable || status != checkFail {
		t.Fatalf("want 503/fail with a down OSD, got %d/%s", code, status)
	}
	if states["osds"] != checkFail {
		t.Fatalf("osds check: %q, want fail", states["osds"])
	}
	if states["meta"] != checkOK {
		t.Fatalf("meta check should stay ok: %q", states["meta"])
	}
}

func TestHealthcheckEvictedIsWarn(t *testing.T) {
	a, cl, _ := startStatusCluster(t)
	ctx := context.Background()
	// Evict osd 2: placement drops it but 2 live hosts still cover
	// min_size 2 -> advisory warn, not a failure.
	if err := meta.SetOSDWeight(ctx, a.ms, 2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	code, status, states := healthReply(t, a.srv.Handler)
	if code != http.StatusOK {
		t.Fatalf("eviction should answer 200, got %d (states %v)", code, states)
	}
	if status != checkWarn {
		t.Fatalf("status: %q, want warn", status)
	}
	if states["osds"] != checkWarn {
		t.Fatalf("osds check: %q, want warn", states["osds"])
	}
}

// TestHealthcheckPassesTokenGate: /healthcheck is a probe endpoint and
// must answer without the bearer token, like /healthz.
func TestHealthcheckPassesTokenGate(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.OpenDir(dir + "/meta")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	a, err := newAdminServer(ms, "127.0.0.1:0", log.New(io.Discard, "", 0), nil, newDiskRegistry(), nil, nil, "s3cr3t", nil)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/healthcheck", nil)
	rec := httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusUnauthorized {
		t.Fatalf("healthcheck must not require the token: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"checks"`) {
		t.Fatalf("expected a verdict body, got %d %s", rec.Code, rec.Body.String())
	}
	// Control: a gated route in the same server still demands it.
	req = httptest.NewRequest(http.MethodGet, "/v1/volumes", nil)
	rec = httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/volumes should be gated, got %d", rec.Code)
	}
}

func TestWorstState(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{nil, checkOK},
		{[]string{checkOK, checkOK}, checkOK},
		{[]string{checkOK, checkWarn}, checkWarn},
		{[]string{checkWarn, checkFail}, checkFail},
		{[]string{checkFail, checkWarn}, checkFail},
	}
	for _, c := range cases {
		if got := worstState(c.in...); got != c.want {
			t.Errorf("worstState(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
