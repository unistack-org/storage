package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------- address policy ----------

func TestSSRFBlockedClasses(t *testing.T) {
	g := newSSRFGuard(false, nil)
	gp := newSSRFGuard(true, nil)
	cases := []struct {
		ip      string
		blocked bool // with the default (strict) policy
	}{
		{ip: "0.0.0.0", blocked: true},

		{ip: "::", blocked: true},
		{ip: "127.0.0.1", blocked: true},
		{ip: "::1", blocked: true},
		{ip: "169.254.169.254", blocked: true}, // cloud metadata
		{ip: "fe80::1", blocked: true},
		{ip: "100.64.1.2", blocked: true}, // CGNAT / Tailscale
		{ip: "100.127.255.255", blocked: true},
		{ip: "10.0.0.5", blocked: true},
		{ip: "172.16.0.9", blocked: true},
		{ip: "192.168.1.1", blocked: true},
		{ip: "fd00::11", blocked: true},
		{ip: "8.8.8.8", blocked: false},
		{ip: "93.184.216.34", blocked: false},
		{ip: "2001:4860:4860::8888", blocked: false},
	}
	for _, c := range cases {
		_, blocked := g.blocked(net.ParseIP(c.ip))
		if blocked != c.blocked {
			t.Errorf("strict: %s blocked=%v, want %v", c.ip, blocked, c.blocked)
		}
		// CGNAT stays blocked under -import-allow-private: an overlay
		// address is never "a mirror on the storage net".
		_, blocked = gp.blocked(net.ParseIP(c.ip))
		wantEscape := c.blocked && strings.Contains(c.ip, "100.")
		if wantEscape && !blocked {
			t.Errorf("escape hatch: %s must stay blocked", c.ip)
		}
		if c.ip == "127.0.0.1" {
			// The escape hatch exists for a private mirror net, not for
			// the daemon's own localhost services: loopback stays
			// refused (it is checked before the allowPrivate case).
			if !blocked {
				t.Errorf("escape hatch: loopback must stay blocked")
			}
		}
	}
}

func TestSSRFDialRefusesDangerousTargets(t *testing.T) {
	g := newSSRFGuard(false, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, addr := range []string{
		"127.0.0.1:80", "[::1]:80", "169.254.169.254:80",
		"10.0.0.5:443", "100.64.0.1:80", "localhost:80", "redis:6379",
	} {
		start := time.Now()
		if _, err := g.DialContext(ctx, "tcp", addr); err == nil {
			t.Errorf("dial %s: accepted, want refusal", addr)
		} else if time.Since(start) > 2*time.Second {
			t.Errorf("dial %s: refused only after %s (policy check must be instant)", addr, time.Since(start))
		}
	}
	// The escape hatch admits the private ranges, and nothing above
	// them: loopback, the metadata address and the overlays stay
	// refused; bare hostnames stay refused.
	gp := newSSRFGuard(true, nil)
	for addr, wantReason := range map[string]string{
		"127.0.0.1:9":        "loopback",
		"169.254.169.254:80": "link-local",
		"100.64.0.1:80":      "carrier-grade",
		"redis:6379":         "bare hostnames",
	} {
		if _, err := gp.DialContext(ctx, "tcp", addr); err == nil || !strings.Contains(err.Error(), wantReason) {
			t.Errorf("allowPrivate: dial %s = %v, want refusal naming %q", addr, err, wantReason)
		}
	}
	// 10.0.0.5 passes the policy: blocked() must not name it, and the
	// dial proceeds to the network layer (it fails on connectivity,
	// not on the guard's verdict).
	if reason, blocked := gp.blocked(net.ParseIP("10.0.0.5")); blocked {
		t.Errorf("allowPrivate: 10.0.0.5 blocked: %s", reason)
	}
	if _, err := gp.DialContext(ctx, "tcp", "10.0.0.5:9"); err != nil && strings.Contains(err.Error(), "refused: private") {
		t.Errorf("allowPrivate: private dial refused by policy: %v", err)
	}
}

func TestSSRFRedirectCap(t *testing.T) {
	g := newSSRFGuard(false, nil)
	mk := func(u string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, u, nil)
		return r
	}
	via := make([]*http.Request, 5)
	for i := range via {
		via[i] = mk("https://a.example/")
	}
	if err := g.checkRedirect(mk("https://b.example/"), via); err == nil {
		t.Errorf("6th hop accepted")
	}
	if err := g.checkRedirect(mk("https://b.example/"), via[:4]); err != nil {
		t.Errorf("5th hop refused: %v", err)
	}
	if err := g.checkRedirect(mk("gopher://b.example/"), via[:1]); err == nil {
		t.Errorf("scheme downgrade accepted")
	}
}

// ---------- the guard on the live import path ----------

// rawAdmin drives the admin handler and returns the status plus the raw
// body (jsonError answers errors as plain text, which doAdminBody drops).
func rawAdmin(t *testing.T, h http.Handler, method, target, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

// allowAllPolicy stands in for -import-allow-private on a net whose
// mirror is reachable: the e2e tests fetch from httptest (loopback),
// which the strict policy refuses by design.
func allowAllPolicy(net.IP) (string, bool) { return "", false }

// TestImportGuardBlocksInternalURL wires the guard exactly as main()
// does and drives the live import path: a URL on a blocked address must
// fail the job naming the policy reason, and the origin must never see
// a connection.
func TestImportGuardBlocksInternalURL(t *testing.T) {
	env := startVacuumEnv(t)
	var hit int
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit++
		w.Header().Set("Content-Length", "1024")
		w.Write(bytes.Repeat([]byte{0xAB}, 1024))
	}))
	defer origin.Close()

	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "iso-vol", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}

	guard := newSSRFGuard(false, log.New(io.Discard, "", 0))
	env.admin.importHTTP = &http.Client{Transport: guard.importTransport(), CheckRedirect: guard.checkRedirect, Timeout: 0}

	body, _ := json.Marshal(map[string]any{"url": origin.URL + "/img.iso", "async": false})
	code, msg := rawAdmin(t, h, http.MethodPost, "/v1/volumes/iso-vol/import", string(body))
	if code != http.StatusBadGateway {
		t.Fatalf("import of a loopback URL: %d %s, want 502", code, msg)
	}
	if !strings.Contains(msg, "loopback") {
		t.Fatalf("import error %q must name the policy reason", msg)
	}
	if hit != 0 {
		t.Fatalf("blocked import reached the origin (%d hits)", hit)
	}

	// The probe endpoint is served by the same client: it must refuse
	// the same address (an unprobed size must not be reported).
	pbody, _ := json.Marshal(map[string]any{"url": origin.URL + "/img.iso"})
	pcode, pmsg := rawAdmin(t, h, http.MethodPost, "/v1/volumes/probe", string(pbody))
	if pcode == http.StatusOK {
		t.Fatalf("probe of a loopback URL accepted: %s", pmsg)
	}
	if !strings.Contains(pmsg, "loopback") {
		t.Fatalf("probe error %q must name the policy reason", pmsg)
	}

	// With the policy admitting the address (the escape hatch on a
	// mirror net), the very same import lands the image.
	guard.policy = allowAllPolicy
	code, msg = rawAdmin(t, h, http.MethodPost, "/v1/volumes/iso-vol/import", string(body))
	if code != http.StatusOK {
		t.Fatalf("import with the address admitted: %d %s, want 200", code, msg)
	}
	if hit == 0 {
		t.Fatalf("admitted import never reached the origin")
	}
	vol, err := env.cl.OpenVolume(ctx, "iso-vol")
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()
	buf := make([]byte, 1024)
	n, err := vol.ReadAt(ctx, buf, 0)
	if err != nil || n != 1024 {
		t.Fatalf("read back: %d %v", n, err)
	}
	for i := range buf {
		if buf[i] != 0xAB {
			t.Fatalf("data mismatch at %d", i)
		}
	}
}

// TestImportGuardRefusesLiveListener proves the dial-time check is the
// real connection gate: a blocked address with a listening server
// never receives a connection.
func TestImportGuardRefusesLiveListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan struct{}, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			c.Close()
		}
	}()
	g := newSSRFGuard(false, log.New(io.Discard, "", 0))
	if _, err := g.DialContext(context.Background(), "tcp", ln.Addr().String()); err == nil {
		t.Fatalf("dial to a live loopback listener accepted")
	}
	select {
	case <-accepted:
		t.Fatalf("guard opened a connection to a blocked listener")
	case <-time.After(50 * time.Millisecond):
	}
}

// ---------- size cap ----------

func TestImportSizeCap(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "cap-vol", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}

	serve := func(n int) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(w, io.LimitReader(bytes.NewReader(bytes.Repeat([]byte{7}, n)), int64(n)))
		}))
	}

	doImport := func(url string) (int, string) {
		b, _ := json.Marshal(map[string]any{"url": url, "async": false})
		return rawAdmin(t, h, http.MethodPost, "/v1/volumes/cap-vol/import", string(b))
	}

	big := serve(3 << 20)
	defer big.Close()
	small := serve(1 << 20)
	defer small.Close()
	guard := newSSRFGuard(false, log.New(io.Discard, "", 0))
	guard.policy = allowAllPolicy // the cap, not the address policy, is under test
	env.admin.importHTTP = &http.Client{Transport: guard.importTransport(), CheckRedirect: guard.checkRedirect, Timeout: 0}

	// Unlimited: the 3 MiB source lands whole.
	env.admin.importMaxSize = 0
	if code, msg := doImport(big.URL); code != http.StatusOK {
		t.Fatalf("uncapped import: %d %s", code, msg)
	}

	// Cap of 2 MiB: the same source fails the job...
	env.admin.importMaxSize = 2 << 20
	code, msg := doImport(big.URL)
	if code != http.StatusBadGateway {
		t.Fatalf("capped import: %d %s, want 502", code, msg)
	}
	if !strings.Contains(msg, "import size limit") {
		t.Fatalf("cap error %q must name the limit", msg)
	}
	// ...while the source under the cap lands fine.
	if code, msg := doImport(small.URL); code != http.StatusOK {
		t.Fatalf("under-cap import: %d %s", code, msg)
	}

	// The body-push endpoint obeys the same cap.
	push := func(n int) (int, string) {
		req := httptest.NewRequest(http.MethodPut, "/v1/volumes/cap-vol/import", bytes.NewReader(bytes.Repeat([]byte{9}, n)))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	if code, msg := push(4 << 20); code == http.StatusOK {
		t.Fatalf("body push over cap accepted: %s", msg)
	}
	if code, msg := push(1 << 20); code != http.StatusOK {
		t.Fatalf("body push under cap: %d %s", code, msg)
	}
}
