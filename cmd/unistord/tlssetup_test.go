package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/tlsconf"
)

// writeTLSMaterial writes a CA and one leaf (valid for cn) into a temp
// directory and returns the paths, so a test can rotate the leaf in
// place afterwards.
func writeTLSMaterial(t *testing.T, cn string, notAfter time.Time) (cert, key, ca string) {
	t.Helper()
	dir := t.TempDir()
	sign := func(tmpl, parent *x509.Certificate, pub any, caKey *ecdsa.PrivateKey) []byte {
		t.Helper()
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return der
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "unistord-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER := sign(caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
	}
	leafDER := sign(leafTmpl, caCert, &leafKey.PublicKey, caKey)
	kder, err := x509.MarshalECPrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, typ string, b []byte) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	return write("cert.pem", "CERTIFICATE", leafDER),
		write("key.pem", "EC PRIVATE KEY", kder),
		write("ca.pem", "CERTIFICATE", caDER)
}

func TestSetupTLSPlanesSelectConsumers(t *testing.T) {
	cert, key, ca := writeTLSMaterial(t, "node-a", time.Now().Add(30*24*time.Hour))
	planeList, err := tlsconf.ParsePlanes("raft,admin")
	if err != nil {
		t.Fatal(err)
	}
	planes := tlsconf.NewPlaneSet(planeList...)
	ts, err := setupTLS(tlsconf.Config{CertFile: cert, KeyFile: key, CAFile: ca}, "", planes, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	if ts.dataServer != nil || ts.dataClient != nil || ts.nbdServer != nil {
		t.Fatal("planes left out of -tls-planes must hand their consumers a nil config (plain)")
	}
	if ts.raftServer == nil || ts.raftClient == nil || ts.adminSrv == nil {
		t.Fatal("selected planes must hand their consumers a TLS config")
	}
	if got := plainPlanes(ts.planes); got != "data, nbd" {
		t.Fatalf("plain planes = %q", got)
	}
	if w := ts.warnings(time.Now()); len(w) != 0 {
		t.Fatalf("fresh material must not warn, got %v", w)
	}
	view := ts.View()
	if !view.Enabled || view.Planes["raft"] != true || view.Planes["data"] != false {
		t.Fatalf("view = %+v", view)
	}
}

func TestSetupTLSRejectsBadFlagCombinations(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	if _, err := setupTLS(tlsconf.Config{}, "", tlsconf.NewPlaneSet(tlsconf.PlaneData), logger); err == nil {
		t.Fatal("-tls-planes without a certificate must be refused")
	}
	cert, key, _ := writeTLSMaterial(t, "node-a", time.Now().Add(time.Hour))
	if _, err := setupTLS(tlsconf.Config{CertFile: cert, KeyFile: key}, "", nil, logger); err == nil {
		t.Fatal("a certificate without -tls-ca must be refused (skip-verify is the exception)")
	}
	if _, err := setupTLS(tlsconf.Config{CertFile: cert, KeyFile: key, SkipVerify: true}, "", nil, logger); err != nil {
		t.Fatalf("skip-verify must not require a CA: %v", err)
	}
}

// TestAdminTLSReloadEndpoints covers the operator surface: /v1/tls
// reports the live material, POST /v1/tls/reload picks up rotated files,
// and a broken file leaves the daemon serving the previous material with
// the failure visible (status, warnings, endpoint answer).
func TestAdminTLSReloadEndpoints(t *testing.T) {
	cert, key, ca := writeTLSMaterial(t, "node-v1", time.Now().Add(time.Hour))
	planeList, err := tlsconf.ParsePlanes("data,raft,nbd,admin")
	if err != nil {
		t.Fatal(err)
	}
	planes := tlsconf.NewPlaneSet(planeList...)
	ts, err := setupTLS(tlsconf.Config{CertFile: cert, KeyFile: key, CAFile: ca}, "", planes, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	a, err := newAdminServer(nil, "127.0.0.1:0", log.New(io.Discard, "", 0), nil, newDiskRegistry(), nil, nil, ts, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}

	getTLS := func() map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/tls", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /v1/tls: %d %s", rec.Code, rec.Body.String())
		}
		var v map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if v := getTLS(); v["enabled"] != true {
		t.Fatalf("GET /v1/tls: %+v", v)
	}

	// Rotate in place: same paths, new subject.
	rotated, rotatedKey, _ := writeTLSMaterial(t, "node-v2", time.Now().Add(72*time.Hour))
	if err := os.WriteFile(cert, mustRead(t, rotated), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, mustRead(t, rotatedKey), 0o600); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/tls/reload", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/tls/reload: %d %s", rec.Code, rec.Body.String())
	}
	if v := getTLS(); v["cert"].(map[string]any)["subject"] != "node-v2" {
		t.Fatalf("reload did not pick up the new leaf: %+v", v)
	}

	// Break the material: the reload fails, the old leaf stays live.
	if err := os.WriteFile(key, []byte("-----BEGIN EC PRIVATE KEY-----\nbroken\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	a.srv.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/tls/reload", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("broken material: want 500, got %d %s", rec.Code, rec.Body.String())
	}
	var bad map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &bad); err != nil {
		t.Fatal(err)
	}
	if bad["error"] == nil || bad["message"] == nil {
		t.Fatalf("failed reload must explain itself: %+v", bad)
	}
	if v := getTLS(); v["cert"].(map[string]any)["subject"] != "node-v2" {
		t.Fatalf("failed reload must keep the last good leaf: %+v", v)
	}
	if w := ts.warnings(time.Now()); len(w) == 0 {
		t.Fatal("a failed reload must show up as a status warning")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestTLSExpiryWarnsThroughStatus pins the operator-visible alarm: a leaf
// that expires within a day (or has expired) surfaces in /v1/status
// warnings, which is what makes `unistorctl healthcheck` exit non-zero.
func TestTLSExpiryWarnsThroughStatus(t *testing.T) {
	cert, key, ca := writeTLSMaterial(t, "node-soon", time.Now().Add(2*time.Hour))
	planeList, err := tlsconf.ParsePlanes("data,admin")
	if err != nil {
		t.Fatal(err)
	}
	ts, err := setupTLS(tlsconf.Config{CertFile: cert, KeyFile: key, CAFile: ca}, "",
		tlsconf.NewPlaneSet(planeList...), log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	w := ts.warnings(time.Now())
	if len(w) != 1 || !strings.Contains(w[0], "expires in") {
		t.Fatalf("expiry inside the warning window must warn, got %v", w)
	}
	if w := ts.warnings(time.Now().Add(48 * time.Hour)); len(w) == 0 {
		t.Fatal("an expired certificate must warn")
	}
}
