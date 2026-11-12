package tlsconf

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mat is a test certificate authority: it signs leaves on demand so a
// test can write (and later replace) the material files of an endpoint.
type mat struct {
	dir   string
	ca    *x509.Certificate
	caKey *ecdsa.PrivateKey
}

// writeMaterial writes a CA, a server and a client certificate/key pair
// (all valid for 127.0.0.1) as PEM files into t.TempDir and returns the
// Config sharing one CA for each side.
func writeMaterial(t *testing.T) (server, client Config) {
	t.Helper()
	m := newMaterial(t)

	sCert, sKey := m.leaf(t, "tlsconf-server")
	cCert, cKey := m.leaf(t, "tlsconf-client")
	return Config{CertFile: sCert, KeyFile: sKey, CAFile: m.caPath(t)},
		Config{CertFile: cCert, KeyFile: cKey, CAFile: m.caPath(t)}
}

func newMaterial(t *testing.T) *mat {
	t.Helper()
	dir, err := os.MkdirTemp(t.TempDir(), "mat")
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "tlsconf-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	m := &mat{dir: dir, ca: ca, caKey: key}
	if err := os.WriteFile(m.caPath(t), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return m
}

// caPath is the PEM file holding the CA certificate.
func (m *mat) caPath(tb testing.TB) string {
	tb.Helper()
	return filepath.Join(m.dir, "ca.pem")
}

// leaf signs a certificate valid for 127.0.0.1/localhost, writes it and
// its key into the material directory, and returns their paths.
func (m *mat) leaf(tb testing.TB, name string) (certPath, keyPath string) {
	tb.Helper()
	return m.writeLeaf(tb, name, name, time.Now().Add(time.Hour))
}

// writeLeaf writes <base>.crt/<base>.key with subject cn and the given
// NotAfter, so a test can rotate a leaf in place (same paths) and tell
// the old certificate from the new one.
func (m *mat) writeLeaf(tb testing.TB, base, cn string, notAfter time.Time) (certPath, keyPath string) {
	tb.Helper()
	writePEM := func(path, typ string, der []byte) {
		tb.Helper()
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			tb.Fatal(err)
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, m.ca, &key.PublicKey, m.caKey)
	if err != nil {
		tb.Fatal(err)
	}
	kder, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	certPath = filepath.Join(m.dir, base+".crt")
	keyPath = filepath.Join(m.dir, base+".key")
	writePEM(certPath, "CERTIFICATE", der)
	writePEM(keyPath, "EC PRIVATE KEY", kder)
	return certPath, keyPath
}

// handshake runs one client dial against a server listener and returns
// the client error (the server side mirrors it: a handshake the client
// rejects is one the server rejects).
func handshake(tb testing.TB, scfg, ccfg *tls.Config) error {
	tb.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", scfg)
	if err != nil {
		tb.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- err
			return
		}
		_, rerr := c.Read(make([]byte, 1))
		c.Close()
		accepted <- rerr
	}()
	c, err := tls.Dial("tcp", ln.Addr().String(), ccfg)
	if err != nil {
		<-accepted
		return err
	}
	defer c.Close()
	if _, err := c.Write([]byte("x")); err != nil {
		return err
	}
	return <-accepted
}

func TestTLSConfHandshake(t *testing.T) {
	serverCfg, clientCfg := writeMaterial(t)
	scfg, err := serverCfg.ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	ccfg, err := clientCfg.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	// ServerName: the leaf covers 127.0.0.1 via IP SAN; Go verifies the
	// IP against the SAN list when ServerName is an IP literal.
	ccfg.ServerName = "127.0.0.1"

	ln, err := tls.Listen("tcp", "127.0.0.1:0", scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer c.Close()
		_, _ = c.Read(make([]byte, 1))
		accepted <- nil
	}()

	c, err := tls.Dial("tcp", ln.Addr().String(), ccfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := <-accepted; err != nil {
		t.Fatalf("accept: %v", err)
	}
}

func TestTLSConfRejectsUntrusted(t *testing.T) {
	serverCfg, _ := writeMaterial(t)
	// A client from a different CA: ClientConfig succeeds (it only loads
	// material), the handshake is what must fail.
	other := writeMaterialOtherCA(t)
	scfg, err := serverCfg.ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	ccfg, err := other.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	ccfg.ServerName = "127.0.0.1"
	ln, err := tls.Listen("tcp", "127.0.0.1:0", scfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			c.Close()
		}
	}()
	if _, err := tls.Dial("tcp", ln.Addr().String(), ccfg); err == nil {
		t.Fatal("untrusted client accepted")
	}
}

func TestTLSConfFlags(t *testing.T) {
	serverCfg, _ := writeMaterial(t)
	if (Config{}).Any() {
		t.Fatal("empty config reports Any")
	}
	c := Config{CertFile: "c"}
	if !c.Any() || c.Complete() {
		t.Fatal("partial config: Any should be true, Complete false")
	}
	if !serverCfg.Complete() {
		t.Fatal("full config: Complete should be true")
	}
	if m := (Config{MinVersion: "1.3"}).minVersion(); m != tls.VersionTLS13 {
		t.Fatal("min version 1.3")
	}
	if m := (Config{}).minVersion(); m != tls.VersionTLS12 {
		t.Fatal("default min version")
	}
}

func writeMaterialOtherCA(t *testing.T) Config {
	t.Helper()
	// Reuse writeMaterial with a fresh CA (each call creates a new one).
	s, c := writeMaterial(t)
	_ = s
	return c
}

func TestParsePlanes(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		err  bool
	}{
		{in: "", want: "admin,data,nbd,raft"},
		{in: "data", want: "data"},
		{in: " raft , ADMIN ", want: "admin,raft"},
		{in: "osd,client,data", want: "data"},
		{in: "meta,export,http", want: "admin,nbd,raft"},
		{in: "raft,", want: "raft"},
		{in: " , ", err: true},
		{in: "kernel", err: true},
	} {
		got, err := ParsePlanes(tc.in)
		if tc.err {
			if err == nil {
				t.Fatalf("ParsePlanes(%q): want error, got %v", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ParsePlanes(%q): %v", tc.in, err)
		}
		if s := NewPlaneSet(got...).String(); s != tc.want {
			t.Fatalf("ParsePlanes(%q) = %q, want %q", tc.in, s, tc.want)
		}
	}
	set := NewPlaneSet(PlaneRaft, PlaneAdmin)
	if !set.Has(PlaneRaft) || set.Has(PlaneData) {
		t.Fatal("PlaneSet membership")
	}
	if (PlaneSet(nil)).Has(PlaneData) {
		t.Fatal("zero PlaneSet must contain nothing")
	}
}

func TestValidate(t *testing.T) {
	m := newMaterial(t)
	cert, key := m.leaf(t, "node-1")
	full := Config{CertFile: cert, KeyFile: key, CAFile: m.caPath(t)}
	if err := full.Validate(); err != nil {
		t.Fatalf("complete material: %v", err)
	}
	for name, c := range map[string]Config{
		"empty":            {},
		"cert without key": {CertFile: cert, CAFile: m.caPath(t)},
		"key without cert": {KeyFile: key, CAFile: m.caPath(t)},
		"no ca":            {CertFile: cert, KeyFile: key},
		"bad min version":  {CertFile: cert, KeyFile: key, CAFile: m.caPath(t), MinVersion: "1.1"},
	} {
		if err := c.Validate(); err == nil {
			t.Fatalf("%s: want error", name)
		}
	}
	// SkipVerify is the one case where a CA file is not required.
	if err := (Config{CertFile: cert, KeyFile: key, SkipVerify: true}).Validate(); err != nil {
		t.Fatalf("skip-verify without ca: %v", err)
	}
}

// SkipVerify keeps the channel encrypted and drops the authentication:
// material from two unrelated CAs must interoperate, which is exactly
// what mutual TLS forbids (TestTLSConfRejectsUntrusted).
func TestSkipVerifyHandshake(t *testing.T) {
	srvMat, cliMat := newMaterial(t), newMaterial(t)
	sCert, sKey := srvMat.leaf(t, "server")
	cCert, cKey := cliMat.leaf(t, "client")

	scfg, err := Config{CertFile: sCert, KeyFile: sKey, SkipVerify: true}.ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	ccfg, err := Config{CertFile: cCert, KeyFile: cKey, SkipVerify: true}.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !ccfg.InsecureSkipVerify {
		t.Fatal("client config must carry InsecureSkipVerify")
	}
	if scfg.ClientAuth != tls.RequestClientCert {
		t.Fatalf("skip-verify server must request, not verify: %v", scfg.ClientAuth)
	}
	ccfg.ServerName = "127.0.0.1"
	if err := handshake(t, scfg, ccfg); err != nil {
		t.Fatalf("skip-verify handshake: %v", err)
	}

	// Same material, verification on: the CAs do not match, so it fails.
	strict, err := Config{CertFile: cCert, KeyFile: cKey, CAFile: cliMat.caPath(t)}.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	strict.ServerName = "127.0.0.1"
	if err := handshake(t, scfg, strict); err == nil {
		t.Fatal("verified client accepted an untrusted server")
	}
}

// A skip-verify server must also accept a peer that presents nothing.
func TestSkipVerifyServerAcceptsNoClientCert(t *testing.T) {
	m := newMaterial(t)
	cert, key := m.leaf(t, "server")
	scfg, err := Config{CertFile: cert, KeyFile: key, SkipVerify: true}.ServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	ccfg := &tls.Config{ServerName: "127.0.0.1", InsecureSkipVerify: true}
	if err := handshake(t, scfg, ccfg); err != nil {
		t.Fatalf("handshake without client certificate: %v", err)
	}
}

// A plane outside the set gets nil, which every consumer treats as
// "run this plane in the clear"; a plane inside it gets a config.
func TestProviderPlanes(t *testing.T) {
	m := newMaterial(t)
	cert, key := m.leaf(t, "node-1")
	p, err := NewProvider(Config{CertFile: cert, KeyFile: key, CAFile: m.caPath(t), Planes: NewPlaneSet(PlaneRaft, PlaneAdmin)})
	if err != nil {
		t.Fatal(err)
	}
	if p.ServerConfigFor(PlaneRaft) == nil || p.HTTPServerConfigFor(PlaneAdmin) == nil {
		t.Fatal("enabled planes must hand out a config")
	}
	if p.ClientConfigFor(PlaneData) != nil || p.ServerConfigFor(PlaneNBD) != nil {
		t.Fatal("disabled planes must hand out nil")
	}
	all, err := NewProvider(Config{CertFile: cert, KeyFile: key, CAFile: m.caPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	for _, pl := range AllPlanes {
		if !all.Enabled(pl) {
			t.Fatalf("a provider with no plane set must cover every plane (%s)", pl)
		}
	}
}

// Replacing the files and reloading must reach the next handshake
// without a restart, and a broken file must not take TLS down.
func TestProviderReload(t *testing.T) {
	m := newMaterial(t)
	cert, key := m.leaf(t, "node-v1")
	cfg := Config{CertFile: cert, KeyFile: key, CAFile: m.caPath(t)}
	p, err := NewProvider(cfg)
	if err != nil {
		t.Fatal(err)
	}
	scfg := p.ServerConfig()
	ccert, ckey := m.leaf(t, "dialer") // same CA: the server verifies the dialer
	ccfg, err := Config{CertFile: ccert, KeyFile: ckey, CAFile: m.caPath(t)}.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	ccfg.ServerName = "127.0.0.1"

	// leafCN runs one handshake and reports the CommonName of the leaf
	// the server presented - the certificate the Provider is serving.
	leafCN := func() string {
		ln, err := tls.Listen("tcp", "127.0.0.1:0", scfg)
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		accepted := make(chan string, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				accepted <- ""
				return
			}
			defer c.Close()
			_, _ = c.Read(make([]byte, 1))
			accepted <- ""
		}()
		c, err := tls.Dial("tcp", ln.Addr().String(), ccfg)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer c.Close()
		if _, err := c.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		<-accepted
		pcs := c.ConnectionState().PeerCertificates
		if len(pcs) == 0 {
			t.Fatal("no peer certificate")
		}
		return pcs[0].Subject.CommonName
	}
	if got := leafCN(); got != "node-v1" {
		t.Fatalf("leaf = %q, want node-v1", got)
	}

	// Rotate: same paths, new leaf.
	if _, _ = m.writeLeaf(t, "node-v1", "node-v2", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := p.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got := leafCN(); got != "node-v2" {
		t.Fatalf("leaf after reload = %q, want node-v2", got)
	}
	st := p.Status()
	if st.Reloads != 1 || st.LastError != "" || st.Subject != "node-v2" {
		t.Fatalf("status after reload: %+v", st)
	}
	if st.ExpirySeconds <= 47*3600 {
		t.Fatalf("expiry_seconds = %v, want ~48h", st.ExpirySeconds)
	}

	// Broken material: the previous leaf keeps serving, the error is
	// reported to the caller and in the status.
	if err := os.WriteFile(key, []byte("-----BEGIN EC PRIVATE KEY-----\nbroken\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := p.Reload(); err == nil {
		t.Fatal("reload of a broken key must fail")
	}
	if got := leafCN(); got != "node-v2" {
		t.Fatalf("leaf after failed reload = %q, want the last good node-v2", got)
	}
	if st := p.Status(); st.LastError == "" || st.Reloads != 2 {
		t.Fatalf("status after failed reload: %+v", st)
	}
}

func TestProviderWatch(t *testing.T) {
	m := newMaterial(t)
	cert, key := m.leaf(t, "node-v1")
	p, err := NewProvider(Config{CertFile: cert, KeyFile: key, CAFile: m.caPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reloads := make(chan string, 8)
	go p.Watch(ctx, 10*time.Millisecond, func(f string, a ...any) {
		reloads <- fmt.Sprintf(f, a...)
	})
	if _, _ = m.writeLeaf(t, "node-v1", "node-v2", time.Now().Add(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-reloads:
		if !strings.Contains(msg, "reloaded") {
			t.Fatalf("watch logged %q", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not reload the material")
	}
	if got := p.Status().Subject; got != "node-v2" {
		t.Fatalf("watch left leaf %q, want node-v2", got)
	}
}

// TestProviderReloadAfterRename covers how a control-plane agent publishes
// material: write beside the live files, rename over them. Renaming keeps
// the source file's mtime, which is newer than the sampled one, so both
// triggers - SIGHUP-style Reload and the mtime watch - see the change.
func TestProviderReloadAfterRename(t *testing.T) {
	m := newMaterial(t)
	cert, key := m.leaf(t, "node-v1")
	p, err := NewProvider(Config{CertFile: cert, KeyFile: key, CAFile: m.caPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	stagingCert, stagingKey := m.writeLeaf(t, "incoming", "node-v2", time.Now().Add(72*time.Hour))
	if err := os.Rename(stagingCert, cert); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(stagingKey, key); err != nil {
		t.Fatal(err)
	}
	if err := p.Reload(); err != nil {
		t.Fatal(err)
	}
	if got := p.Status().Subject; got != "node-v2" {
		t.Fatalf("leaf after rename+reload = %q, want node-v2", got)
	}
}
