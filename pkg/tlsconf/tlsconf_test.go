package tlsconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeMaterial writes a CA, a server and a client certificate/key pair
// (all valid for 127.0.0.1) as PEM files into t.TempDir and returns the
// Config sharing one CA for each side.
func writeMaterial(t *testing.T) (server, client Config) {
	t.Helper()
	dir, err := os.MkdirTemp(t.TempDir(), "mat")
	if err != nil {
		t.Fatal(err)
	}

	writePEM := func(t *testing.T, path, typ string, der []byte) {
		t.Helper()
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	newCA := func(name string) (*x509.Certificate, *ecdsa.PrivateKey, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber:          big.NewInt(1),
			Subject:               pkix.Name{CommonName: name},
			NotBefore:             time.Now().Add(-time.Hour),
			NotAfter:              time.Now().Add(time.Hour),
			IsCA:                  true,
			KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name+"-ca.pem")
		writePEM(t, path, "CERTIFICATE", der)
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return cert, key, path
	}

	leaf := func(ca *x509.Certificate, caKey *ecdsa.PrivateKey, name string) (certPath, keyPath string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			DNSNames:     []string{"localhost"},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kder, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPath = filepath.Join(dir, name+".crt")
		keyPath = filepath.Join(dir, name+".key")
		writePEM(t, certPath, "CERTIFICATE", der)
		writePEM(t, keyPath, "EC PRIVATE KEY", kder)
		return certPath, keyPath
	}

	ca, caKey, caPath := newCA("tlsconf-test-ca")
	sCert, sKey := leaf(ca, caKey, "tlsconf-server")
	cCert, cKey := leaf(ca, caKey, "tlsconf-client")
	return Config{CertFile: sCert, KeyFile: sKey, CAFile: caPath},
		Config{CertFile: cCert, KeyFile: cKey, CAFile: caPath}
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
