package sdsnet

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// makeTLSMaterial builds a throwaway CA plus server and client
// certificates valid for 127.0.0.1 (the dial address in these tests).
func makeTLSMaterial(t *testing.T) (serverCfg, clientCfg *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "sdsnet-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	leaf := func(cn string) tls.Certificate {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			DNSNames:     []string{"localhost"},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}

	serverCfg = &tls.Config{
		Certificates: []tls.Certificate{leaf("sdsnet-server")},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
	clientCfg = &tls.Config{
		Certificates: []tls.Certificate{leaf("sdsnet-client")},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}
	return serverCfg, clientCfg
}

func startTLSServer(t *testing.T, be Backend, cfg *tls.Config) (addr string, shutdown func()) {
	t.Helper()
	s := &Server{Backend: be, TLS: cfg}
	go func() { _ = s.Serve("127.0.0.1:0") }()
	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Addr() == "" {
		t.Fatal("server did not start")
	}
	return s.Addr(), func() { s.Close() }
}

func TestTLSE2E(t *testing.T) {
	serverCfg, clientCfg := makeTLSMaterial(t)
	be := newMemBackend(4096)
	addr, shutdown := startTLSServer(t, be, serverCfg)
	defer shutdown()

	cc, err := DialTLS(addr, "t", 2*time.Second, clientCfg)
	if err != nil {
		t.Fatalf("DialTLS: %v", err)
	}
	payload := []byte("tls-payload")
	ctx := context.Background()
	if err := cc.Write(ctx, oidTest, 100, payload, false); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := make([]byte, len(payload))
	if n, err := cc.Read(ctx, oidTest, 100, got); err != nil || n != len(payload) {
		t.Fatalf("read: n=%d err=%v", n, err)
	}
	if string(got) != string(payload) {
		t.Fatalf("read %q", got)
	}
	if err := cc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestTLSRejectsPlainClient(t *testing.T) {
	serverCfg, _ := makeTLSMaterial(t)
	be := newMemBackend(4096)
	addr, shutdown := startTLSServer(t, be, serverCfg)
	defer shutdown()

	// A plain Dial talks plaintext: the server's handshake fails and the
	// connection must not serve ops.
	cc, err := Dial(addr, "t", 2*time.Second)
	if err == nil {
		// The handshake accepted the gibberish? Writes must fail.
		err := cc.Write(context.Background(), oidTest, 0, []byte("x"), false)
		cc.Close()
		if err == nil {
			t.Fatal("plain client unexpectedly served")
		}
	}
}

func TestTLSRejectsUnknownClient(t *testing.T) {
	serverCfg, _ := makeTLSMaterial(t)
	// A client with a cert from a different CA must be refused.
	_, foreignCfg := makeTLSMaterial(t)
	be := newMemBackend(4096)
	addr, shutdown := startTLSServer(t, be, serverCfg)
	defer shutdown()

	if _, err := DialTLS(addr, "t", 2*time.Second, foreignCfg); err == nil {
		t.Fatal("client from an unrelated CA was accepted")
	}
}

const oidTest = 0x900d
