package meta

// §5.1 (vitastor-ideas): DNS names in peer addresses. The transport
// dials, listens and derives the TLS SNI/verification name from the
// peer address, so a peer may be named (dns-name:port) instead of an
// IP literal. These tests pin that behaviour for the whole raft path:
// transport framing, mTLS name verification and write forwarding.

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// makeRaftTLSMaterial builds a CA plus server/client leaf certificates
// whose SANs are the given DNS names (no IP SANs: a name-verified
// handshake then proves the SNI came from the address, not from an IP).
func makeRaftTLSMaterial(t *testing.T, dnsNames []string) (serverCfg, clientCfg *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "meta-test-ca"},
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
			DNSNames:     dnsNames,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}

	serverCfg = &tls.Config{
		Certificates: []tls.Certificate{leaf("meta-server")},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}
	clientCfg = &tls.Config{
		Certificates: []tls.Certificate{leaf("meta-client")},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS12,
	}
	return serverCfg, clientCfg
}

// TestRaftTransportDNSPeerAddress proves the wire transport reaches a
// peer named by a DNS name: a TLS handshake over "localhost:port"
// succeeds when the leaf cert carries that DNS SAN.
func TestRaftTransportDNSPeerAddress(t *testing.T) {
	srvCfg, cliCfg := makeRaftTLSMaterial(t, []string{"localhost"})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	// The server binds the DNS name, not the IP: net.Listen resolves it.
	addr := net.JoinHostPort("localhost", port)
	srv := newTCPTransport(memberID(addr), addr, nil, srvCfg)
	if err := srv.Start(); err != nil {
		t.Fatalf("listen on DNS name %s: %v", addr, err)
	}
	defer srv.Close()

	peerID := memberID(addr)
	cli := newTCPTransport(9999, "127.0.0.1:0", map[uint64]string{peerID: addr}, cliCfg)
	c, err := cli.connection(peerID)
	if err != nil {
		t.Fatalf("dial DNS peer %s: %v", addr, err)
	}
	defer c.Close()

	// A frame written by the client must be routed by the server.
	mt := raftpb.MsgApp
	from := uint64(9999)
	m := &raftpb.Message{Type: &mt, To: &peerID, From: &from, Context: []byte("ping")}
	data, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.send(peerID, frameMessage, data, 2*time.Second); err != nil {
		t.Fatalf("send to DNS peer: %v", err)
	}
	select {
	case msg := <-srv.MsgC():
		if string(msg.GetContext()) != "ping" {
			t.Fatalf("payload = %q, want %q", msg.GetContext(), "ping")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not routed over DNS-named connection")
	}
	cli.Close()
}

// TestRaftTransportPeerAddressNameVerification: the SNI/verification
// name comes from the dial address, so a peer whose certificate does
// not cover that name must be rejected (mTLS is not just encrypted, it
// is bound to the configured identity).
func TestRaftTransportPeerAddressNameVerification(t *testing.T) {
	// Cert covers only "raft.example.invalid"; the peer is dialed as
	// "localhost" -> the handshake must fail on name verification.
	srvCfg, _ := makeRaftTLSMaterial(t, []string{"raft.example.invalid"})
	_, cliCfg := makeRaftTLSMaterial(t, []string{"localhost"})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	srv := newTCPTransport(memberID(addr), addr, nil, srvCfg)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	peerID := memberID(addr)
	cli := newTCPTransport(9999, "127.0.0.1:0", map[uint64]string{peerID: addr}, cliCfg)
	defer cli.Close()
	if _, err := cli.connection(peerID); err == nil {
		t.Fatal("handshake succeeded against a certificate that does not cover the peer name")
	}
}

// TestRaftClusterDNSNames: a full 3-node TCP cluster whose member
// addresses are all "localhost:port" (DNS names end to end: member
// ids, transport, mTLS) elects a leader and replicates a CAS write.
func TestRaftClusterDNSNames(t *testing.T) {
	if testing.Short() {
		t.Skip("raft cluster test skipped in -short mode")
	}
	const n = 3
	ports := freePorts(t, n*2)
	addrs := make([]string, n)
	proxies := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = net.JoinHostPort("localhost", strconv.Itoa(ports[i]))
		proxies[i] = net.JoinHostPort("localhost", strconv.Itoa(ports[n+i]))
	}
	srvCfg, cliCfg := makeRaftTLSMaterial(t, []string{"localhost"})
	lg := log.New(io.Discard, "", 0)
	if os.Getenv("RAFT_DEBUG") != "" {
		lg = log.New(os.Stderr, "raft: ", 0)
	}
	stores := make([]*RaftStore, n)
	for i := 0; i < n; i++ {
		st, err := OpenRaft(RaftConfig{
			Dir:        t.TempDir(),
			BindAddr:   addrs[i],
			Peers:      addrs,
			ProxyAddr:  proxies[i],
			ProxyPeers: proxies,
			Logger:     lg,
			TLS:        srvCfg,
			ProxyTLS:   cliCfg,
		})
		if err != nil {
			for _, s := range stores {
				if s != nil {
					s.Close()
				}
			}
			t.Fatalf("open %d: %v", i, err)
		}
		stores[i] = st
	}
	t.Cleanup(func() {
		for _, s := range stores {
			if s != nil {
				s.Close()
			}
		}
	})
	waitForLeader(t, stores)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lead := leaderOf(t, stores)
	if _, err := lead.Put(ctx, "osd", "dns", []byte("v1"), 0); err != nil {
		t.Fatalf("leader put: %v", err)
	}
	for _, s := range stores {
		if s == lead {
			continue
		}
		deadline := time.Now().Add(10 * time.Second)
		for {
			b, _, err := s.Get(ctx, "osd", "dns")
			if err == nil && string(b) == "v1" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("follower %s never replicated the write: %v", s.bind, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// TestMemberIDDNSNames pins the property DNS deployments rely on: the
// member id is a pure function of the address string, so every node
// that lists the same peer set agrees on ids no matter how the name
// resolves (and an address change is an id change, forcing SetPeer).
func TestMemberIDDNSNames(t *testing.T) {
	a := memberID("storage-1.example.com:5000")
	b := memberID("storage-1.example.com:5000")
	if a != b {
		t.Fatal("memberID not stable")
	}
	if a == memberID("10.0.0.1:5000") {
		t.Fatal("DNS name and IP collide")
	}
	if a == memberID("storage-1.example.com:5001") {
		t.Fatal("port change must change the id")
	}
}
