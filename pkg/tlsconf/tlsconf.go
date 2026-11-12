// Package tlsconf builds the shared TLS configuration for the unistack
// control and data planes: OSD data connections, raft meta transport,
// the NBD START_TLS upgrade, and the admin HTTP API.
//
// One certificate per node (self-signed is fine — the nodes verify each
// other against a private CA) plus a CA file gives mutual TLS: both
// directions of every unistack connection present and verify a
// certificate. Certificates must list the node's dial addresses as SANs
// — IP literals and/or DNS names: every component dials by the address
// string it was configured with, so peers registered as
// "osd-1.dc1.example:7700" are verified against the DNS SAN
// "osd-1.dc1.example".
//
// Encryption is per plane (Config.Plane): an operator can keep the bulk
// data path plain, the way vitastor deployments run it, while the raft
// control plane and the admin API stay on mTLS. A plane outside the set
// gets a nil *tls.Config, which every consumer already treats as "plain".
//
// Config.SkipVerify turns authentication off while keeping the channel
// encrypted: a client stops checking the peer's chain, a server stops
// verifying client certificates. It exists to stand a cluster up (and to
// keep one running through broken material); nothing enables it silently.
//
// A Provider hands out configurations that read the certificate from disk
// per handshake, so replacing the files and calling Reload (SIGHUP, the
// admin API, Provider.Watch) applies new material without a restart.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Plane is one network plane TLS can be enabled for.
type Plane string

const (
	// PlaneData carries OSD-to-OSD traffic: replicated writes,
	// replication fan-out, recovery. The bulk of the traffic, and the
	// plane an operator is most likely to want plain.
	PlaneData Plane = "data"
	// PlaneRaft is the raft meta transport (and the write proxy that
	// forwards to the leader over it).
	PlaneRaft Plane = "raft"
	// PlaneNBD is the NBD export's START_TLS upgrade, i.e. client
	// connections to the export listener.
	PlaneNBD Plane = "nbd"
	// PlaneAdmin is the operator-facing admin HTTP API.
	PlaneAdmin Plane = "admin"
)

// AllPlanes is the plane set enabled by default, which is the behaviour
// unistord had before planes could be selected: every plane TLS.
var AllPlanes = []Plane{PlaneData, PlaneRaft, PlaneNBD, PlaneAdmin}

// ParsePlanes parses a comma-separated plane list ("-tls-planes"). The
// empty string means every plane. "osd" and "client" are accepted as
// aliases of "data" (the data plane is the OSD listener, and from a
// client's point of view it is the client connection to it).
func ParsePlanes(s string) ([]Plane, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return AllPlanes, nil
	}
	var out []Plane
	for _, part := range strings.Split(s, ",") {
		name := Plane(strings.ToLower(strings.TrimSpace(part)))
		switch name {
		case "":
			continue
		case "osd", "data", "client":
			out = append(out, PlaneData)
		case "raft", "meta":
			out = append(out, PlaneRaft)
		case "nbd", "export":
			out = append(out, PlaneNBD)
		case "admin", "http":
			out = append(out, PlaneAdmin)
		default:
			return nil, fmt.Errorf("tls: unknown plane %q (want data, raft, nbd, admin)", part)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("tls: empty plane list")
	}
	return dedupePlanes(out), nil
}

func dedupePlanes(in []Plane) []Plane {
	seen := map[Plane]bool{}
	var out []Plane
	for _, p := range in {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

// PlaneSet is a set of planes with membership tests. The zero value (and
// a nil set) contains nothing; use AllPlanes for "everything".
type PlaneSet map[Plane]bool

// NewPlaneSet builds a set from a plane list.
func NewPlaneSet(planes ...Plane) PlaneSet {
	s := make(PlaneSet, len(planes))
	for _, p := range planes {
		s[p] = true
	}
	return s
}

// Has reports whether the plane is in the set.
func (s PlaneSet) Has(p Plane) bool { return s[p] }

// String renders the set in flag syntax, e.g. `data,raft`.
func (s PlaneSet) String() string {
	out := make([]string, 0, len(s))
	for p := range s {
		out = append(out, string(p))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// Config describes the on-disk material for one endpoint.
type Config struct {
	// CertFile / KeyFile: the node's certificate and private key (PEM).
	CertFile string
	KeyFile  string
	// CAFile: PEM certificate(s) of the CA that signed the peers'
	// certificates. Required unless SkipVerify is set — it is what makes
	// the channel authenticated; without it a man on the storage net can
	// pose as any OSD/peer.
	CAFile string
	// MinVersion: "1.2" or "1.3" ("" = 1.2).
	MinVersion string
	// SkipVerify keeps the channel encrypted but performs no
	// authentication: a client does not verify the peer's chain or name,
	// and a server asks for a client certificate without verifying it.
	// A deployment in this state can be impersonated by anyone on the
	// network; it is a startup-time warning, not a steady state.
	SkipVerify bool
	// Planes selects which planes the material applies to. The nil set
	// means every plane (see Provider.For).
	Planes PlaneSet
}

// Any reports whether the endpoint was given any TLS material at all.
func (c Config) Any() bool {
	return c.CertFile != "" || c.KeyFile != "" || c.CAFile != ""
}

// Complete reports whether the material is sufficient for mutual TLS.
func (c Config) Complete() bool {
	return c.CertFile != "" && c.KeyFile != "" && c.CAFile != ""
}

// Validate reports why the material cannot be used, if it cannot. A CA
// file is required, because material without one is not mutual TLS —
// except in the one case where the operator asked for no verification.
func (c Config) Validate() error {
	if !c.Any() {
		return fmt.Errorf("tls: no material (set -tls-cert/-tls-key/-tls-ca)")
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return fmt.Errorf("tls: -tls-cert and -tls-key must be given together")
	}
	if c.CAFile == "" && !c.SkipVerify {
		return fmt.Errorf("tls: -tls-ca is required (it is what authenticates the peers); pass -tls-skip-verify to run encrypted-but-unauthenticated")
	}
	switch c.MinVersion {
	case "", "1.2", "1.3":
	default:
		return fmt.Errorf("tls: -tls-min-version %q: want \"1.2\" or \"1.3\"", c.MinVersion)
	}
	return nil
}

// loadCert reads the certificate/key pair fresh from disk, so a Reload
// picks up replaced files.
func (c Config) loadCert() (*tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: certificate/key: %w", err)
	}
	return &cert, nil
}

func (c Config) minVersion() uint16 {
	if c.MinVersion == "1.3" {
		return tls.VersionTLS13
	}
	return tls.VersionTLS12
}

func (c Config) caPool() (*x509.CertPool, error) {
	pem, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("tls: ca file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("tls: ca file %s: no PEM certificates found", c.CAFile)
	}
	return pool, nil
}

// serverAuth is the client-auth policy the material implies for a
// listening endpoint: mutual TLS, or (with SkipVerify) encryption with
// no authentication of the peer.
func (c Config) serverAuth() tls.ClientAuthType {
	if c.SkipVerify {
		return tls.RequestClientCert
	}
	return tls.RequireAndVerifyClientCert
}

// pools reads the CA pool, tolerating its absence in the SkipVerify case.
func (c Config) pools() (*x509.CertPool, error) {
	if c.CAFile == "" {
		return nil, nil
	}
	return c.caPool()
}

// ServerConfig builds the tls.Config for a listening endpoint that
// requires and verifies client certificates (mutual TLS). With
// SkipVerify the certificate is presented but peers are not verified.
//
// The material is read once; a daemon that must pick up replaced files
// without a restart wants Provider.ServerConfig instead.
func (c Config) ServerConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: certificate/key: %w", err)
	}
	pool, err := c.pools()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   c.serverAuth(),
		MinVersion:   c.minVersion(),
	}, nil
}

// HTTPServerConfig builds the tls.Config for the operator-facing admin
// HTTP API: the server always presents its certificate; client
// certificates are required only when CAFile is set (mutual). This lets
// operators use plain "curl --cacert ca.pem" against the API.
func (c Config) HTTPServerConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: certificate/key: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   c.minVersion(),
	}
	pool, err := c.pools()
	if err != nil {
		return nil, err
	}
	cfg.ClientCAs = pool
	if pool != nil {
		cfg.ClientAuth = c.serverAuth()
	}
	return cfg, nil
}

// CAOnlyConfig builds a client tls.Config that trusts CAFile but presents
// no client certificate: for operator tools talking to endpoints that do
// not demand mutual TLS (the admin HTTP API by default).
func CAOnlyConfig(caFile, minVersion string) (*tls.Config, error) {
	c := Config{CAFile: caFile, MinVersion: minVersion}
	pool, err := c.caPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{RootCAs: pool, MinVersion: c.minVersion()}, nil
}

// ClientConfig builds the tls.Config a unistack client uses against a
// TLS-enabled endpoint. The caller supplies the ServerName per
// connection (the dial address host), which Go's tls package uses to
// verify the peer certificate; this config supplies the material.
//
// A Config with no certificate (CA file only) yields a config that
// authenticates the server but presents no client certificate, which is
// what the admin API accepts by default.
func (c Config) ClientConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         c.minVersion(),
		InsecureSkipVerify: c.SkipVerify,
	}
	if c.Any() {
		pool, err := c.pools()
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: certificate/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
