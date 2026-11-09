// Package tlsconf builds the shared TLS configuration for the unistack
// control and data planes: OSD data connections, raft meta transport,
// and the admin HTTP API.
//
// One certificate per node (self-signed is fine — the nodes verify each
// other against a private CA) plus a CA file gives mutual TLS: both
// directions of every unistack connection present and verify a
// certificate. Certificates must list the node's dial addresses (IPs
// included) as SANs; verification is always on, so a bare
// "InsecureSkipVerify" is deliberately not offered.
package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Config describes the on-disk material for one endpoint.
type Config struct {
	// CertFile / KeyFile: the node's certificate and private key (PEM).
	CertFile string
	KeyFile  string
	// CAFile: PEM certificate(s) of the CA that signed the peers'
	// certificates. Required — it is what makes the channel
	// authenticated; without it a man on the storage net can pose as
	// any OSD/peer.
	CAFile string
	// MinVersion: "1.2" or "1.3" ("" = 1.2).
	MinVersion string
}

// Any reports whether the endpoint was given any TLS material at all.
func (c Config) Any() bool {
	return c.CertFile != "" || c.KeyFile != "" || c.CAFile != ""
}

// Complete reports whether the material is sufficient for mutual TLS.
func (c Config) Complete() bool {
	return c.CertFile != "" && c.KeyFile != "" && c.CAFile != ""
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

// ServerConfig builds the tls.Config for a listening endpoint that
// requires and verifies client certificates (mutual TLS).
func (c Config) ServerConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: certificate/key: %w", err)
	}
	pool, err := c.caPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
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
	if c.CAFile != "" {
		pool, err := c.caPool()
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
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
func (c Config) ClientConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: certificate/key: %w", err)
	}
	pool, err := c.caPool()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		MinVersion:   c.minVersion(),
	}, nil
}
