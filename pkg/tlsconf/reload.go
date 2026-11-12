package tlsconf

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Provider owns the on-disk TLS material and hands out tls.Configs that
// read it through callbacks, so replacing the certificate/key files on
// disk and calling Reload switches the running daemon to the new
// material without a restart.
//
// Every *tls.Config a Provider returns shares the Provider: one Reload
// covers all planes (OSD data, raft, NBD, admin) and every connection
// accepted or dialed after it completes. Connections already open keep
// the session they negotiated (TLS has no in-connection rekeying with a
// new certificate) - a peer that must see the new certificate on an
// established data connection needs to reconnect.
//
// The CA file is read once, at construction: Go resolves a peer chain
// against the pool stored in the config, so a rotated CA needs a
// restart. Leaf certificates are what rotates on a short schedule, and
// those do come from disk on every handshake.
type Provider struct {
	cfg Config

	cert atomic.Pointer[tls.Certificate] // current leaf material

	caPool *x509.CertPool // nil when no CA file (SkipVerify material)

	mu       sync.Mutex // serialises Reload, guards the status fields
	lastLoad time.Time
	lastErr  string
	reloads  int64
}

// NewProvider reads the material once and fails on anything unusable, so
// a daemon that starts successfully has valid TLS material in hand.
func NewProvider(c Config) (*Provider, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	p := &Provider{cfg: c}
	cert, err := c.loadCert()
	if err != nil {
		return nil, err
	}
	if c.CAFile != "" {
		pool, err := c.caPool()
		if err != nil {
			return nil, err
		}
		p.caPool = pool
	}
	p.cert.Store(cert)
	p.lastLoad = time.Now()
	return p, nil
}

// Config reports the material the provider was built from.
func (p *Provider) Config() Config { return p.cfg }

// Enabled reports whether the plane is encrypted by this provider.
func (p *Provider) Enabled(plane Plane) bool {
	return p.cfg.Planes == nil || p.cfg.Planes.Has(plane)
}

// ---------- tls.Configs ----------

// ServerConfig returns the mutual-TLS server config for a listening
// plane. The certificate is resolved per handshake from the current
// material (see Provider); the client CA pool is the one read at
// construction.
func (p *Provider) ServerConfig() *tls.Config {
	cfg := p.baseServerConfig()
	cfg.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return p.currentCert()
	}
	return cfg
}

// ServerConfigFor is ServerConfig for one plane, or nil when the plane is
// not encrypted - the value every consumer in unistord already treats as
// "serve this plane in the clear".
func (p *Provider) ServerConfigFor(plane Plane) *tls.Config {
	if !p.Enabled(plane) {
		return nil
	}
	return p.ServerConfig()
}

// HTTPServerConfig returns the admin-HTTP server config: it presents the
// certificate, and demands client certificates only when a CA file is
// set (see Config.HTTPServerConfig).
func (p *Provider) HTTPServerConfig() *tls.Config {
	cfg := p.ServerConfig()
	if p.caPool == nil {
		cfg.ClientAuth = tls.NoClientCert
		cfg.ClientCAs = nil
	}
	return cfg
}

// HTTPServerConfigFor is HTTPServerConfig for the admin plane, or nil
// when the admin plane is not encrypted.
func (p *Provider) HTTPServerConfigFor(plane Plane) *tls.Config {
	if !p.Enabled(plane) {
		return nil
	}
	return p.HTTPServerConfig()
}

// ClientConfig returns the client config for a connecting plane. The
// caller supplies ServerName per connection; the certificate presented
// is the current material (see Provider).
func (p *Provider) ClientConfig() *tls.Config {
	cfg := &tls.Config{
		MinVersion:         p.cfg.minVersion(),
		RootCAs:            p.caPool,
		InsecureSkipVerify: p.cfg.SkipVerify,
		// GetClientCertificate (not Certificates) is what supplies
		// the certificate, so a Reload reaches dials too. A client
		// with no material to present is fine: the callback returns
		// an empty list and the handshake proceeds unauthenticated,
		// which is what the admin API accepts by default.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			c, err := p.currentCert()
			if err != nil {
				return &tls.Certificate{}, nil
			}
			return c, nil
		},
	}
	return cfg
}

// ClientConfigFor is ClientConfig for one plane, or nil when the plane is
// not encrypted.
func (p *Provider) ClientConfigFor(plane Plane) *tls.Config {
	if !p.Enabled(plane) {
		return nil
	}
	return p.ClientConfig()
}

// baseServerConfig builds the parts of a server tls.Config that never
// change at runtime (CA pool, client-auth policy, minimum version).
func (p *Provider) baseServerConfig() *tls.Config {
	cfg := &tls.Config{
		MinVersion: p.cfg.minVersion(),
		ClientCAs:  p.caPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
	if p.caPool == nil {
		cfg.ClientAuth = tls.NoClientCert
	}
	if p.cfg.SkipVerify {
		// Encrypt, do not authenticate: the peer certificate is
		// requested (so a mutual cluster keeps presenting one) but
		// never verified against a chain.
		cfg.ClientAuth = tls.RequestClientCert
	}
	return cfg
}

// ---------- reload ----------

// Reload re-reads the certificate and key. On any error the previous
// material stays in place (a half-written or bogus key file must not
// take a running cluster's planes down), and the error is reported here
// and in Status.
func (p *Provider) Reload() error {
	cert, err := p.cfg.loadCert()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reloads++
	if err != nil {
		p.lastErr = err.Error()
		return err
	}
	p.cert.Store(cert)
	p.lastLoad = time.Now()
	p.lastErr = ""
	return nil
}

// Watch polls the material on interval and reloads when a file's mtime
// moves. It returns when ctx is cancelled. It is the fallback for a
// daemon nobody sends SIGHUP to; Reload (SIGHUP, admin API) is the
// primary trigger. logf (nil = silent) reports every reload outcome.
func (p *Provider) Watch(ctx context.Context, interval time.Duration, logf func(format string, args ...any)) {
	if interval <= 0 {
		return
	}
	st := p.mtimes()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			next := p.mtimes()
			if next == st {
				continue
			}
			st = next
			if err := p.Reload(); err != nil {
				if logf != nil {
					logf("tls: reload failed, keeping previous material: %v", err)
				}
				continue
			}
			if logf != nil {
				logf("tls: reloaded certificate material from %s", p.cfg.CertFile)
			}
		}
	}
}

// mtimes samples the modification times of the material, so a change is
// detected without reading the files. The timestamps are kept at full
// precision: truncating them would make a rewrite that lands in the same
// second as the previous sample invisible forever, because mtime never
// moves backwards on its own.
func (p *Provider) mtimes() [3]time.Time {
	var out [3]time.Time
	for i, f := range []string{p.cfg.CertFile, p.cfg.KeyFile, p.cfg.CAFile} {
		if f == "" {
			continue
		}
		if fi, err := os.Stat(f); err == nil {
			out[i] = fi.ModTime().UTC()
		}
	}
	return out
}

func (p *Provider) currentCert() (*tls.Certificate, error) {
	c := p.cert.Load()
	if c == nil {
		return nil, fmt.Errorf("tls: no certificate material loaded")
	}
	return c, nil
}

// ---------- status ----------

// Status is the operator-facing view of the material: what is loaded,
// when it was loaded, when the leaf expires, and what the last reload
// attempt did. It is what /v1/tls answers with.
type Status struct {
	CertFile   string    `json:"cert_file"`
	KeyFile    string    `json:"key_file"`
	CAFile     string    `json:"ca_file,omitempty"`
	MinVersion string    `json:"min_version"`
	SkipVerify bool      `json:"skip_verify"`
	LoadedAt   time.Time `json:"loaded_at"`
	Reloads    int64     `json:"reloads"`
	LastReload time.Time `json:"last_reload,omitzero"`
	LastError  string    `json:"last_error,omitempty"`

	Subject   string    `json:"subject,omitempty"`
	Issuer    string    `json:"issuer,omitempty"`
	Serial    string    `json:"serial,omitempty"`
	DNSNames  []string  `json:"dns_names,omitempty"`
	IPs       []string  `json:"ips,omitempty"`
	NotBefore time.Time `json:"not_before,omitzero"`
	NotAfter  time.Time `json:"not_after,omitzero"`

	// ExpirySeconds is how long the current leaf has left (negative =
	// already expired): the number a monitoring poll alerts on.
	ExpirySeconds float64 `json:"expiry_seconds"`
}

// Status reports the current material.
func (p *Provider) Status() Status {
	st := Status{
		CertFile:   p.cfg.CertFile,
		KeyFile:    p.cfg.KeyFile,
		CAFile:     p.cfg.CAFile,
		MinVersion: p.cfg.MinVersion,
		SkipVerify: p.cfg.SkipVerify,
	}
	p.mu.Lock()
	st.LoadedAt = p.lastLoad
	st.Reloads = p.reloads
	st.LastError = p.lastErr
	if p.reloads > 0 {
		st.LastReload = p.lastLoad
	}
	p.mu.Unlock()
	leaf := p.leaf()
	if leaf == nil {
		return st
	}
	st.Subject = leaf.Subject.CommonName
	st.Issuer = leaf.Issuer.CommonName
	st.Serial = leaf.SerialNumber.String()
	st.DNSNames = leaf.DNSNames
	for _, ip := range leaf.IPAddresses {
		st.IPs = append(st.IPs, ip.String())
	}
	st.NotBefore, st.NotAfter = leaf.NotBefore, leaf.NotAfter
	st.ExpirySeconds = time.Until(leaf.NotAfter).Seconds()
	return st
}

// NotAfter reports when the current leaf expires (zero time when no
// certificate is loaded).
func (p *Provider) NotAfter() time.Time {
	if leaf := p.leaf(); leaf != nil {
		return leaf.NotAfter
	}
	return time.Time{}
}

func (p *Provider) leaf() *x509.Certificate {
	c, err := p.currentCert()
	if err != nil || len(c.Certificate) == 0 {
		return nil
	}
	leaf, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return nil
	}
	return leaf
}
