package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/unistack-org/storage/pkg/tlsconf"
)

// tlsSetup is the daemon's TLS state: the reloadable certificate
// material plus the per-plane configurations handed to each consumer at
// startup. A nil *tlsSetup (no -tls-* flags) is valid and means "every
// plane in the clear"; a nil field inside it means "this plane is in the
// clear", which is what every consumer already understands.
//
// The configurations are built once and shared. They resolve the
// certificate per handshake through the Provider, so a Reload swaps the
// new files in for every plane at once without a restart; the plane set,
// the CA pool and the verification policy are fixed at startup.
type tlsSetup struct {
	prov      *tlsconf.Provider // storage material (cert/key/ca)
	adminProv *tlsconf.Provider // admin material; nil when it is the same

	planes tlsconf.PlaneSet

	dataServer *tls.Config // OSD listener (plane data)
	dataClient *tls.Config // dials to peer OSDs (plane data)
	raftServer *tls.Config // raft transport listener (plane raft)
	raftClient *tls.Config // raft dials + write proxy (plane raft)
	nbdServer  *tls.Config // NBD START_TLS upgrade (plane nbd)
	adminSrv   *tls.Config // admin HTTP listener (plane admin)
}

// setupTLS validates the -tls-* flags, reads the material once (a daemon
// that starts successfully holds valid material) and logs what is
// encrypted.
func setupTLS(material tlsconf.Config, adminCA string, planes tlsconf.PlaneSet, logger *log.Logger) (*tlsSetup, error) {
	if !material.Any() {
		if len(planes) > 0 {
			return nil, fmt.Errorf("-tls-planes given without -tls-cert/-tls-key: nothing to enable")
		}
		return nil, nil
	}
	material.Planes = planes
	if err := material.Validate(); err != nil {
		return nil, err
	}
	prov, err := tlsconf.NewProvider(material)
	if err != nil {
		return nil, err
	}
	ts := &tlsSetup{prov: prov, planes: planes}

	// The admin API presents the certificate and, unlike the storage
	// planes, does not demand client certificates by default: operators
	// use plain "curl --cacert ca.pem". -tls-client-ca-file (a CA for
	// operator/client certs) turns mutual TLS on for the admin API too,
	// which is why it gets its own provider over the same key material.
	if adminCA == "" {
		ts.adminSrv = prov.HTTPServerConfigFor(tlsconf.PlaneAdmin)
	} else {
		admin := material
		admin.CAFile = adminCA
		ap, err := tlsconf.NewProvider(admin)
		if err != nil {
			return nil, fmt.Errorf("tls: admin client CA: %w", err)
		}
		ts.adminProv = ap
		ts.adminSrv = ap.HTTPServerConfigFor(tlsconf.PlaneAdmin)
	}
	ts.dataServer = prov.ServerConfigFor(tlsconf.PlaneData)
	ts.dataClient = prov.ClientConfigFor(tlsconf.PlaneData)
	ts.raftServer = prov.ServerConfigFor(tlsconf.PlaneRaft)
	ts.raftClient = prov.ClientConfigFor(tlsconf.PlaneRaft)
	ts.nbdServer = prov.ServerConfigFor(tlsconf.PlaneNBD)

	logger.Printf("tls: min %s, cert %s, ca %s, encrypted planes: %s",
		material.MinVersion, material.CertFile, material.CAFile, planes.String())
	if plain := plainPlanes(planes); plain != "" {
		logger.Printf("tls: PLAIN (unencrypted) planes: %s", plain)
	}
	if material.SkipVerify {
		logger.Printf("tls: WARNING -tls-skip-verify is on: channels are ENCRYPTED but NOT AUTHENTICATED. Peer certificates are not verified against %s and peer addresses are not checked, so anyone with access to these ports can pose as any node. Turn it off once the cluster's certificates work.", material.CAFile)
	}
	return ts, nil
}

// allPlaneOrder is the order planes are listed in logs and status.
var allPlaneOrder = []tlsconf.Plane{tlsconf.PlaneData, tlsconf.PlaneRaft, tlsconf.PlaneNBD, tlsconf.PlaneAdmin}

// plainPlanes renders the planes left unencrypted ("" = none).
func plainPlanes(planes tlsconf.PlaneSet) string {
	var out []string
	for _, p := range allPlaneOrder {
		if !planes.Has(p) {
			out = append(out, string(p))
		}
	}
	return strings.Join(out, ", ")
}

// Reload re-reads the certificate material for every plane. The CA file
// is not re-read (rotating a CA means a restart). On failure the previous
// material stays in place and the error is reported to the caller.
func (t *tlsSetup) Reload() error {
	if t == nil {
		return fmt.Errorf("tls: no TLS material configured (-tls-cert/-tls-key/-tls-ca)")
	}
	if err := t.prov.Reload(); err != nil {
		return err
	}
	if t.adminProv != nil {
		return t.adminProv.Reload()
	}
	return nil
}

// Status reports the current material (nil when TLS is off).
func (t *tlsSetup) Status() *tlsconf.Status {
	if t == nil {
		return nil
	}
	st := t.prov.Status()
	return &st
}

// enabled reports whether the plane runs encrypted.
func (t *tlsSetup) enabled(plane tlsconf.Plane) bool {
	return t != nil && t.planes.Has(plane)
}

// tlsView is what the admin API (and the status endpoint) reports about
// TLS: which planes are encrypted and the live certificate material.
type tlsView struct {
	Enabled bool            `json:"enabled"`
	Planes  map[string]bool `json:"planes,omitempty"`
	Plain   string          `json:"plain_planes,omitempty"`
	Cert    *tlsconf.Status `json:"cert,omitempty"`
}

// View renders the TLS state for operators. TLS off yields a view whose
// only meaningful field is Enabled=false.
func (t *tlsSetup) View() tlsView {
	if t == nil {
		return tlsView{}
	}
	v := tlsView{Enabled: true, Planes: map[string]bool{}, Plain: plainPlanes(t.planes), Cert: t.Status()}
	for _, p := range allPlaneOrder {
		v.Planes[string(p)] = t.planes.Has(p)
	}
	return v
}

// warnings reports the TLS conditions an operator should see in
// /v1/status: an expired or soon-to-expire leaf, a failed last reload,
// and authentication being switched off.
func (t *tlsSetup) warnings(now time.Time) []string {
	if t == nil {
		return nil
	}
	st := t.Status()
	if st == nil {
		return nil
	}
	var out []string
	if st.LastError != "" {
		out = append(out, fmt.Sprintf("tls: last certificate reload failed, serving the previous material: %s", st.LastError))
	}
	switch {
	case st.ExpirySeconds <= 0:
		out = append(out, fmt.Sprintf("tls: certificate %s expired on %s — TLS handshakes fail", st.Subject, st.NotAfter.UTC().Format(time.RFC3339)))
	case st.ExpirySeconds < 24*3600:
		out = append(out, fmt.Sprintf("tls: certificate %s expires in %s on %s", st.Subject, (time.Duration(st.ExpirySeconds)*time.Second).Round(time.Hour), st.NotAfter.UTC().Format(time.RFC3339)))
	}
	if st.SkipVerify {
		out = append(out, "tls: -tls-skip-verify is on — traffic is encrypted but peers are not authenticated")
	}
	return out
}

// watchReload polls the material and reloads when a file's mtime moves.
// It is the fallback for a daemon nobody sends SIGHUP to; 0 disables it.
func (t *tlsSetup) watchReload(ctx context.Context, seconds int, logger *log.Logger) {
	if t == nil || seconds <= 0 {
		return
	}
	interval := time.Duration(seconds) * time.Second
	logf := func(f string, a ...any) { logger.Printf(f, a...) }
	go t.prov.Watch(ctx, interval, logf)
	if t.adminProv != nil {
		go t.adminProv.Watch(ctx, interval, logf)
	}
	logger.Printf("tls: watching certificate files every %ds", seconds)
}
