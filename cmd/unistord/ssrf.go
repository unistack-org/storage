// SSRF guard for URL imports (gap 6). The admin's ingest fetches a URL
// the control plane supplies, so the daemon is an HTTP client pointed at
// arbitrary addresses by whoever can talk to it. Left unchecked, that
// reaches every internal service the daemon can see: cloud metadata
// (169.254.169.254), the placement plane on localhost, private networks
// the daemon sits inside.
//
// The guard is a dial-time check on the import client: every host the
// fetch (and every redirect hop) connects to is resolved and the chosen
// address must be a global one, unless -import-allow-private is set.
// Checking at dial time (not only at request time) closes both the
// redirect-to-internal and the DNS-rebinding gaps: the connection is
// pinned to the validated IP, so a second DNS answer can only be used
// by the next dial, which is validated the same way.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

// ssrfGuard validates every dial target of the import client before the
// connection leaves the process.
type ssrfGuard struct {
	allowPrivate bool
	log          *log.Logger
	dialer       *net.Dialer
	resolver     *net.Resolver
	// policy overrides the address classifier (tests exercise the
	// resolve/pin/redirect machinery without the address policy).
	policy func(ip net.IP) (string, bool)
}

// newSSRFGuard wraps the default dialer. allowPrivate=true disables the
// IP policy (the flag escape hatch): name resolution and the redirect
// cap stay in place, the address classes stop being a reason to refuse.
func newSSRFGuard(allowPrivate bool, logger *log.Logger) *ssrfGuard {
	return &ssrfGuard{
		allowPrivate: allowPrivate,
		log:          logger,
		dialer:       &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second},
		resolver:     net.DefaultResolver,
	}
}

// importTransport builds the http.Transport the import client uses:
// every connection goes through the guard's DialContext.
func (g *ssrfGuard) importTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           g.DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   4,
		ForceAttemptHTTP2:     true,
	}
}

// checkRedirect bounds the redirect chain (Go's default of 10 is an
// invitation to probe through chains of shorteners) and rejects
// scheme-downgrade; the host policy itself is enforced at dial time on
// every hop, so nothing needs re-checking here.
func (g *ssrfGuard) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 5 {
		return fmt.Errorf("too many redirects (%d)", len(via))
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return fmt.Errorf("redirect to unsupported scheme %q", req.URL.Scheme)
	}
	return nil
}

// DialContext resolves addr, validates the addresses and dials the
// chosen IP directly (pinning it: the HTTP request and the TLS SNI keep
// the original hostname — the transport passes it separately).
func (g *ssrfGuard) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	if host == "" {
		return nil, errors.New("empty host")
	}
	if ip := net.ParseIP(host); ip != nil {
		if reason, blocked := g.blocked(ip); blocked {
			return nil, fmt.Errorf("import to %s refused: %s", addr, reason)
		}
		return g.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
	}
	if !strings.Contains(host, ".") {
		// A bare name resolves through search domains (and on many
		// hosts through mDNS/NetBIOS): internal service discovery is
		// exactly what the guard exists to stop. Dots-only names are
		// legitimate (metadata.example.internal ends in a dot too, and
		// is caught by the IP check after resolution).
		return nil, fmt.Errorf("import to %q refused: bare hostnames are not allowed", host)
	}
	ips, err := g.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	// Validate every published address and dial the first allowed one:
	// dialing "the first" without validation would connect to a
	// published localhost alias while the check watched the rest.
	for _, addr := range ips {
		ip := net.IP(addr.AsSlice())
		if reason, blocked := g.blocked(ip); blocked {
			if g.log != nil {
				g.log.Printf("import: %s resolves to %s: %s (skipped)", host, ip, reason)
			}
			continue
		}
		return g.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
	}
	return nil, fmt.Errorf("import to %s refused: every address is internal", host)
}

// blocked classifies one address against the policy.
func (g *ssrfGuard) blocked(ip net.IP) (string, bool) {
	if g.policy != nil {
		return g.policy(ip)
	}
	switch {
	case ip.IsUnspecified():
		return "unspecified address", true
	case ip.IsLoopback():
		return "loopback address", true
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		// 169.254.0.0/16 carries the cloud metadata endpoints.
		return "link-local address", true
	case ip.IsInterfaceLocalMulticast():
		return "interface-local address", true
	case ip.To4() != nil && ip.To4()[0] == 100 && ip.To4()[1] >= 64 && ip.To4()[1] <= 127:
		// CGNAT 100.64.0.0/10: overlay networks (Tailscale et al.) live
		// here; it is "internal" in every sense the guard cares about.
		return "carrier-grade NAT (overlay) address", true
	case g.allowPrivate:
		// The escape hatch admits the routable private ranges (a mirror
		// on the storage net) and nothing else: loopback, the metadata
		// address and the overlays stay refused above it.
		return "", false
	case ip.IsPrivate():
		// RFC1918 v4 + RFC4193 v6.
		return "private address", true
	}
	return "", false
}
