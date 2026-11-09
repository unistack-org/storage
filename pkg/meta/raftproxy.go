package meta

// Write forwarding for RaftStore.
//
// raft only accepts proposals on the leader, so a client (or a boot
// sequence) talking to a follower must not give up with "not the
// leader": etcd, Consul and Vitastor all forward writes to the leader
// for exactly this reason. This file adds the same to RaftStore:
//
//   - every node with ProxyAddr set serves a tiny HTTP endpoint that
//     applies one op locally (the node must be the leader);
//   - RaftStore.apply, upon ErrNotLeader/ErrLeadershipLost, looks up
//     the leader's raft address (raft tells us), maps it to the
//     leader's proxy address via ProxyPeers, and POSTs the op there;
//   - if the leader is mid-election the op is retried a few times.
//
// Trust model: the proxy is as trusted as the raft transport itself
// (both are unauthenticated in v0.1) - run it on the storage net only.

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	raft "go.etcd.io/raft/v3"
)

// proxyConfig is the forwarding state of a RaftStore: our listen
// address and the raft-addr -> proxy-addr map covering every voter
// ("" = that voter has no proxy, so it cannot serve as a forward
// target). Zero proxy (proxy == nil on the store) disables forwarding.
type proxyConfig struct {
	Addr  string
	Peers map[string]string
	// tlsSrv/tlsClient mirror RaftConfig.TLS / RaftConfig.ProxyTLS: the
	// proxy speaks https when set.
	tlsSrv    *tls.Config
	tlsClient *tls.Config
}

// proxyOp is the wire format of POST /apply.
type proxyOp struct {
	Op op `json:"op"`
}

type proxyResult struct {
	Result *opResult `json:"result"`
	Leader string    `json:"leader"` // leader raft addr, when not leader
}

// startProxy serves the write-forwarding endpoint. Called from OpenRaft
// when ProxyAddr is set; the server is closed with the store.
func (s *RaftStore) startProxy(cfg RaftConfig) error {
	ln, err := net.Listen("tcp", cfg.ProxyAddr)
	if err != nil {
		return fmt.Errorf("meta: raft proxy listen %s: %w", cfg.ProxyAddr, err)
	}
	peers := map[string]string{}
	for _, p := range cfg.Peers {
		peers[p] = ""
	}
	for i, p := range cfg.Peers {
		if i < len(cfg.ProxyPeers) {
			peers[p] = cfg.ProxyPeers[i]
		}
	}
	s.mu.Lock()
	s.proxy = &proxyConfig{Addr: cfg.ProxyAddr, Peers: peers, tlsSrv: cfg.TLS, tlsClient: cfg.ProxyTLS}
	s.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/apply", s.handleProxyApply)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	s.closers = append(s.closers, &httpCloser{srv})
	go func() {
		if cfg.TLS != nil {
			ln = tls.NewListener(ln, cfg.TLS)
		}
		if serr := srv.Serve(ln); serr != nil && serr != http.ErrServerClosed {
			s.lg.Printf("raft proxy: %v", serr)
		}
	}()
	return nil
}

// httpCloser adapts http.Server to io.Closer (graceless Close is fine
// for an internal endpoint).
type httpCloser struct{ s *http.Server }

func (h *httpCloser) Close() error { return h.s.Close() }

// handleProxyApply applies one op on this node. It never forwards:
// only the local leader applies, and a follower answers with the
// leader's address so the caller can retarget.
func (s *RaftStore) handleProxyApply(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		http.Error(w, "read: "+err.Error(), http.StatusBadRequest)
		return
	}
	var req proxyOp
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "json: "+err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	st := s.currentStatus()
	if st.state != raft.StateLeader || st.lead != memberID(s.bind) {
		json.NewEncoder(w).Encode(proxyResult{Leader: s.Leader()})
		return // result nil => not applied here
	}
	out := proxyResult{}
	res, applyErr := s.applyLocal(req.Op)
	switch {
	case applyErr == nil:
		out.Result = res
	case errors.Is(applyErr, ErrNotFound):
		out.Result = &opResult{Err: opErrNotFound}
	case errors.Is(applyErr, ErrConflict), errors.Is(applyErr, ErrKeyExists):
		out.Result = &opResult{Err: opErrConflict}
	case errors.Is(applyErr, ErrNotLeader):
		out.Result = &opResult{Err: opErrNotLeader} // leadership changed mid-flight
	case errors.Is(applyErr, ErrNoQuorum):
		out.Result = &opResult{Err: opErrNoQuorum}
	default:
		out.Result = &opResult{Err: applyErr.Error()}
	}
	json.NewEncoder(w).Encode(out)
}

// proxyForward sends op to leaderAddr's proxy endpoint.
//
// ok=false means "worth retrying" (no leader yet, leader unreachable,
// leader changed mid-flight); ok=true returns the final verdict. A nil
// proxy config means forwarding is disabled: ok=false with a nil error,
// and the caller keeps its original NotLeader behaviour.
func (s *RaftStore) proxyForward(o op, leaderAddr string) (*opResult, bool, error) {
	s.mu.Lock()
	pr := s.proxy
	s.mu.Unlock()
	if pr == nil {
		return nil, false, nil
	}
	if leaderAddr == "" {
		// No known leader: nothing to forward to, caller retries.
		return nil, false, nil
	}
	target, ok := pr.Peers[leaderAddr]
	if !ok {
		return nil, false, fmt.Errorf("meta: no proxy mapping for leader %s (set -raft-proxy-peers)", leaderAddr)
	}
	if target == "" {
		return nil, false, fmt.Errorf("meta: leader %s has no proxy address (set -raft-proxy-peers)", leaderAddr)
	}

	body, err := json.Marshal(proxyOp{Op: o})
	if err != nil {
		return nil, false, err
	}
	scheme := "http"
	tr := http.DefaultTransport
	if pr.tlsClient != nil {
		scheme = "https"
		tr = &http.Transport{TLSClientConfig: pr.tlsClient.Clone()}
	}
	client := &http.Client{Timeout: 5 * time.Second, Transport: tr}
	resp, err := client.Post(scheme+"://"+target+"/apply", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, true, nil // leader unreachable: caller retries
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, true, fmt.Errorf("meta: leader proxy: http %d", resp.StatusCode)
	}
	var out proxyResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, true, err
	}
	if out.Result == nil {
		return nil, true, nil // leader changed under us: retry
	}
	switch out.Result.Err {
	case "":
		return out.Result, true, nil
	case opErrNotFound:
		return nil, true, ErrNotFound
	case opErrConflict:
		return nil, true, ErrConflict
	case opErrKeyExists:
		return nil, true, ErrKeyExists
	case opErrNotLeader, opErrNoQuorum:
		// Leadership moved or quorum vanished between the leader
		// lookup and the apply: worth a retry.
		return nil, true, nil
	default:
		return nil, true, fmt.Errorf("meta: leader apply: %s", out.Result.Err)
	}
}

// applyLocal proposes op on this node (it must be the leader) and decodes
// the FSM's verdict. The op's CID is fresh (zero in the proxied request),
// so proposeOnce allocates one and waits for the apply on this node.
func (s *RaftStore) applyLocal(o op) (*opResult, error) {
	o.CID = 0
	res, err := s.proposeOnce(o)
	if err != nil {
		return nil, err
	}
	if res.Err != "" {
		return nil, opError(res.Err)
	}
	if res.Idx > 0 {
		if err := s.waitApplied(res.Idx); err != nil {
			return nil, err
		}
	}
	return res, nil
}
