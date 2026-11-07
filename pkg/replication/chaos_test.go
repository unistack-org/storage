package replication

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/sdsnet"
)

// TestCancelDuringFanout: a replica that never answers must not block
// the caller past ctx cancellation; the write is reported as cancelled,
// not acknowledged.
func TestCancelDuringFanout(t *testing.T) {
	var targets []Target
	var servers []*sdsnet.Server
	for i := 0; i < 2; i++ {
		s, tg := startReplica(t)
		servers = append(servers, s)
		targets = append(targets, tg)
	}
	// Frozen replica: accepts connections, answers the handshake, then
	// swallows every request (no replies).
	frozen := startFrozenReplica(t)
	targets = append(targets, Target{Addr: frozen, Export: "vol0"})

	r := New(targets, 2)
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// Quorum 2 is reachable, so this write should COMMIT before the
	// deadline (straggler drain caps at grace, quorum path is fast).
	if err := r.Write(ctx, 5, 0, make([]byte, 4096), false); err != nil {
		t.Fatalf("write with one frozen replica: %v", err)
	}

	// Now cancel with W unreachable: quorum needs 2, only 1 live + frozen.
	servers[0].Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	err := r.Write(ctx2, 6, 0, make([]byte, 4096), false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	for _, s := range servers[1:] {
		s.Close()
	}
}

// TestReplicaRecovery: a replica that was down during one write must
// accept the next ones once it returns; no permanent poisoning of the
// pool (mirrors post-recovery behaviour before full backfill).
func TestReplicaRecovery(t *testing.T) {
	var targets []Target
	var servers []*sdsnet.Server
	for i := 0; i < 3; i++ {
		s, tg := startReplica(t)
		servers = append(servers, s)
		targets = append(targets, tg)
	}
	defer func() {
		for _, s := range servers {
			s.Close()
		}
	}()
	r := New(targets, 2)
	defer r.Close()
	ctx := context.Background()

	if err := r.Write(ctx, 9, 0, bytes.Repeat([]byte("A"), 4096), false); err != nil {
		t.Fatal(err)
	}
	// Replica 2 goes down.
	servers[2].Close()
	if err := r.Write(ctx, 9, 4096, bytes.Repeat([]byte("B"), 4096), false); err != nil {
		t.Fatalf("write with 1 down: %v", err)
	}
	// It returns (fresh server, same backend state).
	restarted := &sdsnet.Server{Backend: newMemBackend(1 << 20)}
	go func() { _ = restarted.Serve(targets[2].Addr) }()
	defer restarted.Close()
	deadline := time.Now().Add(2 * time.Second)
	for restarted.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if restarted.Addr() == "" {
		t.Fatal("replica did not restart")
	}
	// Writes must flow again (pool redials the recovered target).
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := r.Write(wctx, 9, 8192, bytes.Repeat([]byte("C"), 4096), false); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	acked, _ := r.Stats()
	if acked[2] == 0 {
		t.Fatal("recovered replica never acked")
	}
}

// startFrozenReplica serves the handshake + pings but never answers
// read/write requests. Returns its address.
func startFrozenReplica(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go freezeConn(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// freezeConn answers the handshake (and pings, so idle pool health
// looks fine) then swallows every mutating request without replying.
func freezeConn(c net.Conn) {
	fixed := make([]byte, 16)
	if _, err := io.ReadFull(c, fixed); err != nil {
		c.Close()
		return
	}
	f, err := sdsnet.DecodeHandshakeFixed(fixed)
	if err != nil || !f.HasMagic {
		c.Close()
		return
	}
	if f.NameLen > 0 {
		if _, err := io.ReadFull(c, make([]byte, f.NameLen)); err != nil {
			c.Close()
			return
		}
	}
	var rh [sdsnet.ReqHeaderSize]byte
	var resp [sdsnet.RespHeaderSize]byte
	for {
		if _, err := io.ReadFull(c, rh[:]); err != nil {
			c.Close()
			return
		}
		h, err := sdsnet.DecodeReqHeader(rh[:])
		if err != nil {
			c.Close()
			return
		}
		if h.Op == sdsnet.OpPing {
			(&sdsnet.RespHeader{Seq: h.Seq, Status: sdsnet.StatusOK}).Encode(resp[:])
			if _, err := c.Write(resp[:]); err != nil {
				c.Close()
				return
			}
			continue
		}
		// Swallow the payload, never reply: the client blocks until
		// its ctx cancels.
		if h.Length > 0 {
			if _, err := io.ReadFull(c, make([]byte, h.Length)); err != nil {
				c.Close()
				return
			}
		}
	}
}
