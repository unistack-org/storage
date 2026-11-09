package meta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// testRaftCluster starts n in-process RaftStore members on one in-memory
// bus. All share one peer set; the lowest member id bootstraps the group
// (the rest join via the transport once it campaigns).
func testRaftCluster(t *testing.T, n int) []*RaftStore {
	t.Helper()
	bus := NewMemBus()
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("raft-node-%d", i)
	}
	lg := log.New(io.Discard, "", 0)
	if os.Getenv("RAFT_DEBUG") != "" {
		lg = log.New(os.Stderr, "raft: ", 0)
	}

	stores := make([]*RaftStore, n)
	for i := 0; i < n; i++ {
		be := raftBackend{
			st: newMemRaftStorage(),
			tr: newMemTransport(memberID(peers[i]), peers[i], bus),
		}
		st, err := newRaftStore(RaftConfig{
			BindAddr: peers[i],
			Peers:    peers,
			Logger:   lg,
		}, be)
		if err != nil {
			for _, s := range stores {
				if s != nil {
					s.Close()
				}
			}
			t.Fatalf("open raft %d: %v", i, err)
		}
		if err := st.start(); err != nil {
			for _, s := range stores {
				if s != nil {
					s.Close()
				}
			}
			t.Fatalf("start raft %d: %v", i, err)
		}
		stores[i] = st
	}
	t.Cleanup(func() {
		for _, s := range stores {
			if s != nil && !s.isClosed() {
				s.Close()
			}
		}
	})
	return stores
}

func (s *RaftStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// waitForLeader waits until some member reports a leader and every
// member's local view agrees on who it is.
func waitForLeader(t *testing.T, stores []*RaftStore) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		leader := stores[0].Leader()
		if leader != "" {
			allAgree := true
			for _, s := range stores {
				if s.Leader() != leader {
					allAgree = false
					break
				}
			}
			if allAgree {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no raft leader within 15s")
}

// leaderOf returns the member that is currently leader.
func leaderOf(t *testing.T, stores []*RaftStore) *RaftStore {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		leader := stores[0].Leader()
		if leader != "" {
			for _, s := range stores {
				if s.bind == leader && strings.EqualFold(s.RaftState(), "leader") {
					return s
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no member claims leadership within 15s")
	return nil
}

func TestRaftStoreCASAcrossNodes(t *testing.T) {
	stores := testRaftCluster(t, 3)
	lead := leaderOf(t, stores)
	ctx := context.Background()

	// Create with CAS=0 on the leader.
	rev, err := lead.Put(ctx, "volumes", "vm1", []byte(`{"size":1}`), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	// Stale CAS must conflict...
	if _, err := lead.Put(ctx, "volumes", "vm1", []byte(`{"size":2}`), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS: got %v, want ErrConflict", err)
	}
	// ...fresh CAS must win.
	rev2, err := lead.Put(ctx, "volumes", "vm1", []byte(`{"size":2}`), rev)
	if err != nil {
		t.Fatalf("cas put: %v", err)
	}
	if rev2 <= rev {
		t.Fatalf("rev not increasing: %d -> %d", rev, rev2)
	}

	// Every node (including followers) must converge on the value.
	// Replication is async from the client's point of view, so poll.
	want := []byte(`{"size":2}`)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok := true
		for _, s := range stores {
			v, r, err := s.Get(ctx, "volumes", "vm1")
			if err != nil || string(v) != string(want) || r != rev2 {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			for i, s := range stores {
				v, r, err := s.Get(ctx, "volumes", "vm1")
				t.Fatalf("node %d not converged: %s@%d %v (want %s@%d)", i, v, r, err, want, rev2)
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	keys, err := stores[0].List(ctx, "volumes")
	if err != nil || len(keys) != 1 || keys[0] != "vm1" {
		t.Fatalf("list: %v %v", keys, err)
	}

	// Writes on a follower must be rejected with ErrNotLeader, not
	// silently applied.
	for _, s := range stores {
		if s == lead {
			continue
		}
		if _, err := s.Put(ctx, "volumes", "vmX", []byte(`{}`), 0); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("follower put: got %v, want ErrNotLeader", err)
		}
	}
}

func TestRaftStoreReadOnlyOnQuorumLoss(t *testing.T) {
	stores := testRaftCluster(t, 3)
	ctx := context.Background()
	lead := leaderOf(t, stores)
	rev, err := lead.Put(ctx, "volumes", "vm1", []byte(`{"size":1}`), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	// Kill the leader and one follower. The surviving node is a
	// follower that had the committed entry applied (the leader
	// replicates it before answering the client), and it cannot reach
	// a quorum of the three-member group on its own.
	survivor := stores[0]
	if survivor == lead {
		survivor = stores[1]
	}
	var killed []*RaftStore
	for _, s := range stores {
		if s != survivor {
			killed = append(killed, s)
		}
	}
	// Give the surviving follower time to apply the committed entry.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if v, _, err := survivor.Get(ctx, "volumes", "vm1"); err == nil && string(v) == `{"size":1}` {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, s := range killed {
		s.Close()
	}

	// Wait for the surviving node to notice the leader is gone.
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if survivor.Leader() == "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Reads keep working (that is the M6 contract).
	v, _, err := survivor.Get(ctx, "volumes", "vm1")
	if err != nil || string(v) != `{"size":1}` {
		t.Fatalf("read during partition: %s %v", v, err)
	}

	// Writes fail as read-only (no quorum), never as success.
	_, err = survivor.Put(ctx, "volumes", "vm2", []byte(`{}`), 0)
	if err == nil {
		t.Fatal("write succeeded without quorum")
	}
	if !IsReadOnly(err) {
		t.Fatalf("write during partition: got %v, want read-only error", err)
	}
	_, _ = rev, rev
}

func TestRaftStoreWatchOnFollower(t *testing.T) {
	stores := testRaftCluster(t, 3)
	ctx := context.Background()
	lead := leaderOf(t, stores)

	// Watch on a node that is NOT the leader.
	var follower *RaftStore
	for _, s := range stores {
		if s != lead {
			follower = s
			break
		}
	}
	w, err := follower.Watch(ctx, "volumes")
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer w.Stop()

	if _, err := lead.Put(ctx, "volumes", "vm1", []byte(`{}`), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	select {
	case ev := <-w.Events():
		if ev.Type != "put" || ev.Key != "vm1" {
			t.Fatalf("event: %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no watch event on follower within 10s")
	}

	if err := lead.Delete(ctx, "volumes", "vm1", -1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	select {
	case ev := <-w.Events():
		if ev.Type != "delete" || ev.Key != "vm1" {
			t.Fatalf("event: %+v", ev)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no delete event on follower within 10s")
	}

	// Stop must not race-panic with in-flight notifications.
	w.Stop()
}

func TestRaftStoreCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	lg := log.New(os.Stderr, "raft-a: ", 0)
	// Single-member group: it can elect itself and write, so the
	// test covers bolt persistence + reopen-without-rebootstrap.
	peers := []string{"127.0.0.1:17701"}
	st, err := OpenRaft(RaftConfig{Dir: dir, BindAddr: peers[0], Peers: peers, Logger: lg})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	// Single-node cluster: bootstrap makes it a voter, it needs one
	// election to become leader.
	waitForLeader(t, []*RaftStore{st})
	if _, err := st.Put(ctx, "volumes", "vm1", []byte(`{"size":1}`), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Reopen on the same data dir: state must come back from the
	// bolt log / snapshot, and the second Open must not re-bootstrap.
	st2, err := OpenRaft(RaftConfig{Dir: dir, BindAddr: peers[0], Peers: peers, Logger: lg})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	waitForLeader(t, []*RaftStore{st2})
	// FSM log replay after Restore is async: poll until the replicated
	// state is visible locally.
	deadline := time.Now().Add(10 * time.Second)
	for {
		v, _, err := st2.Get(ctx, "volumes", "vm1")
		if err == nil && string(v) == `{"size":1}` {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after reopen: %s %v", v, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// The per-key revision survived the restart, so CAS on it still
	// works and bumps it.
	if _, err := st2.Put(ctx, "volumes", "vm1", []byte(`{"size":9}`), 1); err != nil {
		t.Fatalf("CAS after reopen: %v", err)
	}
	if _, _, err := st2.Get(ctx, "volumes", "vm1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st2.Put(ctx, "volumes", "vm1", []byte(`{"size":9}`), 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS after reopen: got %v, want ErrConflict", err)
	}
	if _, err := st2.Put(ctx, "volumes", "vm1", []byte(`{"size":9}`), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("create-after-reopen: got %v, want ErrConflict", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatalf("close 2: %v", err)
	}
}

func TestRaftStoreConcurrentCAS(t *testing.T) {
	// Ten goroutines racing to CAS the same key from rev N: exactly
	// one must win, the rest must see ErrConflict. This is the
	// map-epoch / volume-create pattern.
	stores := testRaftCluster(t, 3)
	ctx := context.Background()
	lead := leaderOf(t, stores)
	rev, err := lead.Put(ctx, "osd", "map", []byte(`{"epoch":0}`), 0)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var wins, conflicts, other int
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := lead.Put(ctx, "osd", "map", []byte(`{"epoch":1}`), rev)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrConflict):
				conflicts++
			default:
				other++
				t.Logf("unexpected: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if wins != 1 || conflicts != 9 || other != 0 {
		t.Fatalf("wins=%d conflicts=%d other=%d, want 1/9/0", wins, conflicts, other)
	}
}

// TestRaftStoreForwardedWriteReadYourWrites is the boot-sequence
// regression: a node that writes through the leader (forwarding) must
// see its own write from local state immediately afterwards, without
// polling. This is what lets a follower publish the crush map and read
// it back seconds later during startup.
func TestRaftStoreForwardedWriteReadYourWrites(t *testing.T) {
	const n = 3
	ports := freePorts(t, n*2) // n raft + n proxy
	raftAddrs := make([]string, n)
	proxyAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		raftAddrs[i] = fmt.Sprintf("127.0.0.1:%d", ports[i])
		proxyAddrs[i] = fmt.Sprintf("127.0.0.1:%d", ports[n+i])
	}
	lg := log.New(io.Discard, "", 0)
	if os.Getenv("RAFT_DEBUG") != "" {
		lg = log.New(os.Stderr, "raft: ", 0)
	}
	stores := make([]*RaftStore, n)
	for i := 0; i < n; i++ {
		st, err := OpenRaft(RaftConfig{
			Dir:        t.TempDir(),
			BindAddr:   raftAddrs[i],
			Peers:      raftAddrs,
			ProxyAddr:  proxyAddrs[i],
			ProxyPeers: proxyAddrs,
			Logger:     lg,
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

	// Pick a follower: its Put must be forwarded to the leader.
	var follower, leader *RaftStore
	for _, s := range stores {
		if strings.EqualFold(s.RaftState(), "leader") {
			leader = s
		} else {
			follower = s
		}
	}
	if leader == nil || follower == nil {
		t.Fatal("cluster is not a leader plus followers")
	}

	ctx := context.Background()
	if _, err := follower.Put(ctx, "volumes", "vm1", []byte(`{"size":1}`), 0); err != nil {
		t.Fatalf("forwarded put: %v", err)
	}
	// Immediately: no sleep, no poll. The forwarded write returned
	// only after the follower's FSM applied the committed entry.
	v, _, err := follower.Get(ctx, "volumes", "vm1")
	if err != nil || string(v) != `{"size":1}` {
		t.Fatalf("read-your-writes on forwarding follower: %s %v", v, err)
	}
	// The write is on the leader too, of course.
	if v, _, err = leader.Get(ctx, "volumes", "vm1"); err != nil || string(v) != `{"size":1}` {
		t.Fatalf("read on leader: %s %v", v, err)
	}
}

// freePorts reserves n TCP ports by binding and releasing them. There
// is a theoretical race with other processes, acceptable for tests.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, 0, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
		ln.Close()
	}
	return ports
}
