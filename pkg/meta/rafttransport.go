package meta

// Wire transport for the etcd-raft backend (see raftstore.go).
//
// The raft library is transport-agnostic: it produces *raftpb.Message
// envelopes (heartbeats, append-entries, votes) that must be delivered to
// peer nodes, and consumes the same envelopes arriving from peers. This file
// implements that delivery two ways:
//
//   - tcpTransport: length-prefixed protobuf frames over TCP, one persistent
//     outbound connection per peer plus a listener for inbound connections.
//     Snapshots ship on a dedicated connection so a multi-megabyte transfer
//     cannot head-of-line block heartbeats.
//   - memTransport: an in-process bus used by unit tests. Nodes on the bus
//     see each other exactly as over the wire, with no network involved.
//
// Frame layout: 1 magic byte (0xF1 message, 0xF2 snapshot) + 4-byte big
// endian length + protobuf payload.

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	frameMessage  byte = 0xF1
	frameSnapshot byte = 0xF2

	maxMsgFrame   = 16 << 20 // 16 MiB: raft messages are bounded by MaxSizePerMsg
	maxSnapFrame  = 1 << 30  // 1 GiB: FSM snapshots
	raftWriteTO   = 10 * time.Second
	raftDialTO    = 3 * time.Second
	raftSnapWrite = 5 * time.Minute
)

// snapshotSink receives an incoming snapshot on the node's ingest path.
type snapshotSink func(*raftpb.Snapshot)

// wireTransport delivers raft messages to peers. Implementations must be
// safe for concurrent use.
type wireTransport interface {
	Start() error
	MsgC() <-chan *raftpb.Message
	SetSnapshotSink(snapshotSink)
	Send(id uint64, m *raftpb.Message)
	SendSnapshot(id uint64, snap *raftpb.Snapshot) error
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, status raft.SnapshotStatus)
	SetPeer(id uint64, addr string)
	Close() error
}

// ---------------------------------------------------------------------------
// frame codec
// ---------------------------------------------------------------------------

func writeFrame(w io.Writer, magic byte, data []byte) error {
	var hdr [5]byte
	hdr[0] = magic
	binary.BigEndian.PutUint32(hdr[1:5], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r io.Reader, max uint32) (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:5])
	if n > max {
		return 0, nil, fmt.Errorf("raft transport: frame size %d exceeds limit %d", n, max)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return hdr[0], payload, nil
}

// ---------------------------------------------------------------------------
// TCP transport
// ---------------------------------------------------------------------------

// tcpTransport is the production wireTransport.
type tcpTransport struct {
	id   uint64
	addr string
	// tlsCfg, when set, wraps every inbound and outbound connection in
	// mutual TLS (RaftConfig.TLS).
	tlsCfg *tls.Config

	mu    sync.Mutex
	peers map[uint64]string
	conns map[uint64]*net.Conn

	ln   net.Listener
	msgc chan *raftpb.Message
	sink snapshotSink
	stop chan struct{}
}

// newTCPTransport creates a transport that listens at bindAddr and reaches
// the given peers (id => host:port). tlsCfg (nil = plain TCP) enables
// mutual TLS on every connection.
func newTCPTransport(id uint64, bindAddr string, peers map[uint64]string, tlsCfg *tls.Config) *tcpTransport {
	t := &tcpTransport{
		id:     id,
		addr:   bindAddr,
		tlsCfg: tlsCfg,
		peers:  make(map[uint64]string, len(peers)),
		conns:  make(map[uint64]*net.Conn),
		msgc:   make(chan *raftpb.Message, 256),
		stop:   make(chan struct{}),
	}
	for pid, a := range peers {
		t.peers[pid] = a
	}
	return t
}

// tlsClientConn upgrades an established TCP connection to TLS, using the
// peer address host as the SNI/verification name. RaftConfig.TLS is a
// mutual-TLS server config (Certificates + ClientCAs); when dialed as a
// client it has no RootCAs, so the CA pool used to verify peers falls
// back to ClientCAs — in a mutual cluster that is exactly the CA that
// signed every peer certificate (without this, Go would verify peers
// against the system root pool and reject the cluster's own CA).
func tlsClientConn(c net.Conn, addr string, cfg *tls.Config) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	sc := cfg.Clone()
	if sc.ServerName == "" {
		sc.ServerName = host
	}
	if sc.RootCAs == nil && sc.ClientCAs != nil {
		sc.RootCAs = sc.ClientCAs
	}
	tc := tls.Client(c, sc)
	_ = tc.SetDeadline(time.Now().Add(raftDialTO))
	if err := tc.Handshake(); err != nil {
		tc.Close()
		return nil, err
	}
	_ = tc.SetDeadline(time.Time{})
	return tc, nil
}

func (t *tcpTransport) Start() error {
	ln, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("raft transport: listen %s: %w", t.addr, err)
	}
	if t.tlsCfg != nil {
		ln = tls.NewListener(ln, t.tlsCfg)
	}
	t.ln = ln
	go t.acceptLoop()
	return nil
}

func (t *tcpTransport) MsgC() <-chan *raftpb.Message { return t.msgc }

func (t *tcpTransport) SetSnapshotSink(fn snapshotSink) { t.sink = fn }

func (t *tcpTransport) SetPeer(id uint64, addr string) {
	t.mu.Lock()
	if t.peers[id] != addr {
		t.peers[id] = addr
		if c := t.conns[id]; c != nil {
			(*c).Close()
			delete(t.conns, id)
		}
	}
	t.mu.Unlock()
}

// send writes a frame to the persistent connection for id, dialing on
// demand and re-dialing once after a failure. It never blocks for more than
// the write deadline.
func (t *tcpTransport) send(id uint64, magic byte, data []byte, timeout time.Duration) error {
	for attempt := 0; attempt < 2; attempt++ {
		c, err := t.connection(id)
		if err != nil {
			return err
		}
		_ = c.SetWriteDeadline(time.Now().Add(timeout))
		if err := writeFrame(c, magic, data); err == nil {
			return nil
		}
		t.dropConn(id, c)
	}
	return fmt.Errorf("raft transport: send to peer %d failed", id)
}

func (t *tcpTransport) connection(id uint64) (net.Conn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[id]; ok && c != nil {
		return *c, nil
	}
	addr := t.peers[id]
	if addr == "" {
		return nil, fmt.Errorf("raft transport: no address for peer %d", id)
	}
	c, err := net.DialTimeout("tcp", addr, raftDialTO)
	if err != nil {
		return nil, err
	}
	if t.tlsCfg != nil {
		c, err = tlsClientConn(c, addr, t.tlsCfg)
		if err != nil {
			return nil, err
		}
	}
	t.conns[id] = &c
	return c, nil
}

func (t *tcpTransport) dropConn(id uint64, c net.Conn) {
	t.mu.Lock()
	if cur, ok := t.conns[id]; ok && cur != nil && *cur == c {
		delete(t.conns, id)
	}
	t.mu.Unlock()
	_ = c.Close()
}

func (t *tcpTransport) Send(id uint64, m *raftpb.Message) {
	if id == t.id {
		// Loopback: the raft core filters self-addressed messages, so this
		// path exists only for safety.
		t.routeLocal(m)
		return
	}
	data, err := proto.Marshal(m)
	if err != nil {
		return
	}
	if err := t.send(id, frameMessage, data, raftWriteTO); err != nil {
		t.ReportUnreachable(id)
	}
}

func (t *tcpTransport) routeLocal(m *raftpb.Message) {
	if m.GetType() == raftpb.MsgSnap && m.GetSnapshot() != nil {
		if t.sink != nil {
			t.sink(m.GetSnapshot())
		}
		return
	}
	select {
	case t.msgc <- m:
	case <-t.stop:
	}
}

// SendSnapshot ships a snapshot on a dedicated connection, bypassing the
// per-peer message connection entirely.
func (t *tcpTransport) SendSnapshot(id uint64, snap *raftpb.Snapshot) error {
	if id == t.id {
		if t.sink != nil {
			t.sink(snap)
		}
		return nil
	}
	t.mu.Lock()
	addr := t.peers[id]
	t.mu.Unlock()
	if addr == "" {
		return fmt.Errorf("raft transport: no address for peer %d", id)
	}
	c, err := net.DialTimeout("tcp", addr, raftDialTO)
	if err != nil {
		return err
	}
	defer c.Close()
	if t.tlsCfg != nil {
		c, err = tlsClientConn(c, addr, t.tlsCfg)
		if err != nil {
			return err
		}
		defer c.Close()
	}
	_ = c.SetWriteDeadline(time.Now().Add(raftSnapWrite))
	data, err := proto.Marshal(snap)
	if err != nil {
		return err
	}
	return writeFrame(c, frameSnapshot, data)
}

// ReportUnreachable / ReportSnapshot are no-ops for etcd-raft: peer health
// is discovered by the leader's own pipeline (HeartbeatTimeout /
// MsgAppResp), and a snapshot is part of the message stream, so the leader
// resumes replication when the append reply arrives. No callback plumbing
// is needed (unlike hashicorp/raft's Notify*).
func (t *tcpTransport) ReportUnreachable(id uint64) {}

func (t *tcpTransport) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (t *tcpTransport) acceptLoop() {
	for {
		c, err := t.ln.Accept()
		if err != nil {
			select {
			case <-t.stop:
				return
			default:
				continue
			}
		}
		go t.readLoop(c)
	}
}

func (t *tcpTransport) readLoop(c net.Conn) {
	defer c.Close()
	for {
		magic, payload, err := readFrame(c, maxSnapFrame)
		if err != nil {
			return
		}
		switch magic {
		case frameMessage:
			m := &raftpb.Message{}
			if err := proto.Unmarshal(payload, m); err != nil {
				return
			}
			if raft.IsLocalMsg(m.GetType()) && !raft.IsLocalMsgTarget(m.GetFrom()) {
				continue // never trust remote local-only messages
			}
			select {
			case t.msgc <- m:
			case <-t.stop:
				return
			}
		case frameSnapshot:
			snap := &raftpb.Snapshot{}
			if err := proto.Unmarshal(payload, snap); err != nil {
				return
			}
			if t.sink != nil {
				t.sink(snap)
			}
		default:
			return
		}
	}
}

func (t *tcpTransport) Close() error {
	select {
	case <-t.stop:
		return nil
	default:
		close(t.stop)
	}
	var firstErr error
	if t.ln != nil {
		if err := t.ln.Close(); err != nil {
			firstErr = err
		}
	}
	t.mu.Lock()
	for id, c := range t.conns {
		(*c).Close()
		delete(t.conns, id)
	}
	t.mu.Unlock()
	return firstErr
}

// ---------------------------------------------------------------------------
// in-memory transport (tests)
// ---------------------------------------------------------------------------

// memBus is a shared in-process network: the test analogue of a LAN.
// Stopping a node's transport makes it unreachable until restarted.
type memBus struct {
	mu    sync.Mutex
	nodes map[uint64]*memTransport
}

// NewMemBus creates an empty in-process network.
func NewMemBus() *memBus {
	return &memBus{nodes: make(map[uint64]*memTransport)}
}

// PeerIDs returns the ids currently registered on the bus.
func (b *memBus) PeerIDs() []uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]uint64, 0, len(b.nodes))
	for id := range b.nodes {
		ids = append(ids, id)
	}
	return ids
}

// deliver routes a message to its target; reports whether it arrived.
func (b *memBus) deliver(from, to uint64, item memItem) bool {
	b.mu.Lock()
	target := b.nodes[to]
	b.mu.Unlock()
	if target == nil {
		return false
	}
	return target.deliverFrom(from, item)
}

// memItem is a bus payload: a message or a snapshot.
type memItem struct {
	msg  *raftpb.Message
	snap *raftpb.Snapshot
}

// memTransport is a wireTransport attached to a memBus.
type memTransport struct {
	id   uint64
	bus  *memBus
	addr string

	mu     sync.Mutex
	online bool
	msgc   chan *raftpb.Message
	sink   snapshotSink
	stop   chan struct{}
	closed bool
}

// newMemTransport creates a transport for a node on the given bus.
func newMemTransport(id uint64, addr string, bus *memBus) *memTransport {
	return &memTransport{
		id:   id,
		bus:  bus,
		addr: addr,
		msgc: make(chan *raftpb.Message, 256),
		stop: make(chan struct{}),
	}
}

// Start registers the node on the bus (reachable).
func (t *memTransport) Start() error {
	t.mu.Lock()
	t.online = true
	t.mu.Unlock()
	t.bus.mu.Lock()
	t.bus.nodes[t.id] = t
	t.bus.mu.Unlock()
	return nil
}

// Stop marks the node unreachable: the test analogue of killing a process.
// In-flight and future messages to it are dropped.
func (t *memTransport) Stop() {
	t.mu.Lock()
	t.online = false
	t.mu.Unlock()
}

// deliverFrom is called by the bus.
func (t *memTransport) deliverFrom(from uint64, item memItem) bool {
	t.mu.Lock()
	online := t.online
	sink := t.sink
	closed := t.closed
	t.mu.Unlock()
	if !online || closed {
		return false
	}
	if item.snap != nil {
		if sink != nil {
			sink(item.snap)
		}
		return true
	}
	if item.msg.GetType() == raftpb.MsgHup || item.msg.GetType() == raftpb.MsgBeat {
		return true // local-only messages are never accepted from the network
	}
	select {
	case t.msgc <- item.msg:
		return true
	case <-t.stop:
		return false
	}
}

func (t *memTransport) MsgC() <-chan *raftpb.Message { return t.msgc }

func (t *memTransport) SetSnapshotSink(fn snapshotSink) {
	t.mu.Lock()
	t.sink = fn
	t.mu.Unlock()
}

func (t *memTransport) SetPeer(id uint64, addr string) {}

func (t *memTransport) Send(id uint64, m *raftpb.Message) {
	if id == t.id {
		t.deliverFrom(t.id, memItem{msg: m})
		return
	}
	if !t.bus.deliver(t.id, id, memItem{msg: m}) {
		t.ReportUnreachable(id)
	}
}

func (t *memTransport) SendSnapshot(id uint64, snap *raftpb.Snapshot) error {
	if id == t.id {
		t.deliverFrom(t.id, memItem{snap: snap})
		return nil
	}
	if !t.bus.deliver(t.id, id, memItem{snap: snap}) {
		return errors.New("peer unreachable")
	}
	return nil
}

func (t *memTransport) ReportUnreachable(id uint64) {}

func (t *memTransport) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

func (t *memTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.online = false
	close(t.stop)
	t.mu.Unlock()
	t.bus.mu.Lock()
	if t.bus.nodes[t.id] == t {
		delete(t.bus.nodes, t.id)
	}
	t.bus.mu.Unlock()
	return nil
}
