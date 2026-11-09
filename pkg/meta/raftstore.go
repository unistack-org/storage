package meta

// Raft-backed metadata store.
//
// The control plane's metadata (volume registry, OSD membership, cluster
// map, locks, QoS) lives in a replicated state machine: a Raft group
// (go.etcd.io/raft — the library extracted from etcd, embedded in-process,
// no external etcd server) whose FSM is the in-memory KV map from
// MemStore. Every mutation is a log entry replicated to a quorum before it
// is applied; readers serve linearizable reads from the leader and
// (eventually consistent) reads from follower replicas.
//
// Deployment (phase 1, test VMs): run unistord on 3 nodes (a witness node
// with -raft and no storage roles is enough). With 2 storage nodes plus a
// witness, losing one node keeps writes alive; losing two degrades to
// read-only, exactly the semantics we want for the test fleet:
//
//   - Quorum available: full read/write.
//   - Quorum lost: writes fail immediately with ErrNoQuorum / ErrNotLeader;
//     reads keep being served from local state; the OSD layer refuses to
//     create volumes (IsReadOnly).
//
// The implementation is built on raft.RawNode: the library is a pure state
// machine; persistence (bbolt), the wire (see rafttransport.go), snapshot
// cadence and client-proposal correlation are all provided here:
//
//   - proposals carry a correlation id; the leader parks a result channel
//     per proposal, the FSM resolves it when the entry is applied;
//   - every Ready cycle is persisted (hard state + new entries) before
//     messages go out and entries are applied;
//   - snapshots are application-driven: after raftSnapshotThreshold
//     applied entries the FSM state is snapshotted, compacting the log.

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Errors returned by the Raft store.
var (
	// ErrNoQuorum is returned by Put/Delete when the Raft group has no
	// leader (quorum lost). Reads are unaffected: the store degrades to
	// read-only, it does not die.
	ErrNoQuorum = errors.New("meta: raft group has no quorum, writes disabled (read-only mode)")
	// ErrNotLeader is returned by Put/Delete on a follower; the caller can
	// retry against store.Leader() (the proxy transport does exactly that).
	ErrNotLeader = errors.New("meta: not the raft leader, write must go to the leader")
	// ErrInvalidPrefix is returned for prefixes/keys that cannot be stored
	// (empty, containing NUL, or missing the "prefix/key" separator).
	ErrInvalidPrefix = errors.New("meta: invalid prefix")
)

// IsReadOnly reports whether a store refuses writes because its Raft group
// has lost quorum (no leader electable). The OSD layer uses it to refuse
// volume creation while keeping existing volumes serving I/O. A store that
// is not raft-backed is never reported read-only.
func IsReadOnly(err error) bool {
	return errors.Is(err, ErrNoQuorum)
}

// raftApplyTimeout bounds one raft round-trip on the write path: Put/Delete
// must not block forever when the group is healthy but slow.
const raftApplyTimeout = 5 * time.Second

// ---------------------------------------------------------------------------
// FSM commands: the replicated log payload
// ---------------------------------------------------------------------------

// op is a metadata mutation replicated through the raft log.
type op struct {
	Kind string `json:"k"` // "put" | "delete"
	Put  bool   `json:"p"`
	Key  string `json:"key"`
	Val  []byte `json:"v,omitempty"`
	// CAS semantics identical to MemStore: -1 = unconditional,
	// 0 = must not exist, n>0 = must exist with revision n.
	CAS int64 `json:"cas"`
	// CID correlates a proposal with its result channel on the leader.
	CID uint64 `json:"cid,omitempty"`
}

// opResult is what the FSM applies (resolving the proposer's future).
type opResult struct {
	Rev int64  `json:"rev,omitempty"`
	Idx uint64 `json:"idx,omitempty"` // raft index after apply (read-your-writes)
	Err string `json:"err,omitempty"`
}

const (
	opErrNotFound  = "notfound"
	opErrConflict  = "conflict"
	opErrKeyExists = "keyexists"
	opErrNotLeader = "notleader"
	opErrNoQuorum  = "noquorum"
	opErrBadOp     = "badop"
)

var (
	// ErrCASConflict is returned when a CAS write finds a different
	// revision than expected (alias of ErrConflict).
	ErrCASConflict = ErrConflict
	// ErrKeyExists is returned when CAS=0 and the key already exists.
	ErrKeyExists = fmt.Errorf("meta: key exists: %w", ErrConflict)
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// RaftConfig configures a raft meta node.
type RaftConfig struct {
	// Dir holds the raft log, hard state, snapshots and id mapping
	// (bbolt + snapshot files).
	Dir string
	// BindAddr is host:port where this node serves its raft transport.
	BindAddr string
	// Peers: initial/bootstrap members as host:port (this node included).
	// For a fresh cluster every node lists the same complete set; the node
	// with the lowest address bootstraps the group. An empty list makes
	// this node a single-node cluster.
	Peers []string
	// ProxyAddr is the host:port of the TCP write-proxy (see raftproxy.go);
	// empty disables the proxy.
	ProxyAddr string
	// ProxyPeers: the same cluster membership as Peers; the proxy dials
	// the leader of each to forward writes. Empty disables the proxy.
	ProxyPeers []string
	// Logger for raft events (nil = default logger).
	Logger *log.Logger
	// TLS, when set, wraps the raft wire transport (this node's listener
	// and all peer connections, snapshots included) in mutual TLS. Use
	// tlsconf.Config.ServerConfig. Peers must present certificates signed
	// by the same CA.
	TLS *tls.Config
	// ProxyTLS, when set (with TLS), is used by the write-proxy client to
	// dial peer proxies over https (mutual). Use
	// tlsconf.Config.ClientConfig — leaving ServerName empty makes Go
	// verify each peer against the dial address host, so proxy and raft
	// addresses must be covered by the peer's certificate SANs.
	ProxyTLS *tls.Config
}

// RaftStore is a Store replicated by a raft group.
type RaftStore struct {
	cfg  RaftConfig
	lg   *log.Logger
	bind string // this node's raft transport address (cfg.BindAddr)
	node *raft.RawNode
	tr   wireTransport

	// rs is the raft log backend (bolt in production, in-memory in
	// tests); it is touched by the run loop, the startup path and
	// setAddr (which serialises its own writes).
	rs raftStorage

	// snapBase is the index covered by the latest persisted snapshot; a new
	// snapshot is taken once applied grows past snapBase+raftSnapshotThreshold.
	snapBase uint64
	// soft caches the latest SoftState for lock-free observers
	// (IsReadOnly, isLeaderless).
	soft atomic.Pointer[raftStatus]

	// mu guards the FSM state (mirrors DirStore's locking semantics).
	mu       sync.RWMutex
	kvs      map[string]record
	rev      int64
	notify   chan struct{}
	watchers map[*raftWatcher]struct{}
	applied  uint64 // last applied raft index

	// proposals: CID -> result channel, valid only on the leader (entries
	// are only proposed while leading).
	propMu   sync.Mutex
	pending  map[uint64]chan *opResult
	nextCID  atomic.Uint64
	propc    chan []byte
	snapc    chan *raftpb.Snapshot
	statusc  chan chan raftStatus
	tick     *time.Ticker
	stopc    chan struct{}
	donec    chan struct{}
	closeOne sync.Once

	// addr maps member id -> address; persisted, grows via membership.
	addrMu sync.Mutex
	addr   map[uint64]string

	proxy   *proxyConfig
	closers []io.Closer

	closed bool // guarded by mu
}

// raftStatus is a snapshot of the raft state for external observers.
type raftStatus struct {
	lead  uint64
	state raft.StateType
}

// raftWatcher is a polling watcher over the FSM KV map.
type raftWatcher struct {
	prefix string
	rev    int64
	seen   map[string]int64 // last seen revision per key (poll baseline)
	events chan Event
	notify chan struct{} // capacity-1 change ping from the FSM
	stop   chan struct{}
	once   sync.Once
}

// ---------------------------------------------------------------------------
// id mapping / helpers
// ---------------------------------------------------------------------------

// memberID derives the raft id of a member from its address (FNV-1a 64).
func memberID(addr string) uint64 {
	const offset, prime = 14695981039346656037, 1099511628211
	var h uint64 = offset
	for i := 0; i < len(addr); i++ {
		h ^= uint64(addr[i])
		h *= prime
	}
	if h == 0 {
		h = 1 // raft ids start at 1
	}
	return h
}

func uint64Bytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func (s *RaftStore) addrOf(id uint64) string {
	s.addrMu.Lock()
	defer s.addrMu.Unlock()
	return s.addr[id]
}

func (s *RaftStore) setAddr(id uint64, addr string) {
	if addr == "" {
		return
	}
	s.addrMu.Lock()
	changed := s.addr[id] != addr
	if changed {
		s.addr[id] = addr
	}
	b := make(map[string]string, len(s.addr))
	for k, v := range s.addr {
		b[fmtId(k)] = v
	}
	s.addrMu.Unlock()
	if changed && s.rs != nil {
		_ = s.rs.saveAddrMap(b)
	}
}

func fmtId(id uint64) string { return string(uint64Bytes(id)) }

// ---------------------------------------------------------------------------
// raft logger adapter
// ---------------------------------------------------------------------------

// boltLogger adapts our *log.Logger to the raft.Logger interface. raft logs
// at Debug a lot; we route everything to the store logger.
type boltLogger struct{ l *log.Logger }

func (b boltLogger) Debug(v ...interface{})            { b.print(2, v...) }
func (b boltLogger) Debugf(f string, v ...interface{}) { b.printf(2, f, v...) }
func (b boltLogger) Error(v ...interface{})            { b.print(2, v...) }
func (b boltLogger) Errorf(f string, v ...interface{}) { b.printf(2, f, v...) }
func (b boltLogger) Info(v ...interface{})             { b.print(2, v...) }
func (b boltLogger) Infof(f string, v ...interface{})  { b.printf(2, f, v...) }
func (b boltLogger) Warning(v ...interface{})          { b.print(2, v...) }
func (b boltLogger) Warningf(f string, v ...interface{}) {
	b.printf(2, f, v...)
}
func (b boltLogger) Fatal(v ...interface{})            { b.print(2, v...); panic("raft fatal") }
func (b boltLogger) Fatalf(f string, v ...interface{}) { b.printf(2, f, v...); panic("raft fatal") }
func (b boltLogger) Panic(v ...interface{})            { b.print(2, v...); panic(fmt.Sprint(v...)) }
func (b boltLogger) Panicf(f string, v ...interface{}) {
	b.printf(2, f, v...)
	panic(fmt.Sprintf(f, v...))
}

// prefix is the caller-skip depth for the caller's caller (raft internals).
const logSkip = 2

func (b boltLogger) print(skip int, v ...interface{}) {
	if b.l == nil {
		return
	}
	b.l.Output(logSkip+skip-1, fmt.Sprint(v...))
}

func (b boltLogger) printf(skip int, f string, v ...interface{}) {
	if b.l == nil {
		return
	}
	b.l.Output(logSkip+skip-1, fmt.Sprintf(f, v...))
}

// ---------------------------------------------------------------------------
// raft backend interfaces
// ---------------------------------------------------------------------------

// raftStorage is the raft.Storage contract plus the persistence operations
// the Ready loop needs. Implemented by boltRaftStore (production) and
// memRaftStorage (tests).
type raftStorage interface {
	raft.Storage
	Append(ents []*raftpb.Entry) error
	SetHardState(*raftpb.HardState) error
	ApplySnapshot(*raftpb.Snapshot) error
	CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (*raftpb.Snapshot, error)
	Compact(uint64) error
	// appliedIndex / setAppliedIndex persist the FSM apply cursor.
	appliedIndex() uint64
	setAppliedIndex(uint64)
	// saveConfState persists the membership ConfState after a conf change.
	saveConfState(*raftpb.ConfState)
	// metaGet returns a raw key from the metadata bucket (nil if absent).
	metaGet(key []byte) []byte
	// saveAddrMap persists the member-id -> address mapping.
	saveAddrMap(map[string]string) error
	// loadAddrMap returns the persisted mapping (nil,nil when absent).
	loadAddrMap() (map[string]string, error)
	Close() error
}

// memRaftStorage is the in-memory raftStorage used by tests: MemoryStorage
// plus trivial meta persistence.
type memRaftStorage struct {
	*raft.MemoryStorage
	mu       sync.Mutex
	applied  uint64
	conf     *raftpb.ConfState
	meta     map[string][]byte
	addr     map[string]string
	closed   bool
	closeErr error
}

var _ raftStorage = (*memRaftStorage)(nil)

func newMemRaftStorage() *memRaftStorage {
	return &memRaftStorage{
		MemoryStorage: raft.NewMemoryStorage(),
		meta:          map[string][]byte{},
		addr:          map[string]string{},
	}
}

func (m *memRaftStorage) SetHardState(hs *raftpb.HardState) error {
	return m.MemoryStorage.SetHardState(hs)
}

func (m *memRaftStorage) appliedIndex() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applied
}

func (m *memRaftStorage) setAppliedIndex(i uint64) {
	m.mu.Lock()
	m.applied = i
	m.mu.Unlock()
}

func (m *memRaftStorage) saveConfState(cs *raftpb.ConfState) {
	m.mu.Lock()
	m.conf = cs
	m.mu.Unlock()
}

func (m *memRaftStorage) metaGet(key []byte) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if string(key) == string(metaConfState) && m.conf != nil {
		b, err := proto.Marshal(m.conf)
		if err != nil {
			return nil
		}
		return b
	}
	return m.meta[string(key)]
}

func (m *memRaftStorage) saveAddrMap(mp map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addr = make(map[string]string, len(mp))
	for k, v := range mp {
		m.addr[k] = v
	}
	return nil
}

func (m *memRaftStorage) loadAddrMap() (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]string, len(m.addr))
	for k, v := range m.addr {
		out[k] = v
	}
	return out, nil
}

func (m *memRaftStorage) Close() error { return nil }

// raftBackend bundles the persistence and the wire transport of a node;
// tests substitute in-memory implementations.
type raftBackend struct {
	st raftStorage
	tr wireTransport
}

const (
	// raftTick is the logical clock period handed to the raft node. With
	// ElectionTick=10 a election timeout is ~1s, heartbeat every ~100ms.
	raftTick = 100 * time.Millisecond
	// raftSnapshotThreshold: applied entries between snapshots.
	raftSnapshotThreshold = 1024
	// raftApplyWait bounds how long a write waits for its entry to be
	// applied locally (read-your-writes confirmation).
	raftApplyWait = 5 * time.Second
)

// ---------------------------------------------------------------------------
// bbolt-backed raft storage
// ---------------------------------------------------------------------------

var (
	bucketRaftLog   = []byte("raftlog")
	bucketRaftState = []byte("raftstate")
	bucketRaftSnap  = []byte("raftsnap")
	bucketRaftMeta  = []byte("raftmeta")
	metaApplied     = []byte("applied")
	metaAddrMap     = []byte("addrmap")
	metaConfState   = []byte("confstate")
)

// boltRaftStore persists the raft log, hard state and snapshots in a bbolt
// file. Entries are kept in memory as a sliding window (the log is compacted
// by application snapshots, so the window is bounded by raftSnapshotThreshold
// entries); term lookups use an in-memory index rebuilt at open.
type boltRaftStore struct {
	mu    sync.RWMutex
	db    *bolt.DB
	dir   string
	ms    *raft.MemoryStorage // authoritative in-memory view
	ents  []*raftpb.Entry     // entries after the snapshot, ents[i] at index first+i
	terms []uint64            // terms of ents
}

func openBoltRaftStore(dir string) (*boltRaftStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "raft.db"), 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketRaftLog, bucketRaftState, bucketRaftSnap, bucketRaftMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	bs := &boltRaftStore{db: db, dir: dir, ms: raft.NewMemoryStorage()}
	// Load the latest snapshot: bolt bucket first, legacy file second.
	var snap *raftpb.Snapshot
	if err := db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketRaftSnap).Get([]byte("snap")); v != nil {
			snap = &raftpb.Snapshot{}
			return proto.Unmarshal(v, snap)
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	if snap == nil {
		if data, err := os.ReadFile(filepath.Join(dir, "raft.snap")); err == nil {
			snap = &raftpb.Snapshot{}
			if err := proto.Unmarshal(data, snap); err != nil {
				snap = nil
			}
		}
	}
	if snap != nil && !raft.IsEmptySnap(snap) {
		if err := bs.ms.ApplySnapshot(snap); err != nil {
			db.Close()
			return nil, err
		}
	}
	// Restore the persisted hard state (term/vote/commit) into the
	// in-memory storage; raft refuses to restart with applied > commit.
	if err := db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketRaftState).Get([]byte("hs")); v != nil {
			hs := &raftpb.HardState{}
			if err := proto.Unmarshal(v, hs); err != nil {
				return err
			}
			return bs.ms.SetHardState(hs)
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	// Load log entries that follow the snapshot.
	type logEnt struct {
		idx uint64
		e   *raftpb.Entry
	}
	var logEnts []logEnt
	if err := db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRaftLog).ForEach(func(k, v []byte) error {
			idx := binary.BigEndian.Uint64(k)
			e := &raftpb.Entry{}
			if err := proto.Unmarshal(v, e); err != nil {
				return err
			}
			logEnts = append(logEnts, logEnt{idx, e})
			return nil
		})
	}); err != nil {
		db.Close()
		return nil, err
	}
	first := bs.firstIndex()
	for _, le := range logEnts {
		if le.idx >= first {
			bs.ents = append(bs.ents, le.e)
		}
	}
	bs.rebuildTerms()
	return bs, nil
}

// rebuildTerms recomputes the in-memory term index (caller holds write lock).
func (bs *boltRaftStore) rebuildTerms() {
	first := bs.firstIndex()
	t, err := bs.ms.Term(first - 1)
	if err != nil {
		// first-1 was compacted into the snapshot: its term is the
		// snapshot's term.
		if sn, serr := bs.ms.Snapshot(); serr == nil && !raft.IsEmptySnap(sn) && sn.GetMetadata().GetIndex() == first-1 {
			t = sn.GetMetadata().GetTerm()
		} else {
			bs.terms = nil
			return
		}
	}
	bs.terms = make([]uint64, 0, len(bs.ents)+1)
	bs.terms = append(bs.terms, t)
	for _, e := range bs.ents {
		bs.terms = append(bs.terms, e.GetTerm())
	}
}

// InitialState implements raft.Storage.
func (bs *boltRaftStore) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	hs, cs, err := bs.ms.InitialState()
	if err != nil {
		return nil, nil, err
	}
	// Prefer the persisted ConfState (it reflects the latest membership).
	if v := bs.metaGet(metaConfState); v != nil {
		pcs := &raftpb.ConfState{}
		if err := proto.Unmarshal(v, pcs); err == nil {
			cs = pcs
		}
	}
	return hs, cs, nil
}

func (bs *boltRaftStore) metaGet(key []byte) []byte {
	var out []byte
	_ = bs.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketRaftMeta).Get(key); v != nil {
			out = append([]byte(nil), v...)
		}
		return nil
	})
	return out
}

// Entries implements raft.Storage.
func (bs *boltRaftStore) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	first := bs.firstIndex()
	if lo < first || lo >= hi {
		if lo < first {
			return nil, raft.ErrCompacted
		}
		return nil, raft.ErrUnavailable
	}
	start := int(lo - first)
	if start >= len(bs.ents) {
		return nil, raft.ErrUnavailable
	}
	end := int(hi - first)
	if end > len(bs.ents) {
		return nil, raft.ErrUnavailable
	}
	var total uint64
	var out []*raftpb.Entry
	for _, e := range bs.ents[start:end] {
		sz := uint64(proto.Size(e))
		if len(out) > 0 && total+sz > maxSize {
			break
		}
		total += sz
		out = append(out, e)
	}
	return out, nil
}

// Term implements raft.Storage.
func (bs *boltRaftStore) Term(i uint64) (uint64, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	first := bs.firstIndex()
	if i == first-1 {
		return bs.ms.Term(i)
	}
	idx := int(i - first)
	if idx < 0 || idx >= len(bs.terms) {
		if i < first {
			return 0, raft.ErrCompacted
		}
		return 0, raft.ErrUnavailable
	}
	return bs.terms[idx+1], nil
}

// LastIndex implements raft.Storage.
func (bs *boltRaftStore) LastIndex() (uint64, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return uint64(len(bs.ents)) + bs.firstIndex() - 1, nil
}

// FirstIndex implements raft.Storage.
func (bs *boltRaftStore) FirstIndex() (uint64, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.firstIndex(), nil
}

// Snapshot implements raft.Storage.
func (bs *boltRaftStore) Snapshot() (*raftpb.Snapshot, error) {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bs.ms.Snapshot()
}

// Append persists new entries; called by the Ready loop before sending.
func (bs *boltRaftStore) Append(ents []*raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	bs.mu.Lock()
	defer bs.mu.Unlock()
	first := bs.firstIndex()
	start := ents[0].GetIndex()
	if start < first {
		return fmt.Errorf("raft storage: append at %d below first %d", start, first)
	}
	idx := int(start - first)
	if idx > len(bs.ents) {
		return fmt.Errorf("raft storage: append gap at %d (have %d entries from %d)", start, len(bs.ents), first)
	}
	bs.ents = append(bs.ents[:idx], ents...)
	bs.rebuildTerms()
	return bs.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketRaftLog)
		for _, e := range ents {
			v, err := proto.Marshal(e)
			if err != nil {
				return err
			}
			if err := b.Put(uint64Bytes(e.GetIndex()), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// SetHardState persists the vote/term/commit triple.
func (bs *boltRaftStore) SetHardState(hs *raftpb.HardState) error {
	v, err := proto.Marshal(hs)
	if err != nil {
		return err
	}
	return bs.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRaftState).Put([]byte("hs"), v)
	})
}

// ApplySnapshot installs an incoming snapshot (storage side).
func (bs *boltRaftStore) ApplySnapshot(snap *raftpb.Snapshot) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if err := bs.ms.ApplySnapshot(snap); err != nil {
		return err
	}
	bs.ents = nil
	bs.terms = nil
	return bs.db.Batch(func(tx *bolt.Tx) error {
		v, err := proto.Marshal(snap)
		if err != nil {
			return err
		}
		if err := tx.Bucket(bucketRaftSnap).Put([]byte("snap"), v); err != nil {
			return err
		}
		lc := tx.Bucket(bucketRaftLog).Cursor()
		limit := snap.GetMetadata().GetIndex()
		for k, _ := lc.First(); k != nil; k, _ = lc.Next() {
			if binary.BigEndian.Uint64(k) <= limit {
				if err := lc.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// CreateSnapshot builds a snapshot of the FSM state at index i.
func (bs *boltRaftStore) CreateSnapshot(i uint64, cs *raftpb.ConfState, data []byte) (*raftpb.Snapshot, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	snap, err := bs.ms.CreateSnapshot(i, cs, data)
	if err != nil {
		return nil, err
	}
	return snap, bs.db.Batch(func(tx *bolt.Tx) error {
		v, err := proto.Marshal(snap)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketRaftSnap).Put([]byte("snap"), v)
	})
}

// Compact truncates persisted entries below the snapshot index.
func (bs *boltRaftStore) Compact(i uint64) error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	first := bs.firstIndex()
	if i < first {
		return raft.ErrCompacted
	}
	if i > first+uint64(len(bs.ents)) {
		return fmt.Errorf("raft storage: compact index %d out of range", i)
	}
	drop := int(i - first)
	bs.ents = append([]*raftpb.Entry(nil), bs.ents[drop:]...)
	bs.rebuildTerms()
	return bs.db.Batch(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketRaftLog).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if binary.BigEndian.Uint64(k) < i {
				if err := c.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// firstIndex is the ms.FirstIndex convenience for callers already holding the lock.
func (bs *boltRaftStore) firstIndex() uint64 {
	fi, _ := bs.ms.FirstIndex()
	return fi
}

func (bs *boltRaftStore) appliedIndex() uint64 {
	var ap uint64
	_ = bs.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketRaftMeta).Get(metaApplied); v != nil && len(v) == 8 {
			ap = binary.BigEndian.Uint64(v)
		}
		return nil
	})
	return ap
}

func (bs *boltRaftStore) setAppliedIndex(i uint64) {
	_ = bs.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRaftMeta).Put(metaApplied, uint64Bytes(i))
	})
}

func (bs *boltRaftStore) saveConfState(cs *raftpb.ConfState) {
	if v, err := proto.Marshal(cs); err == nil {
		_ = bs.db.Batch(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketRaftMeta).Put(metaConfState, v)
		})
	}
}

// saveAddrMap persists the member-id -> address mapping.
func (bs *boltRaftStore) saveAddrMap(m map[string]string) error {
	v, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return bs.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRaftMeta).Put(metaAddrMap, v)
	})
}

// loadAddrMap returns the persisted mapping.
func (bs *boltRaftStore) loadAddrMap() (map[string]string, error) {
	var m map[string]string
	err := bs.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketRaftMeta).Get(metaAddrMap)
		if v == nil {
			return nil
		}
		return json.Unmarshal(v, &m)
	})
	return m, err
}

func (bs *boltRaftStore) Close() error { return bs.db.Close() }

// ---------------------------------------------------------------------------
// construction
// ---------------------------------------------------------------------------

// newRaftStore wires the FSM, backend and raft node. It does not start the
// run loop (see OpenRaft / test harness).
func newRaftStore(cfg RaftConfig, be raftBackend) (*RaftStore, error) {
	lg := cfg.Logger
	if lg == nil {
		lg = log.New(io.Discard, "", 0)
	}
	s := &RaftStore{
		cfg:      cfg,
		lg:       lg,
		bind:     cfg.BindAddr,
		rs:       be.st,
		tr:       be.tr,
		kvs:      make(map[string]record),
		notify:   make(chan struct{}),
		watchers: make(map[*raftWatcher]struct{}),
		pending:  make(map[uint64]chan *opResult),
		propc:    make(chan []byte),
		snapc:    make(chan *raftpb.Snapshot, 1),
		statusc:  make(chan chan raftStatus, 8),
		tick:     time.NewTicker(raftTick),
		stopc:    make(chan struct{}),
		donec:    make(chan struct{}),
		addr:     make(map[uint64]string),
	}
	// Seed the address map: persisted mapping first, configured peers on top.
	if m, err := be.st.loadAddrMap(); err == nil {
		for k, v := range m {
			if len(k) == 8 {
				s.addr[binary.BigEndian.Uint64([]byte(k))] = v
			}
		}
	}
	for _, p := range cfg.Peers {
		s.addr[memberID(p)] = p
	}
	applied := be.st.appliedIndex()
	s.soft.Store(&raftStatus{})

	rn, err := raft.NewRawNode(&raft.Config{
		ID:                       memberID(cfg.BindAddr),
		ElectionTick:             10,
		HeartbeatTick:            1,
		Storage:                  be.st,
		Applied:                  applied,
		AsyncStorageWrites:       false,
		CheckQuorum:              true,
		PreVote:                  true,
		MaxSizePerMsg:            1 << 20,
		MaxCommittedSizePerReady: 1 << 20,
		MaxInflightMsgs:          256,
		Logger:                   boltLogger{l: lg},
	})
	if err != nil {
		be.tr.Close()
		return nil, err
	}
	s.node = rn
	s.applied = applied
	return s, nil
}

// OpenRaft opens (and bootstraps if needed) a raft-backed meta store.
func OpenRaft(cfg RaftConfig) (*RaftStore, error) {
	if cfg.BindAddr == "" {
		return nil, errors.New("meta: raft store requires BindAddr")
	}
	if cfg.Dir == "" {
		return nil, errors.New("meta: raft store requires Dir")
	}
	bs, err := openBoltRaftStore(cfg.Dir)
	if err != nil {
		return nil, err
	}
	peers := make(map[uint64]string, len(cfg.Peers))
	for _, p := range cfg.Peers {
		peers[memberID(p)] = p
	}
	tr := newTCPTransport(memberID(cfg.BindAddr), cfg.BindAddr, peers, cfg.TLS)
	s, err := newRaftStore(cfg, raftBackend{st: bs, tr: tr})
	if err != nil {
		bs.Close()
		return nil, err
	}
	if err := s.start(); err != nil {
		tr.Close()
		bs.Close()
		return nil, err
	}
	return s, nil
}

// start bootstraps a fresh group if needed, restores the FSM from existing
// state, wires the transport and launches the run loop and write proxy.
func (s *RaftStore) start() error {
	last, err := s.rs.LastIndex()
	if err != nil {
		return err
	}
	hs, _, err := s.rs.InitialState()
	if err != nil {
		return err
	}
	snap, err := s.rs.Snapshot()
	if err != nil {
		return err
	}
	fresh := last == 0 && raft.IsEmptyHardState(hs) && raft.IsEmptySnap(snap)

	if fresh && len(s.cfg.Peers) > 0 {
		// Exactly one node (the lowest member id) bootstraps the group;
		// the others find it via the transport once it campaigns.
		minID, self := uint64(0), memberID(s.cfg.BindAddr)
		for _, p := range s.cfg.Peers {
			id := memberID(p)
			if minID == 0 || id < minID {
				minID = id
			}
		}
		if self == minID {
			peers := make([]raft.Peer, 0, len(s.cfg.Peers))
			for _, p := range s.cfg.Peers {
				peers = append(peers, raft.Peer{ID: memberID(p), Context: []byte(p)})
			}
			if err := s.node.Bootstrap(peers); err != nil {
				return fmt.Errorf("meta: raft bootstrap: %w", err)
			}
		}
	}

	// Restore the FSM from the snapshot plus the committed tail of the log.
	if err := s.restoreFSM(s.rs); err != nil {
		return err
	}

	// Transport: messages arrive on msgc, snapshots on snapc.
	if bbs, ok := s.rs.(*boltRaftStore); ok {
		s.closers = append(s.closers, bbs)
	}
	s.tr.SetSnapshotSink(func(sn *raftpb.Snapshot) {
		select {
		case s.snapc <- sn:
		case <-s.stopc:
		}
	})
	if err := s.tr.Start(); err != nil {
		return fmt.Errorf("meta: raft transport: %w", err)
	}
	if s.cfg.ProxyAddr != "" {
		if err := s.startProxy(s.cfg); err != nil {
			return err
		}
	}
	go s.run()
	return nil
}

// restoreFSM rebuilds the in-memory KV map from the snapshot and the
// committed log tail. Called once at startup, before the run loop exists.
func (s *RaftStore) restoreFSM(st raftStorage) error {
	snap, err := st.Snapshot()
	if err != nil && !errors.Is(err, raft.ErrUnavailable) {
		return err
	}
	snapIdx := uint64(0)
	if snap != nil && !raft.IsEmptySnap(snap) {
		snapIdx = snap.GetMetadata().GetIndex()
		if err := s.loadSnapshotData(snap.GetData()); err != nil {
			return err
		}
	}
	s.snapBase = snapIdx
	applied := s.applied
	if applied < snapIdx {
		applied = snapIdx
	}
	for idx := snapIdx + 1; idx <= applied; idx++ {
		ents, err := st.Entries(idx, idx+1, 1<<20)
		if err != nil || len(ents) == 0 {
			continue
		}
		for _, e := range ents {
			if e.GetType() == raftpb.EntryType_EntryNormal {
				_ = s.applyEntry(e.GetData())
			}
		}
	}
	s.applied = applied
	return nil
}

// ---------------------------------------------------------------------------
// run loop: the single owner of the raft node
// ---------------------------------------------------------------------------

func (s *RaftStore) run() {
	defer close(s.donec)
	for {
		select {
		case <-s.tick.C:
			s.node.Tick()
		case m := <-s.tr.MsgC():
			_ = s.node.Step(m)
		case sn := <-s.snapc:
			s.persistIncomingSnapshot(sn)
		case data := <-s.propc:
			s.handlePropose(data)
		case ch := <-s.statusc:
			st := s.node.BasicStatus()
			ch <- raftStatus{lead: st.Lead, state: st.RaftState}
		case <-s.stopc:
			s.shutdown()
			return
		}
		for s.node.HasReady() {
			s.handleReady(s.node.Ready())
		}
	}
}

// shutdown flushes a final Ready cycle (hard state, e.g. a vote cast in a
// term the peer's Append entries are not needed).
func (s *RaftStore) shutdown() {
	s.tick.Stop()
	if s.node.HasReady() {
		rd := s.node.Ready()
		if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
			_ = s.rs.SetHardState(rd.HardState)
		}
		if err := s.rs.Append(rd.Entries); err != nil {
			s.lg.Printf("raft: final append: %v", err)
		}
		s.node.Advance(rd)
	}
}

// handlePropose submits a client proposal; the caller's result channel is
// resolved by the FSM when the entry is applied (leader side).
func (s *RaftStore) handlePropose(data []byte) {
	if err := s.node.Propose(data); err != nil {
		var o op
		if json.Unmarshal(data, &o) == nil && o.CID != 0 {
			s.resolve(o.CID, &opResult{Err: opErrNoQuorum})
		}
	}
}

// resolve delivers a result to a waiting proposer, if any.
func (s *RaftStore) resolve(cid uint64, res *opResult) {
	s.propMu.Lock()
	ch := s.pending[cid]
	delete(s.pending, cid)
	s.propMu.Unlock()
	if ch != nil {
		select {
		case ch <- res:
		default:
		}
	}
}

func (s *RaftStore) handleReady(rd raft.Ready) {
	// 1. Persist hard state and new entries before any messages go out.
	if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
		if err := s.rs.SetHardState(rd.HardState); err != nil {
			s.lg.Printf("raft: SetHardState: %v", err)
		}
	}
	if err := s.rs.Append(rd.Entries); err != nil {
		s.lg.Printf("raft: Append: %v", err)
	}
	// A snapshot from Ready (incoming transfer) is persisted by the
	// transport sink path; rd.Snapshot is handled below as a safety net.
	if rd.Snapshot != nil && !raft.IsEmptySnap(rd.Snapshot) {
		s.persistIncomingSnapshot(rd.Snapshot)
	}
	// 2. Send messages.
	for i := range rd.Messages {
		m := rd.Messages[i]
		if m.GetTo() == 0 {
			continue
		}
		if m.GetType() == raftpb.MessageType_MsgSnap {
			id := m.GetTo()
			snap := m.GetSnapshot()
			go func() {
				if err := s.tr.SendSnapshot(id, snap); err != nil {
					s.lg.Printf("raft: snapshot send to %x: %v", id, err)
				}
			}()
			continue
		}
		s.tr.Send(m.GetTo(), m)
	}
	// 3. Apply committed entries.
	for _, e := range rd.CommittedEntries {
		if e.GetIndex() <= s.applied {
			continue // already applied (restore path)
		}
		switch e.GetType() {
		case raftpb.EntryType_EntryConfChange, raftpb.EntryType_EntryConfChangeV2:
			cs := s.node.ApplyConfChange(entryConfChange(e))
			s.rs.saveConfState(cs)
			s.wireMembership(e, cs)
		default:
			s.applyCommitted(e)
		}
		s.applied = e.GetIndex()
	}
	if len(rd.CommittedEntries) > 0 {
		s.notifyApplied()
		s.rs.setAppliedIndex(s.applied)
	}
	// 4. Application-driven snapshots.
	if s.applied > s.snapBase+raftSnapshotThreshold {
		if err := s.makeSnapshot(); err != nil {
			s.lg.Printf("raft: snapshot: %v", err)
		}
	}
	// 5. Soft state for external observers.
	if rd.SoftState != nil {
		s.soft.Store(&raftStatus{lead: rd.SoftState.Lead, state: rd.SoftState.RaftState})
	}
	s.node.Advance(rd)
}

// applyCommitted decodes one normal entry, applies it to the FSM and
// resolves the proposer.
func (s *RaftStore) applyCommitted(e *raftpb.Entry) {
	var o op
	if err := json.Unmarshal(e.GetData(), &o); err != nil {
		return
	}
	res := s.applyEntry(e.GetData())
	res.Idx = e.GetIndex()
	if o.CID != 0 {
		s.resolve(o.CID, res)
	}
}

// entryConfChange decodes the ConfChange carried by a conf-change entry so
// it can be handed to RawNode.ApplyConfChange.
func entryConfChange(e *raftpb.Entry) raftpb.ConfChangeI {
	if e.GetType() == raftpb.EntryType_EntryConfChangeV2 {
		cc := &raftpb.ConfChangeV2{}
		if proto.Unmarshal(e.GetData(), cc) == nil {
			return cc
		}
		return &raftpb.ConfChangeV2{}
	}
	cc1 := &raftpb.ConfChange{}
	if proto.Unmarshal(e.GetData(), cc1) == nil {
		return cc1.AsV2()
	}
	return &raftpb.ConfChangeV2{}
}

// wireMembership reacts to a conf change: remembers addresses carried in
// the ConfChange Context and wires the transport peers.
func (s *RaftStore) wireMembership(e *raftpb.Entry, cs *raftpb.ConfState) {
	cc := entryConfChange(e).AsV2()
	inCS := func(id uint64) bool {
		for _, v := range cs.GetVoters() {
			if v == id {
				return true
			}
		}
		return false
	}
	for _, c := range cc.Changes {
		if c.GetType() == raftpb.ConfChangeType_ConfChangeAddNode && len(cc.GetContext()) > 0 {
			addr := string(cc.GetContext())
			s.setAddr(c.GetNodeId(), addr)
			s.tr.SetPeer(c.GetNodeId(), addr)
		} else if inCS(c.GetNodeId()) {
			if a := s.addrOf(c.GetNodeId()); a != "" {
				s.tr.SetPeer(c.GetNodeId(), a)
			}
		}
	}
	for _, id := range cs.GetVoters() {
		if a := s.addrOf(id); a != "" {
			s.tr.SetPeer(id, a)
		}
	}
}

// persistIncomingSnapshot installs a snapshot received over the transport.
// The raft node picks it up from storage on the next Ready cycle.
func (s *RaftStore) persistIncomingSnapshot(sn *raftpb.Snapshot) {
	if sn == nil || raft.IsEmptySnap(sn) {
		return
	}
	if err := s.rs.ApplySnapshot(sn); err != nil {
		s.lg.Printf("raft: ApplySnapshot: %v", err)
		return
	}
	if err := s.restoreFSMFromSnapshot(sn); err != nil {
		s.lg.Printf("raft: snapshot restore: %v", err)
	}
}

// restoreFSMFromSnapshot rebuilds the KV map from a received snapshot.
func (s *RaftStore) restoreFSMFromSnapshot(sn *raftpb.Snapshot) error {
	idx := sn.GetMetadata().GetIndex()
	s.mu.Lock()
	if idx <= s.applied {
		s.mu.Unlock()
		return nil // stale snapshot
	}
	var sk snapKV
	if err := json.Unmarshal(sn.GetData(), &sk); err != nil {
		s.mu.Unlock()
		return err
	}
	s.kvs = make(map[string]record, len(sk.KVs))
	for _, kv := range sk.KVs {
		s.kvs[kv.Key] = record{Rev: kv.Rev, Value: kv.Value}
	}
	if sk.Rev > s.rev {
		s.rev = sk.Rev
	}
	s.applied = idx
	s.snapBase = idx
	s.mu.Unlock()
	// Notifications take their own read locks, so they must run with s.mu
	// released.
	s.rs.setAppliedIndex(idx)
	s.notifyApplied()
	s.notifyWatchers()
	return nil
}

// snapKV is the serialized FSM state stored in a raft snapshot.
type snapKV struct {
	Rev     int64         `json:"rev"`
	Applied uint64        `json:"applied"`
	KVs     []snapEntryKV `json:"kvs"`
}

type snapEntryKV struct {
	Key   string `json:"key"`
	Rev   int64  `json:"rev"`
	Value []byte `json:"val"`
}

// makeSnapshot serializes the FSM state into a raft snapshot and compacts
// the log up to the applied index.
func (s *RaftStore) makeSnapshot() error {
	s.mu.RLock()
	sk := snapKV{Rev: s.rev, Applied: s.applied, KVs: make([]snapEntryKV, 0, len(s.kvs))}
	for k, rec := range s.kvs {
		sk.KVs = append(sk.KVs, snapEntryKV{Key: k, Rev: rec.Rev, Value: rec.Value})
	}
	s.mu.RUnlock()
	sort.Slice(sk.KVs, func(i, j int) bool { return sk.KVs[i].Key < sk.KVs[j].Key })
	data, err := json.Marshal(&sk)
	if err != nil {
		return err
	}
	cs := s.lastConfState()
	if _, err := s.rs.CreateSnapshot(s.applied, cs, data); err != nil {
		return err
	}
	if err := s.rs.Compact(s.applied); err != nil {
		return err
	}
	s.snapBase = s.applied
	return nil
}

// lastConfState reconstructs the current membership ConfState from the
// persisted copy (updated on every conf change apply).
func (s *RaftStore) lastConfState() *raftpb.ConfState {
	if v := s.rs.metaGet(metaConfState); v != nil {
		cs := &raftpb.ConfState{}
		if proto.Unmarshal(v, cs) == nil {
			return cs
		}
	}
	return &raftpb.ConfState{}
}

// ---------------------------------------------------------------------------
// FSM: applying ops to the KV map
// ---------------------------------------------------------------------------

// applyEntry executes one op against the in-memory KV map. It runs only on
// the run-loop goroutine (the apply path).
func (s *RaftStore) applyEntry(data []byte) *opResult {
	var o op
	if err := json.Unmarshal(data, &o); err != nil {
		return &opResult{Err: opErrBadOp}
	}
	prefix, key, ok := splitKey(o.Key)
	if !ok {
		return &opResult{Err: opErrBadOp}
	}
	s.mu.Lock()
	switch o.Kind {
	case "put":
		cur, have := s.kvs[o.Key]
		switch {
		case o.CAS == 0 && have:
			s.mu.Unlock()
			return &opResult{Err: opErrKeyExists}
		case o.CAS > 0 && (!have || cur.Rev != o.CAS):
			s.mu.Unlock()
			return &opResult{Err: opErrConflict}
		}
		s.rev++
		s.kvs[o.Key] = record{Rev: s.rev, Value: o.Val}
		_ = prefix
		_ = key
	case "delete":
		if _, have := s.kvs[o.Key]; !have {
			s.mu.Unlock()
			return &opResult{Err: opErrNotFound}
		}
		cur := s.kvs[o.Key]
		if o.CAS > 0 && cur.Rev != o.CAS {
			s.mu.Unlock()
			return &opResult{Err: opErrConflict}
		}
		delete(s.kvs, o.Key)
	default:
		s.mu.Unlock()
		return &opResult{Err: opErrBadOp}
	}
	s.mu.Unlock()
	s.notifyWatchers()
	if o.Kind == "put" {
		s.mu.RLock()
		rev := s.kvs[o.Key].Rev
		s.mu.RUnlock()
		return &opResult{Rev: rev}
	}
	return &opResult{}
}

// splitKey validates a composite "prefix/key" key. The key must contain at
// least one "/" and must not contain NUL bytes; both parts must be non-empty.
func splitKey(key string) (prefix, rest string, ok bool) {
	if key == "" || strings.ContainsRune(key, 0) {
		return "", "", false
	}
	i := strings.IndexByte(key, '/')
	if i <= 0 || i == len(key)-1 {
		return "", "", false
	}
	return key[:i], key[i+1:], true
}

// loadSnapshotData restores the KV map from snapshot payload (startup path).
func (s *RaftStore) loadSnapshotData(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	var sk snapKV
	if err := json.Unmarshal(data, &sk); err != nil {
		return err
	}
	s.kvs = make(map[string]record, len(sk.KVs))
	for _, kv := range sk.KVs {
		s.kvs[kv.Key] = record{Rev: kv.Rev, Value: kv.Value}
	}
	if sk.Rev > s.rev {
		s.rev = sk.Rev
	}
	return nil
}

// notifyWatchers wakes all watchers; it snapshots the set under RLock and
// pings each watcher's capacity-1 channel without blocking.
func (s *RaftStore) notifyWatchers() {
	s.mu.RLock()
	for w := range s.watchers {
		select {
		case w.notify <- struct{}{}:
		default:
		}
	}
	s.mu.RUnlock()
}

// notifyApplied signals read-your-writes waiters that s.applied advanced.
func (s *RaftStore) notifyApplied() {
	for {
		s.mu.Lock()
		ch := s.notify
		s.mu.Unlock()
		select {
		case ch <- struct{}{}:
			return
		default:
			// channel already has a token, or was swapped: swap a fresh one
			s.mu.Lock()
			if s.notify == ch {
				s.notify = make(chan struct{})
			}
			s.mu.Unlock()
			close(ch)
			return
		}
	}
}

// waitApplied blocks until the store has applied at least idx.
func (s *RaftStore) waitApplied(idx uint64) error {
	deadline := time.After(raftApplyWait)
	for {
		s.mu.RLock()
		applied := s.applied
		ch := s.notify
		s.mu.RUnlock()
		if applied >= idx {
			return nil
		}
		select {
		case <-ch:
		case <-deadline:
			return fmt.Errorf("meta: raft apply of index %d timed out", idx)
		case <-s.stopc:
			return ErrClosed
		}
	}
}

// ---------------------------------------------------------------------------
// Store interface
// ---------------------------------------------------------------------------

// Get reads from the local replica (no raft round-trip: reads stay alive
// when the group is leaderless, at the cost of staleness on followers).
func (s *RaftStore) Get(_ context.Context, prefix, key string) ([]byte, int64, error) {
	full := prefix + "/" + key
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.kvs[full]
	if !ok {
		return nil, 0, ErrNotFound
	}
	return append([]byte(nil), rec.Value...), rec.Rev, nil
}

// List returns the keys stored under prefix (without the prefix), sorted.
func (s *RaftStore) List(_ context.Context, prefix string) ([]string, error) {
	if prefix == "" {
		return nil, ErrInvalidPrefix
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	pre := prefix + "/"
	var out []string
	for k := range s.kvs {
		if strings.HasPrefix(k, pre) {
			out = append(out, k[len(pre):])
		}
	}
	sort.Strings(out)
	return out, nil
}

// Put writes through raft (CAS: -1 any, 0 must-not-exist, n must-exist@n).
func (s *RaftStore) Put(_ context.Context, prefix, key string, value []byte, cas int64) (int64, error) {
	if prefix == "" || key == "" || strings.ContainsRune(key, 0) {
		return 0, fmt.Errorf("meta: invalid key %q/%q", prefix, key)
	}
	res, err := s.apply(op{Kind: "put", Put: true, Key: prefix + "/" + key, Val: value, CAS: cas})
	if err != nil {
		return 0, err
	}
	return res.Rev, nil
}

// Delete removes through raft.
func (s *RaftStore) Delete(_ context.Context, prefix, key string, cas int64) error {
	if prefix == "" || key == "" || strings.ContainsRune(key, 0) {
		return fmt.Errorf("meta: invalid key %q/%q", prefix, key)
	}
	_, err := s.apply(op{Kind: "delete", Key: prefix + "/" + key, CAS: cas})
	return err
}

var _ Store = (*RaftStore)(nil)

// apply proposes an op to raft and waits for its result and local apply.
func (s *RaftStore) apply(o op) (*opResult, error) {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, ErrClosed
	}
	if _, _, ok := splitKey(o.Key); !ok {
		return nil, fmt.Errorf("meta: invalid key %q", o.Key)
	}

	attempt := 0
	for {
		res, err := s.proposeOnce(o)
		if err == nil {
			if res.Err != "" {
				return res, opError(res.Err)
			}
			if res.Idx > 0 {
				if err := s.waitApplied(res.Idx); err != nil {
					return nil, err
				}
			}
			return res, nil
		}
		if !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNoQuorum) {
			return nil, err
		}
		attempt++
		if attempt == 1 {
			if pres, ok, perr := s.proxyForward(o, s.Leader()); ok {
				if perr != nil {
					return nil, perr
				}
				if pres.Err != "" {
					return pres, opError(pres.Err)
				}
				// Read-your-writes: the leader applied the entry at
				// pres.Idx; wait for this node's FSM to catch up so a
				// local read right afterwards sees the write.
				if pres.Idx > 0 {
					if err := s.waitApplied(pres.Idx); err != nil {
						return nil, err
					}
				}
				return pres, nil
			}
		}
		if attempt >= 20 { // ~3s at 150ms
			return nil, err
		}
		select {
		case <-time.After(150 * time.Millisecond):
		case <-s.stopc:
			return nil, ErrClosed
		}
	}
}

// proposeOnce runs one proposal attempt: local if leader, ErrNotLeader
// otherwise.
func (s *RaftStore) proposeOnce(o op) (*opResult, error) {
	st := s.currentStatus()
	switch {
	case st.state == raft.StateLeader && st.lead == memberID(s.bind):
		// We are the leader: propose below.
	case st.state == raft.StateCandidate:
		// Mid-election: nothing can commit right now.
		return nil, ErrNoQuorum
	case st.state == raft.StateLeader || st.lead == raft.None:
		// Leaderless (quorum loss) or stale self-view.
		return nil, ErrNoQuorum
	default:
		return nil, ErrNotLeader
	}
	o.CID = s.nextCID.Add(1)
	ch := make(chan *opResult, 1)
	s.propMu.Lock()
	if s.pending == nil {
		s.pending = make(map[uint64]chan *opResult)
	}
	s.pending[o.CID] = ch
	s.propMu.Unlock()
	data, err := json.Marshal(&o)
	if err != nil {
		s.resolve(o.CID, nil)
		return nil, err
	}
	select {
	case s.propc <- data:
	case <-s.stopc:
		s.resolve(o.CID, nil)
		return nil, ErrClosed
	}
	select {
	case res := <-ch:
		if res == nil {
			return nil, ErrNoQuorum
		}
		return res, nil
	case <-time.After(raftApplyTimeout):
		s.resolve(o.CID, nil)
		return nil, ErrNoQuorum
	case <-s.stopc:
		s.resolve(o.CID, nil)
		return nil, ErrClosed
	}
}

// opError maps wire error codes to sentinel errors.
func opError(code string) error {
	switch code {
	case opErrNotFound:
		return ErrNotFound
	case opErrConflict:
		return ErrConflict
	case opErrKeyExists:
		return ErrKeyExists
	case opErrNotLeader:
		return ErrNotLeader
	case opErrNoQuorum:
		return ErrNoQuorum
	default:
		return fmt.Errorf("meta: raft apply: %s", code)
	}
}

// ---------------------------------------------------------------------------
// status / leadership
// ---------------------------------------------------------------------------

// currentStatus returns the latest known raft status.
func (s *RaftStore) currentStatus() raftStatus {
	ch := make(chan raftStatus, 1)
	select {
	case s.statusc <- ch:
		select {
		case st := <-ch:
			return st
		case <-time.After(2 * time.Second):
		}
	case <-time.After(2 * time.Second):
	case <-s.stopc:
	}
	if p := s.soft.Load(); p != nil {
		return *p
	}
	return raftStatus{}
}

// Leader returns the raft address of the current leader, "" if none.
func (s *RaftStore) Leader() string {
	st := s.currentStatus()
	if st.lead == 0 {
		return ""
	}
	return s.addrOf(st.lead)
}

// RaftState returns the raft state of this node: "Leader", "Follower",
// "Candidate", "PreCandidate".
func (s *RaftStore) RaftState() string {
	switch s.currentStatus().state {
	case raft.StateLeader:
		return "Leader"
	case raft.StateCandidate:
		return "Candidate"
	case raft.StatePreCandidate:
		return "PreCandidate"
	default:
		return "Follower"
	}
}

// RaftMember is one voter of the meta raft group.
type RaftMember struct {
	ID     uint64 // raft member id
	Addr   string // transport address ("" until learned)
	Leader bool   // the member currently leading
}

// RaftMembers lists the raft voters with the current leader flagged —
// the metrics view of the control plane (vitastor_monitor_info style).
// The voter set is the persisted ConfState; addresses come from the
// membership map the run loop maintains.
func (s *RaftStore) RaftMembers() []RaftMember {
	st := s.currentStatus()
	cs := s.lastConfState()
	out := make([]RaftMember, 0, len(cs.Voters))
	for _, id := range cs.Voters {
		out = append(out, RaftMember{ID: id, Addr: s.addrOf(id), Leader: id == st.lead})
	}
	return out
}

// isLeaderless reports whether the group currently has no leader.
func (s *RaftStore) isLeaderless() bool {
	p := s.soft.Load()
	return p == nil || p.lead == 0
}

// ---------------------------------------------------------------------------
// watch / close
// ---------------------------------------------------------------------------

// Watch streams events for keys under prefix, starting from the current
// revision.
func (s *RaftStore) Watch(ctx context.Context, prefix string) (Watcher, error) {
	if prefix == "" {
		return nil, ErrInvalidPrefix
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	w := &raftWatcher{
		prefix: prefix,
		rev:    s.rev,
		seen:   map[string]int64{},
		events: make(chan Event, 64),
		notify: make(chan struct{}, 1),
		stop:   make(chan struct{}),
	}
	// Prime the baseline so the first poll only reports real changes.
	pre := prefix + "/"
	for k, rec := range s.kvs {
		if strings.HasPrefix(k, pre) {
			w.seen[k] = rec.Rev
		}
	}
	s.watchers[w] = struct{}{}
	s.mu.Unlock()
	go s.driveWatcher(w)
	return w, nil
}

// driveWatcher polls KV revisions for one watcher (the FSM mutates in place;
// polling by revision keeps the FSM apply path allocation-free).
func (s *RaftStore) driveWatcher(w *raftWatcher) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-s.stopc:
			return
		case <-w.notify:
			s.pollWatcher(w)
		case <-ticker.C:
			s.pollWatcher(w)
		}
	}
}

// pollWatcher emits events for keys under w.prefix that changed since the
// last poll (new/higher revision -> put; disappeared -> delete).
func (s *RaftStore) pollWatcher(w *raftWatcher) {
	pre := w.prefix + "/"
	s.mu.RLock()
	cur := make(map[string]int64)
	for k, rec := range s.kvs {
		if strings.HasPrefix(k, pre) {
			cur[k] = rec.Rev
		}
	}
	s.mu.RUnlock()

	var out []Event
	for k, rev := range cur {
		if old, had := w.seen[k]; !had || rev > old {
			out = append(out, Event{Type: "put", Key: k[len(pre):], Rev: rev})
		}
	}
	for k := range w.seen {
		if _, still := cur[k]; !still {
			out = append(out, Event{Type: "delete", Key: k[len(pre):]})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rev != out[j].Rev {
			return out[i].Rev < out[j].Rev
		}
		return out[i].Key < out[j].Key
	})
	w.seen = cur
	for _, ev := range out {
		select {
		case w.events <- ev:
		case <-w.stop:
			return
		case <-s.stopc:
			return
		}
	}
}

// Events implements Watcher.
func (w *raftWatcher) Events() <-chan Event { return w.events }

// Stop implements Watcher; safe to call multiple times.
func (w *raftWatcher) Stop() {
	w.once.Do(func() { close(w.stop) })
}

// Close shuts the store down: the run loop is drained, transport and
// storage closed. Idempotent.
func (s *RaftStore) Close() error {
	var err error
	s.closeOne.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.stopc)
		select {
		case <-s.donec:
		case <-time.After(10 * time.Second):
		}
		// Wake any waiters.
		s.notifyApplied()
		s.mu.Lock()
		watchers := make([]*raftWatcher, 0, len(s.watchers))
		for w := range s.watchers {
			watchers = append(watchers, w)
		}
		s.watchers = make(map[*raftWatcher]struct{})
		s.mu.Unlock()
		for _, w := range watchers {
			w.Stop()
		}
		s.propMu.Lock()
		for cid, ch := range s.pending {
			close(ch)
			delete(s.pending, cid)
		}
		s.propMu.Unlock()
		if terr := s.tr.Close(); terr != nil {
			err = terr
		}
		for _, c := range s.closers {
			if cerr := c.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
	})
	return err
}
