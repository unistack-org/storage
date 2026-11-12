// Package osd implements the OSD daemon: an sdsnet server over a local
// object store, registered in the cluster metadata, fenced by crush-map
// epochs.
//
// Each OSD owns one store directory. Exports are named by object
// namespace (volume name); a volume's objects live on the OSDs the
// crush map selects for its PGs. Epoch fencing (sdsnet.SetMinEpoch)
// rejects writes from clients working off a stale map.
package osd

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/qos"
	"github.com/unistack-org/storage/pkg/sdsnet"
	"github.com/unistack-org/storage/pkg/store"
)

// Options configures a Daemon.
type Options struct {
	// ID is the crush OSD id this process owns.
	ID int32
	// Addr is the listen address ("host:port"). Port 0 is allowed;
	// Addr() reports the resolved one.
	Addr string
	// CtrlAddr, when set, is a second listen address served by the
	// same sdsnet server for control traffic (pings, StatFS): operators
	// bind it to a management NIC so cluster health survives a
	// saturated data network. Port 0 is allowed; CtrlAddr() reports the
	// resolved one. Empty = single plane (control shares Addr).
	CtrlAddr string
	// DataDir holds the object store (one directory per OSD).
	DataDir string
	// Meta is the cluster metadata store.
	Meta meta.Store
	// Direct / Preallocate are passed through to the object store.
	Direct      bool
	Preallocate bool
	// CommitMode / FsyncBatch are passed through to the object store:
	// how writes reach durable media (see store.CommitMode) and how
	// concurrent Sync() calls coalesce into one fsync pass.
	CommitMode store.CommitMode
	FsyncBatch time.Duration
	// Intent turns on the write intent log of the object store
	// (vitastor-ideas §2.1); ignored in journal commit mode.
	Intent bool
	// AtomicWrites opts the object store into RWF_ATOMIC writes when
	// the disk publishes an atomic write unit (see store.Options).
	AtomicWrites bool
	// Aggregate caps this disk (one store directory): every op from
	// every client/export passes it before touching the store. This is
	// the "limit iops on the NVMe" knob that keeps one volume from
	// starving its neighbours (nil = unlimited).
	Aggregate *qos.Pair
	// Limits is consulted per connection handshake: it returns the
	// limiter pair for an export name (nil = unlimited). In `-export`
	// mode (one named volume per sdsnet server) it enforces the
	// per-volume class at the disk; in cluster mode exports share the
	// anonymous name and per-volume limits live in the clients.
	Limits func(export string) *qos.Pair
	// TLS, when set, serves the sdsnet data plane over mutual TLS
	// (tlsconf.Config.ServerConfig); clients must then present
	// certificates signed by the same CA.
	TLS *tls.Config
	// Metrics, when set, is attached to the OSD's sdsnet server: the
	// exporter seam for per-op request/conn counters (see
	// sdsnet.ServerMetrics). The binary wires it to its metrics
	// library; the OSD core stays dependency-free.
	Metrics *sdsnet.ServerMetrics
	// Logf receives informational messages (nil = silent).
	Logf func(format string, args ...any)
}

// Daemon is a running OSD.
type Daemon struct {
	opts Options
	srv  *sdsnet.Server
	addr string       // resolved listen address (set by Run)
	ctrl string       // resolved control listen address (set by Run, "" = single plane)
	st   *store.Store // object store (set by Run)

	mu       sync.Mutex
	uuid     string
	watcher  meta.Watcher
	quit     chan struct{}
	wg       sync.WaitGroup
	lastSeen map[string]time.Time // export name -> last activity (stats hook)
}

// New prepares an OSD daemon. The object store and listen socket are
// opened by Run.
func New(opts Options) (*Daemon, error) {
	if opts.Meta == nil {
		return nil, errors.New("osd: Meta required")
	}
	if opts.ID < 0 {
		return nil, fmt.Errorf("osd: invalid id %d", opts.ID)
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	return &Daemon{opts: opts, quit: make(chan struct{}), lastSeen: map[string]time.Time{}}, nil
}

// Run serves until ctx is cancelled or Close is called. It registers the
// OSD in meta, keeps the registration fresh, and maintains the epoch
// fence from the latest crush map.
func (d *Daemon) Run(ctx context.Context) error {
	if err := os.MkdirAll(d.opts.DataDir, 0o755); err != nil {
		return err
	}
	st, err := store.New(d.opts.DataDir, store.Options{
		Direct:      d.opts.Direct,
		Preallocate: d.opts.Preallocate,
		CommitMode:  d.opts.CommitMode,
		FsyncBatch:  d.opts.FsyncBatch,
		Intent:      d.opts.Intent,
	})
	if err != nil {
		return fmt.Errorf("osd: store: %w", err)
	}
	defer st.Close()

	// Bind the listen socket up front (opts.Addr may use port 0) so the
	// meta registration advertises the exact address we will serve on.
	probe, err := net.Listen("tcp", d.opts.Addr)
	if err != nil {
		return fmt.Errorf("osd: listen: %w", err)
	}
	defer probe.Close()
	addr := probe.Addr().String()

	// Optional control-plane listener: the same sdsnet.Server serves it,
	// so ping/StatFS answered there are identical to the data plane —
	// only the network path differs.
	var ctrlLn net.Listener
	ctrlAddr := ""
	if d.opts.CtrlAddr != "" {
		ctrlLn, err = net.Listen("tcp", d.opts.CtrlAddr)
		if err != nil {
			return fmt.Errorf("osd: ctrl listen: %w", err)
		}
		defer ctrlLn.Close()
		ctrlAddr = ctrlLn.Addr().String()
	}

	d.mu.Lock()
	d.addr = addr
	d.ctrl = ctrlAddr
	d.st = st
	d.mu.Unlock()

	srv := &sdsnet.Server{
		Backend:   &objectBackend{st: st},
		Logf:      d.opts.Logf,
		Aggregate: d.opts.Aggregate,
		Limits:    d.opts.Limits,
		TLS:       d.opts.TLS,
		Metrics:   d.opts.Metrics,
	}
	d.mu.Lock()
	d.srv = srv
	d.mu.Unlock()

	// Stable per-OSD uuid.
	if err := d.loadOrCreateUUID(); err != nil {
		return err
	}

	// Registration is a meta write: on a raft cluster this node may be a
	// follower (writes go to the leader) and the leader may not be
	// elected yet at boot. Serve the data plane immediately and register
	// in the background with a retry, so an OSD on a node that is not
	// the leader still joins as soon as the write succeeds.
	go func() {
		for {
			err := d.register()
			if err == nil {
				return
			}
			if meta.IsReadOnly(err) {
				// No quorum / not the leader: keep trying, this is
				// expected before the cluster has a leader.
			} else {
				d.opts.Logf("osd: register: %v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()

	// Epoch fence from the map epoch: bump MinEpoch whenever a newer
	// map lands in meta.
	mapW, err := d.opts.Meta.Watch(ctx, meta.PrefixMaps)
	if err != nil {
		return err
	}
	defer mapW.Stop()

	// Buffer for every listener's Serve error (data + optional ctrl).
	serveErr := make(chan error, 2)
	go func() { serveErr <- srv.ServeListener(probe) }()
	if ctrlLn != nil {
		go func() { serveErr <- srv.ServeListener(ctrlLn) }()
	}

	if err := d.applyLatestEpoch(ctx); err != nil {
		d.opts.Logf("osd: initial epoch: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			// Graceful drain first: parked clients are closed, a
			// request in flight finishes; the deadline bounds it.
			sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := srv.Shutdown(sctx); err != nil {
				d.opts.Logf("osd: shutdown: %v", err)
			}
			scancel()
			d.Close()
			return ctx.Err()
		case <-d.quit:
			return nil
		case err := <-serveErr:
			return err
		case ev := <-mapW.Events():
			if ev.Type != "put" {
				continue
			}
			if err := d.applyLatestEpoch(ctx); err != nil {
				d.opts.Logf("osd: epoch update: %v", err)
			}
		}
	}
}

// applyLatestEpoch reads the newest crush map and raises the fence.
func (d *Daemon) applyLatestEpoch(ctx context.Context) error {
	rec, err := meta.LatestMap(ctx, d.opts.Meta)
	if errors.Is(err, meta.ErrNotFound) {
		return nil // no maps yet: accept everything
	}
	if err != nil {
		return err
	}
	d.mu.Lock()
	srv := d.srv
	d.mu.Unlock()
	if srv == nil {
		return nil
	}
	if rec.Epoch > srv.MinEpoch() {
		srv.SetMinEpoch(rec.Epoch)
		d.opts.Logf("osd: fence raised to epoch %d", rec.Epoch)
	}
	return nil
}

// register writes this OSD's OSDInfo into meta. A restart must not undo
// an "out" marking: if a record already exists we keep its weight and
// UUID and only refresh the address/boot time. A brand-new OSD boots at
// weight 1 (in).
func (d *Daemon) register() error {
	d.mu.Lock()
	addr := d.addr
	if addr == "" {
		addr = d.opts.Addr
	}
	ctrl := d.ctrl
	uuid := d.uuid
	d.mu.Unlock()
	info := meta.OSDInfo{
		ID:       d.opts.ID,
		Addr:     addr,
		CtrlAddr: ctrl,
		UUID:     uuid,
		DataPath: d.opts.DataDir,
		Weight:   1,
		BootedAt: time.Now().Unix(),
	}
	if prev, err := meta.GetOSD(context.Background(), d.opts.Meta, d.opts.ID); err == nil {
		// Preserve the operator's weight (an out=0 OSD stays out across
		// restarts) and the stable UUID; refresh the rest.
		info.Weight = prev.Weight
		if prev.UUID != "" {
			info.UUID = prev.UUID
		}
	} else if !errors.Is(err, meta.ErrNotFound) {
		return err
	}
	_, err := meta.PutOSD(context.Background(), d.opts.Meta, info)
	return err
}

func (d *Daemon) loadOrCreateUUID() error {
	p := filepath.Join(d.opts.DataDir, "uuid")
	b, err := os.ReadFile(p)
	if err == nil && len(b) > 0 {
		d.mu.Lock()
		d.uuid = string(b)
		d.mu.Unlock()
		return nil
	}
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return err
	}
	uuid := hex.EncodeToString(raw[:])
	if err := os.WriteFile(p, []byte(uuid), 0o644); err != nil {
		return err
	}
	d.mu.Lock()
	d.uuid = uuid
	d.mu.Unlock()
	return nil
}

// Addr returns the resolved listen address (valid once Run is serving).
func (d *Daemon) Addr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.addr != "" {
		return d.addr
	}
	return d.opts.Addr
}

// CtrlAddr returns the resolved control-plane listen address ("" when
// the OSD runs a single plane; valid once Run is serving).
func (d *Daemon) CtrlAddr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ctrl != "" {
		return d.ctrl
	}
	return d.opts.CtrlAddr
}

// Fence returns the daemon's current epoch fence (0 = fencing off).
func (d *Daemon) Fence() uint32 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.srv == nil {
		return 0
	}
	return d.srv.MinEpoch()
}

// Server returns the OSD's RPC server (nil before Run is serving). It
// lets a same-process client serve a local-direct connection over a
// socketpair whose peer is handled by this server, bypassing TCP.
func (d *Daemon) Server() *sdsnet.Server {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.srv
}

// DiskStats reports the OSD's own disk accounting for the status API:
// bytes stored in object files, and the filesystem's free/total for the
// store directory (ok=false when the directory is not open yet or the
// filesystem cannot be queried).
func (d *Daemon) DiskStats() (usage, free, total int64, ok bool) {
	d.mu.Lock()
	st := d.st
	d.mu.Unlock()
	if st == nil {
		return 0, 0, 0, false
	}
	free, total, ok = st.DiskSpace()
	return st.Usage(), free, total, ok
}

// JournalStats reports the journal staging footprint for the status
// API: the WAL file size, pending (acknowledged, unapplied) records,
// and the filesystem free/total the journal grows into (ok=false when
// the store is not in journal mode or is not open yet).
func (d *Daemon) JournalStats() (logBytes, pending, free, total int64, ok bool) {
	d.mu.Lock()
	st := d.st
	d.mu.Unlock()
	if st == nil {
		return 0, 0, 0, 0, false
	}
	logBytes, pending, ok = st.JournalStats()
	if !ok {
		return 0, 0, 0, 0, false
	}
	free, total, _ = st.DiskSpace()
	return logBytes, pending, free, total, true
}

// WriteStats returns the store's write-amplification counters (bytes
// the client asked us to write, bytes the journal fsynced, bytes the
// applier flattened into objects, fill and punch traffic, and journal
// syncs). The status API and Prometheus export these so write
// amplification can be measured against the offered load (sdsbench).
func (d *Daemon) WriteStats() store.WriteStats {
	d.mu.Lock()
	st := d.st
	d.mu.Unlock()
	if st == nil {
		return store.WriteStats{}
	}
	return st.WriteStats()
}

// MetaInfo reports the in-memory object meta index population
// (indexed objects and the RAM they occupy).
func (d *Daemon) MetaInfo() (objects, ramBytes int64) {
	d.mu.Lock()
	st := d.st
	d.mu.Unlock()
	if st == nil {
		return 0, 0
	}
	return st.MetaInfo()
}

// IntentStats reports the write intent log counters of the daemon's
// store (zero value when the intent log is not in use), and the damage
// report from the last recovery: acknowledged writes that are not
// durable on disk. The status API surfaces the report as warnings.
func (d *Daemon) IntentStats() (store.IntentStats, []string) {
	d.mu.Lock()
	st := d.st
	d.mu.Unlock()
	if st == nil {
		return store.IntentStats{}, nil
	}
	return st.IntentStats(), st.IntentMismatches()
}

// Close stops the daemon. Safe to call repeatedly and concurrently.
func (d *Daemon) Close() error {
	d.mu.Lock()
	if d.srv != nil {
		d.srv.Close()
	}
	select {
	case <-d.quit:
	default:
		close(d.quit)
	}
	d.mu.Unlock()
	return nil
}

// objectBackend adapts the object store to sdsnet.Backend. Export names
// are accepted and ignored: one OSD serves the object namespace it owns
// (objects are namespaced by OID, volume routing is the client's job).
type objectBackend struct {
	st *store.Store
}

func (b *objectBackend) Size() uint64 { return 0 } // per-object sizing

func (b *objectBackend) ReadAt(_ context.Context, oid uint64, off uint64, p []byte) (int, error) {
	return b.st.ReadAt(object.OID(oid), p, int64(off))
}

func (b *objectBackend) WriteAt(_ context.Context, oid uint64, off uint64, p []byte) error {
	_, err := b.st.WriteAt(object.OID(oid), p, int64(off))
	return err
}

func (b *objectBackend) WriteZeroes(_ context.Context, oid uint64, off uint64, n uint64, sparse bool) error {
	// sparse = the request came from a guest TRIM/DISCARD (or a
	// Write_Zeroes without NO_HOLE): the store punches a real hole so the
	// NVMe gets its blocks back. Otherwise the store zero-fills.
	return b.st.WriteZeroes(object.OID(oid), off, n, sparse)
}

func (b *objectBackend) Sync(_ context.Context) error { return b.st.Sync() }

func (b *objectBackend) Remove(_ context.Context, oid uint64) error {
	return b.st.Remove(object.OID(oid))
}

func (b *objectBackend) Stat(_ context.Context, oid uint64) (uint64, error) {
	return b.st.Stat(object.OID(oid))
}

// StatFS serves OpStatFS: the store's used bytes plus the backing
// filesystem's free/total, for cluster capacity metrics.
func (b *objectBackend) StatFS(_ context.Context) (usage, free, total uint64, err error) {
	f, t, ok := b.st.DiskSpace()
	if !ok {
		return 0, 0, 0, syscall.ENOTSUP
	}
	if u := b.st.Usage(); u > 0 {
		usage = uint64(u)
	}
	if f > 0 {
		free = uint64(f)
	}
	if t > 0 {
		total = uint64(t)
	}
	return usage, free, total, nil
}

// StatInfo implements the statInfoer extension (OpStatInfo): apparent
// size plus physically allocated bytes, straight from the store.
func (b *objectBackend) StatInfo(_ context.Context, oid uint64) (uint64, uint64, error) {
	return b.st.StatInfo(object.OID(oid))
}
