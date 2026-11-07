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
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
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
	// DataDir holds the object store (one directory per OSD).
	DataDir string
	// Meta is the cluster metadata store.
	Meta meta.Store
	// Direct / Preallocate are passed through to the object store.
	Direct      bool
	Preallocate bool
	// Logf receives informational messages (nil = silent).
	Logf func(format string, args ...any)
}

// Daemon is a running OSD.
type Daemon struct {
	opts Options
	srv  *sdsnet.Server
	addr string // resolved listen address (set by Run)

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
	st, err := store.New(d.opts.DataDir, store.Options{Direct: d.opts.Direct, Preallocate: d.opts.Preallocate})
	if err != nil {
		return fmt.Errorf("osd: store: %w", err)
	}
	defer st.Close()

	// Resolve the listen address up front (opts.Addr may use port 0) so
	// the meta registration advertises the real address; sdsnet.Server
	// opens the listening socket itself.
	probe, err := net.Listen("tcp", d.opts.Addr)
	if err != nil {
		return fmt.Errorf("osd: listen: %w", err)
	}
	addr := probe.Addr().String()
	probe.Close()

	d.mu.Lock()
	d.addr = addr
	d.mu.Unlock()

	d.srv = &sdsnet.Server{
		Backend: &objectBackend{st: st},
		Logf:    d.opts.Logf,
	}

	// Stable per-OSD uuid.
	if err := d.loadOrCreateUUID(); err != nil {
		return err
	}

	if err := d.register(); err != nil {
		return fmt.Errorf("osd: register: %w", err)
	}

	// Epoch fence from the map epoch: bump MinEpoch whenever a newer
	// map lands in meta.
	mapW, err := d.opts.Meta.Watch(ctx, meta.PrefixMaps)
	if err != nil {
		return err
	}
	defer mapW.Stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- d.srv.Serve(d.opts.Addr) }()

	if err := d.applyLatestEpoch(ctx); err != nil {
		d.opts.Logf("osd: initial epoch: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
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
	if rec.Epoch > d.srv.MinEpoch() {
		d.srv.SetMinEpoch(rec.Epoch)
		d.opts.Logf("osd: fence raised to epoch %d", rec.Epoch)
	}
	return nil
}

// register writes this OSD's OSDInfo into meta.
func (d *Daemon) register() error {
	d.mu.Lock()
	addr := d.Addr()
	uuid := d.uuid
	d.mu.Unlock()
	_, err := meta.PutOSD(context.Background(), d.opts.Meta, meta.OSDInfo{
		ID:       d.opts.ID,
		Addr:     addr,
		UUID:     uuid,
		DataPath: d.opts.DataDir,
		Weight:   1,
		BootedAt: time.Now().Unix(),
	})
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

// Addr returns the resolved listen address.
func (d *Daemon) Addr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.addr != "" {
		return d.addr
	}
	return d.opts.Addr
}

// Close stops the daemon.
func (d *Daemon) Close() error {
	d.mu.Lock()
	if d.srv != nil {
		d.srv.Close()
	}
	d.mu.Unlock()
	select {
	case <-d.quit:
	default:
		close(d.quit)
	}
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
	// MVP: zero-fill writes. Punch-hole support lands with the raw
	// store; the object store keeps fixed-size files.
	buf := make([]byte, zeroChunk(n))
	for written := uint64(0); written < n; {
		ch := uint64(len(buf))
		if n-written < ch {
			ch = n - written
		}
		if _, err := b.st.WriteAt(object.OID(oid), buf[:ch], int64(off+written)); err != nil {
			return err
		}
		written += ch
	}
	return nil
}

func (b *objectBackend) Sync(_ context.Context) error { return b.st.Sync() }

func zeroChunk(n uint64) int {
	const max = 1 << 20
	if n < max {
		return int(n)
	}
	return max
}
