package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
	"github.com/unistack-org/storage/pkg/vduse"
	"github.com/unistack-org/storage/pkg/vub"
)

// vubDevice is one running vhost-user-blk export.
type vubDevice struct {
	name     string
	volume   string
	socket   string
	readOnly bool
	size     uint64

	exp  *vub.Exporter
	stop context.CancelFunc
}

// vubManager owns the live vhost-user-blk exports. Like vduseManager it
// serves the boot-time -vub specs (start) and the runtime admin API
// (add / remove). QEMU connects to the socket
// (-chardev socket,id=…,path=<socket> -device vhost-user-blk-pci,chardev=…);
// the exporter accepts one VM at a time and reconnects automatically.
type vubManager struct {
	logger *log.Logger
	flags  vubFlags

	mu   sync.Mutex
	devs map[string]*vubDevice
	ctx  context.Context // shutdown context (Close on all exporters)
}

func newVUBManager(logger *log.Logger, flags vubFlags) *vubManager {
	return &vubManager{logger: logger, flags: flags, devs: map[string]*vubDevice{}}
}

// setContext stores the shutdown context whose cancellation closes
// every exporter.
func (m *vubManager) setContext(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
}

// start creates the boot-time exporters from -vub specs.
func (m *vubManager) start(ctx context.Context, es []*nbd.Export, specs []string) error {
	m.setContext(ctx)
	byName := map[string]*nbd.Export{}
	for _, e := range es {
		byName[e.Name] = e
	}
	for _, spec := range specs {
		expName, sock, ro := parseVUBSpec(spec)
		e, ok := byName[expName]
		if !ok {
			return fmt.Errorf("-vhost-user-blk %q: no such -export", spec)
		}
		if err := validateVUBSocket(sock); err != nil {
			return fmt.Errorf("-vhost-user-blk %q: %w", spec, err)
		}
		ro = ro || e.ReadOnly
		// nbd.Backend and vduse.Backend are the same method set, so the
		// export backend satisfies the vub backend interface directly.
		if err := m.create(ctx, expName, "", sock, e.Backend, e.Backend.Size(), ro); err != nil {
			return fmt.Errorf("-vhost-user-blk %q: %w", spec, err)
		}
	}
	return nil
}

// add exports a cluster volume as a vhost-user-blk socket at runtime
// (admin API). Idempotent: re-adding an existing name answers
// created=false with the live registration.
func (m *vubManager) add(volume, name string, cl *client.Cluster, spec meta.VolumeSpec, readOnly bool) (vubDeviceInfo, bool, error) {
	m.mu.Lock()
	if d, ok := m.devs[name]; ok {
		info := vubDeviceInfo{Name: d.name, Socket: d.socket, Volume: d.volume,
			ReadOnly: d.readOnly, Size: d.size, Attached: d.exp.Attached()}
		m.mu.Unlock()
		return info, false, nil
	}
	m.mu.Unlock()
	if cl == nil {
		return vubDeviceInfo{}, false, fmt.Errorf("runtime vhost-user-blk exports need cluster mode (-meta-dir)")
	}
	if _, err := cl.Refresh(context.Background()); err != nil {
		return vubDeviceInfo{}, false, fmt.Errorf("placement: %w", err)
	}
	vol, err := cl.OpenVolume(context.Background(), volume)
	if err != nil {
		return vubDeviceInfo{}, false, err
	}
	size := vol.Size()
	if spec.Size != 0 && spec.Size != size {
		// The record was re-read between the caller's GetVolume and the
		// open (a resize landed); use the live size.
		spec.Size = size
	}
	if name == "" {
		name = volume
	}
	sock := defaultVUBSocket(name)
	if err := m.create(m.shutdownContext(), name, volume, sock, nbd.NewVolumeBackend(vol), size, readOnly); err != nil {
		_ = vol.Close()
		return vubDeviceInfo{}, false, err
	}
	return vubDeviceInfo{Name: name, Socket: sock, Volume: volume, ReadOnly: readOnly, Size: size}, true, nil
}

// openVolume resolves placement and opens a cluster volume handle for
// a boot -vhost-user-blk spec (the manager path that honours a custom
// socket from the spec; add() always derives the default socket).
func (m *vubManager) openVolume(volume string, cl *client.Cluster) (*client.Volume, error) {
	if cl == nil {
		return nil, fmt.Errorf("runtime vhost-user-blk exports need cluster mode (-meta-dir)")
	}
	if _, err := cl.Refresh(context.Background()); err != nil {
		return nil, fmt.Errorf("placement: %w", err)
	}
	return cl.OpenVolume(context.Background(), volume)
}

// remove closes one exporter (and removes its socket).
func (m *vubManager) remove(name string) error {
	m.mu.Lock()
	d, ok := m.devs[name]
	if ok {
		delete(m.devs, name)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("vhost-user-blk export %q not found", name)
	}
	if d.stop != nil {
		d.stop()
	}
	return d.exp.Close()
}

func (m *vubManager) list() []vubDeviceInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]vubDeviceInfo, 0, len(m.devs))
	for _, d := range m.devs {
		out = append(out, vubDeviceInfo{
			Name: d.name, Socket: d.socket, Volume: d.volume, ReadOnly: d.readOnly,
			Size: d.size, Attached: d.exp.Attached(),
		})
	}
	return out
}

// create opens one exporter and serves it (boot and runtime paths).
func (m *vubManager) create(ctx context.Context, name, volume, sock string, be vduse.Backend, size uint64, readOnly bool) error {
	if err := validateVUBSocket(sock); err != nil {
		return err
	}
	m.mu.Lock()
	_, dup := m.devs[name]
	m.mu.Unlock()
	if dup {
		return fmt.Errorf("vhost-user-blk export %q already exists", name)
	}
	if err := os.MkdirAll(filepath.Dir(sock), 0o755); err != nil {
		return fmt.Errorf("vhost-user-blk: socket dir: %w", err)
	}
	run, stop := context.WithCancel(ctx)
	exp, err := vub.Listen(sock, be, size, vub.Options{
		NumQueues:   m.flags.Queues,
		Workers:     m.flags.Workers,
		ReadOnly:    readOnly,
		Trim:        m.flags.Trim,
		WriteZeroes: m.flags.WriteZeroes,
		Logger:      m.logger.Printf,
	})
	if err != nil {
		stop()
		return err
	}
	go exp.Serve()
	go func() {
		<-run.Done()
		_ = exp.Close()
	}()
	m.mu.Lock()
	if _, dup := m.devs[name]; dup {
		m.mu.Unlock()
		stop()
		_ = exp.Close()
		return fmt.Errorf("vhost-user-blk export %q already exists", name)
	}
	m.devs[name] = &vubDevice{name: name, volume: volume, socket: sock, readOnly: readOnly, size: size, exp: exp, stop: stop}
	m.mu.Unlock()
	m.logger.Printf("vhost-user-blk %q -> %s (volume %q, %s, ro=%v): attach with `-chardev socket,id=%s,path=%s -device vhost-user-blk-pci,chardev=%s`",
		name, sock, volume, humanSize(size), readOnly, qdevID(name), sock, qdevID(name))
	return nil
}

// qdevID makes a QEMU chardev id out of an export name.
func qdevID(name string) string {
	out := make([]byte, 0, len(name)+4)
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		out = []byte("vub")
	}
	return "vub" + string(out)
}

// shutdownContext returns the stored shutdown context (exporters
// created before setContext fall back to the background — closeAll
// still quiesces them).
func (m *vubManager) shutdownContext() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

// closeAll quiesces every exporter (the boot path's ctx-cancel cleanup).
func (m *vubManager) closeAll() {
	m.mu.Lock()
	devs := make([]*vubDevice, 0, len(m.devs))
	for _, d := range m.devs {
		devs = append(devs, d)
	}
	m.mu.Unlock()
	for _, d := range devs {
		if d.stop != nil {
			d.stop()
		}
		if err := d.exp.Close(); err != nil {
			m.logger.Printf("vhost-user-blk %s close: %v", d.name, err)
		}
	}
}

// startVUB is the boot entry point for main.go: it wires the manager
// and starts the -vub specs.
func startVUB(ctx context.Context, logger *log.Logger, es []*nbd.Export, specs []string, flags vubFlags) (*vubManager, error) {
	m := newVUBManager(logger, flags)
	if err := m.start(ctx, es, specs); err != nil {
		return nil, err
	}
	return m, nil
}
