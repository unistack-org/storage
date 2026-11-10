//go:build linux

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/unistack-org/sds-nbd/nbd"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/vduse"
)

// vduseDevice is one running VDUSE export.
type vduseDevice struct {
	name     string
	volume   string
	readOnly bool

	dev  *vduse.Device
	stop context.CancelFunc
	size uint64
}

// vduseManager owns the live VDUSE devices. It serves two entry points:
// the boot-time -vduse specs (start) and the runtime admin API (add /
// remove), which lets a control plane attach a volume to a VM without
// restarting the daemon.
//
// On the host the kernel device is attached with:
//
//	modprobe vdpa vduse virtio_vdpa
//	vdpa dev add name <devname> mgmtdev vduse   -> vdpa bus device
//	vdpa -j dev show <devname>                  -> "vhost_vdpa" chardev minor
//	mknod /dev/vhost-vdpa-<minor> c 10 <minor>  (if udev has not made it)
//
// and a VM uses it as a virtio-blk disk:
//
//	-device vhost-vdpa-device-pci,vhostdev=/dev/vhost-vdpa-<minor>,bus=pci.0,addr=0x5
//
// (the vhost-vdpa driver binds to the vduse bus device and exposes a
// /dev/vhost-vdpa-N character device — that node, not a /dev/vdX block
// device, is what QEMU's vhostdev property takes; there is no -chardev
// vdpa). A read-only device advertises VIRTIO_BLK_F_RO and refuses
// writes in the data path; write-bearing guests need a vIOMMU
// (virtio-iommu/intel-iommu) in front of the device for the
// virtio-dma mappings.
type vduseManager struct {
	logger *log.Logger
	flags  vduseFlags

	mu   sync.Mutex
	devs map[string]*vduseDevice
	ctx  context.Context // shutdown context (Close on all devices)
}

func newVDUSEManager(logger *log.Logger, flags vduseFlags) *vduseManager {
	return &vduseManager{logger: logger, flags: flags, devs: map[string]*vduseDevice{}}
}

// setContext stores the shutdown context whose cancellation quiesces
// every device (main wires it; boot specs need it too).
func (m *vduseManager) setContext(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
}

// start creates the boot-time devices from -vduse specs.
func (m *vduseManager) start(ctx context.Context, es []*nbd.Export, specs []string) error {
	m.setContext(ctx)
	byName := map[string]*nbd.Export{}
	for _, e := range es {
		byName[e.Name] = e
	}
	for _, spec := range specs {
		expName, devName := parseVDUSESpec(spec)
		e, ok := byName[expName]
		if !ok {
			return fmt.Errorf("-vduse %q: no such -export", spec)
		}
		if err := validateDevName(devName); err != nil {
			return fmt.Errorf("-vduse %q: %w", spec, err)
		}
		// nbd.Backend and vduse.Backend are the same method set, so the
		// export backend satisfies the VDUSE interface directly.
		if err := m.create(ctx, devName, e.Name, e.Backend, e.ReadOnly); err != nil {
			return fmt.Errorf("-vduse %q: %w", spec, err)
		}
	}
	return nil
}

// add exports a cluster volume as a VDUSE device at runtime (admin
// API). Idempotent: re-adding an existing devname answers created=false
// (a control plane retrying after a lost response must not fail).
func (m *vduseManager) add(volume, devname string, cl *client.Cluster, spec meta.VolumeSpec, readOnly bool) (bool, error) {
	m.mu.Lock()
	_, exists := m.devs[devname]
	m.mu.Unlock()
	if exists {
		return false, nil
	}
	if cl == nil {
		return false, fmt.Errorf("runtime vduse devices need cluster mode (-cluster)")
	}
	if _, err := cl.Refresh(context.Background()); err != nil {
		return false, fmt.Errorf("placement: %w", err)
	}
	vol, err := cl.OpenVolume(context.Background(), volume)
	if err != nil {
		return false, err
	}
	// No SetQoS reset here: OpenVolume resolved the volume's QoS class
	// and the meta watch keeps it live, so the guest's throttle is the
	// SDS-side QoS class (ADR-059: for vduse the throttle is delegated
	// to SDS, the QEMU-side cap does not apply).
	if vol.Size() != spec.Size {
		// The record was re-read between the caller's GetVolume and the
		// open (a resize landed); use the live size.
		spec.Size = vol.Size()
	}
	if err := m.create(m.shutdownContext(), devname, volume, nbd.NewVolumeBackend(vol), readOnly); err != nil {
		_ = vol.Close()
		return false, err
	}
	return true, nil
}

// remove tears a device down: quiesce (Close), then remove the kernel
// device (Destroy) so the name and its resources are fully released.
func (m *vduseManager) remove(name string) error {
	m.mu.Lock()
	d, ok := m.devs[name]
	if ok {
		delete(m.devs, name)
	}
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("vduse device %q not found", name)
	}
	if err := d.dev.Close(); err != nil {
		m.mu.Lock()
		m.devs[name] = d
		m.mu.Unlock()
		return err
	}
	if d.stop != nil {
		d.stop()
	}
	if err := vduse.DestroyDevice(name); err != nil {
		return fmt.Errorf("device %q quiesced, destroy failed: %w", name, err)
	}
	return nil
}

func (m *vduseManager) list() []vduseDeviceInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]vduseDeviceInfo, 0, len(m.devs))
	for _, d := range m.devs {
		out = append(out, vduseDeviceInfo{
			Name: d.name, Volume: d.volume, ReadOnly: d.readOnly,
			Size: d.size, Attached: d.dev.Attached(),
		})
	}
	return out
}

// create registers and serves one device (boot and runtime paths).
func (m *vduseManager) create(ctx context.Context, devname, volume string, be vduse.Backend, readOnly bool) error {
	run, stop := context.WithCancel(ctx)
	dev, err := vduse.NewDevice(devname, vduse.Options{
		NumQueues:   1,
		Trim:        m.flags.Trim,
		WriteZeroes: m.flags.WriteZeroes,
		ReadOnly:    readOnly,
	}, be)
	if err != nil {
		stop()
		return err
	}
	go dev.Serve()
	go watchCapacity(run, devname, dev, be, m.logger)
	m.mu.Lock()
	if _, dup := m.devs[devname]; dup {
		m.mu.Unlock()
		stop()
		_ = dev.Close()
		return fmt.Errorf("device %q already exists", devname)
	}
	m.devs[devname] = &vduseDevice{name: devname, volume: volume, readOnly: readOnly, dev: dev, stop: stop, size: be.Size()}
	m.mu.Unlock()
	m.logger.Printf("vduse %q -> volume %q (%s, ro=%v): attach with `vdpa dev add name %s mgmtdev vduse`",
		devname, volume, humanSize(be.Size()), readOnly, devname)
	return nil
}

// shutdownContext returns the stored shutdown context (devices created
// before setContext fall back to the background — the manager's
// closeAll path still quiesces them).
func (m *vduseManager) shutdownContext() context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

// closeAll quiesces every device (the boot path's ctx-cancel cleanup).
func (m *vduseManager) closeAll() {
	m.mu.Lock()
	devs := make([]*vduseDevice, 0, len(m.devs))
	for _, d := range m.devs {
		devs = append(devs, d)
	}
	m.mu.Unlock()
	for _, d := range devs {
		if d.stop != nil {
			d.stop()
		}
		if err := d.dev.Close(); err != nil {
			m.logger.Printf("vduse %s close: %v", d.name, err)
		}
	}
}

// startVDUSE is the boot entry point kept for main.go: it wires the
// manager and starts the -vduse specs.
func startVDUSE(ctx context.Context, logger *log.Logger, es []*nbd.Export, specs []string, trim, writeZeroes bool) (*vduseManager, error) {
	m := newVDUSEManager(logger, vduseFlags{Trim: trim, WriteZeroes: writeZeroes})
	if err := m.start(ctx, es, specs); err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		m.closeAll()
	}()
	return m, nil
}

// capacityPollInterval bounds how long a resize can take to reach the
// guest. The backend size is a cheap read (a volume's live spec, a file
// stat), so polling it costs nothing.
const capacityPollInterval = 2 * time.Second

// watchCapacity pushes online size changes of an export into the kernel
// device: when the backend grows (or shrinks) the virtio-blk config
// space is rewritten and the config-change interrupt injected, so the
// guest's /dev/vdX follows without a detach. NBD exports need a guest
// reconnect/rescan for the same effect; VDUSE is fully live.
func watchCapacity(ctx context.Context, name string, dev *vduse.Device, be nbd.Backend, logger *log.Logger) {
	last := be.Size()
	t := time.NewTicker(capacityPollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sz := be.Size()
			if sz == last {
				continue
			}
			if err := dev.SetCapacity(sz); err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Printf("vduse %s capacity %s -> %s: %v", name, humanSize(last), humanSize(sz), err)
				return
			}
			logger.Printf("vduse %s: capacity %s -> %s (guest notified)", name, humanSize(last), humanSize(sz))
			last = sz
		}
	}
}
