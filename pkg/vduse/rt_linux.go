//go:build linux

package vduse

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// VDUSE ioctl numbers (uapi/linux/vduse.h), type = VDUSE_BASE = 0x81.
// Linux _IOC encoding: dir<<30 | size<<16 | type<<8 | nr, NONE=0 WRITE=1
// READ=2 (IOR=2, IOW=1, IOWR=3). Keep in sync with the kernel header.
const (
	iocGetAPIVersion  = 0x80088100 // _IOR(0x81, 0x00, u64)
	iocSetAPIVersion  = 0x40088101 // _IOW(0x81, 0x01, u64)
	iocCreateDev      = 0x41508102 // _IOW(0x81, 0x02, vduse_dev_config=336)
	iocDestroyDev     = 0x41008103 // _IOW(0x81, 0x03, char[256])
	iocIotlbGetFD     = 0xc0208110 // _IOWR(0x81, 0x10, vduse_iotlb_entry=32)
	iocDevGetFeatures = 0x80088111 // _IOR(0x81, 0x11, u64)
	iocVQSetup        = 0x40208114 // _IOW(0x81, 0x14, vduse_vq_config=32)
	iocVQGetInfo      = 0xc0308115 // _IOWR(0x81, 0x15, vduse_vq_info=48)
	iocVQSetupKickFD  = 0x40088116 // _IOW(0x81, 0x16, vduse_vq_eventfd=8)
	iocVQInjectIRQ    = 0x40048117 // _IOW(0x81, 0x17, u32)
)

// Control message types (enum vduse_req_type) and results.
const (
	reqGetVQState     = 0
	reqSetStatus      = 1
	reqUpdateIOTLB    = 2
	reqSetVQGroupASID = 3
	reqSetVQReady     = 4
	reqSuspend        = 5

	reqResultOK     = 0
	reqResultFailed = 1

	efdCloexec = 0o2000000 // EFD_CLOEXEC (not in syscall package)

	// virtio device status bits (VDUSE_SET_STATUS payload).
	statusAcknowledge = 1
	statusDriver      = 2
	statusDriverOK    = 4
	statusFeaturesOK  = 8
	statusFailed      = 128

	devMsgSize = 152 // sizeof(vduse_dev_request) == sizeof(vduse_dev_response)
	devNameLen = 256
)

func ioctl(fd uintptr, req uintptr, arg unsafe.Pointer) (uintptr, error) {
	r, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, req, uintptr(arg))
	if errno != 0 {
		return r, errno
	}
	return r, nil
}

// buildDevConfig serialises vduse_dev_config (336 bytes) followed by the
// configuration space (flexible array member).
func buildDevConfig(name string, nq uint16, features uint64, config []byte) []byte {
	cfg := make([]byte, 336+len(config))
	le := binary.LittleEndian
	copy(cfg, name) // name[256], NUL-padded
	le.PutUint32(cfg[256:], VendorID)
	le.PutUint32(cfg[260:], DevIDBlk)
	le.PutUint64(cfg[264:], features)
	le.PutUint32(cfg[272:], uint32(nq))          // vq_num
	le.PutUint32(cfg[276:], 4096)                // vq_align
	le.PutUint32(cfg[332:], uint32(len(config))) // config_size
	copy(cfg[336:], config)
	return cfg
}

// CreateDevice creates a VDUSE device on /dev/vduse/control.
func CreateDevice(name string, nq uint16, features uint64, config []byte) error {
	cf, err := os.OpenFile("/dev/vduse/control", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("vduse: open control: %w", err)
	}
	defer cf.Close()
	var ver uint64
	if _, err := ioctl(cf.Fd(), iocGetAPIVersion, unsafe.Pointer(&ver)); err != nil {
		return fmt.Errorf("vduse: get api version: %w", err)
	}
	// Pin API v0: v1 (vq groups / address spaces) requires ngroups>=1
	// and nas>=1 in the create config and the *_V2 ioctls; v0 is
	// supported by every kernel that has VDUSE at all.
	var zero uint64
	if ver > 0 {
		if _, err := ioctl(cf.Fd(), iocSetAPIVersion, unsafe.Pointer(&zero)); err != nil {
			return fmt.Errorf("vduse: set api version 0 (kernel has %d): %w", ver, err)
		}
	}
	cfg := buildDevConfig(name, nq, features, config)
	if _, err := ioctl(cf.Fd(), iocCreateDev, unsafe.Pointer(&cfg[0])); err != nil {
		return fmt.Errorf("vduse: create dev %q: %w", name, err)
	}
	return nil
}

// DestroyDevice removes a VDUSE device by name.
func DestroyDevice(name string) error {
	cf, err := os.OpenFile("/dev/vduse/control", os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer cf.Close()
	var buf [devNameLen]byte
	copy(buf[:], name)
	if _, err := ioctl(cf.Fd(), iocDestroyDev, unsafe.Pointer(&buf[0])); err != nil && !errors.Is(err, syscall.ENOENT) {
		return fmt.Errorf("vduse: destroy dev %q: %w", name, err)
	}
	return nil
}

// queueState tracks one virtqueue: its ring (valid once the driver has
// enabled it), the kick eventfd, and the injected-IRQ counter.
type queueState struct {
	idx    uint32
	kickFD *os.File // eventfd readable by us, written by the kernel
	ring   *SplitRing
	irqs   uint64

	// drainMu serialises consumers of the ring: a kick and the
	// post-DRIVER_OK flush can run at the same time.
	drainMu sync.Mutex
}

// Device is a running VDUSE virtio-blk device backed by be.
type Device struct {
	name string
	be   Backend
	nq   uint16

	devFD *os.File // open on /dev/vduse/<name>: keeps the device attached
	iotlb *IOTLB
	gm    SliceGuestMem

	mu  sync.Mutex
	vqs []*queueState

	lastFull int64      // unix nanos of last full IOTLB pull (kick gate)
	attached bool       // reconnected to a pre-existing kernel device
	pullMu   sync.Mutex // serialises VDUSE_IOTLB_GET_FD pulls
	trim     bool
	wz       bool
	dbg      bool
	stop     chan struct{}
	done     chan struct{}
}

func (d *Device) logf(format string, args ...any) {
	if d.dbg {
		fmt.Fprintf(os.Stderr, "vduse[%s] "+format, append([]any{d.name}, args...)...)
	}
}

// Options configures NewDevice.
type Options struct {
	// NumQueues is the number of virtqueues (default 1).
	NumQueues uint16
	// Trim / WriteZeroes advertise (and accept) REQ_DISCARD /
	// REQ_WRITE_ZEROES; disable when the backend cannot honour them.
	Trim        bool
	WriteZeroes bool
}

// NewDevice creates (or reattaches to) the kernel device and opens it,
// which attaches it to the vDPA bus; the VM can then bind it with
// `vdpa add ...`.
//
// Reconnect: VDUSE keeps a device alive while no daemon is connected
// (the kernel's ->release splices unanswered messages back for the next
// daemon). If /dev/vduse/<name> already exists we attach to it instead
// of creating: rebuild the queues from the kernel's VQ_GET_INFO, (re-)
// assign kick eventfds (the kernel replays kicks accumulated while we
// were gone), and pull the IOTLB. A device that another process holds
// open fails with EBUSY.
func NewDevice(name string, opts Options, be Backend) (*Device, error) {
	nq := opts.NumQueues
	if nq < 1 {
		nq = 1
	}
	_, statErr := os.Stat("/dev/vduse/" + name)
	attached := statErr == nil
	if !attached {
		features := FeatureMask(opts.Trim, opts.WriteZeroes)
		if err := CreateDevice(name, nq, features, ConfigSpace(be.Size(), nq)); err != nil {
			return nil, err
		}
	}
	df, err := os.OpenFile("/dev/vduse/"+name, os.O_RDWR, 0)
	if err != nil {
		if attached && errors.Is(err, syscall.EBUSY) {
			return nil, fmt.Errorf("vduse: dev %q exists and is open by another daemon", name)
		}
		return nil, fmt.Errorf("vduse: open dev: %w", err)
	}
	d := &Device{
		name:     name,
		be:       be,
		nq:       nq,
		devFD:    df,
		iotlb:    &IOTLB{},
		attached: attached,
		trim:     opts.Trim,
		wz:       opts.WriteZeroes,
		dbg:      os.Getenv("SDS_VDUSE_DEBUG") != "",
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	d.gm = NewGuestMemWithPull(d.iotlb, d.pullOnMiss)
	if err := d.setupVQs(); err != nil {
		d.Close()
		return nil, err
	}
	if attached {
		// The driver may already be (partly) initialised. Rebuild ring
		// state, kick fds and mappings so I/O resumes seamlessly.
		d.mu.Lock()
		for _, q := range d.vqs {
			if err := d.vqRefresh(q); err != nil {
				d.logf("attach vqRefresh q%d: %v\n", q.idx, err)
			}
		}
		d.mu.Unlock()
		d.pullMu.Lock()
		refreshErr := d.iotlbRefresh()
		d.pullMu.Unlock()
		if refreshErr != nil {
			d.logf("attach iotlb refresh: %v\n", refreshErr)
		}
	}
	return d, nil
}

// Attached reports whether this session reattached to a pre-existing
// kernel device (daemon reconnect) rather than creating a new one.
func (d *Device) Attached() bool { return d.attached }

// setupVQs allocates the kick eventfds and assigns them. The kernel
// requires all vqs set up (with max ring size) before a freshly created
// device can attach; on reconnect this re-assigns the fds and the
// kernel replays any kick accumulated while the fd was deassigned.
func (d *Device) setupVQs() error {
	d.vqs = make([]*queueState, d.nq)
	for i := 0; i < int(d.nq); i++ {
		var vc [32]byte
		binary.LittleEndian.PutUint32(vc[0:], uint32(i))
		binary.LittleEndian.PutUint16(vc[4:], 256) // max ring size
		if _, err := ioctl(d.devFD.Fd(), iocVQSetup, unsafe.Pointer(&vc[0])); err != nil {
			return fmt.Errorf("vduse: vq setup %d: %w", i, err)
		}
		efd, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, uintptr(efdCloexec), 0)
		if errno != 0 {
			return fmt.Errorf("vduse: eventfd: %w", errno)
		}
		kf := os.NewFile(uintptr(efd), "vduse-kick")
		var ea [8]byte
		binary.LittleEndian.PutUint32(ea[0:], uint32(i))
		binary.LittleEndian.PutUint32(ea[4:], uint32(int32(efd)))
		if _, err := ioctl(d.devFD.Fd(), iocVQSetupKickFD, unsafe.Pointer(&ea[0])); err != nil {
			kf.Close()
			return fmt.Errorf("vduse: kickfd %d: %w", i, err)
		}
		d.vqs[i] = &queueState{idx: uint32(i), kickFD: kf}
	}
	return nil
}

// Serve runs the control-message loop and kick watchers until Close.
func (d *Device) Serve() {
	go d.msgLoop()
	for _, q := range d.vqs {
		go d.kickLoop(q)
	}
	// On reconnect the driver is already live: requests it submitted
	// while no daemon was connected were timed out and their buffers
	// DMA-unmapped, so we must NOT replay them (touching freed guest
	// pages faults); refreshAndDrain fast-forwards past them. A fresh
	// session must serve what the driver queued at probe (whose
	// buffers are live, awaiting our completion).
	d.refreshAndDrain(!d.attached)
	close(d.done)
}

func (d *Device) Close() error {
	select {
	case <-d.stop:
	default:
		close(d.stop)
	}
	if d.devFD != nil {
		// Deassign kick fds, then closing the dev fd detaches the device.
		for _, q := range d.vqs {
			var ea [8]byte
			binary.LittleEndian.PutUint32(ea[0:], q.idx)
			binary.LittleEndian.PutUint32(ea[4:], 0xFFFFFFFF) // VDUSE_EVENTFD_DEASSIGN
			ioctl(d.devFD.Fd(), iocVQSetupKickFD, unsafe.Pointer(&ea[0]))
			q.kickFD.Close()
		}
		d.devFD.Close()
	}
	for _, r := range d.iotlb.Snapshot() {
		syscall.Munmap(r.Mem)
	}
	// Deliberately NOT destroyed: the kernel keeps a VDUSE device alive
	// while no daemon is connected, so a restarted daemon reattaches and
	// I/O resumes (kicks accumulated while we were gone are replayed).
	// Callers that mean to remove the volume call Destroy.
	return nil
}

// Destroy destroys the kernel device (the device disappears from
// /dev/vduse and the vDPA bus). Close deliberately keeps the device
// alive so a restarted daemon can reattach; callers that really mean
// to remove the volume call Destroy.
func (d *Device) Destroy() error {
	return DestroyDevice(d.name)
}

// ------------------------------------------------------------------
// Control messages
// ------------------------------------------------------------------

// msg offsets in vduse_dev_request: type@0 request_id@4 reserved@8;
// union@24: vq_state{index@24, avail_idx@32}, status{u8@24},
// iova{start@24, last@32}.
func (d *Device) msgLoop() {
	buf := make([]byte, devMsgSize)
	for {
		if _, err := io.ReadFull(d.devFD, buf); err != nil {
			return // device closed
		}
		typ := binary.LittleEndian.Uint32(buf[0:])
		id := binary.LittleEndian.Uint32(buf[4:])
		d.logf("req type=%d id=%d payload=% x\n", typ, id, buf[24:56])
		var resp [devMsgSize]byte
		binary.LittleEndian.PutUint32(resp[0:], id)
		resp[4] = reqResultOK
		switch typ {
		case reqGetVQState:
			idx := binary.LittleEndian.Uint32(buf[24:])
			binary.LittleEndian.PutUint32(resp[24:], idx)
			d.mu.Lock()
			if int(idx) < len(d.vqs) && d.vqs[idx].ring != nil {
				avail, _ := d.vqs[idx].ring.State()
				// struct vduse_vq_state: index u32, then the split
				// union immediately: avail_index u16 at +4.
				binary.LittleEndian.PutUint16(resp[28:], avail)
			}
			d.mu.Unlock()
		case reqSetStatus:
			st := buf[24]
			if st == 0 {
				// Driver reset: rings are invalid from now on.
				d.mu.Lock()
				for _, q := range d.vqs {
					q.ring = nil
				}
				d.mu.Unlock()
			} else if st&statusDriverOK != 0 {
				// The kernel clears vq->kickfd on device reset
				// (virtio_dev_probe resets before driving the
				// device), so (re)assign our kick eventfds now;
				// the kernel replays any kick accumulated while
				// the fd was gone. This mirrors libvduse, which
				// sets kick fds at vhost dev start.
				for _, q := range d.vqs {
					var ea [8]byte
					binary.LittleEndian.PutUint32(ea[0:], q.idx)
					binary.LittleEndian.PutUint32(ea[4:], uint32(int32(q.kickFD.Fd())))
					if _, err := ioctl(d.devFD.Fd(), iocVQSetupKickFD, unsafe.Pointer(&ea[0])); err != nil {
						d.logf("kickfd reassign q%d: %v\n", q.idx, err)
						resp[4] = reqResultFailed
					}
				}
				// Fetch ring addresses, pull the IOTLB (the
				// virtio-vDPA driver maps through the DMA API
				// and sends no IOTLB messages), and drain
				// whatever sits in the avail ring: the driver
				// may have submitted requests before the queue
				// was ready (virtio-blk probe does) and the
				// kernel drops those kicks.
				d.refreshAndDrain(true)
			}
		case reqUpdateIOTLB:
			start := binary.LittleEndian.Uint64(buf[24:])
			last := binary.LittleEndian.Uint64(buf[32:])
			if err := d.iotlbApply(start, last); err != nil {
				resp[4] = reqResultFailed
			}
		case reqSetVQReady:
			// vduse_vq_ready: num u32@24, ready u8@28. The kernel
			// marks the vq ready right after our reply and
			// replays any kick accumulated while it was not;
			// the ring must be live before that happens.
			idx := binary.LittleEndian.Uint32(buf[24:])
			ready := buf[28] != 0
			d.logf("set vq ready: idx=%d ready=%v\n", idx, ready)
			d.mu.Lock()
			if int(idx) < len(d.vqs) {
				if err := d.vqRefresh(d.vqs[idx]); err != nil {
					d.logf("vqRefresh q%d at SET_VQ_READY: %v\n", idx, err)
					resp[4] = reqResultFailed
				}
			}
			d.mu.Unlock()
		case reqSuspend, reqSetVQGroupASID:
			// API v0 does not negotiate these; acknowledge.
		default:
			resp[4] = reqResultFailed
		}
		if _, err := d.devFD.Write(resp[:]); err != nil {
			return
		}
	}
}

// iotlbPullRange pulls kernel IOTLB regions overlapping [start,last]
// (the VDUSE_IOTLB_GET_FD loop). Used on mapping misses: the virtio-vDPA
// driver maps request buffers through the DMA API without any message,
// so a lookup miss is the signal that a new mapping appeared.
// Caller must NOT hold d.mu (Lookup's RLock is taken inside).
func (d *Device) iotlbPullRange(start, last uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for start <= last {
		var e [32]byte
		binary.LittleEndian.PutUint64(e[8:], start)
		binary.LittleEndian.PutUint64(e[16:], last)
		regFD, err := ioctl(d.devFD.Fd(), iocIotlbGetFD, unsafe.Pointer(&e[0]))
		// The kernel answers -EINVAL when no map overlaps the range
		// (uapi/linux/vduse.h: "Return -EINVAL means the IOVA region
		// doesn't exist"); -ENOENT is kept for other kernels.
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.EINVAL) {
			return ErrNotMapped // kernel has nothing covering the range
		}
		if err != nil {
			return fmt.Errorf("vduse: iotlb get fd [%x,%x]: %w", start, last, err)
		}
		moff := binary.LittleEndian.Uint64(e[0:])
		mstart := binary.LittleEndian.Uint64(e[8:])
		mlast := binary.LittleEndian.Uint64(e[16:])
		if old, ok := d.iotlb.Exact(mstart, mlast); ok {
			syscall.Close(int(regFD))
			_ = old
			// Range already mapped; the fd's pages are the same.
			// Keep the existing mapping and stop: a single pull
			// covers contiguous requests.
			break
		}
		base, err := syscall.Mmap(int(regFD), int64(moff), int(mlast-mstart+1),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		syscall.Close(int(regFD))
		if err != nil {
			return fmt.Errorf("vduse: mmap iotlb [%x,%x]: %w", mstart, mlast, err)
		}
		for _, old := range d.iotlb.Add(Region{Start: mstart, Last: mlast, Mem: base}) {
			syscall.Munmap(old.Mem)
		}
		if mlast >= last {
			break
		}
		start = mlast + 1
	}
	return nil
}

// pullOnMiss is the iovaMem pull hook: fetch the kernel mapping for a
// range the local table just missed.
func (d *Device) pullOnMiss(iova, length uint64) error {
	if length == 0 || iova+length-1 < iova {
		return ErrNotMapped
	}
	return d.iotlbPullRange(iova, iova+length-1)
}

// iotlbRefresh re-pulls the kernel's IOTLB and reconciles our mappings.
//
// The kernel NEVER pushes VDUSE_UPDATE_IOTLB in the virtio-vDPA path:
// the driver publishes rings and maps request buffers through the DMA
// API (vduse_dev_map_page), which records IOVA ranges in the kernel
// domain without sending any message. A libvduse-style daemon must pull
// the table with VDUSE_IOTLB_GET_FD (the whole pull below the requested
// start, capped at last; -ENOENT means "no more regions" and ends the
// loop). We re-pull the full range and keep every mapping whose range
// is unchanged, so steady-state calls cost a couple of ioctls and no
// remapping. The caller must hold d.mu.
func (d *Device) iotlbRefresh() error {
	type held struct {
		regFD uintptr
		e     [32]byte
	}
	fresh := map[uint64]Region{} // unchanged regions, by Start
	want := map[uint64]held{}    // changed regions, fd open
	var start uint64
	const last = ^uint64(0)
	for {
		var e [32]byte
		binary.LittleEndian.PutUint64(e[8:], start)
		binary.LittleEndian.PutUint64(e[16:], last)
		regFD, err := ioctl(d.devFD.Fd(), iocIotlbGetFD, unsafe.Pointer(&e[0]))
		// -EINVAL (this kernel) / -ENOENT (older): no map overlaps
		// [start, last] — the pull walked off the end of the table.
		if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.EINVAL) {
			break // end of table
		}
		if err != nil {
			for _, h := range want {
				syscall.Close(int(h.regFD))
			}
			return fmt.Errorf("vduse: iotlb get fd [%x,%x]: %w", start, last, err)
		}
		mstart := binary.LittleEndian.Uint64(e[8:])
		mlast := binary.LittleEndian.Uint64(e[16:])
		if old, ok := d.iotlb.Exact(mstart, mlast); ok {
			fresh[mstart] = old
		} else {
			want[mstart] = held{regFD: regFD, e: e}
		}
		if mlast >= last {
			break
		}
		start = mlast + 1
	}

	var drop []Region
	for _, r := range d.iotlb.Snapshot() {
		if _, ok := fresh[r.Start]; !ok {
			drop = append(drop, r)
		}
	}
	d.iotlb.Replace(fresh)
	// Old and new range are identical, so swapping under the IOTLB lock
	// leaves no lookup gap; add mappings before unmapping the old ones.
	for _, h := range want {
		moff := binary.LittleEndian.Uint64(h.e[0:])
		mstart := binary.LittleEndian.Uint64(h.e[8:])
		mlast := binary.LittleEndian.Uint64(h.e[16:])
		base, err := syscall.Mmap(int(h.regFD), int64(moff), int(mlast-mstart+1),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		syscall.Close(int(h.regFD))
		if err != nil {
			return fmt.Errorf("vduse: mmap iotlb [%x,%x]: %w", mstart, mlast, err)
		}
		// Regions starting at mstart are already accounted for in
		// fresh/drop; Add only trims foreign overlaps.
		for _, old := range d.iotlb.Add(Region{Start: mstart, Last: mlast, Mem: base}) {
			syscall.Munmap(old.Mem)
		}
	}
	for _, r := range drop {
		syscall.Munmap(r.Mem)
	}
	if len(want) > 0 || len(drop) > 0 {
		d.logf("iotlb: %d regions (+%d -%d)\n", len(fresh)+len(want), len(want), len(drop))
	}
	return nil
}

// lockDrains/unlockDrains serialise an IOTLB refresh against every
// queue's drain: refresh munmaps regions the kernel dropped, and a
// concurrent drain may hold slices into exactly those regions (the
// DRIVER_OK refresh in msgLoop races the kickLoop drain; on reconnect
// both can run at once). Lock ordering is always drainMu-then-pullMu.
func (d *Device) lockDrains() {
	for _, q := range d.vqs {
		q.drainMu.Lock()
	}
}

func (d *Device) unlockDrains() {
	for i := len(d.vqs) - 1; i >= 0; i-- {
		d.vqs[i].drainMu.Unlock()
	}
}

// refreshLocked re-pulls the IOTLB with all drains frozen. Caller must
// not hold any drainMu.
func (d *Device) refreshLocked() {
	d.lockDrains()
	d.pullMu.Lock()
	err := d.iotlbRefresh()
	d.pullMu.Unlock()
	d.unlockDrains()
	if err != nil {
		d.logf("iotlb refresh: %v\n", err)
	}
}

// refreshAndDrain re-reads ring addresses and IOTLB mappings from the
// kernel. Called at DRIVER_OK, on attach (reconnect), and at startup.
//
// The avail-ring contents matter in both cases. The driver leaves its
// own DMA buffers mapped until it sees a used entry for them, so
// entries submitted before (or while) the daemon was gone are safe to
// read and MUST be completed: virtio-blk waits on in-flight requests
// forever (there is no blk-mq timeout for it), and an abandoned
// request keeps its page locked — that deadlock blocks every open of
// the block device, device removal, everything.
//
// On reconnect (drain=false) we align our used-ring cursor with the
// driver's used.idx (the pre-crash daemon's completions the driver
// already consumed) and drain the pending avail entries. Requests
// whose buffers were genuinely unmapped fault on read; drainLocked's
// guard completes those with I/O error, which is exactly the semantics
// the guest needs to unblock.
func (d *Device) refreshAndDrain(drain bool) {
	d.mu.Lock()
	for _, q := range d.vqs {
		if err := d.vqRefresh(q); err != nil {
			d.logf("vqRefresh q%d: %v\n", q.idx, err)
		}
	}
	d.mu.Unlock()
	d.refreshLocked()
	if !drain {
		// Reconnect. The entries the device owes the driver are
		// exactly avail[used.idx .. avail.idx-1]: the driver adds
		// one avail slot per submission and releases DMA only when
		// it consumes the matching used slot, so everything from
		// its used.idx up to avail.idx is in flight and must be
		// completed (virtio-blk has no blk-mq timeout). The
		// kernel's stored avail_index is always 0 on the VDUSE
		// path — it is a fresh-device initial value, not a
		// reconnect source of truth. Drain from used.idx, push
		// used from used.idx too.
		for _, q := range d.vqs {
			d.mu.Lock()
			ring := q.ring
			d.mu.Unlock()
			if ring == nil {
				continue
			}
			kAvail, _ := ring.State() // fresh-device cursor (0)
			start, end := kAvail, kAvail
			if av, okA := d.readAvailIdx(q, ring); okA {
				end = av
				if us, okU := d.readUsedIdx(q, ring); okU {
					start = us
				} else {
					start = end // nothing known outstanding
				}
			}
			ring.SetState(start, start)
			d.logf("attach q%d: replaying avail[%d..%d) (used from %d)\n",
				q.idx, start, end, start)
		}
	}
	d.lockDrains()
	for _, q := range d.vqs {
		d.mu.Lock()
		ring := q.ring
		d.mu.Unlock()
		if ring != nil {
			d.drainLocked(q, ring)
		}
	}
	d.unlockDrains()
}

// readUsedIdx / readAvailIdx read the guest ring indices for cursor
// alignment on reconnect. Ring pages may have been unmapped when the
// previous daemon died, so the reads run under the same SIGBUS guard as
// drains; ok=false means the page is dead — the caller keeps the
// fresh-device cursor and the drain's own guard handles the rest.
func (d *Device) readUsedIdx(q *queueState, ring *SplitRing) (used uint16, ok bool) {
	return d.readIdx(q, "used", ring.UsedIdx)
}

func (d *Device) readAvailIdx(q *queueState, ring *SplitRing) (avail uint16, ok bool) {
	return d.readIdx(q, "avail", ring.AvailIdx)
}

func (d *Device) readIdx(q *queueState, what string, fn func() (uint16, error)) (v uint16, ok bool) {
	prev := debug.SetPanicOnFault(true)
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "sdsd: vduse %s q%d: %s idx read fault (%v) — ring pages dead\n",
				d.name, q.idx, what, r)
			v, ok = 0, false
		}
		debug.SetPanicOnFault(prev)
	}()
	x, err := fn()
	if err != nil {
		d.logf("attach q%d %s idx read: %v\n", q.idx, what, err)
		return 0, false
	}
	return x, true
}

// iotlbApply (re)maps every IOVA region overlapping [start,last]; the
// loop shape mirrors QEMU libvduse. Old overlapping regions are removed
// and unmapped.
func (d *Device) iotlbApply(start, last uint64) error {
	for _, r := range d.iotlb.RemoveRange(start, last) {
		syscall.Munmap(r.Mem)
	}
	for start <= last {
		var e [32]byte
		binary.LittleEndian.PutUint64(e[8:], start)
		binary.LittleEndian.PutUint64(e[16:], last)
		// IOTLB_GET_FD returns a fresh fd on the region; the entry is
		// updated with {offset, start, last, perm}.
		regFD, err := ioctl(d.devFD.Fd(), iocIotlbGetFD, unsafe.Pointer(&e[0]))
		if err != nil {
			if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.EINVAL) {
				break // no map overlaps [start,last]: done
			}
			return fmt.Errorf("vduse: iotlb get fd [%x,%x]: %w", start, last, err)
		}
		moff := binary.LittleEndian.Uint64(e[0:])
		mstart := binary.LittleEndian.Uint64(e[8:])
		mlast := binary.LittleEndian.Uint64(e[16:])
		size := int(mlast - mstart + 1)
		base, err := syscall.Mmap(int(regFD), int64(moff), size,
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		syscall.Close(int(regFD))
		if err != nil {
			return fmt.Errorf("vduse: mmap iotlb [%x,%x]: %w", mstart, mlast, err)
		}
		for _, old := range d.iotlb.Add(Region{Start: mstart, Last: mlast, Mem: base}) {
			syscall.Munmap(old.Mem)
		}
		if mlast >= last {
			break
		}
		start = mlast + 1
	}
	return nil
}

// vqRefresh (re)reads a virtqueue's ring addresses from the kernel
// (vduse_vq_info 48B: index@0 num@4 desc@8 avail@16 used@24
// split.avail_index@32 ready@40) and (re)builds its SplitRing.
// The caller must hold d.mu.
func (d *Device) vqRefresh(q *queueState) error {
	var info [48]byte
	binary.LittleEndian.PutUint32(info[0:], q.idx)
	if _, err := ioctl(d.devFD.Fd(), iocVQGetInfo, unsafe.Pointer(&info[0])); err != nil {
		return fmt.Errorf("vduse: vq get info %d: %w", q.idx, err)
	}
	num := binary.LittleEndian.Uint16(info[4:])
	desc := binary.LittleEndian.Uint64(info[8:])
	avail := binary.LittleEndian.Uint64(info[16:])
	used := binary.LittleEndian.Uint64(info[24:])
	// inline union@32: vduse_vq_state_split{avail_index u16} at 32; ready@40.
	availIdx := binary.LittleEndian.Uint16(info[32:])
	ready := info[40] != 0
	d.logf("vq%d info: num=%d desc=%x avail=%x used=%x availIdx=%d ready=%v\n",
		q.idx, num, desc, avail, used, availIdx, ready)
	if !ready {
		q.ring = nil
		return nil
	}
	ring := NewSplitRing(d.gm, num, desc, avail, used)
	ring.SetState(availIdx, availIdx) // used idx is not tracked by kernel: fresh device
	q.ring = ring
	return nil
}

// ------------------------------------------------------------------
// I/O path
// ------------------------------------------------------------------

// kickLoop drains queue q whenever the kernel signals its kick eventfd.
//
// Each kick also (time-gated) re-pulls the IOTLB: the virtio-vDPA driver
// DMA-maps every request's buffers with a fresh IOVA and sends us no
// message, so a kick is the only cheap signal that new mappings exist.
// The gate keeps the syscall cost off the hot path — a lookup miss
// (pullOnMiss) covers the common case of a brand-new IOVA, the refresh
// cleans up regions the driver unmapped.
func (d *Device) kickLoop(q *queueState) {
	b := make([]byte, 8)
	for {
		if _, err := q.kickFD.Read(b); err != nil {
			return
		}
		d.mu.Lock()
		ring := q.ring
		if ring == nil {
			// Kick raced queue enable: try to pick the ring up now.
			if err := d.vqRefresh(q); err == nil {
				ring = q.ring
			}
		}
		now := time.Now().UnixNano()
		stale := now-d.lastFull >= int64(kickRefreshInterval)
		if stale {
			d.lastFull = now
		}
		d.mu.Unlock()
		d.logf("kick q%d ring=%v\n", q.idx, ring != nil)
		if ring == nil {
			continue
		}
		q.drainMu.Lock()
		if stale {
			// Hold drainMu so the refresh cannot unmap buffers of a
			// request being served: the queue must be fully drained
			// first (newly arrived requests pull their own mapping
			// on a lookup miss).
			d.pullMu.Lock()
			if err := d.iotlbRefresh(); err != nil {
				d.logf("iotlb refresh: %v\n", err)
			}
			d.pullMu.Unlock()
		}
		d.drainLocked(q, ring)
		q.drainMu.Unlock()
	}
}

// kickRefreshInterval is the minimum spacing between full IOTLB pulls
// triggered by kicks.
const kickRefreshInterval = 5 * time.Millisecond

// evictMaxSize bounds the IOTLB regions we drop after completing a
// request. The virtio-vDPA driver maps request buffers page-by-page, so
// those regions are small; a QEMU-style mapping of all guest RAM is
// huge, permanent, and must not be torn down per request.
const evictMaxSize = 8 << 20

// evict drops the mappings covering the request's data buffers, which
// the driver unmaps as soon as it observes the used entry: keeping them
// risks touching pages the guest has already recycled.
func (d *Device) evict(segs []Seg) {
	for _, sg := range segs {
		r, ok := d.iotlb.Evict(sg.Addr)
		if !ok {
			continue
		}
		if uint64(len(r.Mem)) > evictMaxSize {
			d.iotlb.Add(r) // big permanent mapping: keep it
			continue
		}
		syscall.Munmap(r.Mem)
	}
}

func (d *Device) drain(q *queueState, ring *SplitRing) {
	q.drainMu.Lock()
	defer q.drainMu.Unlock()
	d.drainLocked(q, ring)
}

// drainLocked serves avail-ring requests; caller must hold q.drainMu.
//
// A request whose guest buffer was DMA-unmapped under us (driver
// timeout racing our completion path, reconnect leftovers) faults on
// touch: a SIGBUS inside the mmap'd regions. debug.SetPanicOnFault
// turns that into a recoverable panic; we evict the suspect region,
// complete the request with an I/O error, and keep the queue — and
// the daemon — alive.
func (d *Device) drainLocked(q *queueState, ring *SplitRing) {
	// Guest-memory faults (touching a DMA-unmapped region) become
	// recoverable panics on this goroutine; see the recover below.
	debug.SetPanicOnFault(true)
	gm := d.gm
	var doneAddrs []Seg
	var curHead int = -1
	var statusAddr uint64
	var hasStatus bool
	var pending []Seg
	var stage string
	defer func() {
		r := recover()
		d.evict(doneAddrs)
		if r == nil {
			return
		}
		fmt.Fprintf(os.Stderr, "sdsd: vduse %s q%d: guest memory fault (%v) head=%d stage=%s — evicting mappings\n",
			d.name, q.idx, r, curHead, stage)
		if !hasStatus {
			// Ring-level fault: the avail/used/desc pages themselves
			// are gone. Dump the table for diagnosis.
			rs := d.iotlb.Snapshot()
			for i, reg := range rs {
				if i >= 6 {
					fmt.Fprintf(os.Stderr, "sdsd: vduse %s q%d: iotlb: ... %d more\n", d.name, q.idx, len(rs)-i)
					break
				}
				fmt.Fprintf(os.Stderr, "sdsd: vduse %s q%d: iotlb: [%x,%x] mapped=%v\n",
					d.name, q.idx, reg.Start, reg.Last, len(reg.Mem) > 0)
			}
		}
		// Drop the per-request mappings covering the buffers we were
		// touching, so the faulting IOVA is never used again; the
		// driver re-maps (with a fresh IOVA) on the next submission.
		for _, sg := range pending {
			if reg, ok := d.iotlb.Evict(sg.Addr); ok {
				if uint64(len(reg.Mem)) <= evictMaxSize {
					syscall.Munmap(reg.Mem)
				} else {
					d.iotlb.Add(reg)
				}
			}
		}
		// Complete the request with an I/O error so the guest is not
		// left waiting: the status segment is device-writeable and
		// normally mapped fine (faults hit the data buffers).
		if hasStatus && curHead >= 0 {
			if werr := gm.Write([]byte{StatusIOErr}, statusAddr); werr == nil {
				if irq, err := ring.PushUsed(uint16(curHead), 0); err == nil && irq {
					d.injectIRQ(q.idx)
				}
			} else {
				fmt.Fprintf(os.Stderr, "sdsd: vduse %s q%d: status write after fault: %v\n", d.name, q.idx, werr)
			}
		}
	}()
	for {
		stage = "next-avail"
		head, err := ring.NextAvail()
		if err != nil || head < 0 {
			return
		}
		curHead = head
		var irq bool
		stage = "read-chain"
		segs, err := ReadChain(gm, int(ring.Size()), uint16(head))
		if err == nil {
			var br *BlkReq
			br, err = ParseBlkChain(segs, uint16(head))
			if err == nil {
				pending = br.DataSegs
				statusAddr, hasStatus = br.StatusSeg.Addr, true
				var hdr BlkReqHeader
				var written uint32
				stage = "read-header"
				hdr, err = br.ReadHeader(gm)
				var status uint8
				if err == nil {
					stage = "exec"
					status, written = d.exec(br, hdr)
					stage = "write-status"
					err = br.WriteStatus(gm, status)
				}
				hasStatus = false
				stage = "push-used"
				if irq, err = ring.PushUsed(uint16(head), written); err == nil {
					if irq {
						d.injectIRQ(q.idx)
					}
					doneAddrs = append(doneAddrs, br.DataSegs...)
				}
			}
		}
		if err != nil {
			// Malformed chain: log once and stop the queue; a broken
			// guest is not worth an infinite error loop.
			fmt.Fprintf(os.Stderr, "sdsd: vduse %s q%d: %v\n", d.name, q.idx, err)
			return
		}
	}
}

func (d *Device) injectIRQ(idx uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], idx)
	ioctl(d.devFD.Fd(), iocVQInjectIRQ, unsafe.Pointer(&b[0]))
}

// exec runs one virtio-blk request and returns (status, bytes-written-
// into-the-guest). Guest buffers are used zero-copy straight from the
// IOTLB mappings: the backend preads/pwrites the guest pages directly,
// exactly like vhost-user does.
func (d *Device) exec(br *BlkReq, hdr BlkReqHeader) (uint8, uint32) {
	gm := d.gm
	off := int64(hdr.Sector) * 512
	switch hdr.Type {
	case ReqIn:
		var n uint32
		for _, sg := range br.DataSegs {
			if sg.Write {
				return StatusIOErr, 0
			}
			b, err := gm.Slice(sg.Addr, int(sg.Len))
			if err != nil {
				return StatusIOErr, 0
			}
			if off+int64(len(b)) > int64(d.be.Size()) {
				return StatusIOErr, 0
			}
			if _, err := d.be.ReadAt(b, off); err != nil {
				return StatusIOErr, 0
			}
			off += int64(len(b))
			n += uint32(len(b))
		}
		return StatusOK, n
	case ReqOut:
		for _, sg := range br.DataSegs {
			if sg.Write {
				return StatusIOErr, 0
			}
			b, err := gm.Slice(sg.Addr, int(sg.Len))
			if err != nil {
				return StatusIOErr, 0
			}
			if off+int64(len(b)) > int64(d.be.Size()) {
				return StatusIOErr, 0
			}
			if _, err := d.be.WriteAt(b, off); err != nil {
				return StatusIOErr, 0
			}
			off += int64(len(b))
		}
		return StatusOK, 0
	case ReqFlush:
		if err := d.be.Flush(); err != nil {
			return StatusIOErr, 0
		}
		return StatusOK, 0
	case ReqGetID:
		// 20-byte serial: zeros (fixed by volume name upstream).
		for _, sg := range br.DataSegs {
			if sg.Write {
				n := int(sg.Len)
				if n > 20 {
					n = 20
				}
				b, err := gm.Slice(sg.Addr, n)
				if err == nil {
					for i := range b {
						b[i] = 0
					}
					return StatusOK, uint32(n)
				}
				return StatusIOErr, 0
			}
		}
		return StatusUnsupp, 0
	case ReqDiscard, ReqZeroOut:
		if (hdr.Type == ReqDiscard && !d.trim) || (hdr.Type == ReqZeroOut && !d.wz) {
			return StatusUnsupp, 0
		}
		// Data segs carry virtio_blk_discard_write_zeroes lists.
		for _, sg := range br.DataSegs {
			if sg.Write {
				return StatusIOErr, 0
			}
			b, err := gm.Slice(sg.Addr, int(sg.Len))
			if err != nil {
				return StatusIOErr, 0
			}
			for i := 0; i+16 <= len(b); i += 16 {
				seg, ok := ParseDiscardSeg(b[i:])
				if !ok {
					return StatusIOErr, 0
				}
				soff := int64(seg.Sector) * 512
				length := seg.NumSectors * 512
				if soff < 0 || soff+int64(length) > int64(d.be.Size()) {
					return StatusIOErr, 0
				}
				var err error
				if hdr.Type == ReqDiscard {
					err = d.be.Trim(soff, length)
				} else {
					// virtio flag bit 0 is BZVFLAG_NO_UNMAP: when set the
					// device must NOT free the space, so punching is only
					// allowed when the bit is clear.
					err = d.be.WriteZeroes(soff, length, seg.Flags&1 == 0)
				}
				if err != nil {
					return StatusIOErr, 0
				}
			}
		}
		return StatusOK, 0
	default:
		return StatusUnsupp, 0
	}
}
