//go:build linux

package vduse

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// VDUSE ioctl numbers (uapi/linux/vduse.h), _IOC(dir,size,type,nr) with
// dir NONE=1 WRITE=2 READ=3 on Linux encoding, type=0x81.
const (
	iocGetAPIVersion  = 0x80080181 // _IOR(0x01, u64)
	iocSetAPIVersion  = 0x40080181 // _IOW(0x01, u64)
	iocCreateDev      = 0x41500281 // _IOW(0x02, vduse_dev_config=336)
	iocDestroyDev     = 0x41000381 // _IOW(0x03, char[256])
	iocIotlbGetFD     = 0xc0201081 // _IOWR(0x10, vduse_iotlb_entry=32)
	iocDevGetFeatures = 0x80081181 // _IOR(0x11, u64)
	iocVQSetup        = 0x40201481 // _IOW(0x14, vduse_vq_config=32)
	iocVQGetInfo      = 0xc0301581 // _IOWR(0x15, vduse_vq_info=48)
	iocVQSetupKickFD  = 0x40081681 // _IOW(0x16, vduse_vq_eventfd=8)
	iocVQInjectIRQ    = 0x40041781 // _IOW(0x17, u32)
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
	if _, err := ioctl(cf.Fd(), iocSetAPIVersion, unsafe.Pointer(&ver)); err != nil {
		return fmt.Errorf("vduse: set api version: %w", err)
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
}

// Device is a running VDUSE virtio-blk device backed by be.
type Device struct {
	name string
	be   Backend
	nq   uint16

	devFD *os.File // open on /dev/vduse/<name>: keeps the device attached
	iotlb *IOTLB
	gm    SliceGuestMem

	mu   sync.Mutex
	vqs  []*queueState
	trim bool
	wz   bool
	stop chan struct{}
	done chan struct{}
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

// NewDevice creates the kernel device and opens it, which attaches it to
// the vDPA bus; the VM can then bind it with `vdpa add ...`.
func NewDevice(name string, opts Options, be Backend) (*Device, error) {
	nq := opts.NumQueues
	if nq < 1 {
		nq = 1
	}
	features := FeatureMask(opts.Trim, opts.WriteZeroes)
	if err := CreateDevice(name, nq, features, ConfigSpace(be.Size(), nq)); err != nil {
		return nil, err
	}
	df, err := os.OpenFile("/dev/vduse/"+name, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("vduse: open dev: %w", err)
	}
	d := &Device{
		name:  name,
		be:    be,
		nq:    nq,
		devFD: df,
		iotlb: &IOTLB{},
		trim:  opts.Trim,
		wz:    opts.WriteZeroes,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	d.gm = NewGuestMem(d.iotlb).(SliceGuestMem)
	// The kernel requires all vqs set up (with max ring size) before the
	// device can attach, and a kick eventfd per queue.
	d.vqs = make([]*queueState, nq)
	for i := 0; i < int(nq); i++ {
		var vc [32]byte
		binary.LittleEndian.PutUint32(vc[0:], uint32(i))
		binary.LittleEndian.PutUint16(vc[4:], 256) // max ring size
		if _, err := ioctl(df.Fd(), iocVQSetup, unsafe.Pointer(&vc[0])); err != nil {
			d.Close()
			return nil, fmt.Errorf("vduse: vq setup %d: %w", i, err)
		}
		efd, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, uintptr(efdCloexec), 0)
		if errno != 0 {
			d.Close()
			return nil, fmt.Errorf("vduse: eventfd: %w", errno)
		}
		kf := os.NewFile(uintptr(efd), "vduse-kick")
		var ea [8]byte
		binary.LittleEndian.PutUint32(ea[0:], uint32(i))
		binary.LittleEndian.PutUint32(ea[4:], uint32(int32(efd)))
		if _, err := ioctl(df.Fd(), iocVQSetupKickFD, unsafe.Pointer(&ea[0])); err != nil {
			kf.Close()
			d.Close()
			return nil, fmt.Errorf("vduse: kickfd %d: %w", i, err)
		}
		d.vqs[i] = &queueState{idx: uint32(i), kickFD: kf}
	}
	return d, nil
}

// Serve runs the control-message loop and kick watchers until Close.
func (d *Device) Serve() {
	go d.msgLoop()
	for _, q := range d.vqs {
		go d.kickLoop(q)
	}
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
				// Driver enabled: fetch ring addresses from the kernel.
				for _, q := range d.vqs {
					if err := d.vqRefresh(q); err != nil {
						resp[4] = reqResultFailed
					}
				}
			}
		case reqUpdateIOTLB:
			start := binary.LittleEndian.Uint64(buf[24:])
			last := binary.LittleEndian.Uint64(buf[32:])
			if err := d.iotlbApply(start, last); err != nil {
				resp[4] = reqResultFailed
			}
		case reqSetVQReady, reqSuspend, reqSetVQGroupASID:
			// API v0 does not negotiate these; acknowledge.
		default:
			resp[4] = reqResultFailed
		}
		if _, err := d.devFD.Write(resp[:]); err != nil {
			return
		}
	}
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
		d.iotlb.Add(Region{Start: mstart, Last: mlast, Mem: base})
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
	availIdx := binary.LittleEndian.Uint16(info[32:])
	ready := info[40] != 0
	d.mu.Lock()
	defer d.mu.Unlock()
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
func (d *Device) kickLoop(q *queueState) {
	b := make([]byte, 8)
	for {
		if _, err := q.kickFD.Read(b); err != nil {
			return
		}
		d.mu.Lock()
		ring := q.ring
		d.mu.Unlock()
		if ring == nil {
			continue
		}
		d.drain(q, ring)
	}
}

func (d *Device) drain(q *queueState, ring *SplitRing) {
	gm := d.gm
	for {
		head, err := ring.NextAvail()
		if err != nil || head < 0 {
			return
		}
		var irq bool
		segs, err := ReadChain(gm, int(ring.Size()), uint16(head))
		if err == nil {
			var br *BlkReq
			br, err = ParseBlkChain(segs, uint16(head))
			if err == nil {
				var hdr BlkReqHeader
				var written uint32
				hdr, err = br.ReadHeader(gm)
				var status uint8
				if err == nil {
					status, written = d.exec(br, hdr)
					err = br.WriteStatus(gm, status)
				}
				if irq, err = ring.PushUsed(uint16(head), written); err == nil && irq {
					d.injectIRQ(q.idx)
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
					err = d.be.WriteZeroes(soff, length, seg.Flags&1 != 0)
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
