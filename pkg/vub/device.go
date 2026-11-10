package vub

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/unistack-org/storage/pkg/vduse"
	"golang.org/x/sys/unix"
)

// Options configures an export.
type Options struct {
	// NumQueues is the number of request queues offered to the guest
	// (num-queues). 1 by default.
	NumQueues int
	// Workers is the number of concurrent in-flight requests per queue.
	// 4 by default.
	Workers int
	// ReadOnly exports the volume as read-only (VIRTIO_BLK_F_RO; every
	// mutating request is refused).
	ReadOnly bool
	// Trim advertises VIRTIO_BLK_F_DISCARD (the backend must implement
	// Trim).
	Trim bool
	// WriteZeroes advertises VIRTIO_BLK_F_WRITE_ZEROES.
	WriteZeroes bool
	// Logger receives operational messages (nil = discard).
	Logger func(format string, args ...any)
}

func (o Options) withDefaults() Options {
	if o.NumQueues < 1 {
		o.NumQueues = 1
	}
	if o.NumQueues > 254 {
		o.NumQueues = 254
	}
	if o.Workers < 1 {
		o.Workers = 4
	}
	if o.Logger == nil {
		o.Logger = func(string, ...any) {}
	}
	return o
}

// Exporter owns the unix socket and serves one QEMU client at a time,
// reconnecting automatically after a disconnect (VM shutdown, live
// migration abort, QEMU restart).
type Exporter struct {
	path string
	ln   net.Listener
	dev  *device

	closeOnce sync.Once
	closed    chan struct{}
}

// Listen creates the vhost-user-blk export at socket path backed by b.
// sizeBytes is the virtual disk capacity.
func Listen(path string, b vduse.Backend, sizeBytes uint64, opts Options) (*Exporter, error) {
	if b == nil {
		return nil, errors.New("vub: nil backend")
	}
	if sizeBytes == 0 {
		return nil, errors.New("vub: zero size")
	}
	opts = opts.withDefaults()
	// QEMU connects to an existing socket: remove any stale one.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("vub: remove stale socket: %w", err)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("vub: listen: %w", err)
	}
	e := &Exporter{
		path:   path,
		ln:     ln,
		closed: make(chan struct{}),
	}
	e.dev = newDevice(b, sizeBytes, opts)
	return e, nil
}

// Path is the socket path the exporter listens on.
func (e *Exporter) Path() string { return e.path }

// Attached reports whether a front-end (QEMU/Dragonball) session is
// live on the socket right now.
func (e *Exporter) Attached() bool { return e.dev.attached() }

// Serve accepts QEMU connections until Close. One client at a time; when
// the client disconnects the queues are torn down and Serve waits for the
// next connection. Returns nil after Close.
func (e *Exporter) Serve() error {
	for {
		c, err := e.ln.Accept()
		if err != nil {
			select {
			case <-e.closed:
				return nil
			default:
			}
			return err
		}
		uc, ok := c.(*net.UnixConn)
		if !ok {
			_ = c.Close()
			continue
		}
		e.dev.opts.Logger("vub: client connected on %s", e.path)
		err = e.dev.serve(WrapConn(uc))
		e.dev.reset()
		if err != nil {
			e.dev.opts.Logger("vub: session ended: %v", err)
		} else {
			e.dev.opts.Logger("vub: client disconnected")
		}
	}
}

// Close stops serving and removes the socket. In-flight guest IO on an
// active session is abandoned (the guest blocks until QEMU detaches the
// device; unistord shutdown order expects exports to be removed first).
func (e *Exporter) Close() error {
	var err error
	e.closeOnce.Do(func() {
		close(e.closed)
		err = e.ln.Close()
		e.dev.reset()
		_ = os.Remove(e.path)
	})
	return err
}

// device is the vhost-user-blk backend state machine. All protocol
// handling runs on the Serve goroutine; only the per-queue dispatchers
// and workers run concurrently (they touch the shared memory table via
// the IOTLB's own locking).
type device struct {
	b    vduse.Backend
	size uint64
	opts Options

	mu sync.Mutex
	// protocol state
	features   uint64 // advertised device features
	negotiated uint64 // guest features (SET_FEATURES)
	protoNegot uint64 // negotiated protocol features
	conn       *Conn
	mem        *vduse.IOTLB
	gm         vduse.SliceGuestMem
	queues     []*queue
	regionFds  []*os.File
	closed     bool
}

// newDevice computes the feature words the backend advertises.
// VIRTIO_RING_F_EVENT_IDX is deliberately NOT offered: we are kicked by
// eventfd on every doorbell, and a used-event index the guest does not
// keep fresh would stall completion pickup (same stance as pkg/vduse).
func newDevice(b vduse.Backend, sizeBytes uint64, opts Options) *device {
	feat := uint64(fBlkSizeMax | fBlkSegMax | fBlkBlkSize | fBlkTopology | fBlkFlush |
		fRingIndirect | fVersion1)
	if opts.ReadOnly {
		feat |= fBlkRO
	}
	if opts.Trim {
		feat |= fBlkDiscard
	}
	if opts.WriteZeroes {
		feat |= fBlkZeroes
	}
	// VHOST_USER_F_PROTOCOL_FEATURES (bit 30): QEMU only drives the
	// GET/SET_PROTOCOL_FEATURES exchange when the backend advertises it.
	feat |= fProtocolFeatures
	return &device{
		b:        b,
		size:     sizeBytes,
		opts:     opts,
		features: feat,
		queues:   make([]*queue, opts.NumQueues),
	}
}

// attached reports a live session (serve bound the connection and the
// device was not reset since).
func (d *device) attached() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.conn != nil && !d.closed
}

// serve runs one vhost-user session until the peer disconnects.
func (d *device) serve(c *Conn) error {
	d.mu.Lock()
	d.conn = c
	d.mu.Unlock()
	for {
		h, body, fds, err := c.ReadMessage()
		if err != nil {
			for _, fd := range fds {
				unix.Close(fd)
			}
			return err
		}
		if err := d.handle(c, h, body, fds); err != nil {
			for _, fd := range fds {
				unix.Close(fd)
			}
			return err
		}
	}
}

// handle processes one request. On success it has consumed fds (taken
// ownership); the caller closes any leftovers.
func (d *device) handle(c *Conn, h Header, body []byte, fds []int) error {
	// needReply: the front-end set NEED_REPLY on this request (only
	// legal when REPLY_ACK is negotiated); the ack must echo the flag.
	needReply := h.Flags&flagNeedReply != 0
	ack := func() error {
		if needReply {
			return c.ReplyU64(h.Request, 0, true)
		}
		return nil
	}
	switch h.Request {
	case ReqGetFeatures:
		d.mu.Lock()
		f := d.features
		d.mu.Unlock()
		return c.ReplyU64(h.Request, f, false)
	case ReqSetFeatures:
		if len(body) < 8 {
			return errors.New("vub: SET_FEATURES short")
		}
		d.mu.Lock()
		d.negotiated = binary.LittleEndian.Uint64(body)
		neg := d.negotiated
		d.mu.Unlock()
		// QEMU keeps bit 30 in the word; mask it out for readability.
		d.opts.Logger("vub: guest features 0x%x (blk 0x%x)", neg, neg&^fProtocolFeatures)
		return ack()
	case ReqGetProtocolFeatures:
		return c.ReplyU64(h.Request, protoFeatures, false)
	case ReqSetProtocolFeatures:
		if len(body) < 8 {
			return errors.New("vub: SET_PROTOCOL_FEATURES short")
		}
		d.mu.Lock()
		d.protoNegot = binary.LittleEndian.Uint64(body) & protoFeatures
		d.opts.Logger("vub: protocol features 0x%x", d.protoNegot)
		d.mu.Unlock()
		return nil // SET_PROTOCOL_FEATURES never has an ack
	case ReqSetOwner, ReqResetOwner:
		return nil
	case ReqResetDevice:
		d.reset()
		return ack()
	case ReqGetQueueNum:
		return c.ReplyU64(h.Request, uint64(d.opts.NumQueues), false)
	case ReqSetMemTable:
		if err := d.setMemTable(body, fds); err != nil {
			return err
		}
		// setMemTable took ownership of every fd (mapped what it
		// needs, closed the rest).
		for i := range fds {
			fds[i] = -1
		}
		return nil
	case ReqSetVringNum:
		idx, num, err := vringState(body)
		if err != nil {
			return err
		}
		if err := d.setVringNum(idx, num); err != nil {
			return err
		}
		// QEMU reads a reply for NUM/ADDR/BASE (vhost_user_exchange:
		// the ack under REPLY_ACK, the payload reply for GET_VRING_BASE
		// only differs in content).
		return ack()
	case ReqSetVringAddr:
		if err := d.setVringAddr(body); err != nil {
			return err
		}
		return ack()
	case ReqSetVringBase:
		idx, avail, err := vringState(body)
		if err != nil {
			return err
		}
		if err := d.setVringBase(idx, uint16(avail)); err != nil {
			return err
		}
		return ack()
	case ReqGetVringBase, ReqGetVringBaseSkipDrain:
		// QEMU sends SKIP_DRAIN (45) on teardown; draining is our
		// documented behaviour and is always safe (workers finish
		// published jobs), so both requests are handled identically.
		idx, _, err := vringState(body)
		if err != nil {
			return err
		}
		return c.ReplyU64(h.Request, d.getVringBase(idx), false)
	case ReqGetMaxMemSlots:
		// QEMU 8+ queries this before SET_MEM_TABLE. We accept an
		// arbitrary number of regions in one table, so advertise a
		// generous bound.
		return c.ReplyU64(h.Request, maxMemSlots, false)
	case ReqSetVringKick:
		return d.setVringFD(body, fds, true)
	case ReqSetVringCall:
		return d.setVringFD(body, fds, false)
	case ReqSetVringEnable:
		idx, en, err := vringState(body)
		if err != nil {
			return err
		}
		d.mu.Lock()
		q, qerr := d.queueAt(idx)
		d.mu.Unlock()
		if qerr != nil {
			return qerr
		}
		if en != 0 {
			q.start()
		} else {
			q.stop()
		}
		return ack()
	case ReqGetConfig:
		// VhostUserConfig request: offset@0 size@4 flags@8.
		if len(body) < configHdrSize {
			return errors.New("vub: GET_CONFIG short")
		}
		off := binary.LittleEndian.Uint32(body)
		size := binary.LittleEndian.Uint32(body[4:])
		cfg := vduse.ConfigSpace(d.size, uint16(d.opts.NumQueues))
		if uint64(off)+uint64(size) > uint64(len(cfg)) || size > configMax {
			return fmt.Errorf("vub: GET_CONFIG off=%d size=%d out of range", off, size)
		}
		// Reply echoes the request framing: header + {offset,size,0}
		// + region bytes, so the total payload is 12+size.
		out := make([]byte, 0, configHdrSize+int(size))
		out = append(out, body[:configHdrSize]...)
		binary.LittleEndian.PutUint32(out[8:], 0) // flags: ok
		out = append(out, cfg[off:off+size]...)
		return c.Reply(h.Request, out, false)
	case ReqSetConfig:
		// Our config space is read-only except wce (fixed at 1):
		// refuse any non-empty write.
		if len(body) < configHdrSize {
			return errors.New("vub: SET_CONFIG short")
		}
		size := binary.LittleEndian.Uint32(body[4:])
		if size == 0 {
			return nil
		}
		return fmt.Errorf("vub: SET_CONFIG: config space is read-only (%d bytes at offset %d)",
			size, binary.LittleEndian.Uint32(body))
	case ReqSetVringErr, ReqSetLogBase, ReqSetLogFD:
		// Not relevant for a KVM-only export: accept and ignore.
		return ack()
	default:
		// Unimplemented requests (ADD/REM_MEM_REG, SET_STATUS,
		// GET_SHMEM_CONFIG, ...) are a hard protocol error: QEMU
		// treats a failed request as a backend shutdown and only
		// probes what the negotiated feature set enables.
		return fmt.Errorf("vub: unsupported request %d", h.Request)
	}
}

// setMemTable maps the guest physical memory and publishes the table.
// Regions are mmap'd shared at the QEMU-provided fd+offset; a GPA maps
// at userspace_addr + (GPA - start) (the IOTLB works in the flat model:
// it keeps the local mmap slice per region).
func (d *device) setMemTable(body []byte, fds []int) error {
	regions, err := parseMemTable(body)
	if err != nil {
		return err
	}
	if len(fds) < len(regions) {
		return fmt.Errorf("vub: SET_MEM_TABLE: %d regions, %d fds", len(regions), len(fds))
	}
	iotlb := &vduse.IOTLB{}
	files := make([]*os.File, 0, len(regions))
	closeAll := func() {
		for _, f := range files {
			_ = f.Close()
		}
		iotlb.ReleaseAll()
	}
	for i, r := range regions {
		if r.Size == 0 {
			continue
		}
		// Keep a private reference: QEMU may close its copy any time.
		f := fdOwner(fds[i])
		files = append(files, f)
		mem, err := unix.Mmap(int(f.Fd()), int64(r.MmapOffset), int(r.Size),
			unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			closeAll()
			return fmt.Errorf("vub: mmap region %d (gpa=%#x size=%d): %w", i, r.GuestPhys, r.Size, err)
		}
		iotlb.Add(vduse.Region{
			Start: r.GuestPhys,
			Last:  r.GuestPhys + r.Size - 1,
			Mem:   mem,
		})
	}
	d.mu.Lock()
	old := d.mem
	oldFds := d.regionFds
	d.mem = iotlb
	d.gm = vduse.NewGuestMemWithPull(iotlb, nil)
	d.regionFds = files
	var running []*queue
	for _, q := range d.queues {
		if q != nil && q.running {
			running = append(running, q)
		}
	}
	d.mu.Unlock()
	// Hot-replaced table: stop the queues referencing the old
	// mappings (QEMU rewrites the table only while quiesced; running
	// queues would race the munmap).
	for _, q := range running {
		q.stop()
	}
	for _, f := range oldFds {
		_ = f.Close()
	}
	if old != nil {
		old.ReleaseAll()
	}
	return nil
}

// queueAt returns (creating on demand) the queue state for idx.
// d.mu must be held.
func (d *device) queueAt(idx int) (*queue, error) {
	if idx < 0 || idx >= len(d.queues) {
		return nil, fmt.Errorf("vub: queue index %d out of range", idx)
	}
	q := d.queues[idx]
	if q == nil {
		q = &queue{idx: idx, num: vqMaxSize, d: d, kickFD: -1}
		d.queues[idx] = q
	}
	return q, nil
}

func (d *device) setVringNum(idx int, num uint32) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	q, err := d.queueAt(idx)
	if err != nil {
		return err
	}
	if q.running {
		return fmt.Errorf("vub: queue %d live: can't set size", idx)
	}
	if num == 0 || num > vqMaxSize {
		return fmt.Errorf("vub: queue %d size %d", idx, num)
	}
	q.num = uint16(num)
	return nil
}

func (d *device) setVringAddr(body []byte) error {
	idx, desc, used, avail, err := vringAddr(body)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	q, err := d.queueAt(idx)
	if err != nil {
		return err
	}
	if q.running {
		return fmt.Errorf("vub: queue %d live: can't set ring address", idx)
	}
	if d.gm == nil {
		return errors.New("vub: SET_VRING_ADDR before SET_MEM_TABLE")
	}
	// NewSplitRing validates the ring addresses against the memory
	// table. Ring state (avail/used cursors) is kept in the device
	// across ring recreation: a QEMU save/restore cycle sends
	// SET_VRING_BASE (avail resume) followed by SET_VRING_ADDR; the
	// used cursor is whatever we last had (fresh rings start at 0,
	// matching QEMU's rebuilt used ring).
	var availIdx, usedIdx uint16
	if q.ring != nil {
		availIdx, usedIdx = q.ring.State()
	} else {
		availIdx, usedIdx = q.base, q.lastUsed
	}
	q.ring = vduse.NewSplitRing(d.gm, q.num, desc, avail, used)
	q.gm = d.gm
	q.ring.SetState(availIdx, usedIdx)
	return nil
}

func (d *device) setVringBase(idx int, avail uint16) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	q, err := d.queueAt(idx)
	if err != nil {
		return err
	}
	q.base = avail
	if q.ring != nil {
		_, usedIdx := q.ring.State()
		q.ring.SetState(avail, usedIdx)
	}
	return nil
}

// getVringBase stops the queue, drains its inflight IO, and returns the
// packed vhost_vring_state reply {index, last avail index} QEMU wants
// (used for a later SET_VRING_BASE resume).
func (d *device) getVringBase(idx int) uint64 {
	d.mu.Lock()
	q := d.queues[idx]
	d.mu.Unlock()
	if q == nil {
		return getVringBaseReply(idx, 0)
	}
	q.stop()
	d.mu.Lock()
	if q.ring != nil {
		availIdx, usedIdx := q.ring.State()
		q.base, q.lastUsed = availIdx, usedIdx
	}
	base := q.base
	d.mu.Unlock()
	return getVringBaseReply(idx, base)
}

// setVringFD attaches a kick (isKick) or call fd to a queue. Payload is
// vhost_vring_state: index in bits 0..7 of the u64, bit 8 = "no fd
// supplied" (poll mode). The fd from the control message becomes ours.
func (d *device) setVringFD(body []byte, fds []int, isKick bool) error {
	idx, noFD, err := kickFD(body)
	if err != nil {
		return err
	}
	var f *os.File
	if !noFD && len(fds) > 0 {
		// SCM_RIGHTS dups arrive in blocking mode, and os.NewFile only
		// registers a descriptor with the Go poller when it is already
		// O_NONBLOCK: without this, a blocking kick read can never be
		// interrupted by stop()'s read deadline and queue teardown
		// hangs forever. (eventfd semantics are unaffected.)
		if _, err := unix.FcntlInt(uintptr(fds[0]), unix.F_SETFL, unix.O_NONBLOCK); err != nil {
			return fmt.Errorf("vub: SET_VRING_FD: O_NONBLOCK: %w", err)
		}
		f = fdOwner(fds[0])
		fds[0] = -1 // consumed
		for i := 1; i < len(fds); i++ {
			fds[i] = -1 // extras are ours to close
			unix.Close(fds[i])
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	q, err := d.queueAt(idx)
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return err
	}
	if q.running {
		if f != nil {
			_ = f.Close()
		}
		return fmt.Errorf("vub: queue %d live: can't replace kick/call fd", idx)
	}
	name := "CALL"
	if isKick {
		name = "KICK"
	}
	if f == nil {
		if noFD && !isKick {
			// A call queue with no fd means "never signal": acceptable
			// (polling guest); for kick it means poll mode.
			d.opts.Logger("vub: q%d SET_VRING_%S: no fd", idx, name)
		}
		if !noFD {
			return fmt.Errorf("vub: SET_VRING_%s: no fd supplied", name)
		}
		// Poll mode: drop the previous fd if any.
		if isKick && q.kick != nil {
			_ = q.kick.Close()
			q.kick = nil
		}
		return nil
	}
	if isKick {
		if q.kick != nil {
			_ = q.kick.Close()
		}
		q.kick = f
		q.kickFD = int32(f.Fd())
		// The dispatcher polls the raw fd (unix.Poll + nonblocking
		// read): SCM_RIGHTS fds are not registered with Go's netpoll
		// on macOS, so a parked os.File read cannot be woken by a
		// deadline there, and poll(2)+timeout is portable to Linux
		// eventfds as well.
		if err := unix.SetNonblock(int(f.Fd()), true); err != nil {
			_ = f.Close()
			q.kick = nil
			q.kickFD = -1
			return fmt.Errorf("vub: SET_VRING_KICK %d: %w", idx, err)
		}
	} else {
		if q.call != nil {
			_ = q.call.Close()
		}
		q.call = f
	}
	return nil
}

// exec runs one parsed virtio-blk request against the backend. It never
// panics on guest-memory faults: callers (workers) run it under a
// recover guard with every data segment pinned.
func (d *device) exec(br *vduse.BlkReq, hdr vduse.BlkReqHeader) (uint8, uint32) {
	gm := d.gm
	off := int64(hdr.Sector) * 512
	// A read-only device refuses every request that mutates the image:
	// F_RO is not advisory, we are the enforcement point.
	if d.opts.ReadOnly {
		switch hdr.Type {
		case vduse.ReqOut, vduse.ReqDiscard, vduse.ReqZeroOut:
			return vduse.StatusIOErr, 0
		}
	}
	switch hdr.Type {
	case vduse.ReqIn:
		// Device writes data: every data segment must be device-
		// writeable (VRING_DESC_F_WRITE).
		for _, sg := range br.DataSegs {
			if !sg.Write {
				return vduse.StatusIOErr, 0
			}
		}
		n, st := d.execIO(br.DataSegs, true, off)
		return st, uint32(n)
	case vduse.ReqOut:
		for _, sg := range br.DataSegs {
			if sg.Write {
				return vduse.StatusIOErr, 0
			}
		}
		_, st := d.execIO(br.DataSegs, false, off)
		return st, 0
	case vduse.ReqFlush:
		if d.b != nil {
			if err := d.b.Flush(); err != nil {
				return vduse.StatusIOErr, 0
			}
		}
		return vduse.StatusOK, 0
	case vduse.ReqGetID:
		// 20-byte serial: zeros (the volume name is fixed upstream).
		for _, sg := range br.DataSegs {
			if sg.Write {
				n := int(sg.Len)
				if n > 20 {
					n = 20
				}
				b, err := gm.Slice(sg.Addr, n)
				if err != nil {
					return vduse.StatusIOErr, 0
				}
				for i := range b {
					b[i] = 0
				}
				return vduse.StatusOK, uint32(n)
			}
		}
		return vduse.StatusUnsupp, 0
	case vduse.ReqDiscard, vduse.ReqZeroOut:
		if (hdr.Type == vduse.ReqDiscard && !d.opts.Trim) ||
			(hdr.Type == vduse.ReqZeroOut && !d.opts.WriteZeroes) {
			return vduse.StatusUnsupp, 0
		}
		for _, sg := range br.DataSegs {
			if sg.Write {
				return vduse.StatusIOErr, 0
			}
			b, err := gm.Slice(sg.Addr, int(sg.Len))
			if err != nil {
				return vduse.StatusIOErr, 0
			}
			for i := 0; i+16 <= len(b); i += 16 {
				seg, ok := vduse.ParseDiscardSeg(b[i:])
				if !ok {
					return vduse.StatusIOErr, 0
				}
				soff := int64(seg.Sector) * 512
				length := seg.NumSectors * 512
				if soff < 0 || soff+int64(length) > int64(d.b.Size()) {
					return vduse.StatusIOErr, 0
				}
				var err error
				if hdr.Type == vduse.ReqDiscard {
					err = d.b.Trim(soff, length)
				} else {
					// virtio flag bit 0 is BZVFLAG_NO_UNMAP: when set
					// the device must NOT free the space.
					err = d.b.WriteZeroes(soff, length, seg.Flags&1 == 0)
				}
				if err != nil {
					return vduse.StatusIOErr, 0
				}
			}
		}
		return vduse.StatusOK, 0
	default:
		return vduse.StatusUnsupp, 0
	}
}

// execIO copies between the backend and a request's data segments. For
// reads (toGuest) it fills the segments in chain order; for writes it
// gathers into one scratch buffer (a single WriteAt beats per-segment
// round-trips for the common single-segment case too: the copy is one
// memcpy from pinned guest memory).
func (d *device) execIO(segs []vduse.Seg, toGuest bool, off int64) (int, uint8) {
	var total int
	if toGuest {
		for _, sg := range segs {
			buf, err := d.gm.Slice(sg.Addr, int(sg.Len))
			if err != nil {
				return total, vduse.StatusIOErr
			}
			n, err := d.b.ReadAt(buf, off+int64(total))
			total += n
			if err != nil {
				if n > 0 {
					return total, vduse.StatusOK // partial: caller completes with what fit
				}
				return total, vduse.StatusIOErr
			}
			if n < int(sg.Len) {
				// Short read: stop, report the bytes produced.
				return total, vduse.StatusOK
			}
		}
		return total, vduse.StatusOK
	}
	// Writes: gather then scatter in one backend call.
	var n int
	for _, sg := range segs {
		n += int(sg.Len)
	}
	buf := make([]byte, n)
	i := 0
	for _, sg := range segs {
		src, err := d.gm.Slice(sg.Addr, int(sg.Len))
		if err != nil {
			return 0, vduse.StatusIOErr
		}
		i += copy(buf[i:], src)
	}
	written, err := d.b.WriteAt(buf, off)
	if err != nil {
		if written > 0 {
			return written, vduse.StatusOK
		}
		return 0, vduse.StatusIOErr
	}
	return written, vduse.StatusOK
}

// reset tears down the whole device state (RESET_DEVICE, owner reset,
// disconnect). Queues stop and drain; rings, fds, and mappings are
// dropped. The backend stays open for the next session.
func (d *device) reset() {
	d.mu.Lock()
	queues := make([]*queue, 0, len(d.queues))
	for i, q := range d.queues {
		if q != nil {
			queues = append(queues, q)
			d.queues[i] = nil
		}
	}
	d.mu.Unlock()
	// Drain the queues BEFORE dropping the memory table: workers may
	// still be executing requests against those mappings.
	for _, q := range queues {
		q.stop()
		q.closeFDs()
	}
	d.mu.Lock()
	old := d.mem
	oldFds := d.regionFds
	d.mem = nil
	d.gm = nil
	d.regionFds = nil
	d.conn = nil
	d.negotiated = 0
	d.protoNegot = 0
	d.mu.Unlock()
	for _, f := range oldFds {
		_ = f.Close()
	}
	if old != nil {
		old.ReleaseAll()
	}
}
