package vub

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/unistack-org/storage/pkg/vduse"
)

// ---------------------------------------------------------------------------
// In-memory backend with instrumentation.
// ---------------------------------------------------------------------------

type memBackend struct {
	mu      sync.Mutex
	data    []byte
	flushes int
	trims   [][3]int64 // {off, length, punchHole?1:0}
	zeros   [][3]int64
}

func newMemBackend(size uint64) *memBackend {
	return &memBackend{data: make([]byte, size)}
}

func (m *memBackend) Size() uint64 { return uint64(len(m.data)) }

func (m *memBackend) ReadAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off < 0 || off >= int64(len(m.data)) {
		return 0, fmt.Errorf("read off %d out of range", off)
	}
	return copy(p, m.data[off:]), nil
}

func (m *memBackend) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if off < 0 || off+int64(len(p)) > int64(len(m.data)) {
		return 0, fmt.Errorf("write off %d len %d out of range", off, len(p))
	}
	copy(m.data[off:], p)
	return len(p), nil
}

func (m *memBackend) Flush() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushes++
	return nil
}

// Trim is the 2-arg vduse.Backend method: ReqDiscard always punches.
func (m *memBackend) Trim(off int64, length uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trims = append(m.trims, [3]int64{off, int64(length), 1})
	return nil
}

func (m *memBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.zeros = append(m.zeros, [3]int64{off, int64(length), b2i(punchHole)})
	return nil
}

func (m *memBackend) Close() error { return nil }

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func (m *memBackend) snap() (flushes int, trims, zeros [][3]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flushes, append([][3]int64(nil), m.trims...), append([][3]int64(nil), m.zeros...)
}

func (m *memBackend) bytes(off, n int) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.data[off:off+n]...)
}

var _ vduse.Backend = (*memBackend)(nil)

// ---------------------------------------------------------------------------
// Guest RAM: a file-backed shared mmap so SCM_RIGHTS of its fd gives
// the backend a mapping of the very same pages (memfd is Linux-only).
// ---------------------------------------------------------------------------

const guestSize = 1 << 20 // 1 MiB is plenty for unit tests

type guestMem struct {
	t    *testing.T
	file *os.File
	mem  []byte
}

func newGuestMem(t *testing.T) *guestMem {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "guestmem")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(guestSize); err != nil {
		t.Fatal(err)
	}
	b, err := unix.Mmap(int(f.Fd()), 0, guestSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	g := &guestMem{t: t, file: f, mem: b}
	t.Cleanup(g.close)
	return g
}

func (g *guestMem) close() {
	_ = unix.Munmap(g.mem)
	_ = g.file.Close()
}

func (g *guestMem) fd() int { return int(g.file.Fd()) }

func (g *guestMem) put(off uint64, b []byte) { copy(g.mem[off:], b) }
func (g *guestMem) get(off uint64, n int) []byte {
	return append([]byte(nil), g.mem[off:off+uint64(n)]...)
}
func (g *guestMem) u16(off uint64) uint16       { return binary.LittleEndian.Uint16(g.mem[off:]) }
func (g *guestMem) putU16(off uint64, v uint16) { binary.LittleEndian.PutUint16(g.mem[off:], v) }
func (g *guestMem) u32(off uint64) uint32       { return binary.LittleEndian.Uint32(g.mem[off:]) }
func (g *guestMem) putU32(off uint64, v uint32) { binary.LittleEndian.PutUint32(g.mem[off:], v) }
func (g *guestMem) u64(off uint64) uint64       { return binary.LittleEndian.Uint64(g.mem[off:]) }
func (g *guestMem) putU64(off uint64, v uint64) { binary.LittleEndian.PutUint64(g.mem[off:], v) }
func (g *guestMem) u8(off uint64) uint8         { return g.mem[off] }
func (g *guestMem) putU8(off uint64, v uint8)   { g.mem[off] = v }

// ---------------------------------------------------------------------------
// Fake QEMU: the vhost-user control channel plus split-ring plumbing.
// ---------------------------------------------------------------------------

const (
	// descriptor flags (VRING_DESC_F_*)
	descNext  = 1 << 0
	descWrite = 1 << 1
)

// virtio-blk request types (virtio_blk_req.type)
const (
	blkIn      = vduse.ReqIn
	blkOut     = vduse.ReqOut
	blkFlush   = vduse.ReqFlush
	blkGetID   = vduse.ReqGetID
	blkDiscard = vduse.ReqDiscard
	blkZeroOut = vduse.ReqZeroOut
)

type vq struct {
	q        *fakeQEMU
	idx      int
	size     uint16
	desc     uint64
	avail    uint64
	used     uint64
	kickW    *os.File // our write end of the kick pipe
	callR    *os.File // our read end of the call pipe
	availIdx uint16   // driver cursor (avail.idx)
	usedSeen uint16   // driver cursor (consumed used.idx)
	seq      uint64   // kick counter
	// head descriptor index -> status byte address, so a test can read
	// back the status of a completed request.
	statusAddr map[uint16]uint64
}

type fakeQEMU struct {
	t   *testing.T
	e   *Exporter
	cli *Conn
	gm  *guestMem
}

// startQEMU brings an Exporter up on a temp socket and connects the
// fake front-end.
func startQEMU(t *testing.T, b vduse.Backend, size uint64, opts Options) *fakeQEMU {
	t.Helper()
	path := t.TempDir() + "/vhost.sock"
	e, err := Listen(path, b, size, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	go func() { _ = e.Serve() }()
	c, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	q := &fakeQEMU{t: t, e: e, gm: newGuestMem(t)}
	q.cli = WrapConn(c.(*net.UnixConn))
	return q
}

func (q *fakeQEMU) request(req uint32, flags uint32, body []byte, fds []int) {
	q.t.Helper()
	if err := q.cli.write(req, flagVersion|flags, body, fds); err != nil {
		q.t.Fatalf("request %d: %v", req, err)
	}
}

func (q *fakeQEMU) reply(wantReq uint32, wantFlags uint32) []byte {
	q.t.Helper()
	q.cli.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer q.cli.c.SetReadDeadline(time.Time{})
	h, body, fds, err := q.cli.ReadMessage()
	for _, fd := range fds {
		unix.Close(fd)
	}
	if err != nil {
		q.t.Fatalf("reply to %d: %v", wantReq, err)
	}
	if h.Request != wantReq {
		q.t.Fatalf("reply request = %d, want %d", h.Request, wantReq)
	}
	if h.Flags&(flagReply|flagVersion) != (flagReply | flagVersion) {
		q.t.Fatalf("reply flags = %#x, want REPLY|VERSION", h.Flags)
	}
	if wantFlags != 0 && h.Flags != flagVersion|flagReply|wantFlags {
		q.t.Fatalf("reply flags = %#x, want %#x", h.Flags, flagVersion|flagReply|wantFlags)
	}
	return body
}

func (q *fakeQEMU) replyU64(wantReq, wantFlags uint32) uint64 {
	q.t.Helper()
	b := q.reply(wantReq, wantFlags)
	if len(b) != 8 {
		q.t.Fatalf("reply to %d: %d bytes, want u64", wantReq, len(b))
	}
	return binary.LittleEndian.Uint64(b)
}

// qcall: request QEMU sends with REPLY_ACK armed (NEED_REPLY set); the
// ack must echo the flag and carry 0.
func (q *fakeQEMU) qcall(req uint32, body []byte) {
	q.t.Helper()
	q.request(req, flagNeedReply, body, nil)
	if v := q.replyU64(req, flagNeedReply); v != 0 {
		q.t.Fatalf("request %d: ack = %d, want 0", req, v)
	}
}

// qfire: fire-and-forget requests (QEMU never reads a reply for these:
// SET_PROTOCOL_FEATURES, SET_MEM_TABLE, SET_VRING_KICK/CALL).
func (q *fakeQEMU) qfire(req uint32, body []byte, fds []int) {
	q.t.Helper()
	q.request(req, 0, body, fds)
}

// qget: exchange-pattern requests (GET_*): reply carries the payload.
func (q *fakeQEMU) qget(req uint32, body []byte) uint64 {
	q.t.Helper()
	q.request(req, 0, body, nil)
	return q.replyU64(req, 0)
}

// handshake runs the QEMU bring-up: features, protocol features, owner,
// mem table.
func (q *fakeQEMU) handshake() uint64 {
	q.t.Helper()
	devFeat := q.qget(ReqGetFeatures, nil)
	if devFeat&(1<<30) == 0 {
		q.t.Fatalf("device features %#x: F_PROTOCOL_FEATURES (bit 30) missing", devFeat)
	}
	protoFeat := q.qget(ReqGetProtocolFeatures, nil)
	want := uint64(1<<protoFMQ | 1<<protoFReplyAck | 1<<protoFConfig | 1<<protoFResetDevice)
	if protoFeat&want != want {
		q.t.Fatalf("protocol features %#x, want at least %#x", protoFeat, want)
	}
	q.qfire(ReqSetProtocolFeatures, u64bytes(protoFeat&want), nil)
	q.qcall(ReqSetFeatures, u64bytes(devFeat))
	q.qfire(ReqSetOwner, nil, nil) // SET_OWNER: no reply in QEMU
	// One region: all guest RAM mapped at GPA 0. QEMU's struct is
	// {u32 nregions; u32 padding; regions[32]} — 40 bytes for one.
	reg := make([]byte, 32)
	binary.LittleEndian.PutUint64(reg[0:], 0)
	binary.LittleEndian.PutUint64(reg[8:], guestSize)
	binary.LittleEndian.PutUint64(reg[16:], 0x7f0000000000)
	body := append(append(u32bytes(1), 0, 0, 0, 0), reg...)
	q.qfire(ReqSetMemTable, body, []int{q.gm.fd()})
	return devFeat
}

func u64bytes(v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return b[:]
}

func u32bytes(v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return b[:]
}

func stateBytes(idx int, num uint32) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b[0:], uint32(idx))
	binary.LittleEndian.PutUint32(b[4:], num)
	return b
}

// setupQueue performs the QEMU queue bring-up: NUM, ADDR, BASE, KICK,
// CALL, ENABLE.
func (q *fakeQEMU) setupQueue(idx int, size uint16) *vq {
	q.t.Helper()
	base := uint64(idx) * (1 << 19)
	v := &vq{
		q: q, idx: idx, size: size,
		desc:       base,
		avail:      base + uint64(size)*16 + 64,
		used:       base + uint64(size)*16 + 64 + 6 + 2*uint64(size) + 64,
		statusAddr: map[uint16]uint64{},
	}
	// A fresh guest boots with zeroed ring memory; a reconnecting fake
	// QEMU reuses the mapping, so clear desc..used tail explicitly.
	q.gm.put(base, make([]byte, v.used+8+2*uint64(size)-base))
	q.qcall(ReqSetVringNum, stateBytes(idx, uint32(size)))
	addr := make([]byte, 40) // vhost_vring_addr {idx, flags, desc, used, avail, log}
	binary.LittleEndian.PutUint32(addr[0:], uint32(idx))
	binary.LittleEndian.PutUint64(addr[8:], v.desc)
	binary.LittleEndian.PutUint64(addr[16:], v.used)
	binary.LittleEndian.PutUint64(addr[24:], v.avail)
	q.qcall(ReqSetVringAddr, addr)
	q.qcall(ReqSetVringBase, stateBytes(idx, 0))
	// KICK: backend reads the eventfd, QEMU (us) writes it. Send the
	// read end; keep the write end.
	kickR, kickW, err := os.Pipe()
	if err != nil {
		q.t.Fatal(err)
	}
	q.qfire(ReqSetVringKick, stateBytes(idx, 0), []int{int(kickR.Fd())})
	_ = kickR.Close() // SendMsgUnix dups the fd; our copy is no longer needed
	// CALL: backend writes the eventfd, QEMU (us) reads it. Send the
	// write end; keep the read end.
	callR, callW, err := os.Pipe()
	if err != nil {
		q.t.Fatal(err)
	}
	q.qfire(ReqSetVringCall, stateBytes(idx, 0), []int{int(callW.Fd())})
	_ = callW.Close()
	v.kickW, v.callR = kickW, callR
	q.t.Cleanup(func() { _ = kickW.Close(); _ = callR.Close() })
	q.qcall(ReqSetVringEnable, stateBytes(idx, 1))
	return v
}

// ---------------------------------------------------------------------------
// Split ring: driver side
// ---------------------------------------------------------------------------

// putDesc writes descriptor slot i.
func (v *vq) putDesc(i uint16, addr uint64, length uint32, flags uint16, next uint16) {
	p := v.desc + uint64(i)*16
	v.q.gm.putU64(p, addr)
	v.q.gm.putU32(p+8, length)
	v.q.gm.putU16(p+12, flags)
	v.q.gm.putU16(p+14, next)
}

// publish makes heads[] (descriptor chain heads) available and rings
// the kick doorbell.
func (v *vq) publish(heads ...uint16) {
	for _, h := range heads {
		v.q.gm.putU16(v.avail+4+2*uint64(v.availIdx%v.size), h)
		v.availIdx++
	}
	v.q.gm.putU16(v.avail+2, v.availIdx)
	var cnt [8]byte
	binary.LittleEndian.PutUint64(cnt[:], v.seq)
	v.seq++
	if _, err := v.kickW.Write(cnt[:]); err != nil {
		v.q.t.Fatalf("kick q%d: %v", v.idx, err)
	}
}

// usedEnt is one completed used-ring entry.
type usedEnt struct{ ID, Len uint32 }

// awaitUsed waits for used.idx to reach want completions past usedSeen.
func (v *vq) awaitUsed(want int) []usedEnt {
	v.q.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if uint16(v.q.gm.u16(v.used+2)-v.usedSeen) >= uint16(want) {
			break
		}
		if time.Now().After(deadline) {
			v.q.t.Fatalf("q%d: timeout waiting for %d used entries (used.idx=%d seen=%d)",
				v.idx, want, v.q.gm.u16(v.used+2), v.usedSeen)
		}
		time.Sleep(time.Millisecond)
	}
	out := make([]usedEnt, 0, want)
	for i := 0; i < want; i++ {
		slot := (v.usedSeen + uint16(i)) % v.size
		p := v.used + 4 + 8*uint64(slot)
		out = append(out, usedEnt{ID: v.q.gm.u32(p), Len: v.q.gm.u32(p + 4)})
	}
	v.usedSeen += uint16(want)
	v.q.gm.putU16(v.used+2, v.usedSeen) // driver consumes (device re-reads it for the wrap test)
	return out
}

// awaitIRQ waits for a call-eventfd signal (one burst = one signal).
func (v *vq) awaitIRQ() {
	v.q.t.Helper()
	v.callR.SetReadDeadline(time.Now().Add(5 * time.Second))
	var b [8]byte
	if _, err := v.callR.Read(b[:]); err != nil {
		v.q.t.Fatalf("q%d call fd: %v", v.idx, err)
	}
}

// status returns the status byte the device wrote for chain head.
func (v *vq) status(head uint16) uint8 {
	v.q.t.Helper()
	addr, ok := v.statusAddr[head]
	if !ok {
		v.q.t.Fatalf("no status address tracked for head %d", head)
	}
	return v.q.gm.u8(addr)
}

// chain places a virtio-blk request (header, data, status) in guest
// memory at the given offsets and wires the descriptor chain at head.
// dataWrite=true marks the data segment device-writeable (IN/read).
func (v *vq) chain(head uint16, reqType uint32, sector uint64, hdrAddr, dataAddr uint64, dataLen uint32, dataWrite bool) {
	v.q.t.Helper()
	hdr := make([]byte, 16)
	binary.LittleEndian.PutUint32(hdr[0:], reqType)
	binary.LittleEndian.PutUint64(hdr[8:], sector)
	v.q.gm.put(hdrAddr, hdr)
	statusAddr := dataAddr + uint64(dataLen)
	v.q.gm.putU8(statusAddr, 0xff)
	v.statusAddr[head] = statusAddr
	v.putDesc(head+0, hdrAddr, 16, descNext, head+1)
	v.putDesc(head+1, dataAddr, dataLen, descFlagOf(dataWrite)|descNext, head+2)
	v.putDesc(head+2, statusAddr, 1, descWrite, 0)
}

func descFlagOf(write bool) uint16 {
	if write {
		return descWrite
	}
	return 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandshake(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{})
	q.handshake()
	if !q.e.Attached() {
		t.Fatal("exporter reports no attached session after handshake")
	}
	if n := q.qget(ReqGetQueueNum, nil); n != 1 {
		t.Fatalf("GET_QUEUE_NUM = %d, want 1", n)
	}
	if n := q.qget(ReqGetMaxMemSlots, nil); n == 0 {
		t.Fatal("GET_MAX_MEM_SLOTS = 0")
	}
	// GET_CONFIG: reply echoes {off,size,flags=0} + config bytes.
	req := make([]byte, 12)
	binary.LittleEndian.PutUint32(req[4:], 60) // offset 0, 60 bytes
	q.request(ReqGetConfig, 0, req, nil)
	body := q.reply(ReqGetConfig, 0)
	if len(body) != 12+60 {
		t.Fatalf("GET_CONFIG reply = %d bytes, want %d", len(body), 72)
	}
	if binary.LittleEndian.Uint32(body[8:]) != 0 {
		t.Fatal("GET_CONFIG reply flags != 0")
	}
	if cap := binary.LittleEndian.Uint64(body[12:]); cap != (1<<20)/512 {
		t.Fatalf("config capacity = %d, want %d", cap, (1<<20)/512)
	}
}

func TestSetConfigRefused(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{})
	q.handshake()
	req := make([]byte, 12)
	binary.LittleEndian.PutUint32(req[4:], 1) // write 1 byte
	q.request(ReqSetConfig, flagNeedReply, req, nil)
	q.cli.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	h, _, _, err := q.cli.ReadMessage()
	q.cli.c.SetReadDeadline(time.Time{})
	if err == nil {
		t.Fatalf("SET_CONFIG accepted: reply req=%d", h.Request)
	}
	// The device treats it as fatal: the session must be dead.
	if q.e.Attached() {
		// give Serve a moment to run reset()
		time.Sleep(100 * time.Millisecond)
		if q.e.Attached() {
			t.Fatal("session still attached after fatal SET_CONFIG")
		}
	}
}

func TestWriteReadRoundtrip(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{})
	q.handshake()
	v := q.setupQueue(0, 64)

	// Layout in guest RAM (all inside the 1 MiB region):
	const (
		hdrA, dataA, respA = 0x1000, 0x2000, 0x3000
		nsec               = 8
		nbytes             = nsec * 512
	)
	payload := make([]byte, nbytes)
	for i := range payload {
		payload[i] = byte(i*7 + 1)
	}

	// OUT (write) at sector 100.
	v.chain(0, blkOut, 100, hdrA, dataA, nbytes, false)
	q.gm.put(dataA, payload)
	v.publish(0)
	used := v.awaitUsed(1)
	v.awaitIRQ()
	if used[0].ID != 0 || used[0].Len != 0 {
		t.Fatalf("used = %+v, want {0,0}", used[0])
	}
	if st := v.status(0); st != vduse.StatusOK {
		t.Fatalf("write status = %d, want OK", st)
	}
	if got := b.bytes(100*512, nbytes); !equalBytes(got, payload) {
		t.Fatal("payload not in backend")
	}

	// IN (read) of the same sector into a fresh buffer.
	v.chain(1, blkIn, 100, hdrA+256, respA, nbytes, true)
	v.publish(1)
	used = v.awaitUsed(1)
	v.awaitIRQ()
	if used[0].ID != 1 || int(used[0].Len) != nbytes {
		t.Fatalf("used = %+v, want {1,%d}", used[0], nbytes)
	}
	if st := v.status(1); st != vduse.StatusOK {
		t.Fatalf("read status = %d, want OK", st)
	}
	if got := q.gm.get(respA, nbytes); !equalBytes(got, payload) {
		t.Fatal("read data mismatch")
	}

	// FLUSH and GET_ID.
	// A real QEMU FLUSH chain is header+status with NO data segment:
	// the chain helper wires three descriptors, so skip the middle one
	// by ending the header descriptor's chain at the status slot.
	v.chain(2, blkFlush, 0, hdrA, dataA, 0, false)
	v.putDesc(2, hdrA, 16, descNext, 4) // head 2 -> skip slot 3, status at 4
	v.publish(2)
	used = v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(2); st != vduse.StatusOK {
		t.Fatalf("flush status = %d", st)
	}
	if fl, _, _ := b.snap(); fl < 1 {
		t.Fatalf("backend flushes = %d, want >= 1", fl)
	}

	v.chain(3, blkGetID, 0, hdrA, respA+4096, 20, true)
	v.publish(3)
	used = v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(3); st != vduse.StatusOK {
		t.Fatalf("get_id status = %d", st)
	}
	if used[0].Len != 20 {
		t.Fatalf("get_id len = %d, want 20", used[0].Len)
	}
	for _, x := range q.gm.get(respA+4096, 20) {
		if x != 0 {
			t.Fatal("get_id: serial not zero-filled")
		}
	}
}

func TestDiscardWriteZeroes(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{Trim: true, WriteZeroes: true})
	q.handshake()
	v := q.setupQueue(0, 64)
	const hdrA, dataA = 0x1000, 0x2000

	// One 16B discard segment: sector 512 (off 256 KiB), 8 sectors,
	// flags=0 -> punch hole.
	seg := make([]byte, 16)
	binary.LittleEndian.PutUint64(seg[0:], 512)
	binary.LittleEndian.PutUint32(seg[8:], 8)
	q.gm.put(dataA, seg)
	v.chain(0, blkDiscard, 0, hdrA, dataA, 16, false)
	v.publish(0)
	v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(0); st != vduse.StatusOK {
		t.Fatalf("discard status = %d", st)
	}

	// One write-zeroes segment with BZVFLAG_NO_UNMAP -> punchHole=false.
	binary.LittleEndian.PutUint32(seg[12:], 1)
	q.gm.put(dataA, seg)
	v.chain(1, blkZeroOut, 0, hdrA, dataA, 16, false)
	v.publish(1)
	v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(1); st != vduse.StatusOK {
		t.Fatalf("write_zeroes status = %d", st)
	}
	_, trims, zeros := b.snap()
	if len(trims) != 1 || trims[0] != [3]int64{512 * 512, 8 * 512, 1} {
		t.Fatalf("trims = %v", trims)
	}
	if len(zeros) != 1 || zeros[0] != [3]int64{512 * 512, 8 * 512, 0} {
		t.Fatalf("zeros = %v", zeros)
	}

	// Out-of-range discard -> IO error.
	binary.LittleEndian.PutUint64(seg[0:], 1<<40)
	binary.LittleEndian.PutUint32(seg[12:], 0)
	q.gm.put(dataA, seg)
	v.chain(2, blkDiscard, 0, hdrA, dataA, 16, false)
	v.publish(2)
	v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(2); st != vduse.StatusIOErr {
		t.Fatalf("out-of-range discard status = %d, want IOERR", st)
	}
}

func TestReadOnlyRefusesWrites(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{ReadOnly: true})
	devFeat := q.handshake()
	if devFeat&(1<<6) == 0 {
		t.Fatal("F_RO (bit 6) not advertised on a read-only device")
	}
	v := q.setupQueue(0, 64)
	const hdrA, dataA = 0x1000, 0x2000
	v.chain(0, blkOut, 0, hdrA, dataA, 512, false)
	v.publish(0)
	v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(0); st != vduse.StatusIOErr {
		t.Fatalf("read-only OUT status = %d, want IOERR", st)
	}
}

func TestUnsupportedWhenDisabled(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{}) // Trim/WriteZeroes off
	q.handshake()
	v := q.setupQueue(0, 64)
	const hdrA, dataA = 0x1000, 0x2000
	seg := make([]byte, 16)
	q.gm.put(dataA, seg)
	v.chain(0, blkDiscard, 0, hdrA, dataA, 16, false)
	v.publish(0)
	v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(0); st != vduse.StatusUnsupp {
		t.Fatalf("discard status = %d, want UNSUPP", st)
	}
}

func TestVringBaseResume(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{})
	q.handshake()
	v := q.setupQueue(0, 64)
	const hdrA, dataA = 0x1000, 0x2000

	// Submit one write, then stop the queue via GET_VRING_BASE.
	v.chain(0, blkOut, 7, hdrA, dataA, 512, false)
	v.publish(0)
	v.awaitUsed(1)
	v.awaitIRQ()
	base := q.qget(ReqGetVringBase, stateBytes(0, 0))
	if want := getVringBaseReply(0, v.availIdx); base != want {
		t.Fatalf("GET_VRING_BASE = %#x, want %#x", base, want)
	}
	// QEMU save/restore: BASE(avail) + ENABLE(1) on the SAME fds;
	// a second request must complete.
	q.qcall(ReqSetVringBase, stateBytes(0, uint32(v.availIdx)))
	q.qcall(ReqSetVringEnable, stateBytes(0, 1))
	v.chain(1, blkOut, 9, hdrA, dataA, 512, false)
	v.publish(1)
	v.awaitUsed(1)
	v.awaitIRQ()
	if st := v.status(1); st != vduse.StatusOK {
		t.Fatalf("resumed write status = %d", st)
	}
	// SKIP_DRAIN (45) answers identically.
	base = q.qget(ReqGetVringBaseSkipDrain, stateBytes(0, 0))
	if want := getVringBaseReply(0, v.availIdx); base != want {
		t.Fatalf("GET_VRING_BASE_SKIP_DRAIN = %#x, want %#x", base, want)
	}
}

func TestResetDevice(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{})
	q.handshake()
	q.setupQueue(0, 64)
	q.qcall(ReqResetDevice, nil)
	// After RESET the mem table is gone: SET_VRING_ADDR must fail, and
	// a failed request kills the session (QEMU reconnects).
	q.request(ReqSetVringAddr, 0, make([]byte, 40), nil)
	q.cli.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, _, err := q.cli.ReadMessage()
	q.cli.c.SetReadDeadline(time.Time{})
	if err == nil {
		t.Fatal("SET_VRING_ADDR succeeded after RESET_DEVICE (mem table should be gone)")
	}
	deadline := time.Now().Add(2 * time.Second)
	for q.e.Attached() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q.e.Attached() {
		t.Fatal("session still attached after fatal request")
	}
}

func TestReconnect(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{})
	q.handshake()
	v := q.setupQueue(0, 64)
	v.chain(0, blkOut, 0, 0x1000, 0x2000, 512, false)
	v.publish(0)
	v.awaitUsed(1)
	v.awaitIRQ()

	// "VM shutdown": client disconnects, queues die with the session.
	_ = q.cli.Close()
	deadline := time.Now().Add(2 * time.Second)
	for q.e.Attached() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if q.e.Attached() {
		t.Fatal("session still attached after client disconnect")
	}
	// "VM boot again": same exporter, fresh handshake.
	q2 := &fakeQEMU{t: t, e: q.e, gm: q.gm}
	c, err := net.Dial("unix", q.e.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	q2.cli = WrapConn(c.(*net.UnixConn))
	q2.handshake()
	v2 := q2.setupQueue(0, 64)
	v2.chain(0, blkIn, 0, 0x4000, 0x5000, 512, true)
	v2.publish(0)
	used := v2.awaitUsed(1)
	v2.awaitIRQ()
	if st := v2.status(0); st != vduse.StatusOK {
		t.Fatalf("post-reconnect read status = %d", st)
	}
	if used[0].Len != 512 {
		t.Fatalf("post-reconnect used len = %d", used[0].Len)
	}
}

func TestMultipleQueues(t *testing.T) {
	b := newMemBackend(1 << 20)
	q := startQEMU(t, b, 1<<20, Options{NumQueues: 2})
	q.handshake()
	if n := q.qget(ReqGetQueueNum, nil); n != 2 {
		t.Fatalf("GET_QUEUE_NUM = %d, want 2", n)
	}
	v0 := q.setupQueue(0, 64)
	v1 := q.setupQueue(1, 64)
	const hdrA, dataA = 0x1000, 0x2000
	// Queue 0 writes pattern A at sector 0; queue 1 writes pattern B at
	// sector 1. Both must land.
	q.gm.put(dataA, bytesVal(0xaa, 512))
	v0.chain(0, blkOut, 0, hdrA, dataA, 512, false)
	q.gm.put(dataA+2048, bytesVal(0xbb, 512))
	v1.chain(0, blkOut, 1, hdrA+1024, dataA+2048, 512, false)
	v0.publish(0)
	v1.publish(0)
	v0.awaitUsed(1)
	v0.awaitIRQ()
	v1.awaitUsed(1)
	v1.awaitIRQ()
	if st := v0.status(0); st != vduse.StatusOK {
		t.Fatalf("q0 status = %d", st)
	}
	if st := v1.status(0); st != vduse.StatusOK {
		t.Fatalf("q1 status = %d", st)
	}
	got := b.bytes(0, 512)
	if !equalBytes(got, bytesVal(0xaa, 512)) {
		t.Fatal("q0 payload missing")
	}
	got = b.bytes(512, 512)
	if !equalBytes(got, bytesVal(0xbb, 512)) {
		t.Fatal("q1 payload missing")
	}
	// v1 ring lives at 512 KiB; its desc slot 0 must be in bounds.
	if v1.desc != 1<<19 {
		t.Fatalf("q1 desc base = %#x, want %#x", v1.desc, 1<<19)
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func bytesVal(v byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = v
	}
	return b
}
