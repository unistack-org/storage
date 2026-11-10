// Package vub implements a vhost-user-blk backend: a virtio-blk disk
// exported over a UNIX socket to a QEMU/Dragonball vmm
// ("-device vhost-user-blk-pci,chardev=unix:/path/sock"), on top of the
// same Backend interface pkg/vduse uses.
//
// This is the Phase 4b export. The heavy lifting (split-virtqueue
// arithmetic, IOTLB guest-memory translation, virtio-blk request
// parsing) is shared with pkg/vduse, so the whole control plane and a
// fake-guest datapath unit-test on macOS dev hosts. The full QEMU+KVM
// end-to-end (fio inside the guest) requires a Linux host: macOS
// homebrew QEMU ships no vhost-user-blk-pci.
//
// Wire protocol: QEMU docs/interop/vhost-user.rst. A message is a
// 12-byte header {u32 request, u32 flags, u32 size} plus a payload;
// eventfds and guest-memory fds travel as SCM_RIGHTS ancillary data.
// Constants below mirror QEMU's hw/virtio/vhost-user.c exactly.
package vub

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// VhostUserRequest numbers (QEMU hw/virtio/vhost-user.c enum). Only the
// subset a vhost-user-blk backend must answer is named.
const (
	ReqGetFeatures           = 1
	ReqSetFeatures           = 2
	ReqSetOwner              = 3
	ReqResetOwner            = 4
	ReqSetMemTable           = 5
	ReqSetLogBase            = 6
	ReqSetLogFD              = 7
	ReqSetVringNum           = 8
	ReqSetVringAddr          = 9
	ReqSetVringBase          = 10
	ReqGetVringBase          = 11
	ReqSetVringKick          = 12
	ReqSetVringCall          = 13
	ReqSetVringErr           = 14
	ReqGetProtocolFeatures   = 15
	ReqSetProtocolFeatures   = 16
	ReqGetQueueNum           = 17
	ReqSetVringEnable        = 18
	ReqGetConfig             = 24
	ReqSetConfig             = 25
	ReqResetDevice           = 34
	ReqGetMaxMemSlots        = 36
	ReqGetVringBaseSkipDrain = 45
)

// VhostUserProtocolFeature bits (QEMU include/hw/virtio/vhost-user.h).
const (
	protoFMQ          = 0  // multi-queue: we support it
	protoFLogShmFD    = 1  // live-migration logging: not supported
	protoFReplyAck    = 3  // ack every set request when negotiated
	protoFBackendReq  = 5  // backend-initiated messages: not used
	protoFPagefault   = 8  // userfault: not used
	protoFConfig      = 9  // GET/SET_CONFIG: we support it (blk sizing)
	protoFHostNotif   = 11 // doorbell mapping: not used
	protoFInflight    = 12 // inflight shm for live migration: not supported
	protoFResetDevice = 13 // RESET_DEVICE: we support it
	protoFStatus      = 16 // GET/SET_STATUS: not supported
)

// protoFeatures is the protocol-feature word we advertise: multi-queue
// (per-queue enable + GET_QUEUE_NUM), REPLY_ACK (QEMU prefers acked
// operation), CONFIG (virtio-blk sizing needs GET_CONFIG), and
// RESET_DEVICE. Everything else (logging shm, pagefault, inflight shm,
// host notifiers, status) is live-migration/advanced tooling we do not
// implement.
const protoFeatures = uint64(1<<protoFMQ) |
	uint64(1<<protoFReplyAck) |
	uint64(1<<protoFConfig) |
	uint64(1<<protoFResetDevice)

// virtio-blk / virtio-ring feature bits used in the GET_FEATURES word
// (linux/virtio_blk.h, linux/virtio_ring.h). vduse keeps its copies
// unexported; the vhost-user word is assembled locally.
const (
	fBlkSizeMax   = 1 << 0  // VIRTIO_BLK_F_SIZE_MAX
	fBlkSegMax    = 1 << 1  // VIRTIO_BLK_F_SEG_MAX
	fBlkGeometry  = 1 << 2  // VIRTIO_BLK_F_GEOMETRY
	fBlkBlkSize   = 1 << 3  // VIRTIO_BLK_F_BLK_SIZE
	fBlkTopology  = 1 << 4  // VIRTIO_BLK_F_TOPOLOGY
	fBlkRO        = 1 << 6  // VIRTIO_BLK_F_RO
	fBlkFlush     = 1 << 9  // VIRTIO_BLK_F_FLUSH
	fBlkDiscard   = 1 << 13 // VIRTIO_BLK_F_DISCARD
	fBlkZeroes    = 1 << 15 // VIRTIO_BLK_F_WRITE_ZEROES
	fRingIndirect = 1 << 28 // VIRTIO_RING_F_INDIRECT_DESC
	fVersion1     = 1 << 32 // VIRTIO_F_VERSION_1
)

// fProtocolFeatures is VHOST_USER_F_PROTOCOL_FEATURES: the transport
// bit QEMU looks for in the GET_FEATURES word to enable the vhost-user
// control plane (GET/SET_PROTOCOL_FEATURES).
const fProtocolFeatures = 1 << 30

const (
	// maxPayload caps request payloads. QEMU's VhostUserPayload union
	// is 268 bytes (VhostUserConfig: 12 + 256); 512 is generous.
	maxPayload = 512
	// maxFDs caps SCM_RIGHTS descriptors per message.
	maxFDs = 64
	// maxMemRegions caps parsed SET_MEM_TABLE regions. GET_MAX_MEM_SLOTS
	// advertises the same bound: every region travels in one table.
	maxMemRegions = 64
	maxMemSlots   = maxMemRegions

	flagVersion   = 0x1    // protocol version (low 2 bits)
	flagReply     = 1 << 2 // reply flag on backend->front-end messages
	flagNeedReply = 1 << 3 // REPLY_ACK: front-end asks for an ack

	// configMax is VHOST_USER_MAX_CONFIG_SIZE.
	configMax = 256
	// configHdrSize is the VhostUserConfig header (offset,size,flags).
	configHdrSize = 12
)

// Header is the 12-byte vhost-user message header.
type Header struct {
	Request uint32
	Flags   uint32
	Size    uint32
}

// Conn is a vhost-user connection over a unix socket: Header+payload
// framing with SCM_RIGHTS fds.
type Conn struct {
	c *net.UnixConn
}

// WrapConn adapts an accepted *net.UnixConn.
func WrapConn(c *net.UnixConn) *Conn { return &Conn{c: c} }

// Close closes the socket.
func (c *Conn) Close() error { return c.c.Close() }

// ReadMessage reads one request. Returned fds (if any) are descriptors
// the kernel dup'd for us: the caller owns them (close, or hand to
// fdOwner). A request with flagNeedReply set expects a ReplyU64 ack.
func (c *Conn) ReadMessage() (Header, []byte, []int, error) {
	// Two exact-size recvmsgs, like QEMU's vhost_user_read: reading
	// the full payload in one large recvmsg would coalesce pipelined
	// messages (QEMU fires SET_VRING_KICK/CALL/ENABLE back-to-back)
	// and drop their fds' association. Ancillary data rides on the
	// recvmsg that first consumes its datagram: QEMU sends
	// header+body in one sendmsg, so the header read claims the fd;
	// the body read carries an oob buffer too, in case the split
	// falls the other way.
	var hb [12]byte
	n, fds, err := c.recvMsg(hb[:])
	if err != nil {
		return Header{}, nil, fds, err
	}
	if n < 12 {
		return Header{}, hb[:n], fds, fmt.Errorf("vub: short header read: %d", n)
	}
	h := Header{
		Request: binary.LittleEndian.Uint32(hb[0:]),
		Flags:   binary.LittleEndian.Uint32(hb[4:]),
		Size:    binary.LittleEndian.Uint32(hb[8:]),
	}
	if h.Size > maxPayload {
		return h, nil, fds, fmt.Errorf("vub: payload too big: %d", h.Size)
	}
	body := make([]byte, h.Size)
	m, bfds, err := c.recvMsg(body)
	fds = append(fds, bfds...)
	if err != nil {
		return h, body, fds, err
	}
	if m < len(body) {
		return h, body, fds, fmt.Errorf("vub: truncated payload: %d of %d", m, len(body))
	}
	return h, body, fds, nil
}

// recvMsg performs one recvmsg with a caller-sized buffer (never more
// than one message's worth, so pipelined messages stay separated) and
// extracts any SCM_RIGHTS descriptors.
func (c *Conn) recvMsg(buf []byte) (int, []int, error) {
	var oob [cmsgSpace]byte
	n, oobn, _, _, err := c.c.ReadMsgUnix(buf, oob[:])
	if err != nil {
		return n, nil, err
	}
	fds, err := parseFDs(oob[:oobn])
	return n, fds, err
}

// cmsgSpace is the ancillary buffer size for maxFDs descriptors:
// CMSG_SPACE(maxFDs*8) as a compile-time constant (the x/sys helper is
// a function, unusable as an array length). CLEN is the 64-bit cmsghdr
// (16 bytes) and the whole block is 8-byte aligned.
const cmsgSpace = ((maxFDs * 8) + 16 + 7) &^ 7

// parseFDs appends the rights from every SCM_RIGHTS control message.
func parseFDs(oob []byte) ([]int, error) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, err
	}
	var fds []int
	for _, m := range msgs {
		if m.Header.Level != unix.SOL_SOCKET || m.Header.Type != unix.SCM_RIGHTS {
			continue
		}
		f, err := unix.ParseUnixRights(&m)
		if err != nil {
			return fds, err
		}
		fds = append(fds, f...)
	}
	return fds, nil
}

// Reply sends a reply to req. QEMU validates reply flags ==
// REPLY_MASK|VERSION (plus NEED_REPLY echoed on the ack path).
func (c *Conn) Reply(req uint32, body []byte, needReply bool) error {
	flags := uint32(flagReply | flagVersion)
	if needReply {
		flags |= flagNeedReply
	}
	return c.write(req, flags, body, nil)
}

// ReplyU64 replies with a single u64 payload (GET_FEATURES,
// GET_PROTOCOL_FEATURES, GET_VRING_BASE, GET_QUEUE_NUM, and the
// REPLY_ACK ok/err value).
func (c *Conn) ReplyU64(req uint32, v uint64, needReply bool) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return c.Reply(req, b[:], needReply)
}

func (c *Conn) write(req, flags uint32, body []byte, fds []int) error {
	buf := make([]byte, 12+len(body))
	binary.LittleEndian.PutUint32(buf[0:], req)
	binary.LittleEndian.PutUint32(buf[4:], flags)
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(body)))
	copy(buf[12:], body)
	var rights []byte
	if len(fds) > 0 {
		rights = unix.UnixRights(fds...)
	}
	_, _, err := c.c.WriteMsgUnix(buf, rights, nil)
	return err
}

// fdOwner wraps a raw descriptor in an *os.File so Go owns it (closed
// exactly once). The caller must not close the raw fd afterwards.
func fdOwner(fd int) *os.File {
	return os.NewFile(uintptr(fd), fmt.Sprintf("vub-fd-%d", fd))
}

// vringState decodes VhostUserVringState {u32 index; u32 num}.
func vringState(body []byte) (idx int, num uint32, err error) {
	if len(body) < 8 {
		return 0, 0, errors.New("vub: vring state payload too short")
	}
	return int(binary.LittleEndian.Uint32(body[0:])), binary.LittleEndian.Uint32(body[4:]), nil
}

// vringAddr decodes struct vhost_vring_addr {u32 index; u32 flags;
// u64 desc; u64 used; u64 avail; u64 log} -> 40 bytes.
func vringAddr(body []byte) (idx int, desc, used, avail uint64, err error) {
	if len(body) < 40 {
		return 0, 0, 0, 0, errors.New("vub: vring addr payload too short")
	}
	idx = int(binary.LittleEndian.Uint32(body[0:]))
	desc = binary.LittleEndian.Uint64(body[8:])
	used = binary.LittleEndian.Uint64(body[16:])
	avail = binary.LittleEndian.Uint64(body[24:])
	return idx, desc, used, avail, nil
}

// kickFD decodes the SET_VRING_KICK/CALL/ERR payload: bits 0-7 are the
// queue index, bit 8 is the "no fd" flag (poll mode). The fd (when
// present) arrives via SCM_RIGHTS.
// kickFD decodes the SET_VRING_KICK/CALL payload: VhostUserVringState
// {u32 index; u32 num} where the front-end packs num = (index << 8) |
// fd, using the VHOST_USER_VRING_NOFD_MASK bit (1<<8) to mean "no fd
// supplied" (poll mode / never signal). The queue index itself is the
// payload's index field.
func kickFD(body []byte) (idx int, noFD bool, err error) {
	if len(body) < 8 {
		return 0, false, errors.New("vub: vring kick payload too short")
	}
	i, num, err := vringState(body)
	if err != nil {
		return 0, false, err
	}
	return i, num&(1<<8) != 0, nil
}

// getVringBaseReply packs the GET_VRING_BASE reply, which is a full
// VhostUserVringState {u32 index; u32 num}: the queue index in the low
// 32 bits and the last avail index in the high 32 bits (QEMU's
// vhost_user_get_vring_base reads reply.state.num as last_avail_idx).
func getVringBaseReply(idx int, availIdx uint16) uint64 {
	return uint64(idx&0xffff) | uint64(availIdx)<<32
}

// memoryRegion is one decoded VhostUserMemoryRegion.
type memoryRegion struct {
	GuestPhys  uint64
	Size       uint64
	Userspace  uint64
	MmapOffset uint64
}

// parseMemTable decodes VhostUserMemory {u32 nregions; u32 padding;
// regions[n] of 32B}.
func parseMemTable(body []byte) ([]memoryRegion, error) {
	if len(body) < 8 {
		return nil, errors.New("vub: mem table payload too short")
	}
	n := int(binary.LittleEndian.Uint32(body[0:]))
	if n < 0 || n > maxMemRegions {
		return nil, fmt.Errorf("vub: mem table nregions=%d", n)
	}
	if len(body) < 8+n*32 {
		return nil, fmt.Errorf("vub: mem table truncated: %d bytes, need %d", len(body), 8+n*32)
	}
	out := make([]memoryRegion, n)
	for i := 0; i < n; i++ {
		p := body[8+i*32:]
		out[i] = memoryRegion{
			GuestPhys:  binary.LittleEndian.Uint64(p[0:]),
			Size:       binary.LittleEndian.Uint64(p[8:]),
			Userspace:  binary.LittleEndian.Uint64(p[16:]),
			MmapOffset: binary.LittleEndian.Uint64(p[24:]),
		}
	}
	return out, nil
}
