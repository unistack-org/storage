// Split virtqueue mechanics, kept portable (pure logic over a GuestMem
// interface) so the descriptor/ring arithmetic is unit-testable on any
// host; the Linux runtime supplies the mmap-backed GuestMem.
package vduse

import (
	"encoding/binary"
	"errors"
)

// Descriptor flags (virtio 1.x, vring_desc.flags).
const (
	DescFlagNext     = 1 << 0 // descriptor continues into next
	DescFlagWrite    = 1 << 1 // device-writeable (driver receive buffer)
	DescFlagIndirect = 1 << 2 // points to an indirect descriptor table
)

// MaxChainLen bounds descriptor-chain walking (seg_max is 256; indirect
// tables are capped at the vring size, so 1024 is generous).
const MaxChainLen = 1024

// ErrBadDesc marks a malformed descriptor table / chain (guest bug or
// corruption); the request is refused, the ring keeps running.
var ErrBadDesc = errors.New("vduse: malformed descriptor")

// GuestMem abstracts guest physical (IOVA) memory: Read copies out of the
// guest, Write copies into it. Implementations must be goroutine-safe.
type GuestMem interface {
	Read(dst []byte, iova uint64) error
	Write(src []byte, iova uint64) error
}

// Seg is one contiguous guest buffer of a descriptor chain. Write marks it
// as device-writeable (a driver receive buffer); clear means the device
// reads it (driver transmit buffer).
type Seg struct {
	Addr  uint64
	Len   uint32
	Write bool
}

// Desc is a decoded vring_desc.
type Desc struct {
	Addr  uint64
	Len   uint32
	Flags uint16
	Next  uint16
}

// DecodeDesc parses one 16-byte descriptor.
func DecodeDesc(b []byte) (Desc, error) {
	if len(b) < 16 {
		return Desc{}, ErrBadDesc
	}
	return Desc{
		Addr:  binary.LittleEndian.Uint64(b[0:]),
		Len:   binary.LittleEndian.Uint32(b[8:]),
		Flags: binary.LittleEndian.Uint16(b[12:]),
		Next:  binary.LittleEndian.Uint16(b[14:]),
	}, nil
}

// ReadChain walks the descriptor chain starting at head, following the
// NEXT flag and expanding one level of INDIRECT tables, and returns the
// segments in chain order. It bounds total descriptors (MaxChainLen) and
// per-segment length (1 GiB) so a malicious/buggy ring cannot hang or
// over-allocate the device.
func ReadChain(gm GuestMem, ringSize int, head uint16) ([]Seg, error) {
	if ringSize <= 0 || int(head) >= ringSize {
		return nil, ErrBadDesc
	}
	var segs []Seg
	total := 0
	var visit func(d Desc, indirect bool) error
	visit = func(d Desc, indirect bool) error {
		if d.Flags&DescFlagIndirect != 0 {
			// An indirect desc is a pointer, not a buffer: it never
			// becomes a segment itself.
			if indirect {
				return ErrBadDesc // nested indirect is forbidden
			}
			n := int(d.Len / 16)
			if n <= 0 || n > ringSize || n > MaxChainLen-total {
				return ErrBadDesc
			}
			tbl := make([]byte, n*16)
			if err := gm.Read(tbl, d.Addr); err != nil {
				return err
			}
			for i := 0; i < n; i++ {
				sd, err := DecodeDesc(tbl[i*16:])
				if err != nil {
					return err
				}
				if err := visit(sd, true); err != nil {
					return err
				}
				if sd.Flags&DescFlagNext == 0 {
					break
				}
			}
			return nil
		}
		if d.Len > 1<<30 {
			return ErrBadDesc
		}
		segs = append(segs, Seg{Addr: d.Addr, Len: d.Len, Write: d.Flags&DescFlagWrite != 0})
		total++
		return nil
	}
	cur := int(head)
	for {
		var dbuf [16]byte
		if err := gm.Read(dbuf[:], uint64(cur)*16); err != nil {
			return nil, err
		}
		d, err := DecodeDesc(dbuf[:])
		if err != nil {
			return nil, err
		}
		if err := visit(d, false); err != nil {
			return nil, err
		}
		if d.Flags&DescFlagNext == 0 || d.Flags&DescFlagIndirect != 0 {
			break // chain end; an indirect table terminates the chain
		}
		cur = int(d.Next)
		if cur >= ringSize || total >= MaxChainLen {
			return nil, ErrBadDesc
		}
	}
	if len(segs) == 0 {
		return nil, ErrBadDesc
	}
	return segs, nil
}

// SplitRing is the device-side state of one split virtqueue: the avail
// consumer / used producer arithmetic exactly as the virtio spec defines.
// All guest addresses are IOVAs resolved through gm.
type SplitRing struct {
	gm   GuestMem
	size uint16

	descBase  uint64 // vring_desc[size]
	availBase uint64 // vring_avail: flags, idx, ring[size], used_event
	usedBase  uint64 // vring_used: flags, idx, ring[size](id,len), avail_event
	lastAvail uint16 // next avail slot to consume
	lastUsed  uint16 // next used slot to fill
}

// Split-ring layout offsets.
const (
	availFlagsOff = 0
	availIdxOff   = 2
	availRingOff  = 4
	usedIdxOff    = 2
	usedRingOff   = 4
)

// NewSplitRing wires a ring at the given guest addresses.
func NewSplitRing(gm GuestMem, size uint16, desc, avail, used uint64) *SplitRing {
	return &SplitRing{gm: gm, size: size, descBase: desc, availBase: avail, usedBase: used}
}

// Size is the ring size (power of two).
func (r *SplitRing) Size() uint16 { return r.size }

// SetState restores device-side cursors (VDUSE_GET_VQ_STATE answer and
// post-RESET initialisation).
func (r *SplitRing) SetState(availIdx, usedIdx uint16) {
	r.lastAvail, r.lastUsed = availIdx, usedIdx
}

// State returns the device-side cursors.
func (r *SplitRing) State() (availIdx, usedIdx uint16) {
	return r.lastAvail, r.lastUsed
}

// NextAvail returns the head of the next available descriptor chain, or
// (-1, nil) when the queue is empty.
func (r *SplitRing) NextAvail() (int, error) {
	var b [2]byte
	if err := r.gm.Read(b[:], r.availBase+availIdxOff); err != nil {
		return -1, err
	}
	idx := binary.LittleEndian.Uint16(b[:])
	if r.lastAvail == idx {
		return -1, nil
	}
	if err := r.gm.Read(b[:], r.availBase+availRingOff+2*uint64(r.lastAvail%r.size)); err != nil {
		return -1, err
	}
	r.lastAvail++
	return int(binary.LittleEndian.Uint16(b[:])), nil
}

// PushUsed appends a used entry (head id, bytes written by device) and
// reports whether an interrupt must be injected for this queue. We honour
// the driver's VRING_AVAIL_F_NO_INTERRUPT suppression flag; we never set
// used.flags (no EVENT_IDX reliance), so "no suppression" means IRQ.
func (r *SplitRing) PushUsed(head uint16, written uint32) (bool, error) {
	slot := uint64(r.lastUsed % r.size)
	var e [8]byte
	binary.LittleEndian.PutUint32(e[0:], uint32(head))
	binary.LittleEndian.PutUint32(e[4:], written)
	if err := r.gm.Write(e[:], r.usedBase+usedRingOff+8*slot); err != nil {
		return false, err
	}
	r.lastUsed++
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], r.lastUsed)
	if err := r.gm.Write(b[:], r.usedBase+usedIdxOff); err != nil {
		return false, err
	}
	if err := r.gm.Read(b[:], r.availBase+availFlagsOff); err != nil {
		return false, err
	}
	irq := b[0]&0x01 == 0 // VRING_AVAIL_F_NO_INTERRUPT unset => notify
	return irq, nil
}

// BlkReq is a validated virtio-blk request chain.
type BlkReq struct {
	HeaderSeg Seg
	DataSegs  []Seg
	StatusSeg Seg
	Head      uint16
}

// ParseBlkChain validates a virtio-blk chain: >=3 segs, 16+-byte header
// (device-readable) first, exactly-1-byte status (device-writeable) last.
func ParseBlkChain(segs []Seg, head uint16) (*BlkReq, error) {
	if len(segs) < 3 {
		return nil, ErrBadDesc
	}
	h, s := segs[0], segs[len(segs)-1]
	if h.Write || h.Len < 16 {
		return nil, ErrBadDesc // header is driver->device
	}
	if !s.Write || s.Len != 1 {
		return nil, ErrBadDesc // status is device->driver, 1 byte
	}
	return &BlkReq{HeaderSeg: h, DataSegs: segs[1 : len(segs)-1], StatusSeg: s, Head: head}, nil
}

// ReadHeader copies the 16-byte request header out of the guest.
func (b *BlkReq) ReadHeader(gm GuestMem) (BlkReqHeader, error) {
	buf := make([]byte, 16)
	if err := gm.Read(buf, b.HeaderSeg.Addr); err != nil {
		return BlkReqHeader{}, err
	}
	h, ok := ParseBlkReqHeader(buf)
	if !ok {
		return h, ErrBadDesc
	}
	return h, nil
}

// WriteStatus stores the virtio_blk_req.status byte.
func (b *BlkReq) WriteStatus(gm GuestMem, status uint8) error {
	return gm.Write([]byte{status}, b.StatusSeg.Addr)
}

// ReadData copies the driver's transmit buffers (a write request) into
// dst, which must be exactly the total data length.
func (b *BlkReq) ReadData(gm GuestMem, dst []byte) error {
	var off int
	for _, sg := range b.DataSegs {
		if sg.Write {
			return ErrBadDesc // OUT data must be driver->device
		}
		if off+int(sg.Len) > len(dst) {
			return ErrBadDesc
		}
		if err := gm.Read(dst[off:off+int(sg.Len)], sg.Addr); err != nil {
			return err
		}
		off += int(sg.Len)
	}
	if off != len(dst) {
		return ErrBadDesc
	}
	return nil
}

// WriteData fills the device-writeable data buffers (a read request) from
// src and returns the total bytes written.
func (b *BlkReq) WriteData(gm GuestMem, src []byte) (int, error) {
	var off int
	for _, sg := range b.DataSegs {
		if !sg.Write {
			return off, ErrBadDesc // IN data must be device->driver
		}
		n := int(sg.Len)
		if off+n > len(src) {
			n = len(src) - off
		}
		if n > 0 {
			if err := gm.Write(src[off:off+n], sg.Addr); err != nil {
				return off, err
			}
			off += n
		}
	}
	return off, nil
}

// DataLen is the total size of all data segments.
func (b *BlkReq) DataLen() uint64 {
	var n uint64
	for _, sg := range b.DataSegs {
		n += uint64(sg.Len)
	}
	return n
}
