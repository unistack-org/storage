package vduse

import (
	"encoding/binary"
	"testing"
)

// memGuest is a sparse GuestMem over a plain map, sufficient for logic
// tests without pulling in an mmap.
type memGuest struct {
	pages map[uint64][]byte
}

func newMemGuest() *memGuest { return &memGuest{pages: map[uint64][]byte{}} }

const pgSize = 4096

func (g *memGuest) load(iova uint64, n int) []byte {
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		pg := g.pages[(iova+uint64(i))/pgSize]
		if pg != nil {
			out[i] = pg[(iova+uint64(i))%pgSize]
		}
	}
	return out
}

func (g *memGuest) Read(dst []byte, iova uint64) error {
	src := g.load(iova, len(dst))
	copy(dst, src)
	return nil
}

func (g *memGuest) Write(src []byte, iova uint64) error {
	for i := range src {
		a := iova + uint64(i)
		p := a / pgSize
		if g.pages[p] == nil {
			g.pages[p] = make([]byte, pgSize)
		}
		g.pages[p][a%pgSize] = src[i]
	}
	return nil
}

// putDesc writes one descriptor at table index i.
func putDesc(g *memGuest, base uint64, i int, addr uint64, length uint32, flags uint16, next uint16) {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:], addr)
	binary.LittleEndian.PutUint32(b[8:], length)
	binary.LittleEndian.PutUint16(b[12:], flags)
	binary.LittleEndian.PutUint16(b[14:], next)
	g.Write(b[:], base+uint64(i)*16)
}

func TestParseBlkReqHeader(t *testing.T) {
	var b [16]byte
	binary.LittleEndian.PutUint32(b[0:], ReqOut)
	binary.LittleEndian.PutUint64(b[8:], 4096)
	h, ok := ParseBlkReqHeader(b[:])
	if !ok || h.Type != ReqOut || h.Sector != 4096 {
		t.Fatalf("got %+v ok=%v", h, ok)
	}
	if _, ok := ParseBlkReqHeader(b[:15]); ok {
		t.Fatal("short header accepted")
	}
}

func TestConfigSpaceLayout(t *testing.T) {
	c := ConfigSpace(2<<30, 4)
	if len(c) != ConfigLen {
		t.Fatalf("len %d", len(c))
	}
	if got := binary.LittleEndian.Uint64(c[0:]); got != (2<<30)/512 {
		t.Fatalf("capacity = %d", got)
	}
	if got := binary.LittleEndian.Uint32(c[20:]); got != 512 {
		t.Fatalf("blk_size = %d", got)
	}
	if c[24] != 3 {
		t.Fatalf("physical_block_exp = %d", c[24])
	}
	if c[32] != 1 {
		t.Fatalf("wce = %d", c[32])
	}
	if got := binary.LittleEndian.Uint16(c[34:]); got != 4 {
		t.Fatalf("num_queues = %d", got)
	}
	if got := binary.LittleEndian.Uint32(c[36:]); got != 1<<20 {
		t.Fatalf("max_discard_sectors = %d", got)
	}
	if c[56] != 1 {
		t.Fatalf("write_zeroes_may_unmap = %d", c[56])
	}
}

func TestReadChainSimple(t *testing.T) {
	g := newMemGuest()
	const descBase = 0
	// header (read) -> data (read) -> status (write)
	putDesc(g, descBase, 0, 0x1000, 16, DescFlagNext, 1)
	putDesc(g, descBase, 1, 0x2000, 512, DescFlagNext, 2)
	putDesc(g, descBase, 2, 0x3000, 1, DescFlagWrite, 0)
	segs, err := ReadChain(g, 8, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 3 || !segs[2].Write || segs[0].Write {
		t.Fatalf("segs = %+v", segs)
	}
	if segs[1].Len != 512 || segs[1].Addr != 0x2000 {
		t.Fatalf("data seg = %+v", segs[1])
	}
}

func TestReadChainIndirect(t *testing.T) {
	g := newMemGuest()
	const descBase, tblBase = 0, 0x9000
	// top-level desc 0 points to an indirect table of 3 descs
	putDesc(g, descBase, 0, tblBase, 3*16, DescFlagIndirect, 0)
	putDesc(g, tblBase, 0, 0x1000, 16, DescFlagNext, 1)
	putDesc(g, tblBase, 1, 0x2000, 512, DescFlagNext, 2)
	putDesc(g, tblBase, 2, 0x3000, 1, DescFlagWrite, 0)
	segs, err := ReadChain(g, 8, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 3 || !segs[2].Write {
		t.Fatalf("segs = %+v", segs)
	}
}

func TestReadChainRejectsNestedIndirect(t *testing.T) {
	g := newMemGuest()
	const descBase, tblBase = 0, 0x9000
	putDesc(g, descBase, 0, tblBase, 16, DescFlagIndirect, 0)
	putDesc(g, tblBase, 0, 0xA000, 16, DescFlagIndirect, 0)
	if _, err := ReadChain(g, 8, 0); err == nil {
		t.Fatal("nested indirect accepted")
	}
}

func TestSplitRingAvailUsed(t *testing.T) {
	g := newMemGuest()
	const desc, avail, used = 0x10000, 0x20000, 0x30000
	r := NewSplitRing(g, 8, desc, avail, used)
	// driver posts avail idx 2, ring[0]=3 ring[1]=5
	g.Write([]byte{0, 0}, avail+availFlagsOff)
	var idx [2]byte
	binary.LittleEndian.PutUint16(idx[:], 2)
	g.Write(idx[:], avail+availIdxOff)
	var e [2]byte
	binary.LittleEndian.PutUint16(e[:], 3)
	g.Write(e[:], avail+availRingOff+0)
	binary.LittleEndian.PutUint16(e[:], 5)
	g.Write(e[:], avail+availRingOff+2)

	h, err := r.NextAvail()
	if err != nil || h != 3 {
		t.Fatalf("first head=%d err=%v", h, err)
	}
	h, err = r.NextAvail()
	if err != nil || h != 5 {
		t.Fatalf("second head=%d err=%v", h, err)
	}
	if h, err := r.NextAvail(); h != -1 || err != nil {
		t.Fatalf("empty head=%d err=%v", h, err)
	}
	// push two used entries; no suppression -> IRQ both times
	if irq, err := r.PushUsed(3, 512); err != nil || !irq {
		t.Fatalf("push1 irq=%v err=%v", irq, err)
	}
	if irq, err := r.PushUsed(5, 0); err != nil || !irq {
		t.Fatalf("push2 irq=%v err=%v", irq, err)
	}
	var uidx [2]byte
	g.Read(uidx[:], used+usedIdxOff)
	if binary.LittleEndian.Uint16(uidx[:]) != 2 {
		t.Fatalf("used idx = %d", binary.LittleEndian.Uint16(uidx[:]))
	}
	var ent [8]byte
	g.Read(ent[:], used+usedRingOff)
	if binary.LittleEndian.Uint32(ent[0:]) != 3 || binary.LittleEndian.Uint32(ent[4:]) != 512 {
		t.Fatalf("used[0] = %v", ent)
	}
	// wrap-around slot arithmetic: 9 more pushes (ids 0..8) -> used idx 11;
	// the last entry lands at slot 11%8 == 3.
	for i := 0; i < 9; i++ {
		if _, err := r.PushUsed(uint16(i), uint32(i)); err != nil {
			t.Fatal(err)
		}
	}
	g.Read(uidx[:], used+usedIdxOff)
	if got := binary.LittleEndian.Uint16(uidx[:]); got != 11 {
		t.Fatalf("used idx after wrap = %d", got)
	}
	g.Read(ent[:], used+usedRingOff+8*2) // 11th entry: slot 10%8 == 2
	if binary.LittleEndian.Uint32(ent[0:]) != 8 {
		t.Fatalf("wrapped entry id = %d", binary.LittleEndian.Uint32(ent[0:]))
	}
}

func TestSplitRingNoInterruptSuppression(t *testing.T) {
	g := newMemGuest()
	r := NewSplitRing(g, 8, 0x10000, 0x20000, 0x30000)
	g.Write([]byte{0x01, 0}, 0x20000+availFlagsOff) // NO_INTERRUPT
	if irq, err := r.PushUsed(1, 0); err != nil || irq {
		t.Fatalf("irq=%v err=%v want suppressed", irq, err)
	}
}

func TestParseBlkChainAndData(t *testing.T) {
	segs := []Seg{
		{Addr: 0x1000, Len: 16, Write: false},  // header
		{Addr: 0x2000, Len: 512, Write: false}, // OUT data
		{Addr: 0x3000, Len: 1, Write: true},    // status
	}
	r, err := ParseBlkChain(segs, 7)
	if err != nil {
		t.Fatal(err)
	}
	if r.Head != 7 || r.DataLen() != 512 {
		t.Fatalf("req = %+v", r)
	}
	// bad: status not 1 byte
	bad := append([]Seg{}, segs...)
	bad[2].Len = 2
	if _, err := ParseBlkChain(bad, 7); err == nil {
		t.Fatal("accepted 2-byte status")
	}
	// read data out of a write request
	g := newMemGuest()
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i)
	}
	g.Write(payload, 0x2000)
	dst := make([]byte, 512)
	if err := r.ReadData(g, dst); err != nil {
		t.Fatal(err)
	}
	for i := range dst {
		if dst[i] != byte(i) {
			t.Fatalf("data mismatch at %d", i)
		}
	}
}
