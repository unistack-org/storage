//go:build linux

package vduse

import (
	"testing"
)

// stubBE is a tiny in-memory Backend for exec tests.
type stubBE struct{ data []byte }

func (s *stubBE) Size() uint64 { return uint64(len(s.data)) }
func (s *stubBE) ReadAt(p []byte, off int64) (int, error) {
	return copy(p, s.data[off:]), nil
}
func (s *stubBE) WriteAt(p []byte, off int64) (int, error) {
	return copy(s.data[off:], p), nil
}
func (s *stubBE) Flush() error                        { return nil }
func (s *stubBE) Trim(off int64, length uint32) error { return nil }
func (s *stubBE) WriteZeroes(off int64, length uint32, punchHole bool) error {
	return nil
}
func (s *stubBE) Close() error { return nil }

// TestExecInWritesGuestData: VIRTIO_BLK_T_IN data segs are device-
// WRITEABLE; exec must fill them (regression: the guard once rejected
// Write=true segs, so every guest read completed with I/O error).
func TestExecInWritesGuestData(t *testing.T) {
	g := newMemGuest()
	be := &stubBE{data: make([]byte, 4096)}
	for i := range be.data {
		be.data[i] = byte(i)
	}
	d := &Device{gm: g, be: be}
	br := &BlkReq{
		HeaderSeg: Seg{Addr: 0x1000, Len: 16},
		DataSegs:  []Seg{{Addr: 0x2000, Len: 512, Write: true}},
		StatusSeg: Seg{Addr: 0x3000, Len: 1, Write: true},
	}
	status, n := d.exec(br, BlkReqHeader{Type: ReqIn, Sector: 2})
	if status != StatusOK || n != 512 {
		t.Fatalf("exec IN: status=%d n=%d, want OK/512", status, n)
	}
	got := g.load(0x2000, 512)
	for i := range got {
		if got[i] != byte(1024+i) {
			t.Fatalf("data mismatch at %d: got %d want %d", i, got[i], 1024+i)
		}
	}
}

// TestExecOutReadsDriverData: T_OUT data segs are device-readable.
func TestExecOutReadsDriverData(t *testing.T) {
	g := newMemGuest()
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i + 7)
	}
	g.Write(payload, 0x2000)
	be := &stubBE{data: make([]byte, 4096)}
	d := &Device{gm: g, be: be}
	br := &BlkReq{
		HeaderSeg: Seg{Addr: 0x1000, Len: 16},
		DataSegs:  []Seg{{Addr: 0x2000, Len: 512, Write: false}},
		StatusSeg: Seg{Addr: 0x3000, Len: 1, Write: true},
	}
	status, _ := d.exec(br, BlkReqHeader{Type: ReqOut, Sector: 1})
	if status != StatusOK {
		t.Fatalf("exec OUT: status=%d, want OK", status)
	}
	for i := range payload {
		if be.data[512+i] != payload[i] {
			t.Fatalf("backend data mismatch at %d", i)
		}
	}
}

// countBE records how many ReadAt/WriteAt calls the backend sees: the
// coalescing in execIO must reduce a many-segment request to ONE call.
type countBE struct {
	stubBE
	reads, writes int
}

func (c *countBE) ReadAt(p []byte, off int64) (int, error) {
	c.reads++
	return c.stubBE.ReadAt(p, off)
}
func (c *countBE) WriteAt(p []byte, off int64) (int, error) {
	c.writes++
	return c.stubBE.WriteAt(p, off)
}

// TestExecCoalescesSegments: a 1MiB write split into 256 page-sized
// segments must reach the backend as a single WriteAt (regression: per-
// segment calls turned one guest request into hundreds of cluster
// round-trips, ~30ms each megabyte).
func TestExecCoalescesSegments(t *testing.T) {
	g := newMemGuest()
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i)
	}
	g.Write(payload, 0x100000)
	be := &countBE{stubBE: stubBE{data: make([]byte, 2<<20)}}
	d := &Device{gm: g, be: be}
	var segs []Seg
	for off := 0; off < len(payload); off += 4096 {
		segs = append(segs, Seg{Addr: uint64(0x100000 + off), Len: 4096})
	}
	br := &BlkReq{
		HeaderSeg: Seg{Addr: 0x1000, Len: 16},
		DataSegs:  segs,
		StatusSeg: Seg{Addr: 0x3000, Len: 1, Write: true},
	}
	status, _ := d.exec(br, BlkReqHeader{Type: ReqOut, Sector: 8})
	if status != StatusOK {
		t.Fatalf("exec OUT: status=%d", status)
	}
	if be.writes != 1 {
		t.Fatalf("backend saw %d WriteAt calls, want 1", be.writes)
	}
	for i := range payload {
		if be.data[4096+i] != payload[i] {
			t.Fatalf("payload mismatch at %d", i)
		}
	}

	// IN path: one ReadAt, scattered across all segments. IN data segs
	// are device-writeable (Write=true).
	for i := range segs {
		segs[i].Write = true
	}
	status, n := d.exec(br, BlkReqHeader{Type: ReqIn, Sector: 8})
	if status != StatusOK || n != uint32(len(payload)) {
		t.Fatalf("exec IN: status=%d n=%d", status, n)
	}
	if be.reads != 1 {
		t.Fatalf("backend saw %d ReadAt calls, want 1", be.reads)
	}
	got := g.load(0x100000, len(payload))
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("scattered data mismatch at %d", i)
		}
	}
}

// TestExecPastVolumeEnd: the coalesced request must still be bounds
// checked against the export size.
func TestExecPastVolumeEnd(t *testing.T) {
	g := newMemGuest()
	be := &countBE{stubBE: stubBE{data: make([]byte, 4096)}}
	d := &Device{gm: g, be: be}
	br := &BlkReq{
		HeaderSeg: Seg{Addr: 0x1000, Len: 16},
		DataSegs:  []Seg{{Addr: 0x2000, Len: 4096}, {Addr: 0x3000, Len: 4096}},
		StatusSeg: Seg{Addr: 0x4000, Len: 1, Write: true},
	}
	status, _ := d.exec(br, BlkReqHeader{Type: ReqOut, Sector: 1})
	if status != StatusIOErr {
		t.Fatalf("status=%d, want IOErr", status)
	}
	if be.writes != 0 {
		t.Fatalf("backend saw %d writes, want 0", be.writes)
	}
}

// TestExecReadOnlyRefusesWrites: a read-only device must refuse every
// mutating request (the F_RO advertisement is enforced at the device,
// not trusted from the guest). Reads and flushes still pass.
func TestExecReadOnlyRefusesWrites(t *testing.T) {
	g := newMemGuest()
	be := &stubBE{data: make([]byte, 4096)}
	for i := range be.data {
		be.data[i] = 0xA5
	}
	d := &Device{gm: g, be: be, ro: true}
	for _, typ := range []uint32{ReqOut, ReqDiscard, ReqZeroOut} {
		br := &BlkReq{
			HeaderSeg: Seg{Addr: 0x1000, Len: 16},
			DataSegs:  []Seg{{Addr: 0x2000, Len: 512}},
			StatusSeg: Seg{Addr: 0x3000, Len: 1, Write: true},
		}
		if typ != ReqOut { // discard/zeroout segs are driver-owned too
			br.DataSegs[0].Write = false
		}
		if st, _ := d.exec(br, BlkReqHeader{Type: typ, Sector: 1}); st != StatusIOErr {
			t.Fatalf("RO exec type %d: status=%d, want IOErr", typ, st)
		}
	}
	for i := range be.data {
		if be.data[i] != 0xA5 {
			t.Fatalf("backend byte %d modified by RO device", i)
		}
	}
	// Read still works on a read-only device.
	br := &BlkReq{
		HeaderSeg: Seg{Addr: 0x1000, Len: 16},
		DataSegs:  []Seg{{Addr: 0x2000, Len: 512, Write: true}},
		StatusSeg: Seg{Addr: 0x3000, Len: 1, Write: true},
	}
	if st, n := d.exec(br, BlkReqHeader{Type: ReqIn, Sector: 0}); st != StatusOK || n != 512 {
		t.Fatalf("RO exec IN: status=%d n=%d, want OK/512", st, n)
	}
}
