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
