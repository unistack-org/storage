package client

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

func TestWritebackOverlay(t *testing.T) {
	wb := &writeback{objs: map[uint64]*wbObj{}, maxBytes: 1 << 20}
	wb.lru.Init()
	page := func(b byte) []byte {
		p := make([]byte, wbPageSize)
		for i := range p {
			p[i] = b
		}
		return p
	}
	// Dirty page 1 of object 3 and the tail half of page 0 of object 4.
	o3 := &wbObj{index: 3, pages: map[uint32][]byte{1: page(0x33)}}
	o3.bytes = wbPageSize
	o4 := &wbObj{index: 4, pages: map[uint32][]byte{0: page(0x44)[:2048]}}
	o4.bytes = wbPageSize
	wb.objs[3], wb.objs[4] = o3, o4
	wb.dirty = 2 * wbPageSize

	// Read spanning object 3 page 1: the cached page must win.
	buf := make([]byte, 3*wbPageSize)
	wb.overlay(buf, 3*object.Size)
	if !bytes.Equal(buf[wbPageSize:2*wbPageSize], page(0x33)) {
		t.Fatal("cached page not overlaid")
	}
	for i, b := range buf[:wbPageSize] {
		if b != 0 {
			t.Fatalf("clean page clobbered at %d", i)
		}
	}
	// Partial tail page: only its first 2048 bytes are dirty.
	buf = make([]byte, wbPageSize)
	wb.overlay(buf, 4*object.Size)
	if !bytes.Equal(buf[:2048], page(0x44)[:2048]) {
		t.Fatal("partial page not overlaid")
	}
	for i, b := range buf[2048:] {
		if b != 0 {
			t.Fatalf("bytes past the dirty tail clobbered at %d", i)
		}
	}
	// Partial read window inside a dirty page.
	buf = make([]byte, 1024)
	wb.overlay(buf, 3*object.Size+wbPageSize+512)
	for i, b := range buf {
		if b != 0x33 {
			t.Fatalf("window inside dirty page not overlaid at %d", i)
		}
	}
}

func TestWritebackErrorOnce(t *testing.T) {
	wb := &writeback{objs: map[uint64]*wbObj{}}
	wb.lru.Init()
	e := context.DeadlineExceeded
	wb.recordErr(e)
	wb.recordErr(context.Canceled) // keep-first
	if err := wb.takeErr(); !errorsIs(err, e) {
		t.Fatalf("takeErr = %v, want %v", err, e)
	}
	if err := wb.takeErr(); err != nil {
		t.Fatalf("second takeErr = %v, want nil", err)
	}
}

func errorsIs(err, target error) bool {
	return err == target
}

// TestWritebackClusterRun exercises the cache through real OSDs: writes
// stay client-side until Flush, reads see them, a second handle (a
// "crash survivor" view) sees them only after the flush.
func TestWritebackClusterRun(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.cl.SetWriteback(4<<20, time.Hour) // big cap, no timer help: Flush does the work
	const size = 64 << 20
	if _, err := c.cl.CreateVolume(ctx, "wb0", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "wb0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	if v.wb == nil {
		t.Fatal("SetWriteback not applied to the opened volume")
	}
	v2, err := c.cl.OpenVolume(ctx, "wb0") // second handle = what the OSDs have
	if err != nil {
		t.Fatalf("OpenVolume 2: %v", err)
	}

	// Full-page writes in two objects plus a partial page (RMW path).
	a := make([]byte, 4096)
	fill(a, 0xA1, 0)
	if n, err := v.WriteAt(ctx, a, 8192); err != nil || n != 4096 {
		t.Fatalf("WriteAt a: n=%d err=%v", n, err)
	}
	b := make([]byte, 4096)
	fill(b, 0xB2, 0)
	if n, err := v.WriteAt(ctx, b, object.Size+4096); err != nil || n != 4096 {
		t.Fatalf("WriteAt b: n=%d err=%v", n, err)
	}
	tail := []byte{0xC3, 0xC3, 0xC3, 0xC3}
	if n, err := v.WriteAt(ctx, tail, size-8); err != nil || n != 4 {
		t.Fatalf("WriteAt tail: n=%d err=%v", n, err)
	}
	if v.wb.peekDirty() == 0 {
		t.Fatal("writes bypassed the cache")
	}

	// Read-your-writes through the cache.
	got := make([]byte, 4096)
	if n, err := v.ReadAt(ctx, got, 8192); err != nil || n != 4096 {
		t.Fatalf("ReadAt a: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, a) {
		t.Fatal("overlay missed write a")
	}
	// The second handle sees nothing: the data is client-side.
	if n, err := v2.ReadAt(ctx, got, 8192); err != nil || n != 4096 {
		t.Fatalf("ReadAt a on v2: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("write reached the OSDs before Flush")
	}

	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if d := v.wb.peekDirty(); d != 0 {
		t.Fatalf("dirty after Flush = %d, want 0", d)
	}
	for _, tc := range []struct {
		off  uint64
		want []byte
	}{
		{8192, a},
		{object.Size + 4096, b},
	} {
		if n, err := v2.ReadAt(ctx, got, tc.off); err != nil || n != 4096 {
			t.Fatalf("ReadAt %d on v2: n=%d err=%v", tc.off, n, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("Flush lost the write at %d", tc.off)
		}
	}
	// Tail partial page: the 4 new bytes plus the untouched rest of the
	// page (the RMW fetch preserved it as zeroes).
	gotTail := make([]byte, 4096)
	if n, err := v2.ReadAt(ctx, gotTail, size-4096); err != nil || n != 4096 {
		t.Fatalf("ReadAt tail on v2: n=%d err=%v", n, err)
	}
	if !bytes.Equal(gotTail[4088:4092], tail) {
		t.Fatal("tail write lost")
	}
	for _, b := range gotTail[:4088] {
		if b != 0 {
			t.Fatal("tail RMW clobbered the rest of the page")
		}
	}

	// Close flushes what the guest never fsynced.
	if n, err := v.WriteAt(ctx, a, 12288); err != nil || n != 4096 {
		t.Fatalf("WriteAt pre-close: n=%d err=%v", n, err)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n, err := v2.ReadAt(ctx, got, 12288); err != nil || n != 4096 {
		t.Fatalf("ReadAt pre-close on v2: n=%d err=%v", n, err)
	}
	if !bytes.Equal(got, a) {
		t.Fatal("Close dropped the unflushed write")
	}
}

// TestWritebackCapPressure checks the memory cap: writes over the cap
// flush older objects (the backpressure) and everything stays readable.
func TestWritebackCapPressure(t *testing.T) {
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.cl.SetWriteback(2*wbPageSize, time.Hour) // two pages of cache
	const size = 64 << 20
	if _, err := c.cl.CreateVolume(ctx, "wb1", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "wb1")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	v2, err := c.cl.OpenVolume(ctx, "wb1")
	if err != nil {
		t.Fatalf("OpenVolume 2: %v", err)
	}
	// One page per object, 12 objects: the cap forces all but the last
	// writes out to the OSDs while the loop runs.
	for i := 0; i < 12; i++ {
		p := make([]byte, 4096)
		fill(p, byte(0x10+i), i)
		if n, err := v.WriteAt(ctx, p, uint64(i)*object.Size); err != nil || n != 4096 {
			t.Fatalf("WriteAt obj %d: n=%d err=%v", i, n, err)
		}
		if d := v.wb.peekDirty(); d > 3*wbPageSize {
			t.Fatalf("cap breached: dirty=%d", d)
		}
	}
	// The cap keeps dirty data at a few pages; the newest writes may
	// still be client-side, so flush before the durability check.
	if err := v.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := 0; i < 12; i++ {
		want := make([]byte, 4096)
		fill(want, byte(0x10+i), i)
		got := make([]byte, 4096)
		if n, err := v2.ReadAt(ctx, got, uint64(i)*object.Size); err != nil || n != 4096 {
			t.Fatalf("ReadAt obj %d: n=%d err=%v", i, n, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("object %d lost under cap pressure", i)
		}
	}
}
