package client

import (
	"bytes"
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

// parallelReads fires n distinct 4K chunk reads concurrently so the
// per-OSD collectors see them in the same window.
func parallelReads(ctx context.Context, t testing.TB, v *Volume, offsets []uint64, size int) [][]byte {
	t.Helper()
	out := make([][]byte, len(offsets))
	errs := make([]error, len(offsets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	for i, off := range offsets {
		wg.Add(1)
		go func(i int, off uint64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			p := make([]byte, size)
			n, err := v.ReadAt(ctx, p, off)
			if err == nil && n != size {
				err = context.DeadlineExceeded
			}
			out[i], errs[i] = p, err
		}(i, off)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
	}
	return out
}

func max1(n int64) int64 {
	if n < 1 {
		return 1
	}
	return n
}

func TestVolumeBatchReadsCoalesce(t *testing.T) {
	batchEnabled = true
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 128 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	// Write a distinctive pattern into the first 4 MiB (1024 chunks).
	const chunks = 1024
	payload := make([]byte, 4096)
	for i := 0; i < chunks; i++ {
		fill(payload, byte(i), i)
		if _, err := v.WriteAt(ctx, payload, uint64(i)*4096); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	d0, s0 := BatchStats()

	// Fire all 1024 reads concurrently: they spread over 3 OSDs, so
	// each collector sees hundreds of slots and must form windows of
	// up to 64. Dispatches must be far fewer than slots.
	offsets := make([]uint64, chunks)
	for i := range offsets {
		offsets[i] = uint64(i) * 4096
	}
	got := parallelReads(ctx, t, v, offsets, 4096)
	for i, p := range got {
		fill(payload, byte(i), i)
		if !bytes.Equal(p, payload) {
			t.Fatalf("chunk %d: data mismatch", i)
		}
	}

	d1, s1 := BatchStats()
	dd, ds := d1-d0, s1-s0
	t.Logf("batch: %d dispatches for %d slots (avg %.1f)", dd, ds, float64(ds)/float64(max1(dd)))
	if ds < chunks/2 {
		t.Fatalf("expected most reads to go through the batcher, slots=%d chunks=%d", ds, chunks)
	}
	if float64(ds)/float64(max1(dd)) < 4 {
		t.Fatalf("windows too shallow: %d slots over %d dispatches", ds, dd)
	}
}

func TestVolumeBatchThinZeros(t *testing.T) {
	batchEnabled = true
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 128 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	// 256 concurrent thin reads of never-written chunks: every replica
	// reports NotFound per slot, the fallthrough must walk all replicas
	// and land on authoritative zeros — never a transport error.
	offsets := make([]uint64, 256)
	for i := range offsets {
		offsets[i] = uint64(i) * 4096
	}
	for round := 0; round < 3; round++ {
		got := parallelReads(ctx, t, v, offsets, 4096)
		for i, p := range got {
			for j, b := range p {
				if b != 0 {
					t.Fatalf("round %d chunk %d byte %d: %02x", round, i, j, b)
				}
			}
		}
	}
}

func TestVolumeBatchWriteReadIntegrity(t *testing.T) {
	batchEnabled = true
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const size = 256 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	// Interleave writes and concurrent batched reads over the same
	// region, then verify the final state chunk-by-chunk.
	const chunks = 2048
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			payload := make([]byte, 4096)
			for i := w; i < chunks; i += 4 {
				fill(payload, byte(i), i)
				if _, err := v.WriteAt(ctx, payload, uint64(i)*4096); err != nil {
					t.Errorf("write %d: %v", i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	offsets := make([]uint64, chunks)
	for i := range offsets {
		offsets[i] = uint64(i) * 4096
	}
	got := parallelReads(ctx, t, v, offsets, 4096)
	payload := make([]byte, 4096)
	for i, p := range got {
		fill(payload, byte(i), i)
		if !bytes.Equal(p, payload) {
			t.Fatalf("chunk %d: final state mismatch", i)
		}
	}
}

func TestVolumeBatchDisabledParity(t *testing.T) {
	batchEnabled = false
	defer func() { batchEnabled = true }()
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 128 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}
	defer v.Close()

	payload := make([]byte, 4096)
	fill(payload, 0x5A, 7)
	if _, err := v.WriteAt(ctx, payload, 3<<20); err != nil {
		t.Fatalf("write: %v", err)
	}
	d0, _ := BatchStats()
	p := make([]byte, 4096)
	n, err := v.ReadAt(ctx, p, 3<<20)
	if err != nil || n != 4096 || !bytes.Equal(p, payload) {
		t.Fatalf("read with batching off: n=%d err=%v", n, err)
	}
	if d1, _ := BatchStats(); d1 != d0 {
		t.Fatalf("batcher ran with batchEnabled=false: %d -> %d", d0, d1)
	}
}

func TestVolumeBatchCloseUnblocks(t *testing.T) {
	batchEnabled = true
	c := newCluster(t, 3)
	c.publishMap(1)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const size = 128 << 20
	if _, err := c.cl.CreateVolume(ctx, "vm0", size, 0, 3, 0, 64, "data"); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}
	v, err := c.cl.OpenVolume(ctx, "vm0")
	if err != nil {
		t.Fatalf("OpenVolume: %v", err)
	}

	// Close while reads are in flight: every reader must return (no
	// parked goroutine), with data, zeros, or an error — not a hang.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := make([]byte, 4096)
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = v.ReadAt(ctx, p, uint64(i)*4096)
				runtime.Gosched()
			}
		}(i)
	}
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		_ = v.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Close hung with reads in flight")
	}
	close(stop)
	waitDone := make(chan struct{})
	go func() { wg.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("reader parked after Close")
	}
}
