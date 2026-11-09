package qos

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNilLimiterAllowsEverything(t *testing.T) {
	var l *Limiter
	for i := 0; i < 1000; i++ {
		if err := l.Wait(context.Background(), 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	l.Close() // must not panic
	iops, bps := l.Stats()
	if iops != 0 || bps != 0 {
		t.Fatalf("nil stats: %d %d", iops, bps)
	}
}

func TestNewZeroIsNil(t *testing.T) {
	if l := New(0, 0); l != nil {
		t.Fatal("New(0,0) must be nil (admin class)")
	}
}

func TestIOPSCeiling(t *testing.T) {
	// 100 ops/s with a 10-op burst window: 10 immediate + then ~1 per 10ms.
	l := New(100, 0)
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := time.Now()
	const ops = 60
	for i := 0; i < ops; i++ {
		if err := l.Wait(ctx, 1); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	// 10 tokens from the burst, 50 more at 100/s => ~500ms expected.
	if elapsed < 400*time.Millisecond {
		t.Fatalf("too fast: %v (expected ~500ms)", elapsed)
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("too slow: %v", elapsed)
	}
}

func TestBytesCeiling(t *testing.T) {
	// 1 MiB/s: 5 x 512 KiB => 2.5 MiB total, 0.5 MiB burst => ~2s.
	l := New(0, 1<<20)
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	for i := 0; i < 5; i++ {
		if err := l.Wait(ctx, 512<<10); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	if elapsed < 1500*time.Millisecond {
		t.Fatalf("too fast: %v", elapsed)
	}
}

func TestOversizedRequestPassesAtFullBucket(t *testing.T) {
	// 1 MiB request against a 10 KiB bucket must NOT deadlock: it waits
	// for the full capacity, then goes through.
	l := New(0, 100<<10) // cap 10 KiB
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if err := l.Wait(ctx, 1<<20); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 1500*time.Millisecond {
		t.Fatalf("oversized request too slow: %v", d)
	}
}

func TestConcurrentWaitsShareRate(t *testing.T) {
	l := New(1000, 0)
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const n = 100
	var wg sync.WaitGroup
	errs := make(chan error, n)
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := l.Wait(ctx, 1); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	// 1000 ops at 1000/s with a 100-op burst: ~900ms expected.
	if elapsed < 700*time.Millisecond {
		t.Fatalf("rate not enforced under concurrency: %v", elapsed)
	}
}

func TestContextCancel(t *testing.T) {
	l := New(1, 0) // 1 op/s, burst 1: second op must block ~1s
	defer l.Close()
	ctx := context.Background()
	if err := l.Wait(ctx, 1); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.Wait(cctx, 1); err != context.Canceled {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestCloseWakesWaiters(t *testing.T) {
	l := New(1, 0)
	if err := l.Wait(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- l.Wait(context.Background(), 1) }()
	time.Sleep(50 * time.Millisecond) // let the waiter park
	l.Close()
	select {
	case err := <-errCh:
		if err != ErrClosed {
			t.Fatalf("want ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the waiter")
	}
}

func TestStats(t *testing.T) {
	l := New(500, 1<<20)
	defer l.Close()
	iops, bps := l.Stats()
	if iops != 500 || bps != 1<<20 {
		t.Fatalf("stats: %d %d", iops, bps)
	}
}
