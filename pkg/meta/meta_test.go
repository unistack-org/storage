package meta

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func openStore(t *testing.T) *DirStore {
	t.Helper()
	s, err := OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.PollInterval = 50 * time.Millisecond
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	if _, _, err := s.Get(ctx, "osds", "1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Get = %v, want ErrNotFound", err)
	}

	rev, err := s.Put(ctx, "osds", "1", []byte(`{"id":1}`), 0)
	if err != nil || rev != 1 {
		t.Fatalf("first Put = (%d, %v)", rev, err)
	}
	// CAS on a fresh key must now fail.
	if _, err := s.Put(ctx, "osds", "1", []byte(`x`), 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale CAS = %v, want ErrConflict", err)
	}
	rev, err = s.Put(ctx, "osds", "1", []byte(`{"id":1,"w":2}`), 1)
	if err != nil || rev != 2 {
		t.Fatalf("Put rev1 = (%d, %v)", rev, err)
	}
	v, rev, err := s.Get(ctx, "osds", "1")
	if err != nil || rev != 2 || string(v) != `{"id":1,"w":2}` {
		t.Fatalf("Get = (%s, %d, %v)", v, rev, err)
	}

	if err := s.Delete(ctx, "osds", "1", 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale Delete = %v, want ErrConflict", err)
	}
	if err := s.Delete(ctx, "osds", "1", 2); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, _, err := s.Get(ctx, "osds", "1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete = %v", err)
	}
}

func TestList(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	for _, k := range []string{"9", "10", "2"} {
		if _, err := s.Put(ctx, "osds", k, []byte(k), -1); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := s.List(ctx, "osds")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10", "2", "9"} // lexical order of file names
	if len(keys) != len(want) {
		t.Fatalf("List = %v", keys)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("List = %v, want %v", keys, want)
		}
	}
	if keys, err := s.List(ctx, "missing"); err != nil || keys != nil {
		t.Fatalf("List(missing) = %v, %v", keys, err)
	}
}

func TestWatch(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	w, err := s.Watch(ctx, "osds")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if _, err := s.Put(ctx, "osds", "1", []byte("a"), -1); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		if ev.Type != "put" || ev.Key != "1" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for put event")
	}

	if _, err := s.Put(ctx, "osds", "1", []byte("b"), -1); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		if ev.Type != "put" || ev.Key != "1" || ev.Rev != 2 {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update event")
	}

	if err := s.Delete(ctx, "osds", "1", -1); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-w.Events():
			if ev.Type == "delete" && ev.Key == "1" {
				return
			}
		case <-deadline:
			t.Fatal("timeout waiting for delete event")
		}
	}
}

// A second store instance watching the same directory sees changes made
// by the first one (cross-process emulation); relies on polling.
func TestWatchCrossInstance(t *testing.T) {
	dir := t.TempDir()
	a, err := OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.PollInterval = 50 * time.Millisecond

	ctx := context.Background()
	w, err := b.Watch(ctx, "osds")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	if _, err := a.Put(ctx, "osds", "7", []byte("x"), -1); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-w.Events():
		if ev.Type != "put" || ev.Key != "7" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cross-instance watch missed the put")
	}
}

func TestRecordsRoundTrip(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	info := OSDInfo{ID: 3, Addr: "127.0.0.1:8003", UUID: "u3", Weight: 0.5, BootedAt: 99}
	if _, err := PutOSD(ctx, s, info); err != nil {
		t.Fatal(err)
	}
	got, err := GetOSD(ctx, s, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got != info {
		t.Fatalf("OSD = %+v want %+v", got, info)
	}
	osds, err := ListOSDs(ctx, s)
	if err != nil || len(osds) != 1 || osds[0].ID != 3 {
		t.Fatalf("ListOSDs = %+v, %v", osds, err)
	}

	spec := VolumeSpec{Name: "vm0", Size: 1 << 30, OIDShift: 22, Replicas: 3, Rule: "data", PGNum: 64}
	if _, err := PutVolume(ctx, s, spec); err != nil {
		t.Fatal(err)
	}
	gotSpec, err := GetVolume(ctx, s, "vm0")
	if err != nil || !reflect.DeepEqual(gotSpec, spec) {
		t.Fatalf("Volume = %+v, %v", gotSpec, err)
	}

	if _, err := LatestMap(ctx, s); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LatestMap empty = %v", err)
	}
	for _, e := range []uint32{2, 10, 7} {
		if _, err := PutMap(ctx, s, MapRecord{Epoch: e, Format: "text", Map: []byte("m")}); err != nil {
			t.Fatal(err)
		}
	}
	rec, err := LatestMap(ctx, s)
	if err != nil || rec.Epoch != 10 {
		t.Fatalf("LatestMap = %+v, %v", rec, err)
	}
}

func TestRemoveOSD(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	if err := RemoveOSD(ctx, s, 7); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RemoveOSD missing = %v, want ErrNotFound", err)
	}

	info := OSDInfo{ID: 7, Addr: "127.0.0.1:8007", UUID: "u7", Weight: 0}
	if _, err := PutOSD(ctx, s, info); err != nil {
		t.Fatal(err)
	}
	if err := RemoveOSD(ctx, s, 7); err != nil {
		t.Fatalf("RemoveOSD: %v", err)
	}
	if _, err := GetOSD(ctx, s, 7); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetOSD after remove = %v, want ErrNotFound", err)
	}
	osds, err := ListOSDs(ctx, s)
	if err != nil || len(osds) != 0 {
		t.Fatalf("ListOSDs after remove = %+v, %v", osds, err)
	}
}

// Stop must make the store forget the watcher: a Put after Stop used to
// deliver into a closed events channel and panic the writer.
func TestWatchStopThenPut(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	w, err := s.Watch(ctx, "osds")
	if err != nil {
		t.Fatal(err)
	}
	w.Stop()
	w.Stop() // idempotent
	for i := 0; i < 200; i++ {
		if _, err := s.Put(ctx, "osds", "1", []byte("x"), -1); err != nil {
			t.Fatal(err)
		}
		if err := s.Delete(ctx, "osds", "1", -1); err != nil {
			t.Fatal(err)
		}
	}
}

// A racing Stop (from another goroutine, e.g. a volume closing) must
// never panic a concurrent Put's notify: events is never closed.
func TestWatchStopRaceWithPut(t *testing.T) {
	s := openStore(t)
	s.PollInterval = 5 * time.Millisecond
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			w, err := s.Watch(ctx, "osds")
			if err != nil {
				return
			}
			time.Sleep(time.Duration(i%5) * time.Millisecond)
			w.Stop()
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _ = s.Put(ctx, "osds", "1", []byte("x"), -1)
			_ = s.Delete(ctx, "osds", "1", -1)
		}(i)
	}
	wg.Wait()
}
