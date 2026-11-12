package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/osd"
)

// map3Hosts: three hosts, one osd each, rep-3-capable.
const map3Hosts = `# begin crush map
device 0 osd.0
device 1 osd.1
device 2 osd.2
type 0 osd
type 1 host
type 2 root
root r {
id -2
alg straw2
hash 0
item host h1
item host h2
item host h3
}
host h1 {
id -3
alg straw2
hash 0
item osd.0 weight 1.000
}
host h2 {
id -4
alg straw2
hash 0
item osd.1 weight 1.000
}
host h3 {
id -5
alg straw2
hash 0
item osd.2 weight 1.000
}
rule data {
id 0
type replicated
step take r
step chooseleaf firstn 0 type host
step emit
}
# end crush map
`

func startStatusCluster(t *testing.T) (*adminAPI, *client.Cluster, []*osd.Daemon) {
	t.Helper()
	dir := t.TempDir()
	ms, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var daems []*osd.Daemon
	for i := 0; i < 3; i++ {
		d, err := osd.New(osd.Options{
			ID:      int32(i),
			Addr:    "127.0.0.1:0",
			DataDir: filepath.Join(dir, fmt.Sprintf("osd%d", i)),
			Meta:    ms,
		})
		if err != nil {
			t.Fatal(err)
		}
		daems = append(daems, d)
		go d.Run(ctx)
		deadline := time.Now().Add(5 * time.Second)
		for {
			infos, err := meta.ListOSDs(ctx, ms)
			if err == nil && len(infos) >= i+1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("osd %d did not register: %v", i, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	cl := client.NewCluster(ms)
	t.Cleanup(func() { cl.Close() })
	if err := cl.MapEpoch(ctx, 1, []byte(map3Hosts)); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.CreateVolume(ctx, "vm0", 128<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	disks := newDiskRegistry()
	a, err := newAdminServer(ms, "127.0.0.1:0", log.New(io.Discard, "", 0), nil, disks, nil, nil, nil, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	return a, cl, daems
}

func getStatus(t *testing.T, h http.Handler) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: %d %s", rec.Code, rec.Body.String())
	}
	var st map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("status json: %v\n%s", err, rec.Body.String())
	}
	return st
}

func TestAdminStatusHealthy(t *testing.T) {
	a, _, _ := startStatusCluster(t)
	st := getStatus(t, a.srv.Handler)
	if st["osds"] != float64(3) || st["osds_live"] != float64(3) {
		t.Fatalf("osds: %+v", st)
	}
	if st["map_epoch"] != float64(1) || st["volumes"] != float64(1) {
		t.Fatalf("epoch/volumes: %+v", st)
	}
	if h, ok := st["volume_health"].([]any); !ok || len(h) != 1 {
		t.Fatalf("volume_health: %+v", st["volume_health"])
	} else {
		vh := h[0].(map[string]any)
		if vh["live_targets"] != float64(3) || vh["min_size"] != float64(2) || vh["writable"] != true {
			t.Fatalf("volume_health: %+v", vh)
		}
	}
	if healthy, _ := st["healthy"].(bool); !healthy {
		t.Fatalf("expected healthy, warnings: %v", st["warnings"])
	}
}

func TestAdminStatusFlagsKilledOSD(t *testing.T) {
	a, _, daems := startStatusCluster(t)
	// Kill osd 2's listener: it stays registered (weight 1) but stops
	// answering pings -> liveness warning, osds_live 2. Placement is
	// unchanged (weight 1), so the volume keeps 3 targets and stays
	// writable: ping liveness and placement health are separate views.
	if err := daems[2].Close(); err != nil {
		t.Fatal(err)
	}
	st := getStatus(t, a.srv.Handler)
	if st["osds_live"] != float64(2) {
		t.Fatalf("osds_live: %v (warnings %v)", st["osds_live"], st["warnings"])
	}
	warns, _ := st["warnings"].([]any)
	found := false
	for _, w := range warns {
		if s, _ := w.(string); strings.Contains(s, "not answering pings") {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing down-OSD warning: %v", warns)
	}
	if h := st["volume_health"].([]any)[0].(map[string]any); h["writable"] != true {
		t.Fatalf("volume should stay writable at placement level: %v", h)
	}
}

func TestAdminStatusFlagsEvictedOSD(t *testing.T) {
	a, cl, _ := startStatusCluster(t)
	ctx := context.Background()
	// Evict osd 2 (weight 0 -> placement drops it; 2 live hosts cover
	// min_size 2, so the volume stays writable).
	if err := meta.SetOSDWeight(ctx, a.ms, 2, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	st := getStatus(t, a.srv.Handler)
	healthy, _ := st["healthy"].(bool)
	if healthy {
		t.Fatalf("expected warnings after eviction: %+v", st)
	}
	warns, _ := st["warnings"].([]any)
	found := false
	for _, w := range warns {
		if s, _ := w.(string); strings.Contains(s, "osd 2 evicted") {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing eviction warning: %v", warns)
	}
	// The evicted daemon still answers pings: liveness is not eviction.
	if st["osds_live"] != float64(3) {
		t.Fatalf("osds_live: %v", st["osds_live"])
	}
	// 3-replica placement on 2 live hosts: 2 targets = min_size 2, so
	// the volume survives one host loss.
	if h := st["volume_health"].([]any)[0].(map[string]any); h["live_targets"] != float64(2) {
		t.Fatalf("live_targets after evict: %v", h)
	}
}

func TestAdminStatusDisksSection(t *testing.T) {
	a, _, _ := startStatusCluster(t)
	// The daemons run in this test process: register them so status can
	// report their local disk accounting.
	for _, id := range []int32{0, 1, 2} {
		d, err := osd.New(osd.Options{
			ID:      id,
			Addr:    "127.0.0.1:1",
			DataDir: t.TempDir(),
			Meta:    a.ms,
		})
		if err != nil {
			t.Fatal(err)
		}
		a.disks.add(id, d)
	}
	st := getStatus(t, a.srv.Handler)
	ds, ok := st["disks"].([]any)
	if !ok || len(ds) != 3 {
		t.Fatalf("disks: %+v", st["disks"])
	}
	d0 := ds[0].(map[string]any)
	if d0["osd"] != float64(0) {
		t.Fatalf("disk 0: %+v", d0)
	}
	// Not started via Run: ok=false, no human strings, no warnings fired.
	if d0["ok"] != false {
		t.Fatalf("unstarted daemon should report ok=false: %+v", d0)
	}
}
