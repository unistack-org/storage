package client

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/osd"
	"github.com/unistack-org/storage/pkg/sdsnet"
)

// TestControlPlaneProbe is the network-split guarantee: an OSD with a
// dedicated control listener answers health probes there, and the
// client's probe round dials CtrlAddr — liveness survives a dead data
// address (the saturated/broken data network case).
func TestControlPlaneProbe(t *testing.T) {
	dir := t.TempDir()
	m, err := meta.OpenDir(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := osd.New(osd.Options{
		ID:       0,
		Addr:     "127.0.0.1:0",
		CtrlAddr: "127.0.0.1:0",
		DataDir:  filepath.Join(dir, "osd0"),
		Meta:     m,
	})
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- d.Run(ctx) }()
	t.Cleanup(func() {
		d.Close()
		<-errc
	})

	deadline := time.Now().Add(5 * time.Second)
	var info meta.OSDInfo
	for {
		infos, err := meta.ListOSDs(ctx, m)
		if err == nil && len(infos) == 1 && infos[0].CtrlAddr != "" {
			info = infos[0]
			break
		}
		select {
		case derr := <-errc:
			t.Fatalf("daemon died: %v", derr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no registration with ctrl addr (err=%v)", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if info.CtrlAddr == info.Addr {
		t.Fatalf("ctrl addr %q must differ from data addr %q (port 0 on both)", info.CtrlAddr, info.Addr)
	}
	if got := info.ControlAddr(); got != info.CtrlAddr {
		t.Fatalf("ControlAddr() = %q, want %q", got, info.CtrlAddr)
	}
	if d.CtrlAddr() != info.CtrlAddr {
		t.Fatalf("daemon CtrlAddr() = %q, registered %q", d.CtrlAddr(), info.CtrlAddr)
	}

	// The control listener answers a ping on its own.
	pp := sdsnet.NewPool(info.CtrlAddr, "vol")
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	if err := pp.Ping(pctx); err != nil {
		pcancel()
		pp.Close()
		t.Fatalf("ping on ctrl addr: %v", err)
	}
	pcancel()
	pp.Close()

	// Break the DATA address in the registry (point it at a refused
	// port); the probe round must still see the OSD alive via ctrl.
	info.Addr = "127.0.0.1:1"
	if _, err := meta.PutOSD(ctx, m, info); err != nil {
		t.Fatal(err)
	}
	cl := NewCluster(m)
	defer cl.Close()
	_, probes, metaErr := cl.ProbeOSDs(ctx, 2*time.Second)
	if metaErr != nil {
		t.Fatal(metaErr)
	}
	if perr := probes[0]; perr != nil {
		t.Fatalf("probe went to the data addr: %v", perr)
	}
	_, probes, disk, metaErr := cl.ProbeOSDsDisk(ctx, 2*time.Second)
	if metaErr != nil {
		t.Fatal(metaErr)
	}
	if perr := probes[0]; perr != nil {
		t.Fatalf("disk probe went to the data addr: %v", perr)
	}
	if _, ok := disk[0]; !ok {
		t.Fatal("StatFS did not answer over the control plane")
	}
}

// TestControlAddrFallback: an OSD registered without a control plane
// (and every record written before the split existed) probes the data
// address.
func TestControlAddrFallback(t *testing.T) {
	c := newCluster(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	infos, err := meta.ListOSDs(ctx, c.meta)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("want 1 osd, got %d", len(infos))
	}
	if infos[0].CtrlAddr != "" {
		t.Fatalf("unexpected ctrl addr %q", infos[0].CtrlAddr)
	}
	if got := infos[0].ControlAddr(); got != infos[0].Addr {
		t.Fatalf("ControlAddr() = %q, want data addr %q", got, infos[0].Addr)
	}
}
