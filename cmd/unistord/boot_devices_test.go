package main

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/unistack-org/storage/pkg/meta"
)

// TestBootClusterDevices: cluster-mode boot specs open through the
// manager callback with the parsed spec (vub name = volume, socket
// from the spec), persist a meta export record for freshly created
// devices, stay silent for devices the callback reports as already
// live, and fail fast on bad specs.
func TestBootClusterDevices(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	ms, err := meta.OpenDir(dir + "/meta")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	logger := log.New(io.Discard, "", 0)

	type call struct {
		volume, name, socket string
		ro                   bool
	}
	var calls []call
	open := func(volume, name, socket string, ro bool) (bool, error) {
		calls = append(calls, call{volume, name, socket, ro})
		return true, nil
	}

	specs := []string{"vm0=/run/vm0.sock", "vm1=/run/vm1.sock:ro"}
	if err := bootClusterDevices(ctx, ms, "vhost-user-blk", specs, map[string]string{"vm1": "gold"}, logger, open); err != nil {
		t.Fatalf("boot: %v", err)
	}
	want := []call{{"vm0", "vm0", "/run/vm0.sock", false}, {"vm1", "vm1", "/run/vm1.sock", true}}
	if len(calls) != len(want) {
		t.Fatalf("open calls = %v, want %v", calls, want)
	}
	for i, c := range calls {
		if c != want[i] {
			t.Fatalf("call %d = %+v, want %+v", i, c, want[i])
		}
	}
	recs, err := meta.ListExportRecords(ctx, ms)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("records = %d, want 2", len(recs))
	}
	byName := map[string]meta.ExportRecord{}
	for _, r := range recs {
		byName[r.Name] = r
	}
	if r := byName["vm0"]; r.Type != "vhost-user-blk" || r.Volume != "vm0" || r.ReadOnly {
		t.Fatalf("record vm0 = %+v", r)
	}
	if r := byName["vm1"]; !r.ReadOnly {
		t.Fatalf("record vm1 = %+v, want read-only", r)
	}

	// Idempotent restart: the callback reports the device as already
	// live (created=false) and no second record is written.
	calls = nil
	already := func(volume, name, socket string, ro bool) (bool, error) {
		calls = append(calls, call{volume, name, socket, ro})
		return true, nil
	}
	if err := bootClusterDevices(ctx, ms, "vhost-user-blk", specs, nil, logger, already); err != nil {
		t.Fatalf("re-boot: %v", err)
	}
	recs, _ = meta.ListExportRecords(ctx, ms)
	if len(recs) != 2 {
		t.Fatalf("records after re-boot = %d, want 2", len(recs))
	}

	// A bad socket is rejected before the manager is called.
	calls = nil
	err = bootClusterDevices(ctx, ms, "vhost-user-blk", []string{"vm2=relative.sock"}, nil, logger, open)
	if err == nil || !strings.Contains(err.Error(), "-vhost-user-blk") {
		t.Fatalf("bad socket: %v, want -vhost-user-blk error", err)
	}
	if len(calls) != 0 {
		t.Fatalf("bad socket opened: %v", calls)
	}

	// A manager failure aborts the boot with the type in the message.
	err = bootClusterDevices(ctx, ms, "vhost-user-blk", []string{"vm3=/run/vm3.sock"}, nil, logger,
		func(string, string, string, bool) (bool, error) { return false, io.ErrUnexpectedEOF })
	if err == nil || !strings.Contains(err.Error(), "vhost-user-blk") {
		t.Fatalf("open failure: %v", err)
	}

	// vduse specs validate the device name.
	err = bootClusterDevices(ctx, ms, "vduse", []string{"vm0=bad/name"}, nil, logger, open)
	if err == nil || !strings.Contains(err.Error(), "-vduse") {
		t.Fatalf("bad devname: %v", err)
	}
}
