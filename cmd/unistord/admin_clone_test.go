package main

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// TestAdminCloneEndpoints walks the clone HTTP surface: POST clone
// (201 + destination view), 404 on a missing source, 409 on a taken
// destination, and the end-to-end story - the clone reads the base
// image, survives the source's DELETE + vacuum, and keeps its own
// writes private.
func TestAdminCloneEndpoints(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := env.cl.CreateVolume(ctx, "base", 16<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	src, err := env.cl.OpenVolume(ctx, "base")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	data := make([]byte, object.Size)
	for i := range data {
		data[i] = byte(i ^ 0x5a)
	}
	if _, err := src.WriteAt(ctx, data, 2<<object.SizeShift); err != nil {
		t.Fatal(err)
	}

	// POST clone -> 201 with the destination's view.
	code, view := doAdminBody(t, h, http.MethodPost, "/v1/volumes/base/clone", `{"name":"vm1"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST clone: %d (%v)", code, view)
	}
	if view["name"] != "vm1" {
		t.Fatalf("clone view name: %+v", view)
	}

	// 409: destination exists. 404: source missing.
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/base/clone", `{"name":"vm1"}`); code != http.StatusConflict {
		t.Fatalf("clone onto an existing name: %d, want 409", code)
	}
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/nosuch/clone", `{"name":"vm2"}`); code != http.StatusNotFound {
		t.Fatalf("clone of a missing source: %d, want 404", code)
	}
	if code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/base/clone", `{}`); code != http.StatusBadRequest {
		t.Fatalf("clone without a name: %d, want 400", code)
	}

	// The clone reads the base image through the shared chain.
	cl, err := env.cl.OpenVolume(ctx, "vm1")
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	got := make([]byte, object.Size)
	if _, err := cl.ReadAt(ctx, got, 2<<object.SizeShift); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("clone block != base image")
	}

	// Private writes: the source must not see them.
	own := make([]byte, object.Size)
	for i := range own {
		own[i] = 0x3c
	}
	if _, err := cl.WriteAt(ctx, own, 3<<object.SizeShift); err != nil {
		t.Fatal(err)
	}
	if _, err := src.ReadAt(ctx, got, 3<<object.SizeShift); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, make([]byte, object.Size)) {
		t.Fatal("source sees the clone's write")
	}

	// DELETE the source + vacuum: the clone keeps its bytes.
	if code, _ = doAdminJSON(t, h, http.MethodDelete, "/v1/volumes/base"); code != http.StatusNoContent {
		t.Fatalf("DELETE source: %d", code)
	}
	res, err := env.cl.VacuumVolume(ctx, "base")
	if err != nil {
		t.Fatal(err)
	}
	_ = res
	if _, err := cl.ReadAt(ctx, got, 2<<object.SizeShift); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("clone block lost after source delete+vacuum")
	}
	if _, err := cl.ReadAt(ctx, got, 3<<object.SizeShift); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, own) {
		t.Fatal("clone private data damaged")
	}
}
