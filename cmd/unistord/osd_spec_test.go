package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseOSDSpec(t *testing.T) {
	for _, tc := range []struct {
		spec, dataDir, addr, ctrl string
		id                        int32
		bad                       bool
	}{
		{spec: "0@/d", dataDir: "/d", addr: "127.0.0.1:0", ctrl: "", id: 0},
		{spec: "1@/d@10.0.0.1:7000", dataDir: "/d", addr: "10.0.0.1:7000", ctrl: "", id: 1},
		{spec: "2@/d@10.0.0.1:7000@10.0.1.1:7100", dataDir: "/d", addr: "10.0.0.1:7000", ctrl: "10.0.1.1:7100", id: 2},
		{spec: "3@/d@:7000@:7100", dataDir: "/d", addr: ":7000", ctrl: ":7100", id: 3},
		{spec: "0@/d@a@b@c", bad: true},
		{spec: "x@/d", bad: true},
		{spec: "0", dataDir: "0", addr: "127.0.0.1:0", id: -1},
		{spec: "/d", dataDir: "/d", addr: "127.0.0.1:0", id: -1}, // bare -data-dir path: id auto-assigned
		{spec: "data/rel", dataDir: "data/rel", addr: "127.0.0.1:0", id: -1},
		{spec: "", bad: true},
	} {
		id, dir, addr, ctrl, err := parseOSDSpec(tc.spec)
		if tc.bad {
			if err == nil {
				t.Errorf("%q: expected error", tc.spec)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", tc.spec, err)
			continue
		}
		if id != tc.id {
			t.Errorf("%q: id %d, want %d", tc.spec, id, tc.id)
		}
		if dir != tc.dataDir || addr != tc.addr || ctrl != tc.ctrl {
			t.Errorf("%q: got (%q,%q,%q), want (%q,%q,%q)", tc.spec, dir, addr, ctrl, tc.dataDir, tc.addr, tc.ctrl)
		}
	}
}

func TestFirstFreeOSDID(t *testing.T) {
	if got := firstFreeOSDID(map[int32]bool{}); got != 1 {
		t.Errorf("empty used: got %d, want 1", got)
	}
	used := map[int32]bool{1: true, 2: true, 4: true}
	if got := firstFreeOSDID(used); got != 3 {
		t.Errorf("used {1,2,4}: got %d, want 3", got)
	}
}

func TestPinnedOSDIDs(t *testing.T) {
	t.Run("fresh dirs stay unpinned", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		pinned, err := pinnedOSDIDs([]string{a, b})
		if err != nil {
			t.Fatal(err)
		}
		if len(pinned) != 0 {
			t.Fatalf("fresh dirs must be unpinned: %v", pinned)
		}
	})
	t.Run("pin roundtrip", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		if err := pinOSDID(a, 5); err != nil {
			t.Fatal(err)
		}
		pinned, err := pinnedOSDIDs([]string{a, b})
		if err != nil {
			t.Fatal(err)
		}
		if pinned[a] != 5 {
			t.Fatalf("pinned = %v, want %s:5", pinned, a)
		}
		if _, ok := pinned[b]; ok {
			t.Fatalf("%s must stay unpinned", b)
		}
	})
	t.Run("corrupt pin is an error", func(t *testing.T) {
		a := t.TempDir()
		if err := os.WriteFile(filepath.Join(a, "id"), []byte("not-a-number"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := pinnedOSDIDs([]string{a}); err == nil {
			t.Fatal("corrupt id pin must be an error (silent id re-assignment would orphan the data)")
		}
	})
	t.Run("duplicate pins", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		if err := pinOSDID(a, 2); err != nil {
			t.Fatal(err)
		}
		if err := pinOSDID(b, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := pinnedOSDIDs([]string{a, b}); err == nil {
			t.Fatal("duplicate id pins must be an error")
		}
	})
}
