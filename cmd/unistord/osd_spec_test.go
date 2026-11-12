package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/store"
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

func TestParseDataDirSpec(t *testing.T) {
	for _, tc := range []struct {
		spec, dir, want string
		bad             bool
	}{
		{spec: "/d", dir: "/d", want: ""},
		{spec: "data/rel", dir: "data/rel", want: ""},
		{spec: "data/rel/", dir: "data/rel", want: ""},
		{spec: "uuid=filesystem=abcd@/d", dir: "/d", want: "filesystem=abcd"},
		{spec: "uuid=serial-1234_56@/d", dir: "/d", want: "serial-1234_56"},
		{spec: "uuid=wwid=naa.6000c299e2a1b2c3@/var/lib/unistord/osd1", dir: "/var/lib/unistord/osd1", want: "wwid=naa.6000c299e2a1b2c3"},
		{spec: "uuid=filesystem=abcd", bad: true},     // no dir
		{spec: "uuid=@/d", bad: true},                 // empty identity
		{spec: "uuid=filesystem=ab cd@/d", bad: true}, // space in identity
		{spec: "uuid=fs;rm -rf@/d", bad: true},        // shell junk in identity
		{spec: "uuid=filesystem=abcd@", bad: true},    // empty dir
		{spec: "", bad: true},
	} {
		sp, err := parseDataDirSpec(tc.spec)
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
		if sp.dir != tc.dir || sp.wantDev != tc.want {
			t.Errorf("%q: got (%q,%q), want (%q,%q)", tc.spec, sp.dir, sp.wantDev, tc.dir, tc.want)
		}
	}
}

func TestReadOSDPins(t *testing.T) {
	t.Run("fresh dirs stay unpinned", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		pinned, err := readOSDPins(specsFor(a, b))
		if err != nil {
			t.Fatal(err)
		}
		if len(pinned) != 2 || pinned[a].id != 0 || pinned[b].id != 0 {
			t.Fatalf("fresh dirs must be unpinned: %v", pinned)
		}
	})
	t.Run("pin roundtrip", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		if err := writeOSDPin(a, 5, "filesystem=aaaa"); err != nil {
			t.Fatal(err)
		}
		pinned, err := readOSDPins(specsFor(a, b))
		if err != nil {
			t.Fatal(err)
		}
		if pinned[a].id != 5 || pinned[a].device != "filesystem=aaaa" {
			t.Fatalf("pinned = %+v, want %s:5/filesystem=aaaa", pinned[a], a)
		}
		if pinned[b].id != 0 || pinned[b].device != "" {
			t.Fatalf("%s must stay unpinned: %+v", b, pinned[b])
		}
		if readDevicePin(a) != "filesystem=aaaa" {
			t.Fatalf("readDevicePin = %q", readDevicePin(a))
		}
	})
	t.Run("no identity means no device pin", func(t *testing.T) {
		// Off Linux (no stable identity) writing an identity pin would
		// pin a fact the restart can change; the id pin still goes out.
		a := t.TempDir()
		if err := writeOSDPin(a, 3, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(a, devicePinFile)); !os.IsNotExist(err) {
			t.Fatalf("device pin must not be written without an identity: %v", err)
		}
		pinned, err := readOSDPins(specsFor(a))
		if err != nil {
			t.Fatal(err)
		}
		if pinned[a].id != 3 || pinned[a].device != "" {
			t.Fatalf("pinned = %+v", pinned[a])
		}
	})
	t.Run("corrupt pin is an error", func(t *testing.T) {
		a := t.TempDir()
		if err := os.WriteFile(filepath.Join(a, idPinFile), []byte("not-a-number"), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readOSDPins(specsFor(a)); err == nil {
			t.Fatal("corrupt id pin must be an error (silent id re-assignment would orphan the data)")
		}
	})
	t.Run("duplicate pins", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		if err := writeOSDPin(a, 2, ""); err != nil {
			t.Fatal(err)
		}
		if err := writeOSDPin(b, 2, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := readOSDPins(specsFor(a, b)); err == nil {
			t.Fatal("duplicate id pins must be an error")
		}
	})
}

func specsFor(dirs ...string) []dataDirSpec {
	out := make([]dataDirSpec, 0, len(dirs))
	for _, d := range dirs {
		out = append(out, dataDirSpec{dir: d})
	}
	return out
}

func TestDeviceMapRoundtrip(t *testing.T) {
	dir := t.TempDir()
	m, err := readDeviceMap(dir)
	if err != nil || m != nil {
		t.Fatalf("missing map: %v, %v (want nil, nil)", m, err)
	}
	if err := writeDeviceMap(dir, map[string]string{"/d1": "filesystem=a", "/d2": "serial=b"}); err != nil {
		t.Fatal(err)
	}
	m, err = readDeviceMap(dir)
	if err != nil {
		t.Fatal(err)
	}
	if m["/d1"] != "filesystem=a" || m["/d2"] != "serial=b" {
		t.Fatalf("map = %v", m)
	}
	if _, err := readDeviceMap(""); err != nil {
		t.Fatalf("empty meta dir: %v", err)
	}
}

func TestAssignDataDirIDs(t *testing.T) {
	// plan builds a dirPlan over a real temp dir; ident is the fake
	// live identity (empty = no stable identity, the non-Linux case).
	plan := func(t *testing.T, dir, ident string, id int32) *dirPlan {
		t.Helper()
		p := &dirPlan{spec: dataDirSpec{dir: dir}, live: store.DeviceIdentity{Path: dir}}
		if ident != "" {
			p.live.Filesystem = strings.TrimPrefix(ident, "filesystem=")
		}
		if id != 0 {
			if err := writeOSDPin(dir, id, ""); err != nil {
				t.Fatal(err)
			}
			p.pin.id = id
		}
		return p
	}
	quiet := func(string, ...any) {}

	t.Run("fresh dirs get sequential ids in listed order", func(t *testing.T) {
		a, b, c := t.TempDir(), t.TempDir(), t.TempDir()
		plans := []*dirPlan{plan(t, a, "filesystem=A", 0), plan(t, b, "filesystem=B", 0), plan(t, c, "filesystem=C", 0)}
		used := map[int32]bool{2: true}
		got, err := assignDataDirIDs(plans, used, quiet)
		if err != nil {
			t.Fatal(err)
		}
		want := []bootOSD{{1, a}, {3, b}, {4, c}}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("pinned dirs keep their ids", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		plans := []*dirPlan{plan(t, a, "filesystem=A", 7), plan(t, b, "filesystem=B", 0)}
		got, err := assignDataDirIDs(plans, map[int32]bool{}, quiet)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].id != 7 || got[1].id != 1 {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("two fresh dirs on one device are fine", func(t *testing.T) {
		a, b := t.TempDir(), t.TempDir()
		plans := []*dirPlan{plan(t, a, "filesystem=A", 0), plan(t, b, "filesystem=A", 0)}
		var warned []string
		got, err := assignDataDirIDs(plans, map[int32]bool{}, func(f string, v ...any) {
			warned = append(warned, fmt.Sprintf(f, v...))
		})
		if err != nil {
			t.Fatal(err)
		}
		if got[0].id == got[1].id {
			t.Fatalf("two daemons cannot share an id: %v", got)
		}
		if len(warned) == 0 {
			t.Fatal("sharing a device must be explained in the log")
		}
	})
	t.Run("2+2 on two devices is refused", func(t *testing.T) {
		a, b, c, d := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
		plans := []*dirPlan{
			plan(t, a, "filesystem=A", 0), plan(t, b, "filesystem=A", 0),
			plan(t, c, "filesystem=B", 0), plan(t, d, "filesystem=B", 0),
		}
		_, err := assignDataDirIDs(plans, map[int32]bool{}, quiet)
		if err == nil {
			t.Fatal("2 dirs on device A + 2 on device B with no pins must be refused")
		}
		for _, want := range []string{"filesystem=A", "filesystem=B", "uuid="} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error must mention %q: %v", want, err)
			}
		}
	})
	t.Run("pinned dirs do not feed the ambiguity", func(t *testing.T) {
		// One fresh dir per device plus pinned residents: the pins
		// already say where everything belongs.
		a, b, c, d := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
		plans := []*dirPlan{
			plan(t, a, "filesystem=A", 1), plan(t, b, "filesystem=A", 0),
			plan(t, c, "filesystem=B", 2), plan(t, d, "filesystem=B", 0),
		}
		got, err := assignDataDirIDs(plans, map[int32]bool{}, quiet)
		if err != nil {
			t.Fatal(err)
		}
		if got[1].id != 3 || got[3].id != 4 {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("unknown devices behave like the old auto-assign", func(t *testing.T) {
		// Off Linux (or on filesystems with no readable UUID) every
		// dir is identity-less: no ambiguity can be proven.
		a, b, c, d := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
		plans := []*dirPlan{plan(t, a, "", 0), plan(t, b, "", 0), plan(t, c, "", 0), plan(t, d, "", 0)}
		got, err := assignDataDirIDs(plans, map[int32]bool{}, quiet)
		if err != nil {
			t.Fatal(err)
		}
		if got[0].id != 1 || got[3].id != 4 {
			t.Fatalf("got %v", got)
		}
	})
}

func TestCheckDataDirSwaps(t *testing.T) {
	type spec struct {
		dir     string
		want    string // uuid= pin
		pinID   int32
		pinDev  string // <dir>/device
		nodeDev string // devices.json
		liveFS  string // live filesystem uuid
		metaDev string // registered OSDInfo.Device for pinID
		wantErr bool
		Warned  bool
	}
	run := func(t *testing.T, s spec) {
		t.Helper()
		dir := t.TempDir()
		if s.pinID != 0 {
			if err := writeOSDPin(dir, s.pinID, s.pinDev); err != nil {
				t.Fatal(err)
			}
		}
		p := &dirPlan{
			spec:    dataDirSpec{dir: dir, wantDev: s.want},
			pin:     osdPin{id: s.pinID, device: s.pinDev},
			nodeDev: s.nodeDev,
			live:    store.DeviceIdentity{Path: dir, Device: "/dev/sda1", Filesystem: s.liveFS},
		}
		registered := map[int32]meta.OSDInfo{}
		if s.metaDev != "" {
			registered[s.pinID] = meta.OSDInfo{ID: s.pinID, Device: s.metaDev}
		}
		warned := false
		err := checkDataDirSwaps([]*dirPlan{p}, registered, func(string, ...any) { warned = true })
		if s.wantErr && err == nil {
			t.Fatal("expected error")
		}
		if !s.wantErr && err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.Warned && !warned {
			t.Fatal("expected a warning")
		}
	}
	const A, B = "aaaa", "bbbb"

	t.Run("no pins: fresh dir boots clean", func(t *testing.T) {
		run(t, spec{liveFS: A})
	})
	t.Run("matching pins boot clean", func(t *testing.T) {
		run(t, spec{pinID: 1, pinDev: "filesystem=" + A, liveFS: A, metaDev: "filesystem=" + A})
	})
	t.Run("legacy raw uuid pin is understood", func(t *testing.T) {
		run(t, spec{pinID: 1, pinDev: A, liveFS: A})
	})
	t.Run("uuid spec mismatch is fatal", func(t *testing.T) {
		run(t, spec{want: "filesystem=" + B, liveFS: A, wantErr: true})
	})
	t.Run("dir pin mismatch with no meta cover is fatal", func(t *testing.T) {
		// The classic swap: the disk under /d1 is now the one /d2 was
		// on. Nothing vouches for the move.
		run(t, spec{pinID: 1, pinDev: "filesystem=" + B, liveFS: A, wantErr: true})
	})
	t.Run("node map pin mismatch is fatal", func(t *testing.T) {
		run(t, spec{pinID: 1, nodeDev: "filesystem=" + B, liveFS: A, wantErr: true})
	})
	t.Run("directory moved on purpose: pin matches live, meta is stale", func(t *testing.T) {
		// Operator moved the dir to a new disk: <dir>/device (traveling
		// with the data) and the live device agree; only meta lags.
		run(t, spec{pinID: 1, pinDev: "filesystem=" + A, liveFS: A, metaDev: "filesystem=" + B, Warned: true})
	})
	t.Run("meta knows a foreign device and the dir cannot vouch", func(t *testing.T) {
		// Legacy dir (no device pin): node map matches live (the disk
		// is where this node last saw it), but the cluster says the id
		// belongs to another disk. Fatal, with the uuid= remedy.
		run(t, spec{pinID: 1, nodeDev: "filesystem=" + A, liveFS: A, metaDev: "filesystem=" + B, wantErr: true})
	})
	t.Run("meta cross-check passes when devices agree", func(t *testing.T) {
		run(t, spec{pinID: 1, pinDev: "filesystem=" + A, liveFS: A, metaDev: "filesystem=" + A})
	})
	t.Run("no live identity and no pins: inert", func(t *testing.T) {
		run(t, spec{pinID: 1})
	})
	t.Run("no live identity with a pin: cannot verify is fatal", func(t *testing.T) {
		// A pin we cannot check is not a licence to serve data.
		run(t, spec{pinID: 1, pinDev: "filesystem=" + A, wantErr: true})
	})
	t.Run("uuid= pin checked before any legacy pin", func(t *testing.T) {
		run(t, spec{want: "filesystem=" + A, pinID: 1, pinDev: "filesystem=" + B, liveFS: A, wantErr: true})
	})
}
