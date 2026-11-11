package main

import "testing"

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
		{spec: "0", bad: true},
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
