package main

import "testing"

func TestParseCreateSpec(t *testing.T) {
	for _, tc := range []struct {
		in, pool, name, size string
		bad                  bool
	}{
		{in: "vol0:10G", name: "vol0", size: "10G"},
		{in: "fast/vm0:128G", pool: "fast", name: "vm0", size: "128G"},
		{in: "vol0", bad: true},
		{in: "fast/vol0", bad: true},
		{in: "fast/vol0:", bad: true},
		{in: "fast/:10G", bad: true},
	} {
		pool, name, size, err := parseCreateSpec(tc.in)
		if tc.bad {
			if err == nil {
				t.Errorf("parseCreateSpec(%q): want error, got %q/%q/%q", tc.in, pool, name, size)
			}
			continue
		}
		if err != nil || pool != tc.pool || name != tc.name || size != tc.size {
			t.Errorf("parseCreateSpec(%q) = %q/%q/%q, %v; want %q/%q/%q", tc.in, pool, name, size, err, tc.pool, tc.name, tc.size)
		}
	}
}
