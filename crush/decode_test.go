package crush

import (
	"os"
	"testing"
)

// TestDecodeSniffsFormats feeds the same map in all three encodings to
// Decode and requires the same placements out of it: Decode is the single
// format-detection point for the package.
func TestDecodeSniffsFormats(t *testing.T) {
	src := readFile(t, "testdata/map.txt")
	text := NewWrapper()
	if err := text.DecodeText(src); err != nil {
		t.Fatal(err)
	}
	js, err := text.EncodeJson()
	if err != nil {
		t.Fatal(err)
	}
	bin, err := text.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := placements(t, text)

	for _, tc := range []struct {
		name string
		src  []byte
	}{
		{"text", src},
		{"text with leading comment", append([]byte("# c\n"), src...)},
		{"json", js},
		{"json with leading whitespace", append([]byte("  \n"), js...)},
		{"binary", bin},
		{"binary golden file", readFile(t, "testdata/map.bin")},
	} {
		m, err := Decode(tc.src)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if len(m.Buckets) != len(text.Buckets) || len(m.Rules) != len(text.Rules) || len(m.Devices) != len(text.Devices) {
			t.Fatalf("%s: decoded %d buckets/%d rules/%d devices, want %d/%d/%d",
				tc.name, len(m.Buckets), len(m.Rules), len(m.Devices),
				len(text.Buckets), len(text.Rules), len(text.Devices))
		}
		got := placements(t, m)
		if len(got) != len(want) {
			t.Fatalf("%s: %d rules, want %d", tc.name, len(got), len(want))
		}
		for i := range want {
			if !int32sEqual(got[i], want[i]) {
				t.Fatalf("%s: rule %d placements differ", tc.name, i)
			}
		}
	}

	// Garbage must surface as an error, not a panic: the text lexer
	// panics on some malformed input when no errHandler is installed.
	if _, err := Decode([]byte("bucket ")); err == nil {
		t.Fatal("garbage decoded without error")
	}
}

// TestEncodeTextRoundTrip decodes a map, re-encodes it as text, decodes
// that text, and requires identical placements.
func TestEncodeTextRoundTrip(t *testing.T) {
	for _, name := range []string{"testdata/map.txt", "testdata/map2.txt", "testdata/map3.txt"} {
		t.Run(name, func(t *testing.T) {
			m, err := Decode(readFile(t, name))
			if err != nil {
				t.Fatal(err)
			}
			txt, err := m.EncodeText()
			if err != nil {
				t.Fatal(err)
			}
			m2, err := Decode(txt)
			if err != nil {
				t.Fatalf("re-parse encoded text: %v\n%s", err, txt)
			}
			a, b := placements(t, m), placements(t, m2)
			if len(a) != len(b) {
				t.Fatalf("%d rules after round trip, want %d", len(b), len(a))
			}
			for i := range a {
				if !int32sEqual(a[i], b[i]) {
					t.Fatalf("rule %d changed across EncodeText", i)
				}
			}
		})
	}
}

// placements compiles the map and returns, per rule, the results of a
// sweep of mapping inputs concatenated.
func placements(t *testing.T, w *Wrapper) [][]int32 {
	t.Helper()
	cm, err := w.Compile()
	if err != nil {
		t.Fatal(err)
	}
	weights := make([]uint32, len(cm.Weight))
	for i := range weights {
		weights[i] = 0x10000
	}
	work := NewWork(cm, 10, nil)
	out := make([][]int32, len(cm.Rules))
	for i := range cm.Rules {
		res := make([]int32, 10)
		var acc []int32
		for x := int32(0); x < 512; x++ {
			n := cm.DoRule(work, int32(i), x, res, weights)
			if n < 0 {
				n = 0
			}
			acc = append(acc, res[:n]...)
		}
		out[i] = acc
	}
	return out
}

func int32sEqual(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func readFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
