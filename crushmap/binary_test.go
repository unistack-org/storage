package crushmap

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/unistack-org/crushkv/crush"
)

func TestBinary(t *testing.T) {
	buf, err := os.ReadFile("testdata/map.bin")
	if err != nil {
		t.Fatal(err)
	}

	m := NewMap()
	if err = m.DecodeBinary(buf); err != nil {
		t.Fatal(err)
	}
	if len(m.Buckets) != 12 {
		t.Errorf("buckets = %d, want 12", len(m.Buckets))
	}
	if len(m.Rules) != 3 {
		t.Errorf("rules = %d, want 3", len(m.Rules))
	}
	if len(m.Devices) != 6 {
		t.Errorf("devices = %d, want 6", len(m.Devices))
	}
	if got := m.Tunables["profile"]; got != "hammer" {
		t.Errorf("tunables profile = %v, want hammer", got)
	}
	// Encode must be byte-stable (canonical form).
	buf2, err := m.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}
	if string(buf) != string(buf2) {
		t.Errorf("re-encode is not byte-identical (%d vs %d bytes)", len(buf), len(buf2))
	}
}

// TestBinaryGolden decodes testdata/map.txt, encodes it to the Ceph
// CrushWrapper binary format, decodes that back, compiles the result and
// checks placements against golden vectors produced by the verbatim Ceph
// C core (same vectors as crush/golden_test.go).
func TestBinaryGolden(t *testing.T) {
	data, err := os.ReadFile("testdata/map.txt")
	if err != nil {
		t.Fatal(err)
	}
	text := NewMap()
	if err := text.DecodeText(data); err != nil {
		t.Fatal(err)
	}
	bin, err := text.EncodeBinary()
	if err != nil {
		t.Fatal(err)
	}

	m := NewMap()
	if err := m.DecodeBinary(bin); err != nil {
		t.Fatal(err)
	}
	cm, err := m.Compile()
	if err != nil {
		t.Fatal(err)
	}

	full := make([]uint32, 6)
	for i := range full {
		full[i] = 0x10000
	}
	out0 := make([]uint32, 6)
	copy(out0, full)
	out0[0] = 0

	work := crush.NewWork(cm, 10, nil)
	res := make([]int32, 3)

	f, err := os.Open("../crush/testdata/golden_maptxt.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	checked := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		prefix := binaryFieldPrefix(fields[0])
		ruleno, _ := strconv.Atoi(fields[0][len(prefix):])
		x, _ := strconv.Atoi(fields[1])
		n, _ := strconv.Atoi(fields[2])
		want := fields[3:]

		weights := full
		if prefix == "W" {
			weights = out0
		}
		got := cm.DoRule(work, int32(ruleno), int32(x), res, weights)
		if got != n {
			t.Fatalf("%s rule %d x %d: n=%d, want %d (%s)", prefix, ruleno, x, got, n, line)
		}
		for i := 0; i < n; i++ {
			w, _ := strconv.Atoi(want[i])
			if int(res[i]) != w {
				t.Fatalf("%s rule %d x %d: result=%v, want %v", prefix, ruleno, x, res[:n], want)
			}
		}
		checked++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if checked == 0 {
		t.Fatal("no golden vectors checked")
	}
	t.Logf("checked %d golden vectors through binary round-trip", checked)
}

func binaryFieldPrefix(s string) string {
	i := 0
	for i < len(s) && !(s[i] >= '0' && s[i] <= '9') {
		i++
	}
	return s[:i]
}
