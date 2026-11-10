package crushmap

import (
	"strings"
	"testing"
	"time"
)

// A truncated or garbage crush map must produce an error quickly — it must
// never hang the lexer at EOF and never panic.
func TestDecodeTextMalformedInput(t *testing.T) {
	cases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"garbage", "not a crush map at all"},
		{"truncated after types", "# begin crush map\ntypes\n0 osd\n1 host"},
		{"truncated bucket header", "root r"},
		{"truncated bucket open", "root r {"},
		{"truncated bucket id", "root r { id"},
		{"truncated bucket id class", "root r { id -2 class"},
		{"truncated bucket alg", "root r { id -2 alg"},
		{"truncated bucket item", "root r { id -2 alg straw2 hash 0 item"},
		{"truncated bucket item ident", "root r { id -2 alg straw2 hash 0 item host"},
		{"unterminated bucket", "root r { id -2 alg straw2 hash 0 item host h1 weight 1.00000"},
		{"truncated device", "device"},
		{"truncated device name", "device 0"},
		{"truncated device class", "device 0 osd.0 class"},
		{"truncated tunable", "tunable require_all"},
		{"truncated tunable val", "tunable require_all_index"},
		{"truncated type", "type"},
		{"truncated type name", "type 7"},
		{"comment only no newline", "# just a comment"},
		{"truncated rule", "rule data"},
		{"truncated rule id", "rule data { id"},
		{"truncated rule ident", "rule data { id 0 min_size 1 max_size 3 type rep"},
		{"truncated rule step", "rule data { id 0 step take r step chooseleaf"},
		{"truncated rule choose", "rule data { id 0 step chooseleaf firstn 0"},
		{"truncated rule take", "rule data { id 0 step take"},
		{"unterminated rule", "rule data { id 0 type replicated step take r step emit"},
		{"truncated failover domain", "failure_domain"},
		{"truncated chooseargs", "chooseargs"},
		{"step chooseleaf firstn 0 type host step emit no rule", "step chooseleaf firstn 0 type host step emit"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan struct{})
			var err error
			go func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("DecodeText(%q) panicked: %v", tc.input, r)
					}
					close(done)
				}()
				m := NewMap()
				err = m.DecodeText([]byte(tc.input))
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("DecodeText(%q) hung", tc.input)
			}
			if err == nil {
				t.Errorf("DecodeText(%q): expected error, got nil", tc.input)
			}
		})
	}
}

// Comment as the last line without trailing newline must not hang.
func TestDecodeTextCommentAtEOF(t *testing.T) {
	m0 := NewMap()
	err := m0.DecodeText([]byte("device 0 osd.0\ndevice 1 osd.1\n" +
		"root root { id -1 alg straw2 hash 0 }\n# trailing comment"))
	if err != nil {
		t.Fatalf("DecodeText with trailing comment: %v", err)
	}
}

// Device lines terminated at EOF (no trailing newline) must parse.
func TestDecodeTextNoTrailingNewline(t *testing.T) {
	m := NewMap()
	err := m.DecodeText([]byte("# begin crush map\ntunable require_max_osd 32\n" +
		"# types\ntype 0 osd\ntype 1 host\n" +
		"# devices\ndevice 0 osd.0\ndevice 1 osd.1\n" +
		"# buckets\nroot root { id -1 alg straw2 hash 0 }"))
	if err != nil {
		t.Fatalf("DecodeText: %v", err)
	}
	if len(m.Devices) != 2 {
		t.Fatalf("devices = %d, want 2", len(m.Devices))
	}
}

// Very long unterminated token must not exhaust memory via runeStack growth
// without bound at EOF (guard in lexNext).
func TestDecodeTextLongGarbageToken(t *testing.T) {
	input := "bucket " + strings.Repeat("x", 1<<20)
	done := make(chan struct{})
	go func() {
		defer close(done)
		m := NewMap()
		if err := m.DecodeText([]byte(input)); err == nil {
			t.Error("expected error for garbage bucket")
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("DecodeText with long garbage token hung")
	}
}

// Ceph-exported maps sometimes pack a whole bucket or rule onto one line
// (`root r { id -2 alg straw2 hash 0 }`) and give nested items an explicit
// id. Both forms must decode with the values in the right fields.
func TestDecodeTextOneLineMap(t *testing.T) {
	src := "type 0 osd\n" +
		"type 1 host\n" +
		"type 2 root\n" +
		"device 0 osd.0\n" +
		"device 1 osd.1\n" +
		"host h1 { id -1 alg straw2 hash 0 item osd.0 id 0 weight 1.000 item osd.1 id 1 weight 1.000 }\n" +
		"root r { id -2 alg straw2 hash 0 item h1 id -1 weight 2.000 }\n" +
		"rule data { id 0 type replicated min_size 1 max_size 10 step take r step chooseleaf firstn 0 type host step emit }\n"
	m := NewMap()
	if err := m.DecodeText([]byte(src)); err != nil {
		t.Fatalf("DecodeText: %v", err)
	}
	if len(m.Buckets) != 2 {
		t.Fatalf("buckets = %d, want 2", len(m.Buckets))
	}
	root, host := m.Buckets[0], m.Buckets[1] // sorted by TypeID descending
	if host.Name != "h1" || host.ID != -1 || host.Alg != "straw2" || host.Hash != "rjenkins1" {
		t.Fatalf("host bucket = %+v", host)
	}
	if len(host.Items) != 2 {
		t.Fatalf("host items = %d, want 2", len(host.Items))
	}
	if host.Items[0].Name != "osd.0" || host.Items[0].ID != 0 || host.Items[0].Weight != 1.0 {
		t.Fatalf("item0 = %+v", host.Items[0])
	}
	if root.Name != "r" || root.ID != -2 || root.Alg != "straw2" {
		t.Fatalf("root bucket = %+v", root)
	}
	if len(root.Items) != 1 || root.Items[0].Name != "h1" || root.Items[0].ID != -1 || root.Items[0].Weight != 2.0 {
		t.Fatalf("root items = %+v", root.Items)
	}
	if len(m.Rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(m.Rules))
	}
	r := m.Rules[0]
	if r.Name != "data" || r.ID != 0 || r.Type != ReplicatedPG || r.MinSize != 1 || r.MaxSize != 10 {
		t.Fatalf("rule = %+v", r)
	}
	if len(r.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(r.Steps))
	}
}

// `id <n> class <c>` inside a bucket must keep both the id and the class.
func TestDecodeTextBucketIDClass(t *testing.T) {
	src := "type 0 osd\ntype 2 root\nroot r { id -2 class ssd alg straw2 hash 0 }\n"
	m := NewMap()
	if err := m.DecodeText([]byte(src)); err != nil {
		t.Fatalf("DecodeText: %v", err)
	}
	if len(m.Buckets) != 1 {
		t.Fatalf("buckets = %d, want 1", len(m.Buckets))
	}
	b := m.Buckets[0]
	if b.ID != -2 || b.IDClass != "ssd" || b.Alg != "straw2" {
		t.Fatalf("bucket = %+v", b)
	}
}

func TestDecodeTextPositionalItem(t *testing.T) {
	src := "# begin crush map\n" +
		"device 0 osd.0\ndevice 1 osd.1\ndevice 2 osd.2\ndevice 3 osd.3\n" +
		"type 0 osd\ntype 1 host\ntype 2 root\n" +
		"max_buckets 4\nmax_rules 1\n" +
		"rule data {\n\tid 0\n\ttype replicated\n\tmin_size 1\n\tmax_size 10\n" +
		"\tstep take r\n\tstep chooseleaf firstn 0 type host\n\tstep emit\n}\n" +
		"host h1 {\n\tid -3\n\talg straw2\n\thash 0\n" +
		"\titem osd.0 weight 1.000\n\titem osd.1 weight 1.000\n}\n" +
		"host h2 {\n\tid -4\n\talg straw2\n\thash 0\n" +
		"\titem osd.2 weight 1.000\n\titem osd.3 weight 1.000\n}\n" +
		"root r {\n\tid -2\n\talg straw2\n\thash 0\n" +
		"\titem host h1\n\titem host h2\n}\n"
	m := NewMap()
	if err := m.DecodeText([]byte(src)); err != nil {
		t.Fatalf("DecodeText: %v", err)
	}
	var root *Bucket
	for _, b := range m.Buckets {
		if b.ID == -2 {
			root = b
		}
	}
	if root == nil {
		t.Fatal("root bucket not found")
	}
	if len(root.Items) != 2 {
		t.Fatalf("root has %d items, want 2", len(root.Items))
	}
	ids := map[int32]bool{}
	for _, it := range root.Items {
		ids[it.ID] = true
		if it.Name == "host" {
			t.Fatalf("item resolved to type name 'host' instead of bucket: %+v", it)
		}
	}
	if !ids[-3] || !ids[-4] {
		t.Fatalf("root item ids = %v, want -3 and -4", ids)
	}
}
