// Package placement turns a CRUSH map plus an OSD address registry into
// an immutable, epoch-versioned placement snapshot: the pg -> OSD table
// clients use on the I/O path.
//
// Design (mirrors Ceph, not Vitastor): CRUSH runs once per PG per map
// epoch, never per I/O. Compile/Rebuild pay the DoRule cost for every PG
// (a 4096-PG rep3 table rebuilds in ~20 ms for a 225-OSD map); the hot
// path is a fibonacci hash of the object id plus one array index, zero
// allocations, ~1 ns. Snapshots are immutable and swapped atomically on
// epoch change, so readers never lock.
package placement

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"github.com/unistack-org/storage/crush"
	"github.com/unistack-org/storage/crushmap"
)

// ErrNoTarget means a PG has fewer live replicas than requested
// (devices with weight 0 / missing from the registry are skipped).
var ErrNoTarget = errors.New("placement: not enough targets for pg")

// Ref is one placement target: where an OSD lives.
type Ref struct {
	OSD    int32
	Addr   string // host:port of the OSD daemon
	Export string // export name the daemon serves the object range under
}

// Registry maps OSD ids (CRUSH device ids) to their locations. Devices
// absent from the registry, or with weight 0 in the map, are treated as
// out: they never appear in pg tables.
type Registry map[int32]Ref

// Options controls the pg table built for a snapshot.
type Options struct {
	// PGNum is the number of placement groups; power of two (the pg
	// selector is then a mask). Required.
	PGNum uint32
	// Rule selects the crush rule by name. "" uses the lowest-id rule.
	Rule string
	// Replicas is the row width of the pg table (the rule's max_size
	// bounds what DoRule can return; this bounds what we store).
	// Required.
	Replicas int
}

// Snapshot is one immutable cluster-map epoch: compiled crush map,
// device weights, registry, and the pg -> OSD table. Safe for
// concurrent readers.
type Snapshot struct {
	Epoch    uint32
	PGNum    uint32 // power of two
	Replicas int
	Rule     string // rule name (for display)

	ruleNo  int32
	cm      *crush.Map
	weights []uint32
	refs    Registry
	pg      []int32 // PGNum * Replicas device ids; -1 = empty slot
	mask    uint32
}

// Compile decodes a CRUSH map (text, binary, or JSON — auto-detected),
// compiles it, and builds the pg table at the given epoch.
func Compile(epoch uint32, src []byte, reg Registry, opts Options) (*Snapshot, error) {
	if opts.PGNum == 0 || bits.OnesCount32(opts.PGNum) != 1 {
		return nil, fmt.Errorf("placement: PGNum must be a power of two, got %d", opts.PGNum)
	}
	if opts.Replicas <= 0 || opts.Replicas > 32 {
		return nil, fmt.Errorf("placement: Replicas must be in 1..32, got %d", opts.Replicas)
	}
	if len(reg) == 0 {
		return nil, errors.New("placement: empty OSD registry")
	}
	m, err := decodeAny(src)
	if err != nil {
		return nil, err
	}
	cm, err := m.Compile()
	if err != nil {
		return nil, fmt.Errorf("placement: compile: %w", err)
	}
	ruleNo, ruleName, err := findRule(m, opts.Rule)
	if err != nil {
		return nil, err
	}
	s := &Snapshot{
		Epoch:    epoch,
		PGNum:    opts.PGNum,
		Replicas: opts.Replicas,
		Rule:     ruleName,
		ruleNo:   ruleNo,
		cm:       cm,
		weights:  cm.Weight,
		refs:     reg,
		mask:     opts.PGNum - 1,
	}
	if err := s.buildTable(); err != nil {
		return nil, err
	}
	return s, nil
}

// DecodeMap decodes a crush map in any supported encoding: JSON
// (leading '{'), Ceph binary (leading CRUSH magic), otherwise text.
// It is the decoder used by Compile, exposed so tools (map dump) can
// re-encode a stored map between formats.
func DecodeMap(src []byte) (*crushmap.Map, error) {
	return decodeAny(src)
}

// FormatMap re-encodes a stored map into "text", "json" (indented)
// or "binary" for `unistorctl map dump -format`. A text source shown
// as text passes through untouched.
func FormatMap(src []byte, format string) ([]byte, error) {
	switch format {
	case "text":
		t := bytes.TrimSpace(src)
		isBin := len(src) >= 4 && binary.LittleEndian.Uint32(src[:4]) == crushmap.Magic
		if !bytes.HasPrefix(t, []byte("{")) && !isBin {
			return src, nil // already text: show it verbatim
		}
		m, err := DecodeMap(src)
		if err != nil {
			return nil, err
		}
		return EncodeMapText(m)
	case "json":
		m, err := DecodeMap(src)
		if err != nil {
			return nil, err
		}
		return json.MarshalIndent(m, "", "  ")
	case "binary":
		m, err := DecodeMap(src)
		if err != nil {
			return nil, err
		}
		return m.EncodeBinary()
	default:
		return nil, fmt.Errorf("placement: unknown format %q (use text, json or binary)", format)
	}
}

// EncodeMapText renders a map in the crushkv text grammar. crushkv
// ships no text encoder, so this is ours; the result re-parses
// (DecodeMap -> Compile) into an equivalent map, and the placement
// unit tests round-trip it.
func EncodeMapText(m *crushmap.Map) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("placement: nil map")
	}
	// Bucket items reference devices by name; decode drops names, so
	// rebuild them from the device table.
	devName := make(map[int32]string, len(m.Devices))
	for _, d := range m.Devices {
		devName[d.ID] = d.Name
	}
	typeName := make(map[crushmap.CrushBucketType]string, len(m.Types))
	for _, t := range m.Types {
		typeName[crushmap.CrushBucketType(t.ID)] = t.Name
	}
	var b strings.Builder
	keys := make([]string, 0, len(m.Tunables))
	for k := range m.Tunables {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "tunable %s %v\n", k, m.Tunables[k])
	}
	for _, d := range m.Devices {
		cls := d.Class
		if cls == "" {
			cls = "general" // the parser's default class
		}
		fmt.Fprintf(&b, "device %d %s class %s\n", d.ID, d.Name, cls)
	}
	for _, t := range m.Types {
		fmt.Fprintf(&b, "type %d %s\n", t.ID, t.Name)
	}
	for _, r := range m.Rules {
		encodeRuleText(&b, r)
	}
	for _, bk := range m.Buckets {
		typ := bk.TypeName
		if typ == "" {
			typ = typeName[bk.TypeID]
		}
		if typ == "" {
			return nil, fmt.Errorf("placement: bucket %q has no type", bk.Name)
		}
		fmt.Fprintf(&b, "%s %s {\n", typ, bk.Name)
		fmt.Fprintf(&b, "\tid %d\n", bk.ID)
		if bk.IDClass != "" {
			fmt.Fprintf(&b, "\tclass %s\n", bk.IDClass)
		}
		if bk.Alg != "" {
			fmt.Fprintf(&b, "\talg %s\n", bk.Alg)
		}
		if bk.Hash != "" {
			h := bk.Hash
			if h == "rjenkins1" {
				h = "0" // the parser only accepts `hash 0` -> rjenkins1
			}
			fmt.Fprintf(&b, "\thash %s\n", h)
		}
		for _, it := range bk.Items {
			name := it.Name
			if name == "" {
				name = devName[it.ID]
			}
			if name == "" {
				return nil, fmt.Errorf("placement: bucket %q item id %d has no name", bk.Name, it.ID)
			}
			fmt.Fprintf(&b, "\titem %s weight %.2f pos %d\n", name, it.Weight, it.Pos)
		}
		b.WriteString("}\n")
	}
	return []byte(b.String()), nil
}

func encodeRuleText(b *strings.Builder, r *crushmap.Rule) {
	fmt.Fprintf(b, "rule %s {\n", r.Name)
	fmt.Fprintf(b, "\tid %d\n\truleset %d\n", r.ID, r.Ruleset)
	typ := "replicated"
	switch r.Type {
	case crushmap.ErasurePG:
		typ = "erasure"
	case 4:
		typ = "msr_firstn"
	case 5:
		typ = "msr_indep"
	}
	fmt.Fprintf(b, "\ttype %s\n\tmin_size %d\n\tmax_size %d\n", typ, r.MinSize, r.MaxSize)
	for _, st := range r.Steps {
		switch {
		case st.Op == "take":
			if st.ItemClass != "" {
				fmt.Fprintf(b, "\tstep take %s class %s\n", st.ItemName, st.ItemClass)
			} else {
				fmt.Fprintf(b, "\tstep take %s\n", st.ItemName)
			}
		case strings.HasPrefix(st.Op, "chooseleaf_"):
			fmt.Fprintf(b, "\tstep chooseleaf %s %d", strings.TrimPrefix(st.Op, "chooseleaf_"), st.Num)
			if st.ItemType != "" {
				fmt.Fprintf(b, " type %s", st.ItemType)
			}
			b.WriteString("\n")
		case strings.HasPrefix(st.Op, "choose_"):
			fmt.Fprintf(b, "\tstep choose %s %d", strings.TrimPrefix(st.Op, "choose_"), st.Num)
			if st.ItemType != "" {
				fmt.Fprintf(b, " type %s", st.ItemType)
			}
			b.WriteString("\n")
		case strings.HasPrefix(st.Op, "choosemsr_"):
			// Text syntax puts the count first: `choosemsr N [type T]`.
			fmt.Fprintf(b, "\tstep choosemsr %d", st.Num)
			if st.ItemType != "" {
				fmt.Fprintf(b, " type %s", st.ItemType)
			}
			b.WriteString("\n")
		case st.Op == "emit":
			b.WriteString("\tstep emit\n")
		default: // set_* and anything else: value rides in Num
			fmt.Fprintf(b, "\tstep %s %d\n", st.Op, st.Num)
		}
	}
	b.WriteString("}\n")
}

// decodeAny auto-detects the encoding: JSON (leading '{'), Ceph binary
// map (leading CRUSH magic), otherwise the text format.
func decodeAny(src []byte) (m *crushmap.Map, err error) {
	// The text lexer panics on malformed input when no errHandler is
	// installed; surface it as an error instead of crashing callers.
	defer func() {
		if r := recover(); r != nil {
			m, err = nil, fmt.Errorf("placement: decode crush map: %v", r)
		}
	}()
	m = crushmap.NewMap()
	trimmed := strings.TrimLeftFunc(string(src), func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '#'
	})
	if strings.HasPrefix(trimmed, "{") {
		if err := m.DecodeJson(src); err != nil {
			return nil, fmt.Errorf("placement: decode json: %w", err)
		}
		return m, nil
	}
	if len(src) >= 4 && binary.LittleEndian.Uint32(src[:4]) == crushmap.Magic {
		if err := m.DecodeBinary(src); err != nil {
			return nil, fmt.Errorf("placement: decode binary: %w", err)
		}
		return m, nil
	}
	if err := m.DecodeText(src); err != nil {
		return nil, fmt.Errorf("placement: decode text: %w", err)
	}
	return m, nil
}

// findRule resolves a rule name to its index in the compiled map.
func findRule(m *crushmap.Map, name string) (int32, string, error) {
	if len(m.Rules) == 0 {
		return 0, "", errors.New("placement: crush map has no rules")
	}
	if name == "" {
		r := m.Rules[0] // decode sorts rules by id
		return int32(r.ID), r.Name, nil
	}
	for _, r := range m.Rules {
		if r.Name == name {
			return int32(r.ID), r.Name, nil
		}
	}
	return 0, "", fmt.Errorf("placement: rule %q not found", name)
}

// PG maps an object id to its placement group. This is the only hashing
// on the I/O path: one multiply and a mask.
func (s *Snapshot) PG(oid uint64) uint32 {
	return uint32((oid*11400714819323198485)>>32) & s.mask
}

// Lookup appends the live targets of oid's pg to dst (in CRUSH
// preference order) and returns the result. Zero allocations when dst
// has enough capacity.
func (s *Snapshot) Lookup(oid uint64, dst []Ref) []Ref {
	pg := s.PG(oid)
	dst = dst[:0]
	row := s.pg[pg*uint32(s.Replicas) : (pg+1)*uint32(s.Replicas)]
	for _, id := range row {
		if id < 0 {
			continue
		}
		ref, ok := s.refs[id]
		if !ok {
			continue
		}
		dst = append(dst, ref)
	}
	return dst
}

// Targets returns the targets of oid (allocates; prefer Lookup). The
// error reports a PG whose live replica count is below Replicas.
func (s *Snapshot) Targets(oid uint64) ([]Ref, error) {
	refs := s.Lookup(oid, make([]Ref, 0, s.Replicas))
	if len(refs) < s.Replicas {
		return refs, fmt.Errorf("%w: pg %d has %d/%d live replicas", ErrNoTarget, s.PG(oid), len(refs), s.Replicas)
	}
	return refs, nil
}

// Row returns the raw device ids of a pg (inspection/testing helper).
func (s *Snapshot) Row(pg uint32) []int32 {
	return s.pg[pg*uint32(s.Replicas) : (pg+1)*uint32(s.Replicas)]
}

// LookupRef resolves a device id to its location.
func (s *Snapshot) LookupRef(id int32) (Ref, bool) {
	ref, ok := s.refs[id]
	return ref, ok
}

// Refs returns the device registry backing this snapshot.
func (s *Snapshot) Refs() Registry { return s.refs }

// buildTable runs the rule for every pg and stores the device rows.
func (s *Snapshot) buildTable() error {
	w := crush.NewWork(s.cm, s.Replicas, nil)
	defer w.Free()
	res := make([]int32, s.Replicas)
	s.pg = make([]int32, s.PGNum*uint32(s.Replicas))
	// Devices absent from the registry are out: zero weight makes every
	// crush algorithm reject them (mapper.c treats weight 0 as out, and
	// chooseleaf re-picks the whole host when its only device is out).
	weights := s.weights
	out := false
	for id := range s.weights {
		if _, ok := s.refs[int32(id)]; !ok {
			out = true
			break
		}
	}
	if out {
		weights = make([]uint32, len(s.weights))
		copy(weights, s.weights)
		for j := range weights {
			if _, ok := s.refs[int32(j)]; !ok {
				weights[j] = 0
			}
		}
	}
	for pg := uint32(0); pg < s.PGNum; pg++ {
		n := s.cm.DoRule(w, s.ruleNo, int32(pg), res, weights)
		row := s.pg[pg*uint32(s.Replicas) : (pg+1)*uint32(s.Replicas)]
		for i := range row {
			if i < n {
				row[i] = res[i]
			} else {
				row[i] = -1
			}
		}
	}
	return nil
}

// Reweight returns a new snapshot at the given epoch with device weights
// replaced (CRUSH reweight: set an OSD's weight to 0 to drain it). The
// crush map is not recompiled; only the pg table is rebuilt, so this is
// the cheap membership-change path. Weights are in CRUSH units (1.0 =
// one full device).
func (s *Snapshot) Reweight(epoch uint32, weights map[int32]float64) (*Snapshot, error) {
	w := make([]uint32, len(s.weights))
	copy(w, s.weights)
	for id, fw := range weights {
		if id < 0 || int(id) >= len(w) {
			return nil, fmt.Errorf("placement: reweight: no device %d", id)
		}
		if fw < 0 || fw > 65535 {
			return nil, fmt.Errorf("placement: reweight: weight %v out of range for osd %d", fw, id)
		}
		w[id] = uint32(float64(fw) * 65536.0)
	}
	ns := &Snapshot{
		Epoch: epoch, PGNum: s.PGNum, Replicas: s.Replicas, Rule: s.Rule,
		ruleNo: s.ruleNo, cm: s.cm, weights: w, refs: s.refs, mask: s.mask,
	}
	if err := ns.buildTable(); err != nil {
		return nil, err
	}
	return ns, nil
}

// WithRegistry returns a snapshot sharing this one's compiled map and
// weights but using reg for target resolution, with the pg table
// rebuilt (devices missing from reg count as out).
func (s *Snapshot) WithRegistry(epoch uint32, reg Registry) (*Snapshot, error) {
	ns := &Snapshot{
		Epoch: epoch, PGNum: s.PGNum, Replicas: s.Replicas, Rule: s.Rule,
		ruleNo: s.ruleNo, cm: s.cm, weights: s.weights, refs: reg, mask: s.mask,
	}
	if err := ns.buildTable(); err != nil {
		return nil, err
	}
	return ns, nil
}

// LiveOSDs lists osd ids present in the current pg table (sorted).
func (s *Snapshot) LiveOSDs() []int32 {
	seen := make(map[int32]bool)
	for _, id := range s.pg {
		if id >= 0 {
			seen[id] = true
		}
	}
	out := make([]int32, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
