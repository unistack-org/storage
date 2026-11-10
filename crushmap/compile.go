package crushmap

import (
	"fmt"
	"math"
	"strconv"

	"github.com/unistack-org/crushkv/crush"
)

// Compile converts the parsed CRUSH map AST into an executable crush.Map
// whose placement decisions are byte-compatible with Ceph.
//
// Weight conversion mirrors CrushCompiler.cc:
//
//	weight = (u32)(float_node(weight) * (float)0x10000)
//
// i.e. a float32 multiply followed by truncation toward zero, clamped to
// the device / bucket weight maxima.
func (m *Map) Compile() (*crush.Map, error) {
	mb := crush.NewMapBuilder()
	t, err := tunablesFromMap(m.Tunables)
	if err != nil {
		return nil, err
	}
	mb.SetTunables(t)

	// Types.
	for _, ty := range m.Types {
		mb.AddType(ty.Name, ty.ID)
	}

	// Devices and their weight table.
	var maxDevice int32
	for _, d := range m.Devices {
		mb.AddDevice(d.Name, d.ID)
		if d.ID > maxDevice {
			maxDevice = d.ID
		}
	}
	weights := make([]uint32, maxDevice+1)
	for _, d := range m.Devices {
		weights[d.ID] = 0 // devices default to out; weights come from item refs below
	}
	// The AST carries device weights on the bucket items that reference them.
	for _, b := range m.Buckets {
		for _, it := range b.Items {
			if it.ID < 0 {
				continue
			}
			if it.ID > maxDevice {
				return nil, fmt.Errorf("bucket %s references device %d out of range", b.Name, it.ID)
			}
			w, werr := scaleDeviceWeight(it.Weight)
			if werr != nil {
				return nil, fmt.Errorf("device %d: %w", it.ID, werr)
			}
			weights[it.ID] = w
		}
	}

	// Buckets in dependency order: every bucket must be built after the
	// buckets it references, so that computed child weights are available.
	// Type-id ordering is not safe (custom types can number arbitrarily),
	// so use a DFS topological sort with cycle detection.
	byID := make(map[int32]*Bucket, len(m.Buckets))
	for _, b := range m.Buckets {
		byID[b.ID] = b
	}
	buckets := make([]*Bucket, 0, len(m.Buckets))
	state := make(map[int32]int) // 0 unvisited, 1 in progress, 2 done
	var visit func(b *Bucket) error
	visit = func(b *Bucket) error {
		switch state[b.ID] {
		case 1:
			return fmt.Errorf("bucket %s: reference cycle", b.Name)
		case 2:
			return nil
		}
		state[b.ID] = 1
		for _, it := range b.Items {
			if it.ID < 0 {
				cb, ok := byID[it.ID]
				if !ok {
					return fmt.Errorf("bucket %s: unknown item id %d", b.Name, it.ID)
				}
				if err := visit(cb); err != nil {
					return err
				}
			}
		}
		state[b.ID] = 2
		buckets = append(buckets, b)
		return nil
	}
	for _, b := range m.Buckets {
		if err := visit(b); err != nil {
			return nil, err
		}
	}

	// itemWeight resolves the 16.16 weight of an item reference. Device
	// weights come from the table above; bucket weights are the values the
	// builder computed when the child bucket was added (the text format has
	// no bucket weight line, so the AST Weight is unreliable). Buckets are
	// compiled deepest-type-first, so children are always built first.
	computedWeights := make(map[int32]uint32)
	itemWeight := func(id int32) (uint32, error) {
		if id >= 0 {
			if int(id) >= len(weights) {
				return 0, fmt.Errorf("device %d out of range", id)
			}
			return weights[id], nil
		}
		if w, ok := computedWeights[id]; ok {
			return w, nil
		}
		b := m.GetBucketByID(id)
		if b == nil {
			return 0, fmt.Errorf("unknown bucket id %d", id)
		}
		return scaleBucketWeight(b.Weight)
	}

	for _, b := range buckets {
		items := make([]crush.ItemSpec, len(b.Items))
		for i, it := range b.Items {
			w, werr := itemWeight(it.ID)
			if werr != nil {
				return nil, fmt.Errorf("bucket %s item %d: %w", b.Name, it.ID, werr)
			}
			items[i] = crush.ItemSpec{ID: it.ID, Weight: w}
		}
		hash, herr := parseHash(b.Hash)
		if herr != nil {
			return nil, fmt.Errorf("bucket %s: %w", b.Name, herr)
		}
		var cerr error
		switch b.Alg {
		case "uniform":
			iw := uint32(0)
			if len(items) > 0 {
				iw = items[0].Weight // builder.c uses item_weight[0]
			}
			cerr = mb.AddUniform(b.ID, int32(b.TypeID), hash, items, iw)
		case "list":
			cerr = mb.AddList(b.ID, int32(b.TypeID), hash, items)
		case "tree":
			cerr = mb.AddTree(b.ID, int32(b.TypeID), hash, items)
		case "straw":
			cerr = mb.AddStraw(b.ID, int32(b.TypeID), hash, items)
		case "straw2":
			cerr = mb.AddStraw2(b.ID, int32(b.TypeID), hash, items)
		default:
			return nil, fmt.Errorf("bucket %s: unknown alg %q", b.Name, b.Alg)
		}
		if cerr != nil {
			return nil, fmt.Errorf("bucket %s: %w", b.Name, cerr)
		}
		if cb, ok := mb.Bucket(b.ID); ok {
			computedWeights[b.ID] = cb.Weight
		}
	}

	// Rules.
	var maxRule int
	for _, r := range m.Rules {
		if int(r.ID) > maxRule {
			maxRule = int(r.ID)
		}
	}
	rules := make([]*crush.Rule, maxRule+1)
	for _, r := range m.Rules {
		cr, rerr := compileRule(r, m)
		if rerr != nil {
			return nil, fmt.Errorf("rule %s: %w", r.Name, rerr)
		}
		rules[r.ID] = cr
	}

	return mb.Build(rules, maxDevice+1, weights), nil
}

// scaleDeviceWeight converts an AST float32 weight to 16.16, clamping at
// crush.MaxDeviceWeight exactly like CrushCompiler::bucket_add_device.
func scaleDeviceWeight(w float32) (uint32, error) {
	if math.IsNaN(float64(w)) || w < 0 {
		return 0, fmt.Errorf("invalid weight %v", w)
	}
	const max = float64(crush.MaxDeviceWeight) // 0xfffe
	scaled := float64(w) * 65536.0
	if scaled > max*65536.0 {
		return uint32(crush.MaxDeviceWeight << 16), nil
	}
	return uint32(scaled), nil
}

func scaleBucketWeight(w float32) (uint32, error) {
	if math.IsNaN(float64(w)) || w < 0 {
		return 0, fmt.Errorf("invalid weight %v", w)
	}
	const max = float64(crush.MaxBucketWeight)
	scaled := float64(w) * 65536.0
	if scaled > max*65536.0 {
		return uint32(crush.MaxBucketWeight << 16), nil
	}
	return uint32(scaled), nil
}

func parseHash(s string) (uint32, error) {
	if s == "" || s == "rjenkins1" {
		return crush.HashRjenkins1, nil
	}
	n, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("unknown hash %q", s)
	}
	return uint32(n), nil
}

// compileRule translates one AST rule into engine steps.
func compileRule(r *Rule, m *Map) (*crush.Rule, error) {
	out := &crush.Rule{
		Ruleset: r.Ruleset,
		Type:    r.Type,
		MinSize: r.MinSize,
		MaxSize: r.MaxSize,
	}
	if out.MinSize == 0 {
		out.MinSize = 1
	}
	if out.MaxSize == 0 {
		out.MaxSize = 100
	}
	for _, st := range r.Steps {
		step, err := compileStep(st, m)
		if err != nil {
			return nil, err
		}
		out.Steps = append(out.Steps, step)
	}
	return out, nil
}

func compileStep(st *Step, m *Map) (crush.Step, error) {
	switch st.Op {
	case "take":
		id, err := resolveItemID(st, m)
		if err != nil {
			return crush.Step{}, err
		}
		return crush.Step{Op: crush.OpTake, Arg1: id}, nil
	case "choose_firstn":
		return crush.Step{Op: crush.OpChooseFirstN, Arg1: st.Num, Arg2: typeID(st, m)}, nil
	case "choose_indep":
		return crush.Step{Op: crush.OpChooseIndep, Arg1: st.Num, Arg2: typeID(st, m)}, nil
	case "chooseleaf_firstn":
		return crush.Step{Op: crush.OpChooseLeafFirstN, Arg1: st.Num, Arg2: typeID(st, m)}, nil
	case "chooseleaf_indep":
		return crush.Step{Op: crush.OpChooseLeafIndep, Arg1: st.Num, Arg2: typeID(st, m)}, nil
	case "emit":
		return crush.Step{Op: crush.OpEmit}, nil
	case "set_choose_tries":
		return crush.Step{Op: crush.OpSetChooseTries, Arg1: st.Num}, nil
	case "set_chooseleaf_tries":
		return crush.Step{Op: crush.OpSetChooseLeafTries, Arg1: st.Num}, nil
	case "set_choose_local_tries":
		return crush.Step{Op: crush.OpSetChooseLocalTries, Arg1: st.Num}, nil
	case "set_choose_local_fallback_tries":
		return crush.Step{Op: crush.OpSetChooseLocalFallbackTries, Arg1: st.Num}, nil
	case "set_chooseleaf_vary_r":
		return crush.Step{Op: crush.OpSetChooseLeafVaryR, Arg1: st.Num}, nil
	case "set_chooseleaf_stable":
		return crush.Step{Op: crush.OpSetChooseLeafStable, Arg1: st.Num}, nil
	case "set_msr_descents":
		return crush.Step{Op: crush.OpSetMSRDescents, Arg1: st.Num}, nil
	case "set_msr_collision_tries":
		return crush.Step{Op: crush.OpSetMSRCollisionTries, Arg1: st.Num}, nil
	case "choosemsr_firstn", "choosemsr_indep":
		return crush.Step{}, fmt.Errorf("msr rules are not supported by this engine build")
	default:
		return crush.Step{}, fmt.Errorf("unknown step op %q", st.Op)
	}
}

func typeID(st *Step, m *Map) int32 {
	if st.ItemType != "" {
		for _, t := range m.Types {
			if t.Name == st.ItemType {
				return t.ID
			}
		}
	}
	return st.ItemTypeID
}

func resolveItemID(st *Step, m *Map) (int32, error) {
	if st.ItemName != "" {
		if st.ItemName[0] >= '0' && st.ItemName[0] <= '9' {
			// numeric device id written as a name
			n, err := strconv.ParseInt(st.ItemName, 10, 32)
			if err == nil {
				return int32(n), nil
			}
		}
		for _, d := range m.Devices {
			if d.Name == st.ItemName {
				return d.ID, nil
			}
		}
		if b := m.GetBucketByName(st.ItemName); b != nil {
			return b.ID, nil
		}
		return 0, fmt.Errorf("unknown item %q", st.ItemName)
	}
	return int32(st.Item), nil
}

// tunablesFromMap converts the loose tunables map (text/binary parsers)
// into a typed crush.Tunables. Recognised profile names select the Ceph
// baseline profile; individual keys override it.
func tunablesFromMap(tm map[string]interface{}) (crush.Tunables, error) {
	t := crush.TunablesOptimal()
	if tm == nil {
		return t, nil
	}
	if prof, ok := tm["profile"].(string); ok {
		switch prof {
		case "legacy", "argonaut":
			t = crush.TunablesLegacy()
		case "bobtail":
			t = crush.TunablesBobtail()
		case "firefly", "ghost":
			t = crush.TunablesFirefly()
		case "hammer", "infernalis", "jewel":
			// jewel = optimal; hammer/infernalis = hammer profile.
			// The AST stores profile name only; per-key overrides below
			// (from binary maps) make it exact. For text maps without
			// explicit keys, treat jewel as optimal and older as their own.
			if prof == "jewel" {
				t = crush.TunablesOptimal()
			} else {
				t = crush.TunablesHammer()
			}
		case "optimal":
			t = crush.TunablesOptimal()
		default:
			return t, fmt.Errorf("unknown tunables profile %q", prof)
		}
	}
	u32 := func(key string, dst *uint32) {
		if v, ok := tm[key]; ok {
			*dst = toUint32(v)
		}
	}
	u32("choose_local_tries", &t.ChooseLocalTries)
	u32("choose_local_fallback_tries", &t.ChooseLocalFallbackTries)
	u32("choose_total_tries", &t.ChooseTotalTries)
	u32("chooseleaf_descend_once", &t.ChooseLeafDescendOnce)
	u32("chooseleaf_vary_r", &t.ChooseLeafVaryR)
	u32("straw_calc_version", &t.StrawCalcVersion)
	u32("allowed_bucket_algs", &t.AllowedBucketAlgs)
	u32("chooseleaf_stable", &t.ChooseLeafStable)
	u32("msr_descents", &t.MSRDescents)
	u32("msr_collision_tries", &t.MSRCollisionTries)
	return t, nil
}

func toUint32(v interface{}) uint32 {
	switch n := v.(type) {
	case int:
		return uint32(n)
	case int32:
		return uint32(n)
	case uint32:
		return n
	case uint64:
		return uint32(n)
	case float64:
		return uint32(n)
	case string:
		v, _ := strconv.ParseUint(n, 10, 32)
		return uint32(v)
	}
	return 0
}
