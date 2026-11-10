package crush

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// EncodeText renders the map in the crush text grammar (the format
// DecodeText parses). The crushkv upstream this codec derives from ships
// no text encoder, so this is ours: it emits tunables, devices, types,
// rules and buckets, and the result re-parses into an equivalent map.
func (m *Wrapper) EncodeText() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("crush: nil map")
	}
	// Bucket items reference devices by name; decode drops names, so
	// rebuild them from the device table.
	devName := make(map[int32]string, len(m.Devices))
	for _, d := range m.Devices {
		devName[d.ID] = d.Name
	}
	typeName := make(map[CrushBucketType]string, len(m.Types))
	for _, t := range m.Types {
		typeName[CrushBucketType(t.ID)] = t.Name
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
			return nil, fmt.Errorf("crush: bucket %q has no type", bk.Name)
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
				return nil, fmt.Errorf("crush: bucket %q item id %d has no name", bk.Name, it.ID)
			}
			fmt.Fprintf(&b, "\titem %s weight %s pos %d\n", name, formatWeight(it.Weight), it.Pos)
		}
		b.WriteString("}\n")
	}
	return []byte(b.String()), nil
}

// formatWeight renders a bucket-item weight the way Ceph does (three
// decimals) when that string round-trips, and the shortest exact
// representation otherwise: text output must decode back to the same
// float32, which "%.2f" silently breaks for weights like 1.875.
func formatWeight(w float32) string {
	if s := strconv.FormatFloat(float64(w), 'f', 3, 32); float32(mustParseFloat(s)) == w {
		return s
	}
	return strconv.FormatFloat(float64(w), 'f', -1, 32)
}

func mustParseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 32)
	return f
}

func encodeRuleText(b *strings.Builder, r *RuleDecl) {
	fmt.Fprintf(b, "rule %s {\n", r.Name)
	fmt.Fprintf(b, "\tid %d\n\truleset %d\n", r.ID, r.Ruleset)
	typ := "replicated"
	switch r.Type {
	case ErasurePG:
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
