package crushmap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/unistack-org/crushkv/crush"
)

// Magic is the CRUSH binary map magic (CRUSH_MAGIC in CrushWrapper.h).
const Magic = uint32(0x00010000)

// Rule step opcodes on the wire (enum crush_opcodes in crush.h).
const (
	CrushRuleStepNoop                        = uint32(0)
	CrushRuleStepTake                        = uint32(1)
	CrushRuleStepChooseFirstN                = uint32(2)
	CrushRuleStepChooseIndep                 = uint32(3)
	CrushRuleStepEmit                        = uint32(4)
	CrushRuleStepChooseLeafFirstN            = uint32(6)
	CrushRuleStepChooseLeafIndep             = uint32(7)
	CrushRuleStepSetChooseTries              = uint32(8)
	CrushRuleStepSetChooseLeafTries          = uint32(9)
	CrushRuleStepSetChooseLocalTries         = uint32(10)
	CrushRuleStepSetChooseLocalFallbackTries = uint32(11)
	CrushRuleStepSetChooseLeafVaryR          = uint32(12)
	CrushRuleStepSetChooseLeafStable         = uint32(13)
	CrushRuleStepSetMSRDescents              = uint32(14)
	CrushRuleStepSetMSRCollisionTries        = uint32(15)
	CrushRuleStepChooseMSR                   = uint32(16)
)

// opWire maps AST step op names (the contract used by compile.go) to wire
// opcodes, and opString is its inverse. noop steps are dropped on decode
// (they carry no meaning for the engine).
var (
	opWire = map[string]uint32{
		"take":                            CrushRuleStepTake,
		"choose_firstn":                   CrushRuleStepChooseFirstN,
		"choose_indep":                    CrushRuleStepChooseIndep,
		"chooseleaf_firstn":               CrushRuleStepChooseLeafFirstN,
		"chooseleaf_indep":                CrushRuleStepChooseLeafIndep,
		"emit":                            CrushRuleStepEmit,
		"set_choose_tries":                CrushRuleStepSetChooseTries,
		"set_chooseleaf_tries":            CrushRuleStepSetChooseLeafTries,
		"set_choose_local_tries":          CrushRuleStepSetChooseLocalTries,
		"set_choose_local_fallback_tries": CrushRuleStepSetChooseLocalFallbackTries,
		"set_chooseleaf_vary_r":           CrushRuleStepSetChooseLeafVaryR,
		"set_chooseleaf_stable":           CrushRuleStepSetChooseLeafStable,
		"set_msr_descents":                CrushRuleStepSetMSRDescents,
		"set_msr_collision_tries":         CrushRuleStepSetMSRCollisionTries,
	}
	opString = map[uint32]string{
		CrushRuleStepTake:                        "take",
		CrushRuleStepChooseFirstN:                "choose_firstn",
		CrushRuleStepChooseIndep:                 "choose_indep",
		CrushRuleStepChooseLeafFirstN:            "chooseleaf_firstn",
		CrushRuleStepChooseLeafIndep:             "chooseleaf_indep",
		CrushRuleStepEmit:                        "emit",
		CrushRuleStepSetChooseTries:              "set_choose_tries",
		CrushRuleStepSetChooseLeafTries:          "set_chooseleaf_tries",
		CrushRuleStepSetChooseLocalTries:         "set_choose_local_tries",
		CrushRuleStepSetChooseLocalFallbackTries: "set_choose_local_fallback_tries",
		CrushRuleStepSetChooseLeafVaryR:          "set_chooseleaf_vary_r",
		CrushRuleStepSetChooseLeafStable:         "set_chooseleaf_stable",
		CrushRuleStepSetMSRDescents:              "set_msr_descents",
		CrushRuleStepSetMSRCollisionTries:        "set_msr_collision_tries",
	}
)

// binReader is a little-endian cursor over the encoded map (ceph::encode
// writes plain little-endian scalars on all supported platforms).
type binReader struct {
	b   []byte
	pos int
}

func (r *binReader) remaining() int { return len(r.b) - r.pos }

func (r *binReader) u32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, errors.New("crush binary: unexpected end of input")
	}
	v := binary.LittleEndian.Uint32(r.b[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *binReader) s32() (int32, error) {
	v, err := r.u32()
	return int32(v), err
}

func (r *binReader) u8() (uint8, error) {
	if r.remaining() < 1 {
		return 0, errors.New("crush binary: unexpected end of input")
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}

func (r *binReader) bytes(n int) ([]byte, error) {
	if r.remaining() < n {
		return nil, errors.New("crush binary: unexpected end of input")
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v, nil
}

// binWriter appends little-endian scalars.
type binWriter struct {
	b []byte
}

func (w *binWriter) u32(v uint32) { w.b = binary.LittleEndian.AppendUint32(w.b, v) }
func (w *binWriter) s32(v int32)  { w.u32(uint32(v)) }
func (w *binWriter) u8(v uint8)   { w.b = append(w.b, v) }

// DecodeBinary parses a binary CRUSH map in Ceph CrushWrapper format
// (the format produced by `crushtool --compile` and stored in the OSDMap).
// It mirrors CrushWrapper::decode byte-for-byte, including the historical
// 32/64-bit string-map key quirk and the gated optional tail sections.
func (cmap *Map) DecodeBinary(buf []byte) error {
	r := &binReader{b: buf}

	magic, err := r.u32()
	if err != nil {
		return err
	}
	if magic != Magic {
		return fmt.Errorf("crush binary: bad magic 0x%08x", magic)
	}

	maxBuckets, err := r.s32()
	if err != nil {
		return err
	}
	maxRules, err := r.u32()
	if err != nil {
		return err
	}
	maxDevices, err := r.s32()
	if err != nil {
		return err
	}
	if maxBuckets < 0 || maxBuckets > 1<<20 || maxRules > 1<<20 || maxDevices < 0 || maxDevices > 1<<24 {
		return errors.New("crush binary: absurd dimensions")
	}

	// Ceph starts from the legacy profile and lets the tail override it.
	legacy := crush.TunablesLegacy()
	tun := map[string]interface{}{
		"choose_local_tries":          legacy.ChooseLocalTries,
		"choose_local_fallback_tries": legacy.ChooseLocalFallbackTries,
		"choose_total_tries":          legacy.ChooseTotalTries,
		"chooseleaf_descend_once":     legacy.ChooseLeafDescendOnce,
		"chooseleaf_vary_r":           legacy.ChooseLeafVaryR,
		"straw_calc_version":          legacy.StrawCalcVersion,
		"allowed_bucket_algs":         legacy.AllowedBucketAlgs,
		"chooseleaf_stable":           legacy.ChooseLeafStable,
		"msr_descents":                uint32(0),
		"msr_collision_tries":         uint32(0),
		"profile":                     "legacy",
	}

	// Buckets.
	cmap.Buckets = nil
	for i := 0; i < int(maxBuckets); i++ {
		algWord, err := r.u32()
		if err != nil {
			return err
		}
		if algWord == 0 {
			continue // empty slot
		}
		b, err := decodeBucket(r, algWord)
		if err != nil {
			return fmt.Errorf("bucket slot %d: %w", i, err)
		}
		cmap.Buckets = append(cmap.Buckets, b)
	}

	// Rules.
	cmap.Rules = nil
	for i := uint32(0); i < maxRules; i++ {
		present, err := r.u32()
		if err != nil {
			return err
		}
		if present == 0 {
			continue
		}
		n, err := r.u32()
		if err != nil {
			return err
		}
		if n > 1<<16 {
			return errors.New("crush binary: absurd rule length")
		}
		ruleset, err := r.u8()
		if err != nil {
			return err
		}
		if uint32(ruleset) != i {
			return errors.New("crush binary: ruleset != rule id; encoding is too old")
		}
		typ, err := r.u8()
		if err != nil {
			return err
		}
		minSize, err := r.u8()
		if err != nil {
			return err
		}
		maxSize, err := r.u8()
		if err != nil {
			return err
		}
		rule := &Rule{
			ID:      uint8(i),
			Ruleset: ruleset,
			Type:    typ,
			MinSize: minSize,
			MaxSize: maxSize,
		}
		for j := uint32(0); j < n; j++ {
			op, err := r.u32()
			if err != nil {
				return err
			}
			arg1, err := r.s32()
			if err != nil {
				return err
			}
			arg2, err := r.s32()
			if err != nil {
				return err
			}
			if op == CrushRuleStepNoop {
				continue
			}
			if op == CrushRuleStepChooseMSR {
				return errors.New("crush binary: msr rules are not supported")
			}
			name, ok := opString[op]
			if !ok {
				return fmt.Errorf("crush binary: unknown step op %d", op)
			}
			st := &Step{Op: name}
			switch op {
			case CrushRuleStepTake:
				st.Item = int(arg1)
			case CrushRuleStepChooseFirstN, CrushRuleStepChooseIndep,
				CrushRuleStepChooseLeafFirstN, CrushRuleStepChooseLeafIndep:
				st.Num = arg1
				st.ItemTypeID = arg2
			default:
				st.Num = arg1
			}
			rule.Steps = append(rule.Steps, st)
		}
		cmap.Rules = append(cmap.Rules, rule)
	}

	// Name info. Ceph once encoded the map keys as int (64-bit on most
	// platforms); the quirk is tolerated by re-reading the length after a
	// zero-length marker.
	typeMap, err := decodeStringMap(r)
	if err != nil {
		return fmt.Errorf("type_map: %w", err)
	}
	nameMap, err := decodeStringMap(r)
	if err != nil {
		return fmt.Errorf("name_map: %w", err)
	}
	ruleNameMap, err := decodeStringMap(r)
	if err != nil {
		return fmt.Errorf("rule_name_map: %w", err)
	}

	cmap.Types = nil
	for id, name := range typeMap {
		cmap.Types = append(cmap.Types, &Type{ID: id, Name: name})
	}
	sort.Slice(cmap.Types, func(i, j int) bool { return cmap.Types[i].ID < cmap.Types[j].ID })

	// Devices: ids 0..max_devices-1, synthesising names when absent.
	devName := make(map[int32]string)
	byID := make(map[int32]*Bucket, len(cmap.Buckets))
	for _, b := range cmap.Buckets {
		byID[b.ID] = b
	}
	cmap.Devices = nil
	for id := int32(0); id < maxDevices; id++ {
		name, ok := nameMap[id]
		if !ok || name == "" {
			name = "osd." + strconv.Itoa(int(id))
		}
		devName[id] = name
		cmap.Devices = append(cmap.Devices, &Device{ID: id, Name: name})
	}

	// Resolve bucket, device and item names.
	for _, b := range cmap.Buckets {
		b.Name = nameMap[b.ID]
		for _, it := range b.Items {
			it.Name = nameMap[it.ID]
		}
	}
	ruleByID := make(map[uint8]*Rule, len(cmap.Rules))
	for _, ru := range cmap.Rules {
		ruleByID[ru.ID] = ru
	}
	for id, name := range ruleNameMap {
		if id < 0 {
			continue
		}
		if ru, ok := ruleByID[uint8(id)]; ok {
			ru.Name = name
		}
	}
	// Resolve chooseleaf types on choose steps from the type map.
	typeName := func(id int32) string { return typeMap[id] }
	for _, ru := range cmap.Rules {
		for _, st := range ru.Steps {
			switch st.Op {
			case "choose_firstn", "choose_indep", "chooseleaf_firstn", "chooseleaf_indep":
				st.ItemType = typeName(st.ItemTypeID)
			case "take":
				if st.Item >= 0 {
					st.ItemName = devName[int32(st.Item)]
				} else if b, ok := byID[int32(st.Item)]; ok {
					st.ItemName = b.Name
				}
			}
		}
	}

	// Optional tail sections, gated on remaining bytes exactly as Ceph does.
	if r.remaining() >= 4 {
		v, err := r.u32()
		if err != nil {
			return err
		}
		tun["choose_local_tries"] = v
	}
	if r.remaining() >= 4 {
		v, err := r.u32()
		if err != nil {
			return err
		}
		tun["choose_local_fallback_tries"] = v
	}
	if r.remaining() >= 4 {
		v, err := r.u32()
		if err != nil {
			return err
		}
		tun["choose_total_tries"] = v
	}
	// chooseleaf_descend_once is __u32 on the wire (crush.h).
	if r.remaining() >= 4 {
		v, err := r.u32()
		if err != nil {
			return err
		}
		tun["chooseleaf_descend_once"] = v
	}
	if r.remaining() >= 1 {
		v, err := r.u8()
		if err != nil {
			return err
		}
		tun["chooseleaf_vary_r"] = uint32(v)
	}
	if r.remaining() >= 1 {
		v, err := r.u8()
		if err != nil {
			return err
		}
		tun["straw_calc_version"] = uint32(v)
	}
	if r.remaining() >= 4 {
		v, err := r.u32()
		if err != nil {
			return err
		}
		tun["allowed_bucket_algs"] = v
	}
	if r.remaining() >= 1 {
		v, err := r.u8()
		if err != nil {
			return err
		}
		tun["chooseleaf_stable"] = uint32(v)
	}
	profileFromTunables(tun)

	// Device classes: class_map (id->class), class_name (class->name),
	// class_bucket (bucket->pos->class; not representable in this AST,
	// consumed and skipped).
	if r.remaining() >= 4 {
		classMap, err := decodeIntMap(r)
		if err != nil {
			return fmt.Errorf("class_map: %w", err)
		}
		className, err := decodeStringMap(r)
		if err != nil {
			return fmt.Errorf("class_name: %w", err)
		}
		if r.remaining() >= 4 {
			n, err := r.u32()
			if err != nil {
				return err
			}
			for i := uint32(0); i < n; i++ {
				if _, err := r.s32(); err != nil {
					return err
				}
				m, err := r.u32()
				if err != nil {
					return err
				}
				for j := uint32(0); j < m; j++ {
					if _, err := r.s32(); err != nil {
						return err
					}
					if _, err := r.s32(); err != nil {
						return err
					}
				}
			}
		}
		for id, cls := range classMap {
			name := className[cls]
			if name == "" || name == "generic" {
				continue
			}
			if id >= 0 {
				for _, d := range cmap.Devices {
					if d.ID == id {
						d.Class = name
					}
				}
			} else if b, ok := byID[id]; ok {
				b.IDClass = name
			}
		}
	}

	// Choose args (per-bucket weight overrides). Stored verbatim; the
	// compiler applies base weights (Ceph applies them in finalize).
	if r.remaining() >= 4 {
		n, err := r.u32()
		if err != nil {
			return err
		}
		cmap.ChooseArgs = nil
		for i := uint32(0); i < n; i++ {
			index, err := r.s32()
			if err != nil {
				return err
			}
			size, err := r.u32()
			if err != nil {
				return err
			}
			for j := uint32(0); j < size; j++ {
				bucketIndex, err := r.u32()
				if err != nil {
					return err
				}
				pos, err := r.u32()
				if err != nil {
					return err
				}
				arg := ChooseArg{BucketID: -1 - int32(bucketIndex)}
				if pos > 1<<16 {
					return errors.New("crush binary: absurd weight_set_positions")
				}
				for k := uint32(0); k < pos; k++ {
					cnt, err := r.u32()
					if err != nil {
						return err
					}
					if cnt > 1<<24 {
						return errors.New("crush binary: absurd weight_set size")
					}
					for l := uint32(0); l < cnt; l++ {
						w, err := r.u32()
						if err != nil {
							return err
						}
						arg.WeightSet = append(arg.WeightSet, float64(w)/65536.0)
					}
				}
				idsSize, err := r.u32()
				if err != nil {
					return err
				}
				if idsSize > 1<<24 {
					return errors.New("crush binary: absurd ids_size")
				}
				for k := uint32(0); k < idsSize; k++ {
					id, err := r.s32()
					if err != nil {
						return err
					}
					arg.IDs = append(arg.IDs, int(id))
				}
				if cmap.ChooseArgs == nil {
					cmap.ChooseArgs = make(map[string]ChooseArg)
				}
				key := strconv.Itoa(int(index))
				if pos > 0 || idsSize > 0 {
					key = key + "/" + strconv.Itoa(int(bucketIndex))
				}
				cmap.ChooseArgs[key] = arg
			}
		}
	}

	// MSR tunables; defaults when the tail is absent.
	if r.remaining() >= 8 {
		v, err := r.u32()
		if err != nil {
			return err
		}
		tun["msr_descents"] = v
		v, err = r.u32()
		if err != nil {
			return err
		}
		tun["msr_collision_tries"] = v
	}

	cmap.Tunables = tun
	cmap.rulesSort()
	cmap.bucketsSort()
	return nil
}

// The crushmap AST bucket-alg enums use the same values as crush.h on the
// wire (1-based: uniform=1..straw2=5); the crush engine constants are the
// same algs 0-based. Pin the relationships at compile time.
const (
	_ = int(CrushAlgUniform) - int(crush.AlgUniform) - 1
	_ = int(CrushAlgStraw2) - int(crush.AlgStraw2) - 1
)

// decodeBucket decodes one non-empty bucket slot. algWord is the wire alg
// already read from the slot header (0 means absent and is handled by the
// caller). Wire algs are 1-based (crush.h: uniform=1..straw2=5), matching
// the crushmap AST enums; the engine constants are 0-based.
func decodeBucket(r *binReader, algWord uint32) (*Bucket, error) {
	if algWord < uint32(CrushAlgUniform) || algWord > uint32(CrushAlgStraw2) {
		return nil, fmt.Errorf("unknown bucket alg %d", algWord)
	}
	id, err := r.s32()
	if err != nil {
		return nil, err
	}
	typWord, err := r.u32()
	if err != nil {
		return nil, err
	}
	alg, err := r.u32()
	if err != nil {
		return nil, err
	}
	if alg != algWord {
		return nil, fmt.Errorf("bucket %d: slot alg %d != header alg %d", id, algWord, alg)
	}
	hash, err := r.u32()
	if err != nil {
		return nil, err
	}
	if hash != crush.HashRjenkins1 {
		return nil, fmt.Errorf("bucket %d: unknown hash %d", id, hash)
	}
	weightWord, err := r.u32()
	if err != nil {
		return nil, err
	}
	size, err := r.u32()
	if err != nil {
		return nil, err
	}
	if size > 1<<24 {
		return nil, fmt.Errorf("bucket %d: absurd size %d", id, size)
	}
	items := make([]int32, size)
	for i := range items {
		items[i], err = r.s32()
		if err != nil {
			return nil, err
		}
	}

	itemW := make([]uint32, size)
	switch alg - 1 { // wire 1-based -> engine 0-based
	case crush.AlgUniform:
		w, err := r.u32()
		if err != nil {
			return nil, err
		}
		for i := range itemW {
			itemW[i] = w
		}
	case crush.AlgList, crush.AlgStraw:
		// size x (item_weight, sum_weight|straw) interleaved
		for i := range itemW {
			itemW[i], err = r.u32()
			if err != nil {
				return nil, err
			}
			if _, err := r.u32(); err != nil { // sum_weights / straws
				return nil, err
			}
		}
	case crush.AlgTree:
		numNodes, err := r.u32()
		if err != nil {
			return nil, err
		}
		if numNodes > 1<<26 {
			return nil, fmt.Errorf("bucket %d: absurd num_nodes %d", id, numNodes)
		}
		nodeW := make([]uint32, numNodes)
		for i := range nodeW {
			nodeW[i], err = r.u32()
			if err != nil {
				return nil, err
			}
		}
		// Item weight of leaf i is the node weight of its leaf node
		// (crush_get_bucket_item_weight for tree buckets).
		for i := range itemW {
			node := crushCalcTreeNode(i)
			if int(node) < len(nodeW) {
				itemW[i] = nodeW[node]
			}
		}
	case crush.AlgStraw2:
		for i := range itemW {
			itemW[i], err = r.u32()
			if err != nil {
				return nil, err
			}
		}
	}

	b := &Bucket{
		TypeID: CrushBucketType(typWord),
		Weight: float32(weightWord) / 65536.0,
		ID:     id,
		Alg:    crushAlgTypeStringMap[CrushAlgType(alg)],
		Hash:   "rjenkins1",
		Size:   size,
	}
	if b.Alg == "" {
		return nil, fmt.Errorf("bucket %d: alg %d has no name", id, alg)
	}
	for i, item := range items {
		b.Items = append(b.Items, &Item{
			ID:     item,
			Weight: float32(itemW[i]) / 65536.0,
			Pos:    i,
		})
	}
	return b, nil
}

// decodeStringMap tolerates the historical int-vs-int32 key bug in Ceph:
// a zero length means the key was written 64-bit, so the next u32 is the
// real length.
func decodeStringMap(r *binReader) (map[int32]string, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	m := make(map[int32]string, min(int(n), 1024))
	for i := uint32(0); i < n; i++ {
		key, err := r.s32()
		if err != nil {
			return nil, err
		}
		size, err := r.u32()
		if err != nil {
			return nil, err
		}
		if size == 0 { // key was 64 bits: skip the high half
			size, err = r.u32()
			if err != nil {
				return nil, err
			}
		}
		if size > 1<<16 {
			return nil, fmt.Errorf("string map: absurd string length %d", size)
		}
		bs, err := r.bytes(int(size))
		if err != nil {
			return nil, err
		}
		m[key] = string(bs)
	}
	return m, nil
}

// decodeIntMap decodes a ceph-encoded map<int32_t,int32_t>.
func decodeIntMap(r *binReader) (map[int32]int32, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	m := make(map[int32]int32, min(int(n), 1024))
	for i := uint32(0); i < n; i++ {
		k, err := r.s32()
		if err != nil {
			return nil, err
		}
		v, err := r.s32()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

// profileFromTunables records a tunables profile label matching the
// decoded knobs, so that tunablesFromMap reproduces them.
func profileFromTunables(tun map[string]interface{}) {
	u := func(k string) uint32 { return toUint32(tun[k]) }
	var prof string
	switch {
	case u("choose_local_tries") == 2 && u("choose_local_fallback_tries") == 5 && u("choose_total_tries") == 19:
		prof = "legacy"
	case u("choose_local_tries") == 0 && u("choose_local_fallback_tries") == 0 && u("choose_total_tries") == 50 &&
		u("chooseleaf_descend_once") == 1 && u("chooseleaf_vary_r") == 0 && u("chooseleaf_stable") == 0:
		prof = "bobtail"
	case u("choose_local_tries") == 0 && u("choose_local_fallback_tries") == 0 && u("choose_total_tries") == 50 &&
		u("chooseleaf_descend_once") == 1 && u("chooseleaf_vary_r") == 1 && u("chooseleaf_stable") == 0:
		prof = "firefly"
	case u("choose_local_tries") == 0 && u("choose_local_fallback_tries") == 0 && u("choose_total_tries") == 50 &&
		u("chooseleaf_descend_once") == 1 && u("chooseleaf_vary_r") == 1 && u("chooseleaf_stable") == 1 &&
		u("allowed_bucket_algs") == uint32(crush.TunablesHammer().AllowedBucketAlgs):
		prof = "hammer"
	case u("choose_local_tries") == 0 && u("choose_local_fallback_tries") == 0 && u("choose_total_tries") == 50 &&
		u("chooseleaf_descend_once") == 1 && u("chooseleaf_vary_r") == 1 && u("chooseleaf_stable") == 1:
		prof = "optimal"
	default:
		// Keep legacy as a conservative label; tunablesFromMap applies
		// per-key overrides on top of the profile anyway.
		prof = "legacy"
	}
	tun["profile"] = prof
}

// EncodeBinary serialises the map in Ceph CrushWrapper format. The output
// is byte-compatible with `crushtool --compile` on Quincy+ (ruleset==ruleid,
// min/max size 1/100) and always writes the full modern tail.
func (cmap *Map) EncodeBinary() ([]byte, error) {
	w := &binWriter{}
	w.u32(Magic)

	maxBucketIndex := -1
	for _, b := range cmap.Buckets {
		if b.ID >= 0 {
			return nil, fmt.Errorf("crush binary: bucket id %d must be negative", b.ID)
		}
		if idx := crushBucketIndex(b.ID); idx > maxBucketIndex {
			maxBucketIndex = idx
		}
	}
	maxRules := 0
	for _, r := range cmap.Rules {
		if int(r.ID)+1 > maxRules {
			maxRules = int(r.ID) + 1
		}
	}
	maxDevices := int32(0)
	for _, d := range cmap.Devices {
		if d.ID < 0 {
			return nil, fmt.Errorf("crush binary: device id %d must be non-negative", d.ID)
		}
		if d.ID+1 > maxDevices {
			maxDevices = d.ID + 1
		}
	}
	for _, b := range cmap.Buckets {
		for _, it := range b.Items {
			if it.ID >= maxDevices {
				maxDevices = it.ID + 1
			}
		}
	}

	w.s32(int32(maxBucketIndex + 1))
	w.u32(uint32(maxRules))
	w.s32(maxDevices)

	// Buckets, in slot order.
	bucketByIndex := make(map[int]*Bucket, len(cmap.Buckets))
	for _, b := range cmap.Buckets {
		idx := crushBucketIndex(b.ID)
		if _, dup := bucketByIndex[idx]; dup {
			return nil, fmt.Errorf("crush binary: duplicate bucket id %d", b.ID)
		}
		bucketByIndex[idx] = b
	}
	for i := 0; i <= maxBucketIndex; i++ {
		b, ok := bucketByIndex[i]
		if !ok {
			w.u32(0)
			continue
		}
		if err := encodeBucket(w, b); err != nil {
			return nil, fmt.Errorf("bucket %d: %w", b.ID, err)
		}
	}

	// Rules, in slot order (ruleset == rule id).
	ruleByIndex := make(map[int]*Rule, len(cmap.Rules))
	for _, r := range cmap.Rules {
		if _, dup := ruleByIndex[int(r.ID)]; dup {
			return nil, fmt.Errorf("crush binary: duplicate rule id %d", r.ID)
		}
		ruleByIndex[int(r.ID)] = r
	}
	for i := 0; i < maxRules; i++ {
		ru, ok := ruleByIndex[i]
		if !ok {
			w.u32(0)
			continue
		}
		steps := make([]uint32, 0, len(ru.Steps)*3)
		for _, st := range ru.Steps {
			op, ok := opWire[st.Op]
			if !ok {
				return nil, fmt.Errorf("rule %d: unknown step op %q", i, st.Op)
			}
			var arg1, arg2 int32
			switch op {
			case CrushRuleStepTake:
				arg1 = int32(st.Item)
				if st.ItemName != "" {
					id, err := resolveItemID(st, cmap)
					if err != nil {
						return nil, fmt.Errorf("rule %d: %w", i, err)
					}
					arg1 = id
				}
			case CrushRuleStepChooseFirstN, CrushRuleStepChooseIndep,
				CrushRuleStepChooseLeafFirstN, CrushRuleStepChooseLeafIndep:
				arg1 = st.Num
				arg2 = typeID(st, cmap)
			default:
				arg1 = st.Num
			}
			steps = append(steps, op, uint32(arg1), uint32(arg2))
		}
		w.u32(1)
		w.u32(uint32(len(ru.Steps)))
		w.u8(uint8(i)) // ruleset == ruleid
		w.u8(ru.Type)
		minSize, maxSize := ru.MinSize, ru.MaxSize
		if minSize == 0 {
			minSize = 1
		}
		if maxSize == 0 {
			maxSize = 100
		}
		w.u8(minSize)
		w.u8(maxSize)
		for _, v := range steps {
			w.u32(v)
		}
	}

	// Name info (32-bit keys: the modern encoding).
	encodeStringMap := func(m map[int32]string) {
		keys := make([]int, 0, len(m))
		for k := range m {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		w.u32(uint32(len(keys)))
		for _, k := range keys {
			s := m[int32(k)]
			w.s32(int32(k))
			w.u32(uint32(len(s)))
			w.b = append(w.b, s...)
		}
	}

	typeMap := make(map[int32]string, len(cmap.Types))
	for _, t := range cmap.Types {
		typeMap[t.ID] = t.Name
	}
	encodeStringMap(typeMap)

	nameMap := make(map[int32]string, len(cmap.Buckets)+len(cmap.Devices))
	for _, b := range cmap.Buckets {
		if b.Name != "" {
			nameMap[b.ID] = b.Name
		}
	}
	for _, d := range cmap.Devices {
		if d.Name != "" {
			nameMap[d.ID] = d.Name
		}
	}
	encodeStringMap(nameMap)

	ruleNameMap := make(map[int32]string, len(cmap.Rules))
	for _, ru := range cmap.Rules {
		if ru.Name != "" {
			ruleNameMap[int32(ru.ID)] = ru.Name
		}
	}
	encodeStringMap(ruleNameMap)

	// Tunables. Resolve profile + overrides into typed values so the
	// encoder always writes a complete, self-consistent tail.
	t, err := tunablesFromMap(cmap.Tunables)
	if err != nil {
		return nil, err
	}
	w.u32(t.ChooseLocalTries)
	w.u32(t.ChooseLocalFallbackTries)
	w.u32(t.ChooseTotalTries)
	w.u32(t.ChooseLeafDescendOnce)
	w.u8(uint8(t.ChooseLeafVaryR))
	w.u8(uint8(t.StrawCalcVersion))
	w.u32(t.AllowedBucketAlgs)
	w.u8(uint8(t.ChooseLeafStable))

	// Device classes.
	classes := make(map[string]int32)
	classOf := func(name string) int32 {
		if name == "" || name == "generic" {
			return 0
		}
		if id, ok := classes[name]; ok {
			return id
		}
		// class id 0 is "generic"; assign new ids from 1
		id := int32(len(classes) + 1)
		classes[name] = id
		return id
	}
	classMap := make(map[int32]int32)
	for _, d := range cmap.Devices {
		if c := classOf(d.Class); c != 0 {
			classMap[d.ID] = c
		}
	}
	for _, b := range cmap.Buckets {
		if c := classOf(b.IDClass); c != 0 {
			classMap[b.ID] = c
		}
	}
	// Assign ids in name order for determinism.
	if len(classes) > 0 {
		names := make([]string, 0, len(classes))
		for n := range classes {
			names = append(names, n)
		}
		sort.Strings(names)
		for i, n := range names {
			classes[n] = int32(i + 1)
		}
		// Rebuild classMap with final ids.
		classMap = make(map[int32]int32)
		for _, d := range cmap.Devices {
			if c := classOf(d.Class); c != 0 {
				classMap[d.ID] = c
			}
		}
		for _, b := range cmap.Buckets {
			if c := classOf(b.IDClass); c != 0 {
				classMap[b.ID] = c
			}
		}
	}
	encodeIntMap := func(m map[int32]int32) {
		keys := make([]int, 0, len(m))
		for k := range m {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		w.u32(uint32(len(keys)))
		for _, k := range keys {
			w.s32(int32(k))
			w.s32(m[int32(k)])
		}
	}
	encodeIntMap(classMap)
	className := make(map[int32]string, len(classes))
	for n, id := range classes {
		className[id] = n
	}
	encodeStringMap(className)
	w.u32(0) // class_bucket: per-position classes are not representable

	// Choose args (all stored under outer index 0).
	if len(cmap.ChooseArgs) == 0 {
		w.u32(0)
	} else {
		keys := make([]string, 0, len(cmap.ChooseArgs))
		for k := range cmap.ChooseArgs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		args := make([]ChooseArg, 0, len(keys))
		for _, k := range keys {
			args = append(args, cmap.ChooseArgs[k])
		}
		w.u32(1) // one choose_args entry (default index 0)
		w.s32(0)
		w.u32(uint32(len(args)))
		for _, a := range args {
			bucketIndex := uint32(crushBucketIndex(a.BucketID))
			w.u32(bucketIndex)
			if len(a.WeightSet) > 0 {
				w.u32(1)
				w.u32(uint32(len(a.WeightSet)))
				for _, f := range a.WeightSet {
					s, err := scaleDeviceWeight(float32(f))
					if err != nil {
						return nil, fmt.Errorf("choose_args: %w", err)
					}
					w.u32(s)
				}
			} else {
				w.u32(0)
			}
			w.u32(uint32(len(a.IDs)))
			for _, id := range a.IDs {
				w.s32(int32(id))
			}
		}
	}

	// MSR tunables.
	w.u32(t.MSRDescents)
	w.u32(t.MSRCollisionTries)

	return w.b, nil
}

// encodeBucket writes one bucket slot: u32 alg (0=absent, handled by the
// caller), then the header, items and the per-alg tail, exactly as
// CrushWrapper::encode.
func encodeBucket(w *binWriter, b *Bucket) error {
	algType, ok := crushAlgStringTypeMap[b.Alg]
	if !ok || algType == CrushAlgInvalid {
		return fmt.Errorf("unknown alg %q", b.Alg)
	}
	algWord := uint32(algType) // AST enum and wire alg are both 1-based
	hash, err := parseHash(b.Hash)
	if err != nil {
		return err
	}
	weight, err := scaleBucketWeight(b.Weight)
	if err != nil {
		return err
	}
	size := uint32(len(b.Items))
	if b.Size != 0 && b.Size != size {
		return fmt.Errorf("bucket size %d != %d items", b.Size, size)
	}

	w.u32(algWord)
	w.s32(b.ID)
	w.u32(uint32(b.TypeID))
	w.u32(algWord)
	w.u32(hash)
	w.u32(weight)
	w.u32(size)
	for _, it := range b.Items {
		w.s32(it.ID)
	}

	itemWeights := make([]uint32, len(b.Items))
	for i, it := range b.Items {
		var s uint32
		if it.ID < 0 {
			s, err = scaleBucketWeight(it.Weight)
		} else {
			s, err = scaleDeviceWeight(it.Weight)
		}
		if err != nil {
			return fmt.Errorf("item %d: %w", it.ID, err)
		}
		itemWeights[i] = s
	}

	switch algWord - 1 { // wire 1-based -> engine 0-based
	case crush.AlgUniform:
		var iw uint32
		if len(itemWeights) > 0 {
			iw = itemWeights[0]
		}
		w.u32(iw)
	case crush.AlgList:
		// sum_weights are suffix sums (crush_create_list)
		sum := uint32(0)
		sums := make([]uint32, len(itemWeights))
		for i := len(itemWeights) - 1; i >= 0; i-- {
			sum += itemWeights[i]
			sums[i] = sum
		}
		for i, iw := range itemWeights {
			w.u32(iw)
			w.u32(sums[i])
		}
	case crush.AlgTree:
		n := len(itemWeights)
		depth := crushCalcDepth(n)
		numNodes := uint32(0)
		nodeW := []uint32(nil)
		if n > 0 {
			numNodes = uint32(1) << uint(depth)
			nodeW = make([]uint32, numNodes)
			for i, iw := range itemWeights {
				node := crushCalcTreeNode(i)
				nodeW[node] += iw
				for j := 1; j < depth; j++ {
					node = crushTreeBuilderParent(node)
					nodeW[node] += iw
				}
			}
		}
		w.u32(numNodes)
		for _, nw := range nodeW {
			w.u32(nw)
		}
	case crush.AlgStraw:
		// straw multipliers are recomputed by the engine on decode
		// (crush_calc_straw), so the wire values are placeholders.
		for _, iw := range itemWeights {
			w.u32(iw)
			w.u32(0x10000)
		}
	case crush.AlgStraw2:
		for _, iw := range itemWeights {
			w.u32(iw)
		}
	}
	return nil
}

// crushBucketIndex maps a (negative) bucket id to its slot index,
// matching crush_bucket_index() in crush.h.
func crushBucketIndex(id int32) int { return int(-1 - id) }

// Tree layout helpers replicated from crush/builder.go (the encoder must
// produce the same node_weights the engine builds).
func crushCalcTreeNode(i int) int32 { return int32(((i + 1) << 1) - 1) }

func crushTreeBuilderHeight(n int32) int {
	h := 0
	for n&1 == 0 {
		h++
		n >>= 1
	}
	return h
}

func crushTreeBuilderParent(n int32) int32 {
	h := crushTreeBuilderHeight(n)
	if n&(1<<uint(h+1)) != 0 { // on_right
		return n - int32(1)<<uint(h)
	}
	return n + int32(1)<<uint(h)
}

func crushCalcDepth(size int) int {
	if size == 0 {
		return 0
	}
	depth := 1
	t := size - 1
	for t != 0 {
		t >>= 1
		depth++
	}
	return depth
}
