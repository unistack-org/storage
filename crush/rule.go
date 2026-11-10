package crush

// Step is one instruction of a CRUSH rule (struct crush_rule_step).
type Step struct {
	Op   uint32
	Arg1 int32
	Arg2 int32
}

// Rule is a CRUSH rule: a small program executed by DoRule.
type Rule struct {
	Ruleset uint8
	Type    uint8 // RuleTypeReplicated / RuleTypeErasure
	MinSize uint8
	MaxSize uint8
	Steps   []Step
}

// DoRule executes rule ruleno for input x (a PG id, hashed by the caller)
// and appends the placed device ids into result, returning how many were
// written. weights is the per-device 16.16 weight table. This is a literal
// port of crush_do_rule (the non-MSR path) from mapper.c: the w/o swap
// buffer dance, the "+1" on choose_total_tries, and the per-step
// set_choose_* overrides all behave as in Ceph.
//
// DoRule performs no allocations: all scratch space lives in w.
// MSR rule types (rule types 4/5) are not supported and yield 0.
func (m *Map) DoRule(w *Work, ruleno int32, x int32, result []int32, weights []uint32) int {
	if uint32(ruleno) >= uint32(m.MaxRules) || ruleno < 0 {
		return 0
	}
	rule := m.Rules[ruleno]
	if rule == nil {
		return 0
	}
	if rule.Type == RuleTypeMSRFirstN || rule.Type == RuleTypeMSRIndep {
		return 0 // MSR is out of scope; Ceph keeps it experimental too
	}

	resultMax := len(result)
	resultLen := 0

	// The original choose_total_tries value was off by one (it counted
	// "retries", not "tries"): add one.
	chooseTries := m.ChooseTotalTries + 1
	chooseLeafTries := uint32(0)
	// The local tries values counted "retries" and need no adjustment.
	chooseLocalRetries := m.ChooseLocalTries
	chooseLocalFallbackRetries := m.ChooseLocalFallbackTries
	varyR := m.ChooseLeafVaryR
	stable := m.ChooseLeafStable

	a, b, c := w.a, w.b, w.c
	if len(a) < resultMax || len(c) < resultMax {
		// Caller passed a result larger than NewWork's resultMax.
		a = make([]int32, resultMax)
		b = make([]int32, resultMax)
		c = make([]int32, resultMax)
	}
	wbuf, obuf := a, b
	var wsize int

	for _, cur := range rule.Steps {
		firstn := false

		switch cur.Op {
		case OpTake:
			if (cur.Arg1 >= 0 && cur.Arg1 < m.MaxDevices) ||
				(-1-cur.Arg1 >= 0 && -1-cur.Arg1 < m.MaxBuckets && m.Buckets[-1-cur.Arg1] != nil) {
				wbuf[0] = cur.Arg1
				wsize = 1
			}

		case OpSetChooseTries:
			if cur.Arg1 > 0 {
				chooseTries = uint32(cur.Arg1)
			}
		case OpSetChooseLeafTries:
			if cur.Arg1 > 0 {
				chooseLeafTries = uint32(cur.Arg1)
			}
		case OpSetChooseLocalTries:
			if cur.Arg1 >= 0 {
				chooseLocalRetries = uint32(cur.Arg1)
			}
		case OpSetChooseLocalFallbackTries:
			if cur.Arg1 >= 0 {
				chooseLocalFallbackRetries = uint32(cur.Arg1)
			}
		case OpSetChooseLeafVaryR:
			if cur.Arg1 >= 0 {
				varyR = uint32(cur.Arg1)
			}
		case OpSetChooseLeafStable:
			if cur.Arg1 >= 0 {
				stable = uint32(cur.Arg1)
			}

		case OpChooseLeafFirstN, OpChooseFirstN:
			firstn = true
			fallthrough
		case OpChooseLeafIndep, OpChooseIndep:
			if wsize == 0 {
				break
			}
			recurseToLeaf := cur.Op == OpChooseLeafFirstN || cur.Op == OpChooseLeafIndep

			osize := 0
			for i := 0; i < wsize; i++ {
				numrep := cur.Arg1
				if numrep <= 0 {
					numrep += int32(resultMax)
					if numrep <= 0 {
						continue
					}
				}
				j := 0
				bno := -1 - wbuf[i]
				if bno < 0 || bno >= m.MaxBuckets || m.Buckets[bno] == nil {
					continue // w[i] is probably ItemNone
				}

				if firstn {
					var recurseTries uint32
					if chooseLeafTries != 0 {
						recurseTries = chooseLeafTries
					} else if m.ChooseLeafDescendOnce != 0 {
						recurseTries = 1
					} else {
						recurseTries = chooseTries
					}
					n := m.chooseFirstn(w, m.Buckets[bno], weights, x, numrep, cur.Arg2,
						obuf[osize:], j, resultMax-osize,
						chooseTries, recurseTries,
						chooseLocalRetries, chooseLocalFallbackRetries,
						recurseToLeaf, varyR, stable, c[osize:], 0)
					osize += n - j
				} else {
					outSize := int(numrep)
					if outSize > resultMax-osize {
						outSize = resultMax - osize
					}
					recurseTries := chooseLeafTries
					if recurseTries == 0 {
						recurseTries = 1
					}
					m.chooseIndep(w, m.Buckets[bno], weights, x, int32(outSize), numrep, cur.Arg2,
						obuf[osize:], j,
						chooseTries, recurseTries, recurseToLeaf, c[osize:], 0)
					osize += outSize
				}
			}

			if recurseToLeaf {
				// Copy the final leaf values into the output set.
				copy(obuf[:osize], c[:osize])
			}

			// Swap o and w buffers.
			wbuf, obuf = obuf, wbuf
			wsize = osize

		case OpEmit:
			for i := 0; i < wsize && resultLen < resultMax; i++ {
				result[resultLen] = wbuf[i]
				resultLen++
			}
			wsize = 0

		default:
			// Unknown op: skip, as Ceph does.
		}
	}

	return resultLen
}

// Placement is a convenience wrapper: run rule ruleno for x and return a
// fresh slice of placed device ids. For the allocation-free hot path, call
// DoRule with a reusable result buffer.
func (m *Map) Placement(w *Work, ruleno, x int32, weights []uint32, resultMax int) []int32 {
	res := make([]int32, resultMax)
	n := m.DoRule(w, ruleno, x, res, weights)
	return res[:n]
}
