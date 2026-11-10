package crush

// Map is the immutable CRUSH mapping: flat bucket/rule tables indexed by id,
// exactly like struct crush_map. Buckets are indexed by -1-id (nil holes
// allowed), rules by number. Build once with MapBuilder, then share across
// goroutines; per-goroutine mutable state lives in Work.
type Map struct {
	MaxBuckets int32
	MaxRules   int32
	MaxDevices int32

	Buckets []*Bucket // index = -1 - bucket id
	Rules   []*Rule

	Tunables

	// Weight is the per-device weight table (16.16 fixed point) used for
	// "is out" checks: index = device id.
	Weight []uint32
}

// Work is the per-worker scratch state (Ceph's crush_work). It is NOT safe
// for concurrent use; allocate one per goroutine. All buffers are carved out
// of an Arena (raw mmap'd memory outside the Go heap), so mapping after
// NewWork performs no allocations and feeds the GC nothing: the entire
// working set is GC-invisible.
type Work struct {
	arena   *Arena       // owned when NewWork created it; nil if caller-owned
	buckets []WorkBucket // pointer data stays on the Go heap (write barriers);
	// only the int32 buffers it points to are arena (GC-invisible) memory

	// do_rule scratch: w/o swap between a and b; c is fixed.
	a, b, c []int32
}

// NewWork prepares per-worker state for m. resultMax is the largest result
// slice that will be passed to DoRule. The arena carries every buffer; pass
// nil for a private 64 KiB arena owned by the Work (call Work.Free to
// release it). Passing a caller-owned arena allows pooling/reuse across Work
// instances (the arena must not be Reset while the Work is in use, and must
// outlive it).
func NewWork(m *Map, resultMax int, arena *Arena) *Work {
	own := arena == nil
	if own {
		arena = NewArena(64 << 10)
	}
	w := &Work{
		arena:   arena,
		buckets: make([]WorkBucket, len(m.Buckets)),
		a:       arena.AllocInt32(resultMax),
		b:       arena.AllocInt32(resultMax),
		c:       arena.AllocInt32(resultMax),
	}
	for i, b := range m.Buckets {
		if b == nil {
			continue
		}
		// Ceph allocates the permutation for every bucket because the
		// firstn local-fallback path permutes any bucket type.
		perm := arena.AllocInt32(b.size())
		if perm == nil {
			perm = arena.AllocInt32(1)
		}
		w.buckets[i].perm = perm
	}
	return w
}

// Free releases the arena if NewWork owns it.
func (w *Work) Free() {
	if w.arena != nil {
		w.arena.Free()
	}
}

func (b *Bucket) size() int { return len(b.Items) }

func (w *Work) wb(b *Bucket) *WorkBucket {
	return &w.buckets[-1-b.ID]
}

// isOut reports whether device item is marked "out" (failed, fully
// offloaded) for input x. Port of is_out() from mapper.c.
func (m *Map) isOut(weight []uint32, item, x int32) bool {
	if int32(len(weight)) <= item {
		return true
	}
	w := weight[item]
	if w >= WeightOne {
		return false
	}
	if w == 0 {
		return true
	}
	return int32(hash32_2(HashRjenkins1, uint32(x), uint32(item))&0xffff) >= int32(w)
}

// chooseFirstn is crush_choose_firstn from mapper.c: choose numrep distinct
// items of type typ from bucket, writing into out starting at outpos; with
// recurseToLeaf the leaves are written to out2. Returns the new outpos.
func (m *Map) chooseFirstn(w *Work, bucket *Bucket, weight []uint32,
	x, numrep, typ int32, out []int32, outpos, outSize int,
	tries, recurseTries, localRetries, localFallbackRetries uint32,
	recurseToLeaf bool, varyR, stable uint32, out2 []int32, parentR int32) int {

	count := outSize
	repStart := int32(outpos)
	if stable != 0 {
		repStart = 0
	}

	for rep := repStart; rep < numrep && count > 0; rep++ {
		var item int32
		var ftotal, flocal uint32
		skipRep := false
		in := bucket

		for { // descent retries
			retryDescent := false
			in = bucket
			flocal = 0 // reset per descent attempt (mapper.c)

			for { // bucket-local retries
				retryBucket := false
				collide := false
				reject := false

				r := rep + parentR + int32(ftotal)

				if in.size() == 0 {
					reject = true
				} else {
					if localFallbackRetries > 0 &&
						flocal >= uint32(in.size()>>1) &&
						flocal > localFallbackRetries {
						// Exhaustive bucket search via permutation.
						s := w.wb(in).bucketPermute(in.Hash, x, r, in.ID, int32(in.size()))
						item = in.Items[s]
					} else {
						item = in.Choose(w.wb(in), x, r)
					}

					if item >= m.MaxDevices {
						skipRep = true
						break
					}

					itemtype := int32(0)
					if item < 0 {
						idx := -1 - item
						if idx >= m.MaxBuckets {
							skipRep = true
							break
						}
						itemtype = m.Buckets[idx].Type
					}

					if itemtype != typ {
						if item >= 0 || (-1-item) >= m.MaxBuckets {
							skipRep = true
							break
						}
						in = m.Buckets[-1-item]
						retryBucket = true
						continue
					}

					for i := 0; i < outpos; i++ {
						if out[i] == item {
							collide = true
							break
						}
					}

					if !collide && recurseToLeaf {
						if item < 0 {
							var subR int32
							if varyR > 0 {
								subR = r >> (varyR - 1)
							}
							nr := int32(outpos) + 1
							if stable != 0 {
								nr = 1
							}
							n := m.chooseFirstn(w, m.Buckets[-1-item], weight, x, nr, 0,
								out2, outpos, count, recurseTries, 0,
								localRetries, localFallbackRetries,
								false, varyR, stable, nil, subR)
							if n <= outpos {
								reject = true
							}
						} else {
							// Already a leaf.
							out2[outpos] = item
						}
					}

					if !reject && !collide && itemtype == 0 {
						reject = m.isOut(weight, item, x)
					}
				}

				// The "reject" label in mapper.c.
				if reject || collide {
					ftotal++
					flocal++
					switch {
					case collide && flocal <= localRetries:
						retryBucket = true
					case localFallbackRetries > 0 &&
						flocal <= uint32(in.size())+localFallbackRetries:
						retryBucket = true
					case ftotal < tries:
						retryDescent = true
					default:
						skipRep = true
					}
				}

				if skipRep || !retryBucket {
					break
				}
			}

			if skipRep || !retryDescent {
				break
			}
		}

		if skipRep {
			continue
		}

		out[outpos] = item
		outpos++
		count--
	}

	return outpos
}

// chooseIndep is crush_choose_indep from mapper.c: positionally stable
// breadth-first mapping. Places left items starting at outpos; leaves go to
// out2 when recurseToLeaf.
func (m *Map) chooseIndep(w *Work, bucket *Bucket, weight []uint32,
	x int32, left, numrep, typ int32, out []int32, outpos int,
	tries, recurseTries uint32, recurseToLeaf bool, out2 []int32, parentR int32) {

	endpos := outpos + int(left)
	for rep := outpos; rep < endpos; rep++ {
		out[rep] = ItemUndef
		if out2 != nil {
			out2[rep] = ItemUndef
		}
	}

	for ftotal := uint32(0); left > 0 && ftotal < tries; ftotal++ {
		for rep := outpos; rep < endpos; rep++ {
			if out[rep] != ItemUndef {
				continue
			}

			in := bucket
			for {
				// The choice is based on the output position even in
				// nested calls, spreading data movement over devices.
				r := int32(rep) + parentR
				if in.Alg == AlgUniform && int32(in.size())%numrep == 0 {
					r += (numrep + 1) * int32(ftotal)
				} else {
					r += numrep * int32(ftotal)
				}

				if in.size() == 0 {
					break // empty bucket: try again on the next ftotal
				}

				item := in.Choose(w.wb(in), x, r)
				if item >= m.MaxDevices {
					out[rep] = ItemNone
					if out2 != nil {
						out2[rep] = ItemNone
					}
					left--
					break
				}

				itemtype := int32(0)
				if item < 0 {
					if -1-item < m.MaxBuckets {
						itemtype = m.Buckets[-1-item].Type
					}
				}

				if itemtype != typ {
					if item >= 0 || (-1-item) >= m.MaxBuckets {
						out[rep] = ItemNone
						if out2 != nil {
							out2[rep] = ItemNone
						}
						left--
						break
					}
					in = m.Buckets[-1-item]
					continue
				}

				collide := false
				for i := outpos; i < endpos; i++ {
					if out[i] == item {
						collide = true
						break
					}
				}
				if collide {
					break
				}

				if recurseToLeaf {
					if item < 0 {
						m.chooseIndep(w, m.Buckets[-1-item], weight, x, 1, numrep, 0,
							out2, rep, recurseTries, 0, false, nil, r)
						if out2[rep] == ItemNone {
							break // placed nothing; no leaf
						}
					} else if out2 != nil {
						out2[rep] = item
					}
				}

				if itemtype == 0 && m.isOut(weight, item, x) {
					break
				}

				out[rep] = item
				left--
				break
			}
		}
	}

	for rep := outpos; rep < endpos; rep++ {
		if out[rep] == ItemUndef {
			out[rep] = ItemNone
		}
		if out2 != nil && out2[rep] == ItemUndef {
			out2[rep] = ItemNone
		}
	}
}
