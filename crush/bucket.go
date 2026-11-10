package crush

import "math"

// Bucket is one CRUSH bucket (a node of the device hierarchy). The layout is
// intentionally pointer-friendly to the GC: Items and the per-algorithm
// payload slices are the only references, all arithmetic data is in flat
// integer slices. A Map (and its buckets) is immutable after build, so any
// number of goroutines may call Choose on it concurrently, each with its own
// Work.
type Bucket struct {
	ID     int32
	Type   int32
	Alg    uint32
	Hash   uint32
	Weight uint32 // 16.16 fixed point, sum of children
	Items  []int32

	// Alg-specific payloads.
	ItemWeight uint32   // uniform: common weight of every item
	ItemW      []uint32 // list/straw/straw2: per-item weights
	SumW       []uint32 // list: running weight sums
	NumNodes   uint32   // tree: 1 << depth
	NodeW      []uint32 // tree: binary-tree node weights
	Straws     []uint32 // straw: precomputed draw multipliers
}

// WorkBucket is the per-worker mutable state of a bucket (the permutation
// cache from Ceph's crush_work_bucket). Keeping it out of Bucket is what
// makes the map itself lock-free for readers.
type WorkBucket struct {
	permX uint32
	permN uint32
	perm  []int32
}

// bucketPermute is bucket_perm_choose from mapper.c: returns s, the pr-th
// element of a random permutation of [0,size) seeded by x.
func (wb *WorkBucket) bucketPermute(hash uint32, x, r, id, size int32) uint32 {
	pr := uint32(r) % uint32(size)
	var s uint32

	if wb.permX != uint32(x) || wb.permN == 0 {
		wb.permX = uint32(x)

		// Optimize the common r == 0 case.
		if pr == 0 {
			s = hash32_3(hash, uint32(x), uint32(id), 0) % uint32(size)
			wb.perm[0] = int32(s)
			wb.permN = 0xffff // magic value, see below
			return s
		}

		for i := uint32(0); i < uint32(size); i++ {
			wb.perm[i] = int32(i)
		}
		wb.permN = 0
	} else if wb.permN == 0xffff {
		// Clean up after the r == 0 case above.
		for i := uint32(1); i < uint32(size); i++ {
			wb.perm[i] = int32(i)
		}
		wb.perm[wb.perm[0]] = 0
		wb.permN = 1
	}

	// Extend the permutation up to pr.
	for wb.permN <= pr {
		p := wb.permN
		// No point in swapping the final entry.
		if p < uint32(size)-1 {
			i := hash32_3(hash, uint32(x), uint32(id), p) % (uint32(size) - p)
			if i != 0 {
				wb.perm[p], wb.perm[p+i] = wb.perm[p+i], wb.perm[p]
			}
		}
		wb.permN++
	}
	return uint32(wb.perm[pr])
}

func (b *Bucket) uniformChoose(wb *WorkBucket, x, r int32) int32 {
	s := wb.bucketPermute(b.Hash, x, r, b.ID, int32(len(b.Items)))
	return b.Items[s]
}

func (b *Bucket) listChoose(x, r int32) int32 {
	for i := len(b.Items) - 1; i >= 0; i-- {
		w := uint64(hash32_4(b.Hash, uint32(x), uint32(b.Items[i]), uint32(r), uint32(b.ID)) & 0xffff)
		w *= uint64(b.SumW[i])
		w >>= 16
		if w < uint64(b.ItemW[i]) {
			return b.Items[i]
		}
	}
	// Degenerate sums: Ceph falls back to items[0].
	return b.Items[0]
}

func treeHeight(n int32) int {
	h := 0
	for n&1 == 0 {
		h++
		n >>= 1
	}
	return h
}

func (b *Bucket) treeChoose(x, r int32) int32 {
	n := int32(b.NumNodes >> 1)
	for n&1 == 0 { // while !terminal(n)
		w := b.NodeW[n]
		t := uint64(hash32_4(b.Hash, uint32(x), uint32(n), uint32(r), uint32(b.ID))) * uint64(w)
		t >>= 32

		h := treeHeight(n)
		l := n - int32(1)<<uint(h-1) // left(n)
		if t < uint64(b.NodeW[l]) {
			n = l
		} else {
			n = n + int32(1)<<uint(h-1) // right(n)
		}
	}
	return b.Items[n>>1]
}

func (b *Bucket) strawChoose(x, r int32) int32 {
	var highDraw uint64
	high := 0
	for i := range b.Items {
		draw := uint64(hash32_3(b.Hash, uint32(x), uint32(b.Items[i]), uint32(r))&0xffff) * uint64(b.Straws[i])
		if i == 0 || draw > highDraw {
			high = i
			highDraw = draw
		}
	}
	return b.Items[high]
}

func (b *Bucket) straw2Choose(x, r int32) int32 {
	var highDraw int64
	high := 0
	for i := range b.Items {
		var draw int64
		if b.ItemW[i] != 0 {
			u := hash32_3(b.Hash, uint32(x), uint32(b.Items[i]), uint32(r)) & 0xffff
			// draw = ln(u)/weight, both in fixed point; ln is negative,
			// so a larger weight yields a larger (less negative) draw.
			draw = (crushLn(u) - 0x1000000000000) / int64(b.ItemW[i])
		} else {
			draw = math.MinInt64
		}
		if i == 0 || draw > highDraw {
			high = i
			highDraw = draw
		}
	}
	return b.Items[high]
}

// Choose picks one item of the bucket for input x at round r. wb carries the
// per-worker permutation cache (required for uniform buckets; may be nil for
// the other algorithms).
func (b *Bucket) Choose(wb *WorkBucket, x, r int32) int32 {
	switch b.Alg {
	case AlgUniform:
		return b.uniformChoose(wb, x, r)
	case AlgList:
		return b.listChoose(x, r)
	case AlgTree:
		return b.treeChoose(x, r)
	case AlgStraw:
		return b.strawChoose(x, r)
	case AlgStraw2:
		return b.straw2Choose(x, r)
	default:
		return b.Items[0]
	}
}
