package crush

import "math"

// MapBuilder assembles a Map from bucket descriptions, mirroring the
// builder.c functions (crush_make_*_bucket, crush_calc_straw).
type MapBuilder struct {
	buckets  map[int32]*Bucket
	types    map[string]int32
	devices  map[string]int32
	maxRules int32
	tunables Tunables
}

// NewMapBuilder starts an empty map builder.
func NewMapBuilder() *MapBuilder {
	return &MapBuilder{
		buckets:  make(map[int32]*Bucket),
		types:    make(map[string]int32),
		devices:  make(map[string]int32),
		maxRules: MaxRules,
		tunables: TunablesOptimal(),
	}
}

func (mb *MapBuilder) SetTunables(t Tunables) { mb.tunables = t }
func (mb *MapBuilder) Tunables() Tunables     { return mb.tunables }

// AddType registers a device-type name ("osd", "host", ...) with its level.
func (mb *MapBuilder) AddType(name string, id int32) { mb.types[name] = id }

// AddDevice registers a device (osd) id; weights are supplied to Build.
func (mb *MapBuilder) AddDevice(name string, id int32) { mb.devices[name] = id }

func (mb *MapBuilder) DeviceID(name string) (int32, bool) {
	id, ok := mb.devices[name]
	return id, ok
}

func (mb *MapBuilder) TypeID(name string) (int32, bool) {
	id, ok := mb.types[name]
	return id, ok
}

// ItemSpec is one child of a bucket being built.
type ItemSpec struct {
	ID     int32
	Weight uint32 // 16.16 fixed point
}

// AddUniform builds a uniform bucket (all items share itemWeight).
func (mb *MapBuilder) AddUniform(id, typ int32, hash uint32, items []ItemSpec, itemWeight uint32) error {
	b := &Bucket{ID: id, Type: typ, Alg: AlgUniform, Hash: hash, ItemWeight: itemWeight}
	b.Items = make([]int32, len(items))
	for i, it := range items {
		b.Items[i] = it.ID
	}
	b.Weight = uint32(len(items)) * itemWeight
	return mb.addBucket(b)
}

// AddList builds a list bucket; sum_weights are cumulative.
func (mb *MapBuilder) AddList(id, typ int32, hash uint32, items []ItemSpec) error {
	b := &Bucket{ID: id, Type: typ, Alg: AlgList, Hash: hash}
	n := len(items)
	b.Items = make([]int32, n)
	b.ItemW = make([]uint32, n)
	b.SumW = make([]uint32, n)
	var w uint64
	for i, it := range items {
		b.Items[i] = it.ID
		b.ItemW[i] = it.Weight
		w += uint64(it.Weight)
		if w > 0xffffffff {
			w = 0xffffffff
		}
		b.SumW[i] = uint32(w)
	}
	b.Weight = uint32(w)
	return mb.addBucket(b)
}

// AddTree builds a binary tree bucket in leaf order.
func (mb *MapBuilder) AddTree(id, typ int32, hash uint32, items []ItemSpec) error {
	b := &Bucket{ID: id, Type: typ, Alg: AlgTree, Hash: hash}
	n := len(items)
	b.Items = make([]int32, n)
	if n == 0 {
		return mb.addBucket(b)
	}

	depth := calcDepth(n)
	b.NumNodes = uint32(1 << depth)
	b.NodeW = make([]uint32, b.NumNodes)

	for i, it := range items {
		b.Items[i] = it.ID
		node := crushCalcTreeNode(i)
		b.NodeW[node] = it.Weight
		b.Weight += it.Weight
		for j := 1; j < depth; j++ {
			node = treeBuilderParent(node)
			b.NodeW[node] += it.Weight
		}
	}
	return mb.addBucket(b)
}

// AddStraw builds a straw bucket and precomputes straws with
// crush_calc_straw (honouring mb.tunables.StrawCalcVersion).
func (mb *MapBuilder) AddStraw(id, typ int32, hash uint32, items []ItemSpec) error {
	b := &Bucket{ID: id, Type: typ, Alg: AlgStraw, Hash: hash}
	n := len(items)
	b.Items = make([]int32, n)
	b.ItemW = make([]uint32, n)
	b.Straws = make([]uint32, n)
	for i, it := range items {
		b.Items[i] = it.ID
		b.ItemW[i] = it.Weight
		b.Weight += it.Weight
	}
	crushCalcStraw(b, mb.tunables.StrawCalcVersion)
	return mb.addBucket(b)
}

// AddStraw2 builds a straw2 bucket (integer fixed-point draws, exactly as
// Ceph; this is where the old crushkv float64 math diverged).
func (mb *MapBuilder) AddStraw2(id, typ int32, hash uint32, items []ItemSpec) error {
	b := &Bucket{ID: id, Type: typ, Alg: AlgStraw2, Hash: hash}
	n := len(items)
	b.Items = make([]int32, n)
	b.ItemW = make([]uint32, n)
	for i, it := range items {
		b.Items[i] = it.ID
		b.ItemW[i] = it.Weight
		b.Weight += it.Weight
	}
	return mb.addBucket(b)
}

func (mb *MapBuilder) addBucket(b *Bucket) error {
	if b.ID >= 0 {
		return errBucketID
	}
	pos := -1 - b.ID
	mb.buckets[b.ID] = b
	if int32(len(mb.buckets)) > mb.maxBuckets() {
		// maxBuckets tracked at Build time
	}
	_ = pos
	return nil
}

func (mb *MapBuilder) maxBuckets() int32 { return int32(len(mb.buckets)) }

// Bucket returns a bucket added so far (used by compilers that need the
// builder-computed bucket weight when wiring parent item weights).
func (mb *MapBuilder) Bucket(id int32) (*Bucket, bool) {
	b, ok := mb.buckets[id]
	return b, ok
}

// Build freezes the buckets into an immutable Map. maxDevices bounds the
// device-id space; weights carries the current per-device weights (a copy
// is kept; mutate via a fresh Build when weights change wholesale).
func (mb *MapBuilder) Build(rules []*Rule, maxDevices int32, weights []uint32) *Map {
	var maxIdx int32 = -1
	for id := range mb.buckets {
		if -1-id > maxIdx {
			maxIdx = -1 - id
		}
	}
	size := maxIdx + 1
	m := &Map{
		MaxBuckets: size,
		MaxRules:   int32(len(rules)),
		MaxDevices: maxDevices,
		Buckets:    make([]*Bucket, size),
		Rules:      rules,
		Tunables:   mb.tunables,
		Weight:     weights,
	}
	for id, b := range mb.buckets {
		m.Buckets[-1-id] = b
	}
	return m
}

// crushCalcTreeNode is the leaf node number for leaf i in a binary tree.
func crushCalcTreeNode(i int) int32 { return int32(((i + 1) << 1) - 1) }

func treeBuilderHeight(n int32) int {
	h := 0
	for n&1 == 0 {
		h++
		n >>= 1
	}
	return h
}

func treeBuilderParent(n int32) int32 {
	h := treeBuilderHeight(n)
	if n&(1<<uint(h+1)) != 0 { // on_right
		return n - int32(1)<<uint(h)
	}
	return n + int32(1)<<uint(h)
}

func calcDepth(size int) int {
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

// crushCalcStraw is crush_calc_straw from builder.c (both v0 and v1),
// precomputing the straw multipliers for a straw bucket.
func crushCalcStraw(b *Bucket, version uint32) {
	size := len(b.Items)
	if size == 0 {
		return
	}
	weights := b.ItemW
	reverse := make([]int, size)
	if size > 0 {
		reverse[0] = 0
	}
	// Reverse sort by weight (simple insertion sort, matching Ceph exactly
	// including the equal-weight ordering quirks).
	for i := 1; i < size; i++ {
		inserted := false
		for j := 0; j < i; j++ {
			if weights[i] < weights[reverse[j]] {
				for k := i; k > j; k-- {
					reverse[k] = reverse[k-1]
				}
				reverse[j] = i
				inserted = true
				break
			}
		}
		if !inserted { // j == i in C
			reverse[i] = i
		}
	}

	numleft := size
	straw := 1.0
	wbelow := 0.0
	lastw := 0.0

	i := 0
	for i < size {
		if version == 0 {
			if weights[reverse[i]] == 0 {
				b.Straws[reverse[i]] = 0
				i++
				continue
			}
			b.Straws[reverse[i]] = uint32(straw * 0x10000)
			i++
			if i == size {
				break
			}
			if weights[reverse[i]] == weights[reverse[i-1]] {
				continue
			}
			wbelow += (float64(weights[reverse[i-1]]) - lastw) * float64(numleft)
			for j := i; j < size; j++ {
				if weights[reverse[j]] == weights[reverse[i]] {
					numleft--
				} else {
					break
				}
			}
			wnext := float64(numleft) * float64(int64(weights[reverse[i]])-int64(weights[reverse[i-1]]))
			pbelow := wbelow / (wbelow + wnext)
			straw *= math.Pow(1.0/pbelow, 1.0/float64(numleft))
			lastw = float64(weights[reverse[i-1]])
		} else {
			if weights[reverse[i]] == 0 {
				b.Straws[reverse[i]] = 0
				i++
				numleft--
				continue
			}
			b.Straws[reverse[i]] = uint32(straw * 0x10000)
			i++
			if i == size {
				break
			}
			wbelow += (float64(weights[reverse[i-1]]) - lastw) * float64(numleft)
			numleft--
			wnext := float64(numleft) * float64(int64(weights[reverse[i]])-int64(weights[reverse[i-1]]))
			pbelow := wbelow / (wbelow + wnext)
			straw *= math.Pow(1.0/pbelow, 1.0/float64(numleft))
			lastw = float64(weights[reverse[i-1]])
		}
	}
}

var errBucketID = &CrushError{"bucket id must be negative"}

// CrushError is a simple error type for the crush package.
type CrushError struct{ Msg string }

func (e *CrushError) Error() string { return e.Msg }
