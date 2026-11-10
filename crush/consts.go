package crush

// Constants mirror Ceph's src/crush/crush.h. They MUST stay byte-compatible:
// CRUSH placement is only reproducible across implementations when every
// constant, hash and arithmetic step matches the reference.
const (
	// Magic is CRUSH_MAGIC: every binary-serialized crush map starts with it.
	Magic = uint32(0x00010000)

	// ItemUndef marks an output slot that no placement attempt has filled yet.
	ItemUndef = int32(0x7ffffffe)
	// ItemNone marks a slot that was attempted but could not be placed.
	ItemNone = int32(0x7fffffff)

	// Bucket algorithm ids.
	AlgUniform = uint32(0)
	AlgList    = uint32(1)
	AlgTree    = uint32(2)
	AlgStraw   = uint32(3)
	AlgStraw2  = uint32(4)

	// Hash ids. Only rjenkins1 exists.
	HashRjenkins1 = uint32(0)

	// Rule opcodes.
	OpNoop                        = uint32(0)
	OpTake                        = uint32(1)
	OpChooseFirstN                = uint32(2)
	OpChooseIndep                 = uint32(3)
	OpEmit                        = uint32(4)
	OpChooseLeafFirstN            = uint32(6)
	OpChooseLeafIndep             = uint32(7)
	OpSetChooseTries              = uint32(8)
	OpSetChooseLeafTries          = uint32(9)
	OpSetChooseLocalTries         = uint32(10)
	OpSetChooseLocalFallbackTries = uint32(11)
	OpSetChooseLeafVaryR          = uint32(12)
	OpSetChooseLeafStable         = uint32(13)
	OpSetMSRDescents              = uint32(14)
	OpSetMSRCollisionTries        = uint32(15)
	OpChooseMSR                   = uint32(16)

	// Rule types.
	RuleTypeReplicated = uint8(1)
	RuleTypeErasure    = uint8(3)
	RuleTypeMSRFirstN  = uint8(4)
	RuleTypeMSRIndep   = uint8(5)

	MaxRuleSteps = 32
	MaxRules     = 256
	MaxDepth     = 10
	MaxDepthT    = 11

	// WeightOne is 1.0 in the 16.16 fixed point used for device weights.
	WeightOne = uint32(0x10000)

	// MaxDeviceWeight / MaxBucketWeight bound 16.16 fixed-point weights
	// so that sums of buckets cannot overflow u32.
	MaxDeviceWeight = uint64(0xffffffff)>>16 - 1
	MaxBucketWeight = uint64(0xffffffff)>>16 - 2
)

// Tunables carries the crush tunable knobs. The zero value equals the
// "legacy"/"argonaut" profile; use TunablesOptimal for modern Ceph defaults.
type Tunables struct {
	ChooseLocalTries         uint32
	ChooseLocalFallbackTries uint32
	ChooseTotalTries         uint32
	ChooseLeafDescendOnce    uint32
	ChooseLeafVaryR          uint32
	StrawCalcVersion         uint32
	AllowedBucketAlgs        uint32
	ChooseLeafStable         uint32
	MSRDescents              uint32
	MSRCollisionTries        uint32
}

// TunablesLegacy is the Ceph "legacy"/"argonaut" profile.
func TunablesLegacy() Tunables {
	return Tunables{
		ChooseLocalTries:         2,
		ChooseLocalFallbackTries: 5,
		ChooseTotalTries:         19,
		AllowedBucketAlgs:        algMask(AlgUniform) | algMask(AlgList) | algMask(AlgStraw),
	}
}

// TunablesBobtail is the Ceph "bobtail" profile.
func TunablesBobtail() Tunables {
	return Tunables{
		ChooseTotalTries:      50,
		ChooseLeafDescendOnce: 1,
		AllowedBucketAlgs:     algMask(AlgUniform) | algMask(AlgList) | algMask(AlgStraw) | algMask(AlgTree),
	}
}

// TunablesFirefly is the Ceph "firefly" profile (bobtail + chooseleaf_vary_r).
func TunablesFirefly() Tunables {
	t := TunablesBobtail()
	t.ChooseLeafVaryR = 1
	return t
}

// TunablesHammer is the Ceph "hammer" profile (firefly + straw2 buckets).
func TunablesHammer() Tunables {
	t := TunablesFirefly()
	t.AllowedBucketAlgs = algMask(AlgUniform) | algMask(AlgList) | algMask(AlgStraw) | algMask(AlgTree) | algMask(AlgStraw2)
	return t
}

// TunablesOptimal is the Ceph "optimal"/"jewel" profile (hammer + chooseleaf_stable).
func TunablesOptimal() Tunables {
	t := TunablesHammer()
	t.ChooseLeafStable = 1
	return t
}

func algMask(a uint32) uint32 { return 1 << a }
