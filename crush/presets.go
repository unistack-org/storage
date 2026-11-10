package crush

// Ready-made rule presets so operators never hand-write CRUSH rule text.
// Each preset returns a *Rule ready to pass to MapBuilder.Build for the
// crush engine. (The text/JSON decoders have their own AST types —
// Wrapper, RuleDecl — so hand-write text there, or build a Map via
// MapBuilder directly.)
// The pool/volume "size" (replica or shard count) is supplied by the
// caller at placement time as len(result): every preset uses numrep=0
// ("as many as the pool asks for"), like Ceph's stock rules.
//
// Mapping to Vitastor pool options (docs/config/pool.ru.md):
//
//   - RackAwareRep3    ~ failure_domain=rack: every replica lands in a
//     distinct bucket of the chosen type (distinct racks; pass the DC
//     type for distinct data centers).
//   - EC2Plus2AcrossDCs ~ level_placement dc=NN...N: erasure shards placed
//     independently across data centers. numrep=0 makes the shard count
//     equal the pool size (k+m), and "indep" forces each shard into a DC
//     untouched by the other shards, so k+m DCs are required. Vitastor's
//     uneven dc=112233 (several shards per DC) has no single-rule CRUSH
//     analogue: build one rule per shard group, each taking the group's
//     DC bucket (see DCPreferred).
//   - DCPreferred      ~ dc ?= "x": placement pinned inside one named DC;
//     create one rule per DC. Note DoRule addresses rules by position in
//     the rules slice (Ruleset is Ceph-compat metadata clients may mirror),
//     so build the slice in a known order and point a volume at its DC's
//     rule index.

// RackAwareRep3 returns a replicated rule whose replicas land in distinct
// buckets of type failureType (e.g. the "rack" type id) under rootID.
// It is Ceph's stock replicated rule with the failure domain moved from
// host to rack: three replicas occupy three distinct racks. CRUSH only
// guarantees distinctness at the chosen domain level — replicas may still
// share a data center above the racks. To spread replicas across DCs,
// pass the datacenter type id as failureType (a DC is then the rack).
// The rule applies to any pool size from 1 to 10 (choose the count at
// placement time).
func RackAwareRep3(rootID, failureType int32) *Rule {
	return &Rule{
		Ruleset: 0,
		Type:    RuleTypeReplicated,
		MinSize: 1,
		MaxSize: 10,
		Steps: []Step{
			{Op: OpTake, Arg1: rootID},
			{Op: OpChooseLeafFirstN, Arg1: 0, Arg2: failureType},
			{Op: OpEmit},
		},
	}
}

// EC2Plus2AcrossDCs returns an erasure rule that spreads the pool's
// shards (k+m of them, i.e. the placement-time result size) one per
// distinct bucket of type dcType (the "datacenter" type id) under rootID.
// For a 2+2 pool that means four shards in four distinct DCs: losing a
// whole DC costs one shard, and 2+2 tolerates the loss of any two.
// Requires at least k+m DCs. The set_choose*_tries bumps match Ceph's
// standard erasure profile: independence across many domains needs more
// placement attempts than plain firstn.
func EC2Plus2AcrossDCs(rootID, dcType int32) *Rule {
	return &Rule{
		Ruleset: 0,
		Type:    RuleTypeErasure,
		MinSize: 1,
		MaxSize: 10,
		Steps: []Step{
			{Op: OpTake, Arg1: rootID},
			{Op: OpSetChooseLeafTries, Arg1: 5},
			{Op: OpSetChooseTries, Arg1: 100},
			{Op: OpChooseLeafIndep, Arg1: 0, Arg2: dcType},
			{Op: OpEmit},
		},
	}
}

// DCPreferred returns a replicated rule pinned inside one domain bucket
// (a datacenter's bucket id, dcBucketID): replicas are chosen from that
// bucket only, spreading over distinct buckets of type failureType
// (racks) within it. Create one rule per DC; DoRule addresses rules by
// position in the rules slice, so build the slice in a known order and
// route a volume's PGs to the index of its preferred DC's rule — the
// CRUSH analogue of Vitastor's `dc ?= "x"`. The ruleset field carries
// the DC id as Ceph-compatible metadata for tooling.
func DCPreferred(ruleset uint8, dcBucketID, failureType int32) *Rule {
	return &Rule{
		Ruleset: ruleset,
		Type:    RuleTypeReplicated,
		MinSize: 1,
		MaxSize: 10,
		Steps: []Step{
			{Op: OpTake, Arg1: dcBucketID},
			{Op: OpChooseLeafFirstN, Arg1: 0, Arg2: failureType},
			{Op: OpEmit},
		},
	}
}
