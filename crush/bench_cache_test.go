package crush_test

import (
	"testing"

	"github.com/unistack-org/crushkv/crush"
)

// The "CRUSH is heavier than Vitastor" claim is about the raw cost of
// computing a placement per object. Ceph never does that: it computes
// CRUSH once per PG per map epoch and caches pg->OSD. These benchmarks
// measure the hot path of the cached design (hash oid -> pg, table
// lookup) so it can be compared against the raw DoRule cost.

const benchPGNum = 4096 // pgs, power of two so the modulo is a mask

var (
	pgTable [benchPGNum][3]int32 // pg -> replica osds (one epoch)
	pgMap   = func() map[int32][3]int32 {
		m := make(map[int32][3]int32, benchPGNum)
		for i := range pgTable {
			pgTable[i] = [3]int32{int32(i), int32(i) + 1, int32(i) + 2}
			m[int32(i)] = pgTable[i]
		}
		return m
	}()
)

// fibonacciHash is what an OID->pg hash costs: one multiply + shift.
// (xxhash on 8-byte keys is the same order.)
func fibonacciHash(oid uint64) uint32 {
	return uint32((oid * 11400714819323198485) >> 44)
}

var cacheSink [3]int32

func BenchmarkPGCacheArray(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		oid := uint64(i) * 0x9E3779B97F4A7C15
		pg := fibonacciHash(oid) % benchPGNum
		cacheSink = pgTable[pg]
	}
}

func BenchmarkPGCacheMap(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		oid := uint64(i) * 0x9E3779B97F4A7C15
		pg := int32(fibonacciHash(oid) % benchPGNum)
		cacheSink = pgMap[pg]
	}
}

// BenchmarkPGRebuild measures the amortized CRUSH cost: recomputing the
// whole pg->OSD table for all 4096 pgs (rep3) after a map epoch change.
// On a running cluster this happens on failures/topology changes, not on
// the I/O path.
func BenchmarkPGRebuild(b *testing.B) {
	m := buildThreeTier(3, 3, 5, 5) // 225 osds
	w := crush.NewWork(m, 10, nil)
	defer w.Free()
	res := make([]int32, 3)
	b.ReportAllocs()
	b.SetBytes(benchPGNum)
	for i := 0; i < b.N; i++ {
		for pg := 0; pg < benchPGNum; pg++ {
			m.DoRule(w, 0, int32(pg), res, m.Weight)
		}
	}
}
