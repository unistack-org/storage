package crush

import "math/bits"

// crushLn computes 2^44 * log2(xin + 1) using the same fixed-point lookup
// tables as Ceph (mapper.c crush_ln). The result is a signed 64-bit value
// in the range [-2^44*11.09, 2^44]; it is the integer core of the straw2
// algorithm. Using float64 math here (as the original crushkv did) breaks
// placement compatibility with Ceph, so this port is arithmetic-exact.
func crushLn(xin uint32) int64 {
	x := xin + 1

	iexpon := 15
	if x&0x18000 == 0 {
		// Normalize: shift the significant bits up into [2^16, 2^17).
		bits32 := bits.LeadingZeros32(x&0x1FFFF) - 16
		x <<= uint(bits32)
		iexpon = 15 - bits32
	}

	index1 := (x >> 8) << 1
	// RH ~ 2^56/index1
	rh := rhLHTbl[index1-256]
	// LH ~ 2^48 * log2(index1/256)
	lh := rhLHTbl[index1+1-256]

	// RH*x ~ 2^48 * (2^15 + xf), xf < 2^8
	xl64 := int64(x) * rh
	xl64 >>= 48

	result := int64(iexpon)
	result <<= 12 + 32

	index2 := xl64 & 0xff
	// LL ~ 2^48 * log2(1.0 + index2/2^15)
	ll := llTbl[index2]

	lh += ll
	lh >>= 48 - 12 - 32
	result += lh

	return result
}
