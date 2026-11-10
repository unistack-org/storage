package crush

const (
	MaxVal   = uint32(0xFFFFFFFF)
	HashSeed = uint32(1315423911)
)

func hash1(a uint32) uint32 {
	hash := HashSeed ^ a
	x := uint32(231232)
	y := uint32(1232)
	b := a
	hashMix(&b, &x, &hash)
	hashMix(&y, &a, &hash)

	return hash
}

func hash2(a, b uint32) uint32 {
	hash := HashSeed ^ a ^ b
	var x = uint32(231232)
	var y = uint32(1232)
	hashMix(&a, &b, &hash)
	hashMix(&x, &a, &hash)
	hashMix(&b, &y, &hash)

	return hash
}

func hash3(a, b, c uint32) uint32 {
	hash := HashSeed ^ a ^ b ^ c
	x := uint32(231232)
	y := uint32(1232)
	hashMix(&a, &b, &hash)
	hashMix(&c, &x, &hash)
	hashMix(&y, &a, &hash)
	hashMix(&b, &x, &hash)
	hashMix(&y, &c, &hash)

	return hash
}

func hash4(a, b, c, d uint32) uint32 {
	hash := HashSeed ^ a ^ b ^ c ^ d
	x := uint32(231232)
	y := uint32(1232)
	hashMix(&a, &b, &hash)
	hashMix(&c, &d, &hash)
	hashMix(&a, &x, &hash)
	hashMix(&y, &b, &hash)
	hashMix(&c, &x, &hash)
	hashMix(&y, &d, &hash)

	return hash
}

func hash5(a, b, c, d, e uint32) uint32 {
	hash := HashSeed ^ a ^ b ^ c ^ d ^ e
	x := uint32(231232)
	y := uint32(1232)
	hashMix(&a, &b, &hash)
	hashMix(&c, &d, &hash)
	hashMix(&e, &x, &hash)
	hashMix(&y, &a, &hash)
	hashMix(&b, &x, &hash)
	hashMix(&y, &c, &hash)
	hashMix(&d, &x, &hash)
	hashMix(&y, &e, &hash)

	return hash
}

/*
 * Robert Jenkins' function for mixing 32-bit values
 * http://burtleburtle.net/bob/hash/evahash.html
 * a, b = random bits, c = input and output
 */
func hashMix(a, b, c *uint32) {
	(*a) -= *b
	(*a) -= *c
	*a = *a ^ (*c >> 13)
	*b -= *c
	*b -= *a
	*b = *b ^ (*a << 8)
	*c -= *a
	*c -= *b
	*c = *c ^ (*b >> 13)
	*a -= *b
	*a -= *c
	*a = *a ^ (*c >> 12)
	*b -= *c
	*b -= *a
	*b = *b ^ (*a << 16)
	*c -= *a
	*c -= *b
	*c = *c ^ (*b >> 5)
	*a -= *b
	*a -= *c
	*a = *a ^ (*c >> 3)
	*b -= *c
	*b -= *a
	*b = *b ^ (*a << 10)
	*c -= *a
	*c -= *b
	*c = *c ^ (*b >> 15)
}
