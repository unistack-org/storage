package crush

// Hash dispatch, mirroring crush_hash32_2/3/4/5 in crush.h. Only
// rjenkins1 exists in Ceph; the type field is carried through every
// bucket so future hashes can be added without touching call sites.

func hash32_2(hashType, a, b uint32) uint32 {
	switch hashType {
	case HashRjenkins1:
		return hash2(a, b)
	default:
		return hash2(a, b)
	}
}

func hash32_3(hashType, a, b, c uint32) uint32 {
	switch hashType {
	case HashRjenkins1:
		return hash3(a, b, c)
	default:
		return hash3(a, b, c)
	}
}

func hash32_4(hashType, a, b, c, d uint32) uint32 {
	switch hashType {
	case HashRjenkins1:
		return hash4(a, b, c, d)
	default:
		return hash4(a, b, c, d)
	}
}
