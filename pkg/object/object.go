// Package object defines the fundamental addressing unit of the storage:
// a fixed-size object addressed by a 64-bit ObjectID (OID), the same
// addressing model used by Sheepdog (2–4 MiB objects) and Vitastor (4 MiB).
//
// A volume is a contiguous byte range mapped onto objects:
//
//	object index = offset >> shift
//	offset in object = offset & (objectSize - 1)
//
// The shift-based split keeps the hot path branch-free and allocation-free.
package object

// SizeShift is the base-2 logarithm of the object size. 22 = 4 MiB, the
// Vitastor standard; it divides evenly into typical 4 KiB storage block
// sizes and keeps per-object metadata and replication records small.
const SizeShift = 22

// Size is the object size in bytes.
const Size = uint64(1) << SizeShift

// Mask is the in-object offset mask (Size-1).
const Mask = Size - 1

// OID is a 64-bit ObjectID. The cluster-level allocator decides how OIDs
// are assigned to (volume, index) pairs; within a single node a volume
// commonly uses OID = volumeBase + index for contiguity.
type OID uint64

// NilOID is the reserved "no object" value.
const NilOID = OID(0)

// Split maps a logical byte offset within a volume to the object index and
// the byte offset inside that object.
func Split(off uint64) (index, inObject uint64) {
	return off >> SizeShift, off & Mask
}

// Offset converts an object index back to the volume byte offset of the
// object's start.
func Offset(index uint64) uint64 { return index << SizeShift }

// NumObjects returns how many objects are needed to cover size bytes.
func NumObjects(size uint64) uint64 { return (size + Mask) >> SizeShift }
