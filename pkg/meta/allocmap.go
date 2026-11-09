package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// PrefixAllocMaps holds one allocation bitmap per volume (key: volume
// name, value: AllocMapRecord): which object-granularity blocks of the
// volume currently hold data on the OSDs. It is the accounting source
// for the volume quota (plan M0.4): a write that first touches a block
// must have room under the quota, a sparse trim that clears a whole
// block gives the room back.
const PrefixAllocMaps = "allocmap"

// AllocMapRecord is a volume's allocation bitmap. Bits are per object
// (Granularity bytes, fixed cluster-wide at object.Size); the record
// lives under the volume's name. The bitmap is little-endian bit order
// (word w covers indices 64w..64w+63, LSB first), matching the client's
// in-memory representation.
type AllocMapRecord struct {
	// Rev is the record revision the bitmap was read at; the client
	// compare-and-swaps with it, so concurrent allocators never lose a
	// bit (last-writer-wins would under-count usage and let a volume
	// silently exceed its quota).
	Rev int64 `json:"rev"`
	// Bits is the packed bitmap (ceil(nbits/8) bytes, nbits = volume
	// object count). One bit covers object.Size bytes.
	Bits []byte `json:"bits"`
}

// GetAllocMap reads a volume's allocation map. A volume that never
// allocated anything has no record: that is returned as an all-zero
// bitmap with Rev 0 (the CAS convention for a nonexistent key), not an
// error.
func GetAllocMap(ctx context.Context, s Store, name string, nbits int) (AllocMapRecord, error) {
	var rec AllocMapRecord
	if nbits < 0 {
		return rec, fmt.Errorf("meta: allocmap %s: negative size", name)
	}
	rec.Bits = make([]byte, (nbits+7)/8)
	b, rev, err := s.Get(ctx, PrefixAllocMaps, name)
	if err == ErrNotFound {
		return rec, nil
	}
	if err != nil {
		return rec, err
	}
	var stored AllocMapRecord
	if err := json.Unmarshal(b, &stored); err != nil {
		return rec, fmt.Errorf("meta: allocmap %s: %w", name, err)
	}
	// Grow-only online resize: the volume object count (nbits) rises, so
	// a stored bitmap that is shorter than requested is the pre-resize
	// prefix. The freshly grown objects are unallocated (bits 0), which
	// is exactly the all-zero tail this record is zeroed to, so copying
	// the stored prefix into the wider bitmap is the truthful view. A
	// bitmap longer than requested means the volume shrank, which
	// resize never does: treat it as the corruption it is.
	if len(stored.Bits) > len(rec.Bits) {
		return rec, fmt.Errorf("meta: allocmap %s: bitmap %d bytes, want %d (volume shrank?)",
			name, len(stored.Bits), len(rec.Bits))
	}
	copy(rec.Bits, stored.Bits)
	rec.Rev = rev
	return rec, nil
}

// PutAllocMap stores the bitmap, CAS-ing on rev (0 = expect no record
// yet). Returns the new revision. Callers merge in their bit, CAS, and
// on ErrCASRetry re-read and re-merge (the merge is idempotent, so the
// loop is safe).
func PutAllocMap(ctx context.Context, s Store, name string, rec AllocMapRecord) (int64, error) {
	b, err := json.Marshal(rec)
	if err != nil {
		return 0, err
	}
	return s.Put(ctx, PrefixAllocMaps, name, b, rec.Rev)
}

// DeleteAllocMap removes a volume's allocation map (volume deletion).
// A volume that never allocated (no record) is not an error.
func DeleteAllocMap(ctx context.Context, s Store, name string) error {
	err := s.Delete(ctx, PrefixAllocMaps, name, -1)
	if err == ErrNotFound {
		return nil
	}
	return err
}

// MutateAlloc re-reads a volume's bitmap, applies mutate, and CAS-writes
// it back, retrying on conflict (mutate must be idempotent, e.g. set or
// clear a bit). When mutate reports no change nothing is written. It
// returns the committed bitmap (callers keep a cached copy and refresh
// it here). This is the accounting primitive behind quota charge/release:
// concurrent writers each set their own bit without losing a sibling's,
// and the over-quota decision is made against the exact bitmap the CAS
// commits, so two racing writers can never both be charged for the last
// block of room.
func MutateAlloc(ctx context.Context, s Store, name string, nbits int, mutate func(bits []byte) bool) ([]byte, bool, error) {
	for {
		rec, err := GetAllocMap(ctx, s, name, nbits)
		if err != nil {
			return nil, false, err
		}
		if !mutate(rec.Bits) {
			return rec.Bits, false, nil // bitmap already in the desired state
		}
		if _, err := PutAllocMap(ctx, s, name, rec); err != nil {
			if errors.Is(err, ErrConflict) {
				continue // racing allocator: re-read, re-merge, retry
			}
			return nil, false, err
		}
		return rec.Bits, true, nil
	}
}

// AllocMapPopcount counts set bits.
func AllocMapPopcount(bits []byte) int {
	n := 0
	for _, b := range bits {
		// Kernighan: clear the lowest set bit per iteration.
		for b != 0 {
			b &= b - 1
			n++
		}
	}
	return n
}

// Bit helpers shared by meta and the client's in-memory copy.
func allocBit(bits []byte, i int) bool { return bits[i>>3]&(1<<(i&7)) != 0 }

func setAllocBit(bits []byte, i int) bool {
	w := &bits[i>>3]
	m := byte(1) << (i & 7)
	if *w&m != 0 {
		return false
	}
	*w |= m
	return true
}

func clearAllocBit(bits []byte, i int) bool {
	w := &bits[i>>3]
	m := byte(1) << (i & 7)
	if *w&m == 0 {
		return false
	}
	*w &^= m
	return true
}
