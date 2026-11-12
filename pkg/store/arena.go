package store

import (
	"github.com/unistack-org/storage/pkg/iouring"
	"github.com/unistack-org/storage/pkg/mem"
)

// ringArena is the store's registered-buffer arena: one big region
// handed to io_uring once (IORING_REGISTER_BUFFERS), carved into equal
// slots, with a slot pool as the queue's flow control. The kernel
// pins the arena's pages at registration, so ops that reference a slot
// (READ_FIXED/WRITE_FIXED with BufIndex) skip the per-I/O page pinning
// plain ops pay; with RingHugePages the arena is a handful of huge
// pages, so that pinning table and the TLB footprint stay tiny.
type ringArena struct {
	ar   *mem.Arena
	idx  int // registered-buffer index returned by the ring
	free chan int
}

// initRingArena builds, registers and pools the arena. It is called
// once at New after the ring exists; any failure leaves the store on
// the plain (unregistered) ring path, which is always available.
func (s *Store) initRingArena(ring iouring.Ring) error {
	slots := s.opts.RingArenaSlots
	size := s.opts.RingArenaSlot
	if size <= 0 {
		size = 64 << 10
	}
	ar, err := mem.NewArena(slots, size, s.opts.RingHugePages)
	if err != nil {
		return err
	}
	idx, err := ring.RegisterBuffer(ar.Data)
	if err != nil {
		ar.Close()
		return err
	}
	a := &ringArena{ar: ar, idx: idx, free: make(chan int, slots)}
	for i := 0; i < slots; i++ {
		a.free <- i
	}
	s.arena = a
	return nil
}

// acquire takes a free slot, waiting while the pool is exhausted: with
// every slot in flight, the queue is full and this is exactly the
// backpressure the ring wants — slots return on completion (the ring
// fires every queued op, also at Close), so the wait is bounded by one
// I/O round-trip depth.
func (a *ringArena) acquire() (int, []byte) {
	i := <-a.free
	return i, a.ar.Slot(i)
}

func (a *ringArena) release(i int) { a.free <- i }

func hugeNote(huge bool) string {
	if huge {
		return " (huge pages)"
	}
	return ""
}
