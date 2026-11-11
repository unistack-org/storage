//go:build linux

package iouring

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Raw io_uring ABI (uapi/linux/io_uring.h). Hand-rolled so the data path
// carries no x/sys dependency. The first 48 bytes of the SQE layout are a
// stable kernel ABI: opcode@0, fd@4, off@8, addr@16, len@24, user_data@32.
const (
	opNop   = 0
	opRead  = 22 // IORING_OP_READ (kernel >= 5.6)
	opWrite = 23 // IORING_OP_WRITE (kernel >= 5.6)

	enterGetevents = 1 << 0

	offSQRing = 0
	offCQRing = 0x8000000
	offSQES   = 0x10000000

	sqeSize = 64
	cqeSize = 16

	wakeSentinel = ^uint64(0) // user_data of the Close wake-up NOP
)

type sqOffsets struct {
	head, tail, ringMask, ringEntries, flags, dropped, array uint32
	_                                                        [12]byte
}

type cqOffsets struct {
	head, tail, ringMask, ringEntries, overflow, cqes, flags uint32
	_                                                        [12]byte
}

// ioParams mirrors struct io_uring_params (120 bytes).
type ioParams struct {
	sqEntries, cqEntries, flags, sqThreadCPU, sqThreadIdle, features, wqFd uint32
	resv                                                                   [3]uint32
	sqOff                                                                  sqOffsets
	cqOff                                                                  cqOffsets
}

// uring is the Linux io_uring implementation.
//
// SQE production is serialised by producerMu (the submit goroutine plus
// the Close wake-up NOP). The completion goroutine is the sole consumer
// of the CQ and the sole owner of completed slots.
type uring struct {
	ringFd int

	sqRingMap, cqRingMap, sqesMap []byte

	sqHead, sqTail, sqMask, sqArray *uint32
	sqEntries                       uint32
	cqHead, cqTail, cqMask          *uint32
	cqes                            unsafe.Pointer

	// slotsMu guards slots[]: the SQE producer writes a slot before
	// publishing its SQE (sqTail), and the completion loop reads it
	// after the kernel posts the matching CQE. The kernel mediates the
	// data dependency, which the race detector cannot see, so the mutex
	// both serialises the accesses and supplies the happens-before edge.
	slotsMu sync.Mutex
	slots   []Op
	ids     chan uint32

	filesMu sync.Mutex
	files   []*os.File // keep registered *os.File alive for the ring's lifetime

	// work carries ops to submitLoop. Submit sends under submitMu and
	// Close closes the channel under the same mutex, so a send can
	// never race the close; submitLoop exits once work is closed and
	// drained, guaranteeing every accepted op reaches the kernel.
	work chan Op

	// submitMu serialises Submit (send) against Close (close). It is
	// deliberately distinct from producerMu: submitLoop may drain work
	// (it needs producerMu for SQ/kick) while a Submit blocks holding
	// submitMu, which is what lets Close proceed.
	submitMu sync.Mutex

	// outstanding counts submitted ops whose completion callback has
	// not run: incremented when Submit enqueues an op, decremented per
	// CQE by the completion loop and by kick (or the defensive no-slot
	// path) when an op the kernel never referenced is reclaimed. The
	// completion loop must not exit while it is positive, even after
	// Close: those ops still need their callbacks.
	outstanding atomic.Int64

	producerMu sync.Mutex
	wg         sync.WaitGroup
	closer     sync.Once
	closed     atomic.Bool
}

func uringSetup(entries uint32) (int, ioParams, error) {
	var p ioParams
	fd, _, errno := syscall.Syscall6(sysIoUringSetup, uintptr(entries), uintptr(unsafe.Pointer(&p)), 0, 0, 0, 0)
	if errno != 0 {
		return 0, p, errno
	}
	return int(fd), p, nil
}

// uringEnter submits toSubmit SQEs and (with getevents) waits for at
// least minComplete completions. It returns the number of SQEs the kernel
// consumed.
func uringEnter(fd uintptr, toSubmit, minComplete uint32, getevents bool) (int, error) {
	var flags uintptr
	if getevents {
		flags |= enterGetevents
	}
	r1, _, errno := syscall.Syscall6(sysIoUringEnter, fd, uintptr(toSubmit), uintptr(minComplete), flags, 0, 0)
	if errno != 0 {
		return int(r1), errno
	}
	return int(r1), nil
}

func New(entries int) (Ring, error) {
	if entries < 8 {
		entries = 8
	}
	n := uint32(1)
	for n < uint32(entries) {
		n <<= 1
	}
	fd, p, err := uringSetup(n)
	if err != nil {
		return nil, fmt.Errorf("io_uring_setup: %w", err)
	}
	r := &uring{ringFd: fd}
	if err := r.mapRings(&p); err != nil {
		syscall.Close(r.ringFd)
		return nil, err
	}
	r.slots = make([]Op, p.sqEntries)
	r.ids = make(chan uint32, p.sqEntries)
	for i := uint32(0); i < p.sqEntries; i++ {
		r.ids <- i
	}
	r.work = make(chan Op, p.sqEntries)
	r.wg.Add(2)
	go r.submitLoop()
	go r.completionLoop()
	return r, nil
}

func (r *uring) mapRings(p *ioParams) error {
	sqRing, err := mmapRing(r.ringFd, offSQRing, int(p.sqOff.array)+4*int(p.sqEntries))
	if err != nil {
		return fmt.Errorf("mmap sq ring: %w", err)
	}
	cqRing, err := mmapRing(r.ringFd, offCQRing, int(p.cqOff.cqes)+16*int(p.cqEntries))
	if err != nil {
		return fmt.Errorf("mmap cq ring: %w", err)
	}
	sqes, err := mmapRing(r.ringFd, offSQES, sqeSize*int(p.sqEntries))
	if err != nil {
		return fmt.Errorf("mmap sqes: %w", err)
	}
	r.sqRingMap, r.cqRingMap, r.sqesMap = sqRing, cqRing, sqes
	u32 := func(b []byte, off uint32) *uint32 { return (*uint32)(unsafe.Pointer(&b[off])) }
	r.sqHead = u32(sqRing, p.sqOff.head)
	r.sqTail = u32(sqRing, p.sqOff.tail)
	r.sqMask = u32(sqRing, p.sqOff.ringMask)
	r.sqArray = u32(sqRing, p.sqOff.array)
	r.cqHead = u32(cqRing, p.cqOff.head)
	r.cqTail = u32(cqRing, p.cqOff.tail)
	r.cqMask = u32(cqRing, p.cqOff.ringMask)
	r.cqes = unsafe.Pointer(&cqRing[p.cqOff.cqes])
	r.sqEntries = p.sqEntries
	return nil
}

func mmapRing(fd int, offset, length int) ([]byte, error) {
	b, err := syscall.Mmap(fd, int64(offset), length,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// fillSqe writes one SQE at index idx. The caller owns slot and op.
func (r *uring) fillSqe(idx uint32, opCode uint8, fd int32, buf []byte, off int64, userData uint64) {
	p := unsafe.Pointer(uintptr(unsafe.Pointer(&r.sqesMap[0])) + uintptr(idx)*sqeSize)
	for i := 0; i < sqeSize; i += 8 { // zero: flags/link/etc must be 0
		*(*uint64)(unsafe.Add(p, i)) = 0
	}
	*(*uint8)(unsafe.Add(p, 0)) = opCode
	*(*int32)(unsafe.Add(p, 4)) = fd
	if len(buf) > 0 {
		*(*uint64)(unsafe.Add(p, 8)) = uint64(off)
		*(*uint64)(unsafe.Add(p, 16)) = uint64(uintptr(unsafe.Pointer(unsafe.SliceData(buf))))
		*(*uint32)(unsafe.Add(p, 24)) = uint32(len(buf))
	}
	*(*uint64)(unsafe.Add(p, 32)) = userData
	// Index array: SQE index == ring slot index (fixed identity map).
	*(*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(r.sqArray)) + uintptr(idx)*4)) = idx
}

// submitLoop is the data-path producer: collect a batch, fill SQEs, one
// io_uring_enter per batch. It exits only after Close has closed the
// work channel and the channel is drained, so every accepted op is
// either handed to the kernel or failed by kick's reclaim path.
func (r *uring) submitLoop() {
	defer r.wg.Done()
	batch := make([]uint32, 0, 128)
	cap := int(r.sqEntries)
	if cap > 128 {
		cap = 128
	}
	for {
		first, ok := <-r.work
		if !ok {
			return
		}
		batch = batch[:0]
		r.submitOp(first, &batch)
		drained := false
	collect:
		for len(batch) < cap {
			select {
			case op, ok2 := <-r.work:
				if !ok2 {
					drained = true // channel closed and fully drained
					break collect
				}
				r.submitOp(op, &batch)
			default:
				break collect
			}
		}
		r.kick(len(batch), batch)
		if drained {
			return
		}
	}
}

// submitOp hands one op to the SQ. A full SQ flushes the batch collected
// so far (the kernel consumes entries, freeing space) and retries: the
// data path applies backpressure through the work channel instead of
// failing ops the kernel was never told about.
func (r *uring) submitOp(op Op, batch *[]uint32) {
	for !r.ringSubmit(op, batch) {
		k := *batch
		*batch = k[:0]
		r.kick(len(k), k)
	}
}

// ringSubmit fills one op and appends its slot to batch (producerMu
// held). It reports whether the op is resolved: false means the SQ was
// full (the slot was never taken, so the caller must free SQ entries and
// retry), true means the op is queued (slot appended) or was failed here
// (no free slot). On a non-true return no slot is taken, so the batch
// owner stays the single owner of every handed-out slot.
func (r *uring) ringSubmit(op Op, batch *[]uint32) bool {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	tail := atomic.LoadUint32(r.sqTail)
	if tail-atomic.LoadUint32(r.sqHead) >= r.sqEntries {
		return false // SQ full: caller flushes the batch and retries
	}
	select {
	case slot := <-r.ids:
		idx := tail & atomic.LoadUint32(r.sqMask)
		opCode := uint8(opRead)
		if op.Write {
			opCode = opWrite
		}
		// Publish the slot contents before the SQE: once sqTail is
		// advanced the kernel may complete it instantly, and the
		// completion loop must see this op (not a zeroed slot).
		r.slotsMu.Lock()
		r.slots[slot] = op
		r.slotsMu.Unlock()
		r.fillSqe(idx, opCode, op.Fd, op.Buf, op.Off, uint64(slot))
		atomic.StoreUint32(r.sqTail, tail+1)
		*batch = append(*batch, slot)
		return true
	default:
		// Defensive: ids capacity equals sqEntries, so this is
		// unreachable while the SQ-full check gates publishing. The op
		// was counted outstanding at Submit and the kernel will never
		// see it: undo the count.
		r.outstanding.Add(-1)
		op.Done0(errors.New("iouring: no free slots"))
		return true // resolved with an error: nothing to retry
	}
}

// kick wakes the kernel with the batch; SQEs the kernel did not consume
// (EINTR partial, refused batch) get an immediate error callback. Those
// slots were never visible to the kernel, so the completion loop cannot
// complete them: reclaiming them here is race-free.
// kick hands the batch's SQEs to the kernel. Ops are counted as
// outstanding at Submit-enqueue; completions (CQE) and reclaiming
// un-kernelled SQEs here are the only decrements, so re-submitting
// the same SQEs (backpressure re-kick) must not touch the counter.
// SQEs the kernel did not consume (EINTR partial, refused enter) get
// an immediate error callback: the kernel never referenced those
// slots, so the completion loop cannot complete them and reclaiming
// them here is race-free.
func (r *uring) kick(n int, batch []uint32) {
	if n == 0 {
		return
	}
	r.producerMu.Lock()
	consumed, err := uringEnter(uintptr(r.ringFd), uint32(n), 0, false)
	r.producerMu.Unlock()
	if err == nil && consumed >= n {
		return
	}
	if consumed < 0 {
		consumed = 0
	}
	if err != nil && !errors.Is(err, syscall.EINTR) {
		// The kernel refused the enter and consumed nothing: every
		// SQE in the batch stays unreferenced by the kernel and must
		// be reclaimed (a partial consumption always reports EINTR).
		consumed = 0
	}
	// Reclaimed ops were counted at Submit and the kernel never saw
	// them: drop their outstanding count so completionLoop can exit.
	r.outstanding.Add(int64(consumed - n))
	for _, slot := range batch[consumed:n] {
		r.slotsMu.Lock()
		op := r.slots[slot]
		r.slots[slot] = Op{}
		r.slotsMu.Unlock()
		r.ids <- slot
		op.Done0(err)
	}
}

// completionLoop is the sole CQ consumer.
func (r *uring) completionLoop() {
	defer r.wg.Done()
	for {
		for {
			head := atomic.LoadUint32(r.cqHead)
			tail := atomic.LoadUint32(r.cqTail)
			if head == tail {
				break
			}
			mask := atomic.LoadUint32(r.cqMask)
			c := unsafe.Pointer(uintptr(r.cqes) + uintptr(head&mask)*cqeSize)
			userData := *(*uint64)(c)
			res := *(*int32)(unsafe.Add(c, 8))
			atomic.StoreUint32(r.cqHead, head+1)
			if userData == wakeSentinel {
				continue
			}
			r.outstanding.Add(-1)
			slot := uint32(userData)
			r.slotsMu.Lock()
			op := r.slots[slot]
			r.slots[slot] = Op{}
			r.slotsMu.Unlock()
			r.ids <- slot
			n := int(res)
			var err error
			if res < 0 {
				err, n = syscall.Errno(-res), 0
			}
			if op.Done != nil {
				op.Done(n, err)
			}
		}
		if r.closed.Load() && r.outstanding.Load() <= 0 {
			return // every enqueued op was completed or reclaimed by kick
		}
		if _, err := uringEnter(uintptr(r.ringFd), 0, 1, true); err != nil {
			if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) {
				continue
			}
			if r.closed.Load() {
				return
			}
			return
		}
	}
}

// Submit queues ops to the ring. Senders serialise against Close on
// submitMu: once Close has closed the work channel, no Send can slip
// in behind it, so every op that reaches the channel is drained and
// kicked by submitLoop before it exits.
func (r *uring) Submit(ops []Op) error {
	r.submitMu.Lock()
	if r.closed.Load() {
		r.submitMu.Unlock()
		for _, op := range ops {
			op.DoneGo(ErrClosed)
		}
		return ErrClosed
	}
	for _, op := range ops {
		if len(op.Buf) == 0 {
			op.DoneGo(nil)
			continue
		}
		// Counted before enqueue: from here until the CQE (or kick's
		// reclaim) the op is outstanding and completionLoop must stay
		// alive to resolve it.
		r.outstanding.Add(1)
		r.work <- op
	}
	r.submitMu.Unlock()
	return nil
}

// RegisterFile keeps f alive while the ring may use its fd.
func (r *uring) RegisterFile(fd int32, f *os.File) {
	r.filesMu.Lock()
	r.files = append(r.files, f)
	r.filesMu.Unlock()
}

func (r *uring) Close() error {
	var err error
	r.closer.Do(func() {
		r.closed.Store(true)
		// Under submitMu this serialises with Submit's send, so no
		// op can be enqueued after the channel closes; submitLoop
		// drains everything already in it, kicks, and exits.
		r.submitMu.Lock()
		close(r.work)
		r.submitMu.Unlock()
		r.wake() // NOP completion unblocks the enter in completionLoop
		r.wg.Wait()
		err = syscall.Close(r.ringFd)
		for _, b := range [][]byte{r.sqRingMap, r.cqRingMap, r.sqesMap} {
			if len(b) > 0 {
				syscall.Munmap(b)
			}
		}
	})
	return err
}

// wake submits a NOP with wakeSentinel user_data so a completionLoop
// blocked in enter(GetEvents) wakes, sees closed, and exits.
func (r *uring) wake() {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	tail := atomic.LoadUint32(r.sqTail)
	idx := tail & atomic.LoadUint32(r.sqMask)
	r.fillSqe(idx, opNop, -1, nil, 0, wakeSentinel)
	atomic.StoreUint32(r.sqTail, tail+1)
	uringEnter(uintptr(r.ringFd), 1, 0, false)
}
