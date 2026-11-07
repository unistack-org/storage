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

	slots []Op
	ids   chan uint32

	filesMu sync.Mutex
	files   []*os.File // keep registered *os.File alive for the ring's lifetime

	work chan Op
	done chan struct{}

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
	r.done = make(chan struct{})
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

// produceOne acquires a slot, fills the SQE at tail, and publishes.
func (r *uring) produceOne(op Op, slot uint32) bool {
	tail := atomic.LoadUint32(r.sqTail)
	if tail-atomic.LoadUint32(r.sqHead) >= r.sqEntries {
		return false
	}
	idx := tail & atomic.LoadUint32(r.sqMask)
	opCode := uint8(opRead)
	if op.Write {
		opCode = opWrite
	}
	r.fillSqe(idx, opCode, op.Fd, op.Buf, op.Off, uint64(slot))
	atomic.StoreUint32(r.sqTail, tail+1)
	return true
}

// submitLoop is the data-path producer: collect a batch, fill SQEs, one
// io_uring_enter per batch.
func (r *uring) submitLoop() {
	defer r.wg.Done()
	batch := make([]uint32, 0, 128)
	for {
		var first Op
		ok := false
		select {
		case <-r.done:
			r.drainWork()
			return
		case first = <-r.work:
			ok = true
		}
		if ok {
			batch = batch[:0]
			r.ringSubmit(first, &batch)
		collect:
			for len(batch) < 128 {
				select {
				case op := <-r.work:
					r.ringSubmit(op, &batch)
				default:
					break collect
				}
			}
			r.kick(len(batch), batch)
		}
	}
}

// ringSubmit fills one op and appends its slot to batch (producerMu
// held). On sq-full/no-slot the slot is never taken, so the batch owner
// (completions) stays the single owner of every handed-out slot.
func (r *uring) ringSubmit(op Op, batch *[]uint32) {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	tail := atomic.LoadUint32(r.sqTail)
	if tail-atomic.LoadUint32(r.sqHead) >= r.sqEntries {
		op.Done0(errors.New("iouring: sq full"))
		return
	}
	select {
	case slot := <-r.ids:
		idx := tail & atomic.LoadUint32(r.sqMask)
		opCode := uint8(opRead)
		if op.Write {
			opCode = opWrite
		}
		r.fillSqe(idx, opCode, op.Fd, op.Buf, op.Off, uint64(slot))
		atomic.StoreUint32(r.sqTail, tail+1)
		r.slots[slot] = op
		*batch = append(*batch, slot)
	default:
		op.Done0(errors.New("iouring: no free slots"))
	}
}

// kick wakes the kernel with the batch; SQEs the kernel did not consume
// (EINTR partial, refused batch) get an immediate error callback. Those
// slots were never visible to the kernel, so the completion loop cannot
// complete them: reclaiming them here is race-free.
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
		consumed = 0 // kernel refused the batch wholesale
	}
	for _, slot := range batch[consumed:n] {
		op := r.slots[slot]
		r.slots[slot] = Op{}
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
			slot := uint32(userData)
			op := r.slots[slot]
			r.slots[slot] = Op{}
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
		if r.closed.Load() {
			return // drainWork + kick guarantee every op got a callback
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

func (r *uring) Submit(ops []Op) error {
	if r.closed.Load() {
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
		r.work <- op
	}
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
		close(r.done) // submitLoop: drains work (ErrClosed), exits
		r.wake()      // NOP completion unblocks the enter in completionLoop
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

func (r *uring) drainWork() {
	for {
		select {
		case op := <-r.work:
			op.DoneGo(ErrClosed)
		default:
			return
		}
	}
}
