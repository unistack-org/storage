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
	opNop        = 0
	opReadFixed  = 4  // IORING_OP_READ_FIXED (fd in fixed-file table)
	opWriteFixed = 5  // IORING_OP_WRITE_FIXED
	opRead       = 22 // IORING_OP_READ (kernel >= 5.6)
	opWrite      = 23 // IORING_OP_WRITE (kernel >= 5.6)

	iosqeFixedFile = 1 << 0 // SQE flags: fd indexes the fixed-file table

	registerFiles   = 0 // IORING_REGISTER_FILES: int32 fd array
	registerBuffers = 2 // IORING_REGISTER_BUFFERS: iovec array

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

	// Fixed-file table (kernel-side, slot-indexed). fileSlots maps an
	// fd to its slot; fixedFiles is the array handed to
	// IORING_REGISTER_FILES (-1 = unused slot). Registration replaces
	// the whole table; the kernel snapshots per-SQE tables for ops
	// already in flight, so slot reuse under a submitted op is safe,
	// and UnregisterFile before closing an fd keeps a closed fd from
	// ever being referenced.
	fileSlots  map[int32]int
	fixedFiles []int32
	// fixedOK is cleared when the kernel refuses fixed files/buffers
	// (old kernel, locked memory), after which ops use plain opcodes.
	fixedOK atomic.Bool

	// Registered buffers: each RegisterBuffer appends an iovec to the
	// kernel table (the kernel pins its pages at registration and
	// refcounts per-op, so UnregisterBuffer while an op is in flight
	// is safe). Op.BufIndex is 1-based over this slice.
	bufMu sync.Mutex
	bufs  [][]byte

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

// New creates a ring with the given number of entries.
func New(entries int) (Ring, error) { return newPinned(entries, nil) }

// NewPinned creates a ring whose internal worker goroutines (the
// submit/completion loops) run with pin() called on their OS thread
// before any work: pass a closure invoking CPU affinity pinning (see
// pkg/cpu) to keep the ring's threads on dedicated cores. A nil pin
// behaves like New.
func NewPinned(entries int, pin func()) (Ring, error) {
	return newPinned(entries, pin)
}

func newPinned(entries int, pin func()) (Ring, error) {
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
	r.fixedOK.Store(true)
	r.fileSlots = map[int32]int{}
	r.bufs = [][]byte{}
	r.wg.Add(2)
	go func() {
		if pin != nil {
			pin()
		}
		r.submitLoop()
	}()
	go func() {
		if pin != nil {
			pin()
		}
		r.completionLoop()
	}()
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
// fileSlot >= 0 (fixed-file path): IOSQE_FIXED_FILE with the slot in
// the fd field. bufIndex >= 0: READ_FIXED/WRITE_FIXED with the kernel's
// 0-based buf_index at byte 40 (addr/len address bytes inside that
// registered buffer).
func (r *uring) fillSqe(idx uint32, opCode uint8, fd int32, fileSlot, bufIndex int, buf []byte, off int64, userData uint64) {
	p := unsafe.Pointer(uintptr(unsafe.Pointer(&r.sqesMap[0])) + uintptr(idx)*sqeSize)
	for i := 0; i < sqeSize; i += 8 { // zero: flags/link/etc must be 0
		*(*uint64)(unsafe.Add(p, i)) = 0
	}
	*(*uint8)(unsafe.Add(p, 0)) = opCode
	if fileSlot >= 0 {
		*(*uint8)(unsafe.Add(p, 1)) = iosqeFixedFile
		fd = int32(fileSlot)
	}
	*(*int32)(unsafe.Add(p, 4)) = fd
	if len(buf) > 0 {
		*(*uint64)(unsafe.Add(p, 8)) = uint64(off)
		*(*uint64)(unsafe.Add(p, 16)) = uint64(uintptr(unsafe.Pointer(unsafe.SliceData(buf))))
		*(*uint32)(unsafe.Add(p, 24)) = uint32(len(buf))
	}
	if bufIndex >= 0 {
		*(*uint16)(unsafe.Add(p, 40)) = uint16(bufIndex)
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
// retry), true means the op is queued (slot appended). On a false return
// no slot is taken, so the batch owner stays the single owner of every
// handed-out slot.
func (r *uring) ringSubmit(op Op, batch *[]uint32) bool {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	tail := atomic.LoadUint32(r.sqTail)
	if tail-atomic.LoadUint32(r.sqHead) >= r.sqEntries {
		return false // SQ full: caller flushes the batch and retries
	}
	// A slot is returned to ids only when its op completes (CQE drained)
	// or is reclaimed, so a full ids pool means every slot is in flight.
	// Blocking here is the data path's flow control: completions refill
	// ids without producerMu, so this cannot wedge the completion loop.
	slot := <-r.ids
	idx := tail & atomic.LoadUint32(r.sqMask)
	opCode := uint8(opRead)
	if op.Write {
		opCode = opWrite
	}
	fileSlot, bufIndex := -1, -1
	if r.fixedOK.Load() {
		if s, ok := r.fileSlots[op.Fd]; ok {
			fileSlot = s - 1 // kernel table is 0-based
		}
		if op.BufIndex >= 1 && op.BufIndex <= len(r.bufs) {
			bufIndex = op.BufIndex - 1
		}
	}
	if bufIndex >= 0 {
		// READ_FIXED/WRITE_FIXED: addr/len address the registered
		// buffer; IOSQE_FIXED_FILE is added when the fd is fixed too.
		if op.Write {
			opCode = opWriteFixed
		} else {
			opCode = opReadFixed
		}
	}
	// Publish the slot contents before the SQE: once sqTail is
	// advanced the kernel may complete it instantly, and the
	// completion loop must see this op (not a zeroed slot).
	r.slotsMu.Lock()
	r.slots[slot] = op
	r.slotsMu.Unlock()
	r.fillSqe(idx, opCode, op.Fd, fileSlot, bufIndex, op.Buf, op.Off, uint64(slot))
	atomic.StoreUint32(r.sqTail, tail+1)
	*batch = append(*batch, slot)
	return true
}

// kick hands the batch's SQEs to the kernel. The batch is exactly the
// unpublished SQ region (all fills and kicks serialise on producerMu,
// and unconsumed SQEs are pruned below), so the consumed count is the
// sqHead delta: authoritative even when wake() flushed SQEs early.
// Ops are counted outstanding at Submit-enqueue; CQEs and reclaiming
// kernel-unreferenced SQEs here are the only decrements.
// SQEs the kernel did not consume (EINTR partial, refused enter) get
// an immediate error callback: the kernel never referenced those
// slots, so the completion loop cannot complete them and reclaiming
// them here is race-free.
func (r *uring) kick(n int, batch []uint32) {
	if n == 0 {
		return
	}
	r.producerMu.Lock()
	headBefore := atomic.LoadUint32(r.sqHead)
	_, err := uringEnter(uintptr(r.ringFd), uint32(n), 0, false)
	consumed := int(atomic.LoadUint32(r.sqHead) - headBefore)
	if consumed < 0 || consumed > n {
		consumed = 0 // cannot happen without SQPOLL; be safe
	}
	if consumed < n {
		// Drop the SQEs the kernel never referenced (see the flush-SQ
		// idiom in liburing): their slots are reclaimed below, and a
		// later enter must not submit SQEs whose slots were recycled.
		atomic.StoreUint32(r.sqTail, atomic.LoadUint32(r.sqHead))
	}
	// Reclaimed ops were counted at Submit and the kernel never saw
	// them: drop their outstanding count. If that empties the ring
	// during Close, wake the completion loop itself: after this point
	// no CQE can arrive to wake it, and Close's wake() may have raced
	// ahead of this reclaim while ops were still counted.
	r.outstanding.Add(int64(consumed - n))
	if r.closed.Load() && r.outstanding.Load() <= 0 {
		r.wakeLocked()
	}
	r.producerMu.Unlock()
	if consumed >= n {
		return
	}
	if err == nil {
		err = syscall.EINTR // kernel consumed less than asked
	}
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
				// A fixed-file/fixed-buffer SQE refused with EINVAL
				// (unsupported layout on an unusual kernel): turn the
				// fast path off permanently and retry as plain I/O so
				// the data path never surfaces the ABI error.
				if syscall.Errno(-res) == syscall.EINVAL && r.fixedOK.Load() &&
					(op.BufIndex >= 1) {
					r.fixedOK.Store(false)
					op.BufIndex = 0
					r.outstanding.Add(1)
					select {
					case r.work <- op:
						continue
					default:
						r.outstanding.Add(-1)
					}
				}
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

// enqueueOp adds a single op to the ring. It assumes submitMu is held
// and the closed check has been done by the caller. An op with an empty
// buffer is completed synchronously (no I/O to issue).
func (r *uring) enqueueOp(op Op) {
	if len(op.Buf) == 0 {
		op.DoneGo(nil)
		return
	}
	// Counted before enqueue: from here until the CQE (or kick's
	// reclaim) the op is outstanding and completionLoop must stay
	// alive to resolve it.
	r.outstanding.Add(1)
	r.work <- op
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
		r.enqueueOp(op)
	}
	r.submitMu.Unlock()
	return nil
}

// SubmitSingle is Submit of one op: same empty-buffer and closed-ring
// handling, no per-I/O []Op wrap.
func (r *uring) SubmitSingle(op Op) error {
	r.submitMu.Lock()
	if r.closed.Load() {
		r.submitMu.Unlock()
		op.DoneGo(ErrClosed)
		return ErrClosed
	}
	r.enqueueOp(op)
	r.submitMu.Unlock()
	return nil
}

// RegisterFile keeps f alive while the ring may use its fd and adds the
// fd to the kernel's fixed-file table, so later ops on it are submitted
// as fixed-file I/O (the kernel skips per-I/O fd get/put).
func (r *uring) RegisterFile(fd int32, f *os.File) {
	r.filesMu.Lock()
	r.files = append(r.files, f)
	r.filesMu.Unlock()

	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	if r.fileSlots[fd] != 0 {
		return // already registered
	}
	slot := len(r.fileSlots) + 1 // 1-based; 0 means "not registered"
	// Grow the kernel array to slot.
	for len(r.fixedFiles) < slot {
		r.fixedFiles = append(r.fixedFiles, -1)
	}
	r.fileSlots[fd] = slot
	r.fixedFiles[slot-1] = fd
	if err := r.registerSyscall(registerFiles, r.fixedFiles); err != nil {
		// Fixed files unavailable (old/locked-down kernel): plain
		// opcodes remain correct, so degrade permanently.
		r.fixedOK.Store(false)
		delete(r.fileSlots, fd)
		r.fixedFiles[slot-1] = -1
	}
}

// UnregisterFile drops fd from the fixed-file table. In-flight ops keep
// the kernel's snapshot of the old table, so closing the fd afterwards
// is safe.
func (r *uring) UnregisterFile(fd int32) {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	slot, ok := r.fileSlots[fd]
	if !ok {
		return
	}
	delete(r.fileSlots, fd)
	r.fixedFiles[slot-1] = -1
	if err := r.registerSyscall(registerFiles, r.fixedFiles); err != nil {
		r.fixedOK.Store(false)
	}
	// Drop the keep-alive reference only from the kernel side; the
	// *os.File stays in files[] (Close must not close fds the store
	// owns; the store closes them itself).
}

// RegisterBuffer registers data as a fixed buffer and returns its
// 1-based index for Op.BufIndex. The kernel pins the pages here, so
// fixed-buffer ops skip per-I/O page pinning. The caller keeps data
// alive until UnregisterBuffer.
func (r *uring) RegisterBuffer(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("iouring: empty buffer")
	}
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	r.bufMu.Lock()
	r.bufs = append(r.bufs, data)
	all := r.bufs
	r.bufMu.Unlock()
	if err := r.registerSyscall(registerBuffers, all); err != nil {
		r.bufMu.Lock()
		r.bufs = all[:len(all)-1]
		r.bufMu.Unlock()
		r.fixedOK.Store(false)
		return 0, fmt.Errorf("io_uring_register buffers: %w", err)
	}
	return len(all), nil
}

// UnregisterBuffer drops a registered buffer; ops in flight complete
// against the kernel's reference.
func (r *uring) UnregisterBuffer(index int) {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	r.bufMu.Lock()
	if index < 1 || index > len(r.bufs) {
		r.bufMu.Unlock()
		return
	}
	next := make([][]byte, 0, len(r.bufs)-1)
	next = append(next, r.bufs[:index-1]...)
	next = append(next, r.bufs[index:]...)
	r.bufs = next
	all := r.bufs
	r.bufMu.Unlock()
	if err := r.registerSyscall(registerBuffers, all); err != nil {
		r.fixedOK.Store(false)
	}
}

// registerSyscall replaces the kernel's files/buffers table. Callers
// hold producerMu (registration serialises with SQE production).
func (r *uring) registerSyscall(opcode int, arg any) error {
	var ptr unsafe.Pointer
	var n uint32
	switch v := arg.(type) {
	case []int32:
		if len(v) == 0 {
			ptr, n = nil, 0
		} else {
			ptr, n = unsafe.Pointer(unsafe.SliceData(v)), uint32(len(v))
		}
	case [][]byte:
		if len(v) == 0 {
			ptr, n = nil, 0
		} else {
			iov := make([]syscall.Iovec, len(v))
			for i, b := range v {
				iov[i] = syscall.Iovec{Base: unsafe.SliceData(b), Len: uint64(len(b))}
			}
			ptr, n = unsafe.Pointer(unsafe.SliceData(iov)), uint32(len(iov))
		}
	}
	_, _, errno := syscall.Syscall6(sysIoUringRegister, uintptr(r.ringFd),
		uintptr(opcode), uintptr(ptr), uintptr(n), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
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

// wake unblocks a completionLoop parked in enter(GetEvents).
func (r *uring) wake() {
	r.producerMu.Lock()
	defer r.producerMu.Unlock()
	r.wakeLocked()
}

// wakeLocked is wake with producerMu held. Any completion wakes the
// loop, so a NOP is only needed when nothing is in flight; the NOP is
// flushed through the SQ first, so a full SQ can never make the fill
// overwrite an unpublished SQE.
func (r *uring) wakeLocked() {
	if head, tail := atomic.LoadUint32(r.sqHead), atomic.LoadUint32(r.sqTail); tail != head {
		uringEnter(uintptr(r.ringFd), tail-head, 0, false)
	}
	if r.outstanding.Load() > 0 {
		return // an in-flight op's CQE will wake the loop
	}
	tail := atomic.LoadUint32(r.sqTail)
	idx := tail & atomic.LoadUint32(r.sqMask)
	r.fillSqe(idx, opNop, -1, -1, -1, nil, 0, wakeSentinel)
	atomic.StoreUint32(r.sqTail, tail+1)
	uringEnter(uintptr(r.ringFd), 1, 0, false)
}
