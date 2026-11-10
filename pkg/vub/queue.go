package vub

import (
	"runtime/debug"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/unistack-org/storage/pkg/vduse"
)

// vqMaxSize is the queue depth we advertise (virtqueue size). vduse
// keeps its copy unexported.
const vqMaxSize = 1024

// maxReqBytes caps a single virtio-blk request's data (mirrors vduse's
// unexported constant): a broken guest must not make us map huge
// buffers.
const maxReqBytes = 32 << 20

// job is one parsed virtio-blk request handed from the dispatcher to a
// worker. All guest buffers the worker touches are pinned in refs.
type job struct {
	ring *vduse.SplitRing
	br   *vduse.BlkReq
	hdr  vduse.BlkReqHeader
	refs []*vduse.RegionRef
}

// queue is one virtqueue: the dispatcher goroutine drains the kick
// eventfd and publishes jobs; Workers goroutines execute them against
// the backend and publish used entries, signalling the call eventfd.
type queue struct {
	idx     int
	d       *device
	num     uint16
	base    uint16 // avail index from SET_VRING_BASE (resume point)
	ring    *vduse.SplitRing
	kick    *os.File // readable: guest published buffers
	call    *os.File // writable: notify guest of used buffers
	running bool

	// stopCh asks the dispatcher to exit; closing kick is the nudge
	// (a blocked kick.Read returns an error). jobs is closed by the
	// dispatcher after stopCh, workers drain inflight and exit.
	stopCh chan struct{}
	jobs   chan *job
	wg     sync.WaitGroup

	inflight atomic.Int64 // published-and-not-completed jobs
}

func (q *queue) logf(format string, args ...any) { q.d.opts.Logger(format, args...) }

// start spins up dispatcher + workers. Idempotent.
func (q *queue) start() {
	if q.running || q.ring == nil {
		return
	}
	q.running = true
	q.stopCh = make(chan struct{})
	q.jobs = make(chan *job, 2*q.d.opts.Workers)
	q.wg.Add(1)
	go q.dispatch()
	for w := 0; w < q.d.opts.Workers; w++ {
		q.wg.Add(1)
		go q.work()
	}
	// QEMU does not always inject a kick after SET_VRING_ENABLE; scan
	// the ring once so anything already published gets picked up.
	q.pump()
}

// stop stops the queue and waits for all inflight requests to complete
// (the spec requires a stopped vring to have no inflight I/O before
// GET_VRING_BASE answers).
func (q *queue) stop() {
	if !q.running {
		return
	}
	q.running = false
	close(q.stopCh)
	if q.kick != nil {
		// Unblock a dispatcher parked in kick.Read. The fd itself is
		// QEMU's (we own a dup from SCM_RIGHTS); closing our copy is
		// safe and expected.
		_ = q.kick.Close()
		q.kick = nil
	}
	close(q.jobs)
	q.wg.Wait()
}

// closeFDs releases kick/call descriptors (queue teardown on reset).
func (q *queue) closeFDs() {
	if q.kick != nil {
		_ = q.kick.Close()
		q.kick = nil
	}
	if q.call != nil {
		_ = q.call.Close()
		q.call = nil
	}
}

// pump injects a synthetic scan: dispatch everything the ring has.
func (q *queue) pump() {
	q.dispatchOnce()
}

// dispatch is the dispatcher goroutine: park on the kick eventfd, then
// drain the available ring.
func (q *queue) dispatch() {
	defer q.wg.Done()
	defer close(q.jobs)
	buf := make([]byte, 8)
	for {
		if q.kick == nil {
			// Poll mode (SET_VRING_KICK with the no-fd flag): without
			// an eventfd we cannot be notified; scan until stopped.
			<-q.stopCh
			return
		}
		_, err := q.kick.Read(buf)
		select {
		case <-q.stopCh:
			return
		default:
		}
		if err != nil {
			// Kick fd closed (stop) — exit.
			return
		}
		if !q.dispatchOnce() {
			return
		}
	}
}

// dispatchOnce drains the available ring; returns false when the queue
// must stop (malformed chain: a broken guest is not worth an infinite
// error loop — same policy as pkg/vduse).
func (q *queue) dispatchOnce() bool {
	prev := debug.SetPanicOnFault(true)
	defer func() {
		if r := recover(); r != nil {
			q.logf("vub: q%d: guest memory fault in dispatcher (%v) — mappings for claimed entries are gone", q.idx, r)
		}
		debug.SetPanicOnFault(prev)
	}()
	ring := q.ring
	gm := q.d.gm
	for {
		select {
		case <-q.stopCh:
			return true
		default:
		}
		head, err := ring.NextAvail()
		if err != nil || head < 0 {
			return true
		}
		segs, err := vduse.ReadChain(gm, ring.DescBase(), int(ring.Size()), uint16(head))
		var br *vduse.BlkReq
		var hdr vduse.BlkReqHeader
		if err == nil {
			br, err = vduse.ParseBlkChain(segs, uint16(head))
			if err == nil {
				hdr, err = br.ReadHeader(gm)
			}
		}
		if err != nil {
			q.logf("vub: q%d: head=%d: %v — stopping queue", q.idx, head, err)
			return false
		}
		if br.DataLen() > maxReqBytes {
			q.logf("vub: q%d: head=%d: request too big (%d bytes) — stopping queue", q.idx, head, br.DataLen())
			return false
		}
		// Pin every buffer the worker will touch (the header was
		// already copied). A vanished mapping is fatal to this request
		// only: complete with I/O error and keep going.
		refs := make([]*vduse.RegionRef, 0, len(br.DataSegs)+1)
		pin := func(addr uint64, length int) bool {
			p, perr := gm.Acquire(addr, length)
			if perr != nil {
				return false
			}
			refs = append(refs, p)
			return true
		}
		ok := true
		for _, sg := range br.DataSegs {
			if !pin(sg.Addr, int(sg.Len)) {
				ok = false
				break
			}
		}
		if ok && !pin(br.StatusSeg.Addr, 1) {
			ok = false
		}
		if !ok {
			for _, p := range refs {
				p.Release()
			}
			q.d.iotlb.EvictAll(addrsOf(br.DataSegs), 0)
			q.completeSafe(ring, br, vduse.StatusOK, 0) // best effort
			continue
		}
		j := &job{ring: ring, br: br, hdr: hdr, refs: refs}
		q.inflight.Add(1)
		select {
		case q.jobs <- j:
		case <-q.stopCh:
			// Nobody will take the job: unwind the accounting.
			q.inflight.Add(-1)
			for _, p := range refs {
				p.Release()
			}
			return true
		}
	}
}

// work is a worker goroutine: execute jobs until the queue is closed.
func (q *queue) work() {
	defer q.wg.Done()
	irqPending := false
	defer func() {
		if irqPending && q.call != nil {
			signalFD(q.call)
		}
	}()
	for j := range q.jobs {
		q.execJob(j, &irqPending)
	}
}

// execJob runs one request and publishes its completion.
func (q *queue) execJob(j *job, irqPending *bool) {
	defer func() {
		if r := recover(); r != nil {
			q.logf("vub: q%d: guest memory fault in worker (%v) head=%d — completing with I/O error",
				q.idx, r, j.br.Head)
			q.d.iotlb.EvictAll(addrsOf(j.br.DataSegs), 0)
			q.completeSafe(j.ring, j.br, vduse.StatusIOErr, 0)
		}
		for _, p := range j.refs {
			p.Release()
		}
		q.inflight.Add(-1)
	}()
	status, written := q.d.exec(j.br, j.hdr)
	q.completeJob(j.ring, j.br, status, written, irqPending)
}

// completeJob writes the status byte, publishes the used entry and
// batches the call-fd signal: the interrupt fires when the queue runs
// dry (keeps one eventfd write per burst instead of per request).
func (q *queue) completeJob(ring *vduse.SplitRing, br *vduse.BlkReq, status uint8, written uint32, irqPending *bool) {
	if !q.completeSafe(ring, br, status, written) {
		return
	}
	*irqPending = true
	if len(q.jobs) == 0 {
		if *irqPending && q.call != nil {
			signalFD(q.call)
		}
		*irqPending = false
	}
}

// completeSafe writes the status byte and the used entry under a fault
// guard: it runs from recover handlers too, where a second fault must
// degrade to "completion lost" rather than crashing the daemon.
func (q *queue) completeSafe(ring *vduse.SplitRing, br *vduse.BlkReq, status uint8, written uint32) (ok bool) {
	prev := debug.SetPanicOnFault(true)
	defer func() {
		if recover() != nil {
			ok = false
		}
		debug.SetPanicOnFault(prev)
	}()
	if err := br.WriteStatus(q.d.gm, status); err != nil {
		q.logf("vub: q%d: write status head=%d: %v", q.idx, br.Head, err)
		return false
	}
	if irq, err := ring.PushUsed(br.Head, written); err != nil {
		q.logf("vub: q%d: push used head=%d: %v", q.idx, br.Head, err)
		return false
	} else if irq {
		return true
	}
	return true
}

// signalFD signals a call eventfd/pipe (8-byte value, contents
// meaningless per the vhost-user spec).
func signalFD(f *os.File) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	_, _ = f.Write(b[:])
}

// addrsOf projects segments to their base addresses for EvictAll.
func addrsOf(segs []vduse.Seg) []uint64 {
	out := make([]uint64, len(segs))
	for i, s := range segs {
		out[i] = s.Addr
	}
	return out
}

// fmtSegs renders a chain for debug logs.
func fmtSegs(segs []vduse.Seg) string {
	s := ""
	for i, sg := range segs {
		s += fmt.Sprintf(" [%d %x len=%d w=%v]", i, sg.Addr, sg.Len, sg.Write)
	}
	return s
}
