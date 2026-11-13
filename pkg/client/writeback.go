package client

import (
	"container/list"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/unistack-org/storage/pkg/object"
)

// wbPageSize is the writeback buffer's page. Guest I/O is 4 KiB aligned
// for every backend we serve (NBD, vduse, VUB), so the page is the unit
// that turns "many random 4 KiB writes" into "one run per object" and
// makes read-modify-write (partial page) rare.
const wbPageSize = 4096

// writeback is the volume's client-side writeback cache (ideas 3.5,
// the client_enable_writeback analogue): WriteAt copies into a page
// cache and completes; dirty pages are merged into runs per object and
// written to the replicas in the background (timer, memory cap,
// explicit Flush). The guest sees memcpy latency plus full request
// merging; the durability contract changes exactly as in Vitastor - a
// crash of the *client* process loses unflushed data - so the cache is
// opt-in (Cluster.SetWriteback) and Flush is the fsync.
//
// Ordering rules that keep it credible for a real guest:
//   - reads overlay dirty pages (read-your-writes through the cache);
//   - at most one flusher per object: writes that land while an old
//     copy of their pages is in flight queue behind the in-flight run
//     (o.mu, drained in a loop so nothing is skipped);
//   - zeroes/trim, snapshots, rollback, Flush and Close flush the
//     volume first, so a background run never crosses them;
//   - the first flush error is kept and reported once to the next
//     WriteAt/Flush (the classic writeback error rule); the pages stay
//     dirty and are retried.
//
// Locking: a page is never mutated after being stored (a write builds
// a fresh page and swaps the map entry), so readers only need a map
// lookup under wb.mu. wb.mu guards objs, lru, dirty and err; o.mu
// serialises writers and the flusher of one object and is always taken
// before wb.mu.
type writeback struct {
	v        *Volume
	maxBytes int64
	every    time.Duration

	mu    sync.Mutex        // guards objs, lru, dirty, err
	objs  map[uint64]*wbObj // object index -> dirty pages
	lru   list.List         // *wbObj, least-recently-dirtied first
	dirty int64             // bytes held in the cache
	err   error             // keep-first flush error

	stop chan struct{}
	wg   sync.WaitGroup
}

type wbObj struct {
	index uint64
	mu    sync.Mutex // writers vs the flusher of this object
	// Guarded by wb.mu. pages is replaced, never mutated in place.
	pages map[uint32][]byte // page index inside the object -> page
	bytes int
	el    *list.Element // LRU position, nil when not in the LRU
}

// newWriteback starts the periodic flusher. maxBytes <= 0 disables the
// cache (callers must not build it at all in that case).
func newWriteback(v *Volume, maxBytes int64, every time.Duration) *writeback {
	wb := &writeback{
		v:        v,
		maxBytes: maxBytes,
		every:    every,
		objs:     map[uint64]*wbObj{},
		stop:     make(chan struct{}),
	}
	wb.lru.Init()
	wb.wg.Add(1)
	go wb.run()
	return wb
}

func (wb *writeback) run() {
	defer wb.wg.Done()
	t := time.NewTicker(wb.every)
	defer t.Stop()
	for {
		select {
		case <-wb.stop:
			return
		case <-t.C:
			wb.flushAll(context.Background())
		}
	}
}

// Close stops the flusher and flushes what is left. Volume.Close calls
// it after the batchers stopped, so no new writes can arrive.
func (wb *writeback) Close() error {
	close(wb.stop)
	wb.wg.Wait()
	err := wb.flushAll(context.Background())
	wb.mu.Lock()
	if wb.err != nil && err == nil {
		err = wb.err
	}
	wb.mu.Unlock()
	return err
}

// takeErr returns the kept flush error, reported once.
func (wb *writeback) takeErr() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	err := wb.err
	wb.err = nil
	return err
}

func (wb *writeback) recordErr(err error) {
	wb.mu.Lock()
	if wb.err == nil {
		wb.err = err
	}
	wb.mu.Unlock()
}
// objAt returns the live cache entry of an object, creating it and
// moving it to the LRU tail. Callers must hold wb.mu.
func (wb *writeback) objAt(index uint64) *wbObj {
	o := wb.objs[index]
	if o == nil {
		o = &wbObj{index: index}
		wb.objs[index] = o
	}
	if o.el == nil {
		o.el = wb.lru.PushBack(o)
	} else {
		wb.lru.MoveToBack(o.el)
	}
	return o
}

// objectLocked returns the object's entry with o.mu held. It re-checks
// that the entry is still the live one after taking o.mu: a flusher may
// have removed it while we waited, and pages stored into a detached
// entry would be lost to both readers and later flushes.
func (wb *writeback) objectLocked(index uint64) *wbObj {
	for {
		wb.mu.Lock()
		o := wb.objAt(index)
		wb.mu.Unlock()
		o.mu.Lock()
		wb.mu.Lock()
		fresh := wb.objs[index] == o
		wb.mu.Unlock()
		if fresh {
			return o
		}
		o.mu.Unlock()
	}
}

// submit copies p into the cache and returns the number of bytes
// accepted (the caller reports them to the guest as a completed write).
func (wb *writeback) submit(ctx context.Context, off uint64, p []byte) (int, error) {
	touched := make(map[uint64]struct{}, 4)
	total := 0
	for total < len(p) {
		cur := off + uint64(total)
		index, inObj := object.Split(cur)
		n := len(p) - total
		if rest := int(object.Size - inObj); n > rest {
			n = rest
		}
		if err := wb.putPage(index, uint32(inObj/wbPageSize), int(inObj%wbPageSize), p[total:total+n]); err != nil {
			return total, err
		}
		touched[index] = struct{}{}
		total += n
	}
	wb.enforceCap(ctx, touched)
	return total, nil
}

// putPage merges data into the cached page (index, opg), fetching the
// page from the replicas when the write does not cover it fully.
func (wb *writeback) putPage(index uint64, opg uint32, base int, data []byte) error {
	o := wb.objectLocked(index)
	defer o.mu.Unlock()

	start := index*object.Size + uint64(opg)*wbPageSize
	size := wb.v.Size()
	lim := int(wbPageSize)
	if start+wbPageSize > size { // volume tail: never cache past EOV
		lim = int(size - start)
	}
	if base >= lim || len(data) == 0 {
		return nil
	}
	if base+len(data) > lim {
		data = data[:lim-base]
	}

	wb.mu.Lock()
	old := o.pages[opg]
	wb.mu.Unlock()

	page := make([]byte, wbPageSize)
	switch {
	case old != nil:
		copy(page, old) // merge into the newest cached version
	case base == 0 && len(data) == wbPageSize:
		// Full page: the write is authoritative, no fetch.
	default:
		// Read-modify-write. Volume offsets, not object-relative ones:
		// ReadAt wants a position in the volume.
		if _, err := wb.v.ReadAt(context.Background(), page[:lim], start); err != nil {
			return err
		}
	}
	copy(page[base:base+len(data)], data)

	wb.mu.Lock()
	if o.pages == nil {
		o.pages = map[uint32][]byte{}
	}
	if _, dup := o.pages[opg]; !dup {
		o.bytes += len(page)
		wb.dirty += int64(len(page))
	}
	o.pages[opg] = page
	wb.lru.MoveToBack(o.el)
	wb.mu.Unlock()
	return nil
}

// enforceCap flushes whole objects (LRU first, skipping the objects the
// caller just dirtied, which are the hottest) until the cache fits.
// It blocks when every candidate is being flushed - that is the cache's
// backpressure: the guest cannot outrun the OSDs.
func (wb *writeback) enforceCap(ctx context.Context, hot map[uint64]struct{}) {
	for {
		wb.mu.Lock()
		if wb.dirty <= wb.maxBytes {
			wb.mu.Unlock()
			return
		}
		var victim *wbObj
		for e := wb.lru.Front(); e != nil; e = e.Next() {
			o := e.Value.(*wbObj)
			if _, busy := hot[o.index]; busy {
				continue
			}
			if o.mu.TryLock() {
				victim = o
				break
			}
		}
		wb.mu.Unlock()
		if victim == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Microsecond):
			}
			continue
		}
		wb.drain(ctx, victim)
		victim.mu.Unlock()
	}
}

// overlay copies dirty pages over a buffer just read from the replicas
// for the volume range [off, off+len(buf)).
func (wb *writeback) overlay(buf []byte, off uint64) {
	if len(buf) == 0 {
		return
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if wb.dirty == 0 {
		return
	}
	end := off + uint64(len(buf))
	for index := off / object.Size; index*object.Size < end; index++ {
		o := wb.objs[index]
		if o == nil || len(o.pages) == 0 {
			continue
		}
		base := index * object.Size
		for opg, page := range o.pages {
			poff := base + uint64(opg)*wbPageSize
			lo := max(poff, off)
			hi := min(poff+uint64(len(page)), end)
			if lo < hi {
				copy(buf[lo-off:hi-off], page[lo-poff:hi-poff])
			}
		}
	}
}

func (wb *writeback) peekDirty() int64 {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.dirty
}

// flushAll writes every dirty page. Volume.Flush calls it.
func (wb *writeback) flushAll(ctx context.Context) error {
	wb.mu.Lock()
	all := make([]*wbObj, 0, len(wb.objs))
	for _, o := range wb.objs {
		all = append(all, o)
	}
	wb.mu.Unlock()
	var first error
	for _, o := range all {
		o.mu.Lock()
		err := wb.drain(ctx, o)
		o.mu.Unlock()
		if err != nil && first == nil {
			first = err
		}
	}
	return first
}

// drain sends the object's dirty pages, repeatedly: writes that arrive
// while a run is in flight must not be skipped (the caller holds o.mu,
// so they queue and become visible to the next pass).
func (wb *writeback) drain(ctx context.Context, o *wbObj) error {
	for {
		wb.mu.Lock()
		pages := o.pages
		if len(pages) == 0 {
			o.pages = nil
			o.bytes = 0
			if o.el != nil {
				wb.lru.Remove(o.el)
				o.el = nil
			}
			delete(wb.objs, o.index)
			wb.mu.Unlock()
			return nil
		}
		o.pages = nil
		o.bytes = 0
		wb.dirty -= int64(len(pages)) * wbPageSize
		wb.mu.Unlock()

		err := wb.writeRuns(ctx, o.index, pages)
		if err != nil {
			// Keep the pages that were not sent (writeRuns trimmed the
			// map), re-register the object, report the error once.
			wb.mu.Lock()
			if len(pages) > 0 {
				o.pages = pages
				o.bytes = len(pages) * wbPageSize
				wb.dirty += int64(len(pages)) * wbPageSize
			}
			if wb.objs[o.index] == o && o.el == nil {
				o.el = wb.lru.PushBack(o)
			}
			wb.mu.Unlock()
			wb.recordErr(err)
			return err
		}
	}
}

// writeRuns merges dirty pages into maximal runs of adjacent pages and
// sends each as one replica request. On failure the map is trimmed to
// what remains to be written, so the caller can keep the rest dirty.
func (wb *writeback) writeRuns(ctx context.Context, index uint64, pages map[uint32][]byte) error {
	opgs := make([]int, 0, len(pages))
	for opg := range pages {
		opgs = append(opgs, int(opg))
	}
	sort.Ints(opgs)
	for i := 0; i < len(opgs); {
		j := i
		for j+1 < len(opgs) && opgs[j+1] == opgs[j]+1 {
			j++
		}
		first, last := uint32(opgs[i]), uint32(opgs[j])
		start := index*object.Size + uint64(first)*wbPageSize
		n := int(wbPageSize) * (int(last-first) + 1)
		buf := make([]byte, 0, n)
		for opg := first; opg <= last; opg++ {
			buf = append(buf, pages[opg]...)
		}
		// The volume tail page is padded to a full page in the cache:
		// clip the payload at the volume end.
		if size := wb.v.Size(); start+uint64(n) > size {
			n = int(size - start)
			buf = buf[:n]
		}
		if err := wb.v.writeChunk(ctx, buf, uint64(n), index, uint64(first)*wbPageSize, false, false); err != nil {
			// Runs before this one are durable - drop them; the failing
			// run and everything after it stay dirty for the retry.
			for k := 0; k < i; k++ {
				delete(pages, uint32(opgs[k]))
			}
			return err
		}
		i = j + 1
	}
	return nil
}
