//go:build !linux

package iouring

import (
	"os"
	"runtime"
	"sync"
)

// New returns the portable fallback ring: a fixed worker pool running
// pread/pwrite via os.File on the given fds. Semantics match the Linux
// io_uring implementation from the caller's perspective.
func New(entries int) (Ring, error) {
	if entries <= 0 {
		entries = 256
	}
	r := &fallbackRing{
		files: map[int32]*os.File{},
		work:  make(chan Op, entries),
	}
	for i := 0; i < runtime.NumCPU(); i++ {
		r.wg.Add(1)
		go r.worker()
	}
	return r, nil
}

type fallbackRing struct {
	mu      sync.Mutex // guards closed + work sends (never held while a worker needs it)
	filesMu sync.Mutex
	files   map[int32]*os.File
	work    chan Op
	wg      sync.WaitGroup
	closed  bool
}

// registerFile maps an fd to an *os.File so the workers can use Go's
// poller/fd cache. Callers on non-Linux platforms must Register an fd
// before submitting (the Linux implementation registers via
// IORING_REGISTER_FILES automatically from Submit).
func (r *fallbackRing) RegisterFile(fd int32, f *os.File) {
	r.filesMu.Lock()
	r.files[fd] = f
	r.filesMu.Unlock()
}

func (r *fallbackRing) file(fd int32) (*os.File, bool) {
	r.filesMu.Lock()
	f, ok := r.files[fd]
	r.filesMu.Unlock()
	return f, ok
}

func (r *fallbackRing) worker() {
	defer r.wg.Done()
	for op := range r.work {
		n, err := r.do(op)
		if op.Done != nil {
			op.Done(n, err)
		}
	}
}

func (r *fallbackRing) do(op Op) (int, error) {
	f, ok := r.file(op.Fd)
	if !ok {
		return 0, os.ErrClosed
	}
	if op.Write {
		return f.WriteAt(op.Buf, op.Off)
	}
	return f.ReadAt(op.Buf, op.Off)
}

func (r *fallbackRing) Submit(ops []Op) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		for _, op := range ops {
			if op.Done != nil {
				go op.Done(0, ErrClosed)
			}
		}
		return ErrClosed
	}
	for _, op := range ops {
		r.work <- op
	}
	return nil
}

func (r *fallbackRing) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.work)
	r.mu.Unlock()
	r.wg.Wait()
	return nil
}
