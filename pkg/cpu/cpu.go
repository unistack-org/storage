// Package cpu pins OS threads to CPU cores for latency-sensitive I/O
// workers (io_uring submit/completion loops, acceptors).
//
// Linux semantics that make this useful from Go: sched_setaffinity is
// per-thread, and a newly created thread inherits the CPU mask of the
// thread that created it. Pinning an early main thread (before the
// runtime spawns its workers) therefore shades the whole process,
// while PinCurrent (which also calls runtime.LockOSThread) pins one
// long-lived worker goroutine exactly.
//
// On non-Linux systems every pinning call reports ErrUnsupported and
// has no effect; the CPU-list parser stays available (and tested).
package cpu

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// ErrUnsupported is returned by the pinning functions on platforms
// without thread-affinity support.
var ErrUnsupported = errors.New("cpu: thread affinity unsupported on this platform")

// ErrEmptyList is returned by ParseCPUs for inputs that select no CPU.
var ErrEmptyList = errors.New("cpu: empty CPU list")

var pinned atomic.Bool

// Pinned reports whether PinCurrent has pinned the current thread.
func Pinned() bool { return pinned.Load() }

// ParseCPUs parses a Linux cpulist: "0", "0,2", "0-3", "0-3,8,10-11".
// Ranges are inclusive and may be written reversed ("3-0"). The result
// is sorted and deduplicated.
func ParseCPUs(s string) ([]int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, ErrEmptyList
	}
	var out []int
	seen := map[int]bool{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("cpu: empty element in %q", s)
		}
		loStr, hiStr, isRange := strings.Cut(part, "-")
		lo, err := strconv.Atoi(strings.TrimSpace(loStr))
		if err != nil {
			return nil, fmt.Errorf("cpu: bad CPU %q in %q", loStr, s)
		}
		if lo < 0 {
			return nil, fmt.Errorf("cpu: negative CPU in %q", s)
		}
		if !isRange {
			if !seen[lo] {
				seen[lo] = true
				out = append(out, lo)
			}
			continue
		}
		hi, err := strconv.Atoi(strings.TrimSpace(hiStr))
		if err != nil {
			return nil, fmt.Errorf("cpu: bad CPU range end %q in %q", hiStr, s)
		}
		if hi < 0 {
			return nil, fmt.Errorf("cpu: negative CPU in %q", s)
		}
		if lo > hi {
			lo, hi = hi, lo
		}
		if hi-lo > 4096 {
			return nil, fmt.Errorf("cpu: range %q is unreasonably wide", part)
		}
		for c := lo; c <= hi; c++ {
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
	}
	if len(out) == 0 {
		return nil, ErrEmptyList
	}
	sort.Ints(out)
	return out, nil
}

// Available reports the number of CPUs visible to the process. Callers
// use it to reject a pin list that names cores which do not exist.
func Available() int { return runtime.NumCPU() }
