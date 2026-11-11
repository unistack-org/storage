//go:build linux

package cpu

import (
	"fmt"
	"runtime"
)

// PinCurrent locks the current goroutine to its OS thread and pins
// that thread to the given CPUs (Linux cpulist syntax, e.g. "2" or
// "2-3"). A worker goroutine that runs for the process lifetime
// (ring submit/completion loops, accept loops) should call it first
// thing. The list must name existing CPUs and at most 64 of them.
func PinCurrent(cpus string) error {
	list, err := ParseCPUs(cpus)
	if err != nil {
		return err
	}
	max := Available()
	var mask uint64
	for _, c := range list {
		if c >= max {
			return fmt.Errorf("cpu: %d is not a valid CPU (0..%d)", c, max-1)
		}
		if c >= 64 {
			return fmt.Errorf("cpu: CPU %d exceeds the 64-CPU mask", c)
		}
		mask |= 1 << uint(c)
	}
	runtime.LockOSThread()
	if err := schedSetAffinity(mask); err != nil {
		runtime.UnlockOSThread()
		return fmt.Errorf("sched_setaffinity %q: %w", cpus, err)
	}
	pinned.Store(true)
	return nil
}
