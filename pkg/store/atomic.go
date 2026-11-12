package store

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Atomic geometry of the backing device, detected at Open:
//
//	atomicMax   device atomic write unit max (bytes); 0 = the device
//	            publishes no atomic write unit, RWF_ATOMIC is never used
//	atomicUnit  current atomic write unit (bytes); writes that are
//	            aligned to and no larger than this value *and* the
//	            store's AtomicSize are candidates for RWF_ATOMIC
//
// The zero values disable the atomic path everywhere, which is the
// portable default.
// atomicWritePossible reports whether [off, off+len(p)) qualifies for an
// RWF_ATOMIC write: the store asked for it, the device can do it, no
// earlier attempt was rejected, the size is within the configured
// atomic size and the device's unit, and the offset is aligned to the
// device's logical block size.
func (s *Store) atomicWritePossible(off int64, n int) bool {
	if !s.opts.AtomicWrites || s.atomicFailed.Load() {
		return false
	}
	max, unit := s.atomicMax.Load(), s.atomicUnit.Load()
	if max <= 0 || unit <= 0 {
		return false
	}
	size := s.opts.AtomicSize
	if size <= 0 {
		size = 4096
	}
	// The kernel (block: atomic writes) requires the length to be a
	// multiple of atomic_write_unit_min and the offset aligned to it.
	return int64(n) <= size && int64(n) <= max && int64(n)%unit == 0 && off%unit == 0
}

// DiskAtomicInfo is the probe-disk report for one block device (or the
// device backing a filesystem path).
type DiskAtomicInfo struct {
	Path              string // the path that was probed
	Device            string // resolved block device (e.g. /dev/nvme0n1), "" if unknown
	LogicalBlockSize  int64  // queue/logical_block_size
	PhysicalBlockSize int64  // queue/physical_block_size
	AtomicMax         int64  // queue/atomic_write_unit_max in bytes (0 = not published)
	AtomicMin         int64  // queue/atomic_write_unit_min in bytes
	WriteZeroes       int64  // queue/write_zeroes (1 = HW zero blocks)
	RWFAtomic         bool   // kernel interface: RWF_ATOMIC usable on this device
	KernelSupported   bool   // sysfs publishes the atomic queue attributes at all
}

// ProbeDisk reports the atomic-write geometry of the block device
// backing path (a device node, a mount point, or any file: its
// filesystem's device is used). The values come from the kernel's
// block queue sysfs attributes, which is exactly what RWF_ATOMIC
// enforces: a device with atomic_write_unit_max == 0 cannot perform
// atomic writes at any size.
//
// The detection is Linux-specific in what it reads (sysfs); off Linux
// the returned info carries only Path and the portable parts.
func ProbeDisk(path string) (DiskAtomicInfo, error) {
	info := DiskAtomicInfo{Path: path}
	if _, err := os.Stat(path); err != nil {
		return info, err
	}
	if err := probeDiskImpl(path, &info); err != nil {
		return info, err
	}
	return info, nil
}

// queueInt reads one sysfs queue attribute; missing/garbage is 0.
func readQueueInt(queueDir, name string) int64 {
	b, err := os.ReadFile(filepath.Join(queueDir, name))
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func queueFileExists(queueDir, name string) bool {
	_, err := os.Stat(filepath.Join(queueDir, name))
	return err == nil
}
