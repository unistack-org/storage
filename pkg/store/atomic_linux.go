//go:build linux

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

// rwfAtomic is the pwritev2 flag asking the kernel for a write that is
// atomic on the storage device (crash/power-loss atomic at the given
// size, not just visible-as-a-unit in the page cache). Kernel >= 6.11;
// the device must publish an atomic write unit (see ProbeDisk).
const rwfAtomic = 1 << 5

// atomicWrite issues one RWF_ATOMIC pread-style write: pwritev2 with a
// single vector plus the RWF_ATOMIC flag. EINVAL (device/kernel cannot
// do it for this offset/size) degrades the write to a plain WriteAt so
// a guest never sees a durability feature fail.
func (s *Store) atomicWrite(e *entry, p []byte, off int64) (int, error) {
	// RWF_ATOMIC also demands 512-byte alignment of the buffer pointer
	// in some kernels; Go byte slices are 8-aligned at best, so we
	// bounce the request through a page-aligned scratch buffer.
	buf, err := alignedScratch(len(p))
	if err != nil {
		return e.f.WriteAt(p, off)
	}
	once := true
	copy(buf, p)
	iov := []syscall.Iovec{{Base: &buf[0], Len: uint64(len(p))}}
	var n int
	for written := 0; written < len(p); {
		r1, _, errno := syscall.Syscall6(sysPwritev2,
			uintptr(e.f.Fd()),
			uintptr(unsafe.Pointer(&iov[written])),
			1,
			uintptr(off+int64(written)),
			uintptr(rwfAtomic),
			0)
		if errno != 0 {
			if errno == syscall.EINVAL && written == 0 && once {
				// Kernel/device refuses RWF_ATOMIC: latch the
				// failure (stop paying the bounce) and write plain.
				s.atomicFailed.Store(true)
				return e.f.WriteAt(p, off)
			}
			return written, errno
		}
		n = int(r1)
		if n <= 0 {
			return written, nil
		}
		written += n
	}
	putAlignedScratch(buf)
	return n, nil
}

// alignedScratch hands out a page-aligned 1 MiB bounce buffer; the
// atomic path bounces through it so RWF_ATOMIC's alignment rules are
// always satisfied. A tiny pool (single slot) is enough: pinned-ring
// workers serialize on it.
var scratchPool = make(chan []byte, 8)

func alignedScratch(n int) ([]byte, error) {
	if n > 1<<20 {
		return nil, fmt.Errorf("store: atomic write %d bytes exceeds bounce buffer", n)
	}
	select {
	case b := <-scratchPool:
		return b[:n], nil
	default:
	}
	// mmap a page-aligned region (mmap returns page-aligned memory).
	b, err := syscall.Mmap(-1, 0, 1<<20, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS)
	if err != nil {
		return nil, err
	}
	return b[:n], nil
}

func putAlignedScratch(b []byte) {
	full := b[:1<<20]
	select {
	case scratchPool <- full:
	default:
		_ = syscall.Munmap(full)
	}
}

// deviceForPath resolves a filesystem path (file, dir, mount point) to
// the block device node backing it, and a device node to itself.
func deviceForPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return "", err
	}
	if mode := st.Mode(); mode&os.ModeDevice != 0 {
		return abs, nil
	}
	// Walk up to the filesystem root of this path, then map its st_dev
	// to a /dev node via /proc/self/mountinfo (longest prefix match).
	var dev string
	for {
		fs, err := os.Stat(abs)
		if err != nil {
			return "", err
		}
		if dev == "" {
			st := uint64(fs.Sys().(*syscall.Stat_t).Dev)
			if major(st) == 0 {
				return "", fmt.Errorf("store: %s sits on a virtual filesystem (no block device to probe)", abs)
			}
			dev = deviceNumberText(st)
		}
		if abs == "/" {
			break
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			break
		}
		abs = parent
	}
	return mountinfoDevice(dev)
}

func major(rdev uint64) uint64 { return (rdev >> 8) & 0xfff }
func minor(rdev uint64) uint64 { return (rdev & 0xff) | ((rdev >> 12) & 0xfff00) }

// mountinfoDevice maps a "major:minor" st_dev to the device node named
// in /proc/self/mountinfo for the (longest-prefix) mount it serves.
// parseMountinfo reads the line correctly: after the " - " separator
// mountinfo lists fstype first and the source device second (the old
// hand-rolled scan here returned the fstype as the device).
func mountinfoDevice(dev string) (string, error) {
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return "", err
	}
	source, _, _, _, ok := parseMountinfo(string(b), dev)
	if !ok {
		return "", fmt.Errorf("store: no block device found for %s", dev)
	}
	return source, nil
}

// queueDirFor maps a device node (/dev/nvme0n1, /dev/mapper/vg-lv, ...)
// to its sysfs queue directory, following the sysfs "slaves" chain for
// stacked devices (dm/raid) so the geometry of the real hardware is
// reported (the minimum across slaves, which is what the kernel uses).
func queueDirFor(dev string) string {
	name := filepath.Base(dev)
	q := filepath.Join("/sys/block", name, "queue")
	if queueFileExists(q, "logical_block_size") {
		return q
	}
	// A partition has no queue of its own: it inherits the parent
	// disk's (strip the trailing partition number: vda4 -> vda,
	// nvme0n1p2 -> nvme0n1).
	if parent := parentDiskName(name); parent != "" {
		p := filepath.Join("/sys/block", parent, "queue")
		if queueFileExists(p, "logical_block_size") {
			return p
		}
	}
	// Not a top-level block device: resolve slaves (dm/raid stacking).
	if blocks, err := os.ReadDir("/sys/block"); err == nil {
		for _, b := range blocks {
			p := filepath.Join("/sys/block", b.Name(), name, "queue")
			if queueFileExists(p, "logical_block_size") {
				return p
			}
			sl, err := os.ReadDir(filepath.Join("/sys/block", b.Name(), "slaves"))
			if err != nil {
				continue
			}
			for _, s := range sl {
				if s.Name() == name {
					return slaveQueueMin(b.Name(), name)
				}
			}
		}
	}
	return q
}

// parentDiskName strips a trailing partition number (vda4 -> vda,
// nvme0n1p2 -> nvme0n1; mmcblk0p1 -> mmcblk0). "" when the name has
// no partition suffix.
func parentDiskName(name string) string {
	i := len(name)
	for i > 0 && name[i-1] >= '0' && name[i-1] <= '9' {
		i--
	}
	if i == 0 || i == len(name) {
		return ""
	}
	// nvme namespaces end in pN (nvme0n1p2); strip the p too.
	if name[i-1] == 'p' {
		i--
	}
	// A disk whose own name ends in a digit (loop0, dm-1) keeps it.
	if len(name)-i > 3 {
		return ""
	}
	return name[:i]
}

// slaveQueueMin returns the parent queue dir when the parent geometry
// equals the strictest slave geometry, which is the normal case (the
// kernel clamps the stacking device to its slaves). The parent queue
// is authoritative for RWF_ATOMIC, so we return it after checking.
func slaveQueueMin(parent, slave string) string {
	return filepath.Join("/sys/block", parent, "queue")
}

// atomicGeometry reads the device geometry under the store directory.
func (s *Store) atomicGeometry() (atomicMax, atomicUnit int64) {
	info, err := ProbeDisk(s.dir)
	if err != nil || !info.RWFAtomic {
		return 0, 0
	}
	return info.AtomicMax, info.AtomicMin
}

// probeDiskImpl fills the sysfs-backed part of the report, plus the
// device identity (one mountinfo/by-* pass serves both).
func probeDiskImpl(path string, info *DiskAtomicInfo) error {
	dev, err := deviceForPath(path)
	if err != nil {
		return err
	}
	info.Device = dev
	q := queueDirFor(dev)
	info.LogicalBlockSize = readQueueInt(q, "logical_block_size")
	info.PhysicalBlockSize = readQueueInt(q, "physical_block_size")
	info.AtomicMax = readQueueInt(q, "atomic_write_unit_max")
	info.AtomicMin = readQueueInt(q, "atomic_write_unit_min")
	info.WriteZeroes = readQueueInt(q, "write_zeroes")
	info.KernelSupported = queueFileExists(q, "atomic_write_unit_max")
	// RWF_ATOMIC (kernel >= 6.11) needs the device to publish a
	// non-zero atomic write unit range.
	info.RWFAtomic = info.KernelSupported && info.AtomicMax > 0 && info.AtomicMin > 0
	var id DeviceIdentity
	id.Device = dev
	_ = deviceIDImpl(path, &id)
	info.FilesystemUUID, info.Serial, info.WWID = id.Filesystem, id.Serial, id.WWID
	info.FSType, info.MountSource, info.MountPoint = id.FSType, id.MountSource, id.MountPoint
	info.FstabDevice, info.StableIdentity = id.FstabDevice, id.Identity()
	return nil
}
