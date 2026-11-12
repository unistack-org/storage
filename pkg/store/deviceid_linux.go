//go:build linux

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// deviceIDImpl fills what Linux knows about the device backing path:
// the mount table entry (source, fstype, mount point, options), the
// block device node, the filesystem UUID (from /dev/disk/by-uuid,
// which udev populates for every filesystem that has one), and the
// device serial / WWID (the stable identity of a filesystemless disk,
// and the label of any disk).
func deviceIDImpl(path string, id *DeviceIdentity) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	st, err := os.Stat(abs)
	if err != nil {
		return err
	}
	if mode := st.Mode(); mode&os.ModeDevice != 0 {
		// A device node: the identity of the node itself.
		id.Device = canonicalDevice(abs)
		id.MountSource = id.Device
		fillByLinks(id)
		return nil
	}
	// Filesystem holding the path. Its st_dev maps to a mountinfo
	// entry; deviceForPath (shared with ProbeDisk) resolves the node.
	id.Device, _ = deviceForPath(abs) // "" on tmpfs/fuse/overlay
	b, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil
	}
	source, mount, options, fstype, ok := parseMountinfo(string(b), deviceNumberText(st.Sys().(*syscall.Stat_t).Dev))
	if ok {
		id.MountSource, id.MountPoint, id.MountOptions, id.FSType = source, mount, options, fstype
		if id.FstabDevice == "" {
			if ent, found := fstabEntryForMount(id.MountPoint); found {
				id.FstabDevice = ent.device
			}
		}
	}
	fillByLinks(id)
	return nil
}

// fillByLinks reads the /dev/disk/by-* identity links. The filesystem
// UUID must match the exact filesystem device (a partition's by-uuid
// link names that fs, never the parent disk); serial and WWID come
// from the device chain (the node, its parent disk for a partition,
// the slaves of a dm/raid stack), so a filesystemless disk is still
// identified by its hardware serial.
func fillByLinks(id *DeviceIdentity) {
	if id.Device == "" {
		return
	}
	canon := canonicalDevice(id.Device)
	self := map[string]bool{canon: true}
	all := map[string]bool{canon: true}
	if parent := parentDiskNode(canon); parent != "" {
		all[parent] = true
	}
	for _, s := range slaveNodes(canon) {
		all[s] = true
	}
	if name, _, ok := scanByLinks("uuid", self); ok {
		id.Filesystem = name
	}
	if serial, _, ok := scanByLinks("id", all); ok {
		id.Serial = serial
	}
	if wwid, _, ok := scanByLinks("wwid", all); ok {
		id.WWID = wwid
	}
}

// parentDiskNode returns the canonical node of the disk a partition
// sits on (/dev/sda3 -> /dev/sda, /dev/nvme0n1p2 -> /dev/nvme0n1), ""
// when the node is not a partition.
func parentDiskNode(node string) string {
	base := filepath.Base(node)
	parent := parentDiskName(base)
	if parent == "" || parent == base {
		return ""
	}
	return filepath.Join(filepath.Dir(node), parent)
}

// slaveNodes returns the canonical device nodes listed in a stacked
// device's sysfs slaves directory (dm/raid), empty for a plain disk.
func slaveNodes(node string) []string {
	name := filepath.Base(node)
	sl, err := os.ReadDir(filepath.Join("/sys/block", name, "slaves"))
	if err != nil {
		return nil
	}
	var out []string
	for _, s := range sl {
		out = append(out, canonicalDevice(filepath.Join("/dev", s.Name())))
	}
	return out
}

// deviceNumberText renders an st_dev as mountinfo's "major:minor".
func deviceNumberText(rdev uint64) string {
	return fmt.Sprintf("%d:%d", major(rdev), minor(rdev))
}

// fstabEntryForMount returns the fstab entry whose mount point equals
// m (the mount table is authoritative for the device; the fstab line
// only labels the operator's configured source, e.g. "UUID=...").
func fstabEntryForMount(m string) (fstabEntry, bool) {
	if m == "" {
		return fstabEntry{}, false
	}
	content, err := fstabReader()
	if err != nil {
		return fstabEntry{}, false
	}
	for _, e := range parseFstab(content) {
		if filepath.Clean(e.mount) == filepath.Clean(m) {
			return e, true
		}
	}
	return fstabEntry{}, false
}
