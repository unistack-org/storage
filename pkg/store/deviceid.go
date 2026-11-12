package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Device identity answers the question an operator cannot see from a
// mount point: which block device backs this data directory. An OSD id
// lives inside the data directory (<dir>/id), so an operator who mounts
// disks by kernel name (/dev/sda, /dev/sdb) and restarts the host after
// the enumeration changed (disks rewired, a new disk inserted) silently
// trades the disks between mount points: every id then lands on foreign
// data and the object layout of the whole cluster drifts.
//
// A DeviceIdentity is the stable description of the backing device:
// the filesystem UUID first (stable across rewiring, repartitioning and
// host moves), the device serial / WWID for filesystems that have no
// UUID, and the kernel device node as a label only - the node name is
// exactly what a swap changes. The identity is recorded at boot in the
// data directory (<dir>/device), in the node's meta directory (the
// path -> device map) and in the cluster registry (OSDInfo.Device), and
// every restart re-checks all three, so a swap becomes a fatal startup
// error instead of silent corruption.
//
// Reading it is platform-specific: deviceid_linux.go uses the mount
// table, /dev/disk/by-* and sysfs; off Linux only the portable parts
// are filled and every strict check is inert, because a guessed
// identity is worse than none.

// DeviceIdentity describes the block device backing a path.
type DeviceIdentity struct {
	// Path is the filesystem path that was probed.
	Path string
	// Device is the resolved block device node (/dev/sda1,
	// /dev/mapper/vg-lv, ...), "" when this platform cannot resolve
	// it, or when the path sits on a filesystem with no block device
	// behind it (tmpfs, fuse, overlay).
	Device string
	// Filesystem is the filesystem UUID of the mounted fs: the
	// preferred stable identity, "" when not readable.
	Filesystem string
	// Serial is the device id published by /dev/disk/by-id (for a
	// raw-device OSD this is the only stable id), "" when absent.
	Serial string
	// WWID is the world-wide device id (/dev/disk/by-wwid), "" when
	// absent.
	WWID string
	// FSType is the mounted filesystem type (ext4, xfs, ...).
	FSType string
	// MountSource is how the kernel mount table names the device
	// backing the path (/dev/sda1, /dev/mapper/vg-lv, tmpfs, ...),
	// "" when the mount is not readable.
	MountSource string
	// MountPoint is the mount point of the filesystem holding Path.
	MountPoint string
	// MountOptions are that filesystem's mount options.
	MountOptions string
	// FstabDevice is the /etc/fstab device spec serving this mount
	// point ("UUID=...", "/dev/disk/by-id/...", "/dev/sdb1", ...), ""
	// when the fs is not mounted from fstab. It exists to warn about
	// mounts by kernel name, the root cause of the id swap.
	FstabDevice string
}

// Stable reports whether the identity survives a mount swap: a
// filesystem UUID, a serial or a WWID is stable; a bare kernel device
// node is exactly what a swap changes.
func (id DeviceIdentity) Stable() bool {
	return id.Filesystem != "" || id.Serial != "" || id.WWID != ""
}

// Identity returns the canonical identity string ("" when no stable
// identity is known): "filesystem=<fs uuid>", "serial=<id>" or
// "wwid=<wwid>". This is the form stored in the pin files, in the node
// device map and in OSDInfo.Device, and the form accepted by
// "-data-dir uuid=<identity>@<dir>".
func (id DeviceIdentity) Identity() string {
	switch {
	case id.Filesystem != "":
		return "filesystem=" + id.Filesystem
	case id.Serial != "":
		return "serial=" + id.Serial
	case id.WWID != "":
		return "wwid=" + id.WWID
	}
	return ""
}

// IdentityLabel renders the identity for humans: the stable form when
// one exists, the device node (or mount source) as a label otherwise.
func (id DeviceIdentity) IdentityLabel() string {
	if s := id.Identity(); s != "" {
		return s
	}
	switch {
	case id.Device != "":
		return id.Device
	case id.MountSource != "":
		return id.MountSource
	}
	return "unknown device"
}

// MountedByKernelName reports the operator's mount spec when the
// filesystem serving this path is pinned to a volatile /dev/sdX-style
// name - a configuration where a reboot can trade disks between mount
// points. "" when the mount is stable (UUID=..., by-id, LVM/MD),
// unknown (no fstab entry), or not a block device.
func (id DeviceIdentity) MountedByKernelName() string {
	d := id.FstabDevice
	if d == "" || !strings.HasPrefix(d, "/dev/") || strings.HasPrefix(d, "/dev/disk/") {
		return ""
	}
	base := filepath.Base(d)
	// /dev/sda1, /dev/vdb, /dev/nvme0n1p2: names the kernel assigns in
	// probe order. /dev/mapper/*, /dev/md*, /dev/loop* (loop devices
	// are not disks an operator rewires) are stable by construction.
	for _, p := range []string{"sd", "vd", "hd", "nvme"} {
		if strings.HasPrefix(base, p) {
			return d
		}
	}
	return ""
}

// DeviceID reports the identity of the block device backing path (a
// device node, a mount point, or any file: the filesystem holding it
// is used). Detection quality is platform-specific; Path and the fstab
// label are filled everywhere, the rest where the platform exposes it.
func DeviceID(path string) (DeviceIdentity, error) {
	id := DeviceIdentity{Path: path}
	if _, err := os.Stat(path); err != nil {
		return id, err
	}
	if ent, ok := fstabEntryFor(path); ok {
		id.FstabDevice, id.MountPoint, id.MountOptions = ent.device, ent.mount, ent.options
	}
	if err := deviceIDImpl(path, &id); err != nil {
		return id, err
	}
	return id, nil
}

// NormalizeIdentity renders a stored identity string for humans: the
// legacy raw fs-UUID form gains its "filesystem=" prefix, anything
// else (including the path labels) is returned unchanged.
func NormalizeIdentity(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, "=") {
		return s
	}
	return "filesystem=" + s
}

// ValidateDeviceID checks a freshly detected identity against the
// identity pinned for a directory (the <dir>/device file, the node
// device map, an OSDInfo.Device record, or the uuid=<identity> segment
// of a -data-dir entry):
//
//	want == ""            acceptance: nothing was pinned (legacy pin)
//	want with no "="      legacy raw fs-UUID pin, checked as filesystem=<uuid>
//	want "path=..."       acceptance: a kernel-name label documents the
//	                      device without constraining it (written where
//	                      no stable identity is readable)
//	anything else         the detected identity must match exactly
//
// A mismatch means the mount points and the disks traded places since
// the pin was written; presenting this directory under the id pinned
// for the other disk would corrupt the cluster's object layout, so the
// daemon must refuse to start. The error carries the errIdentityMismatch
// marker so a caller with stronger evidence (meta) can downgrade it.
func ValidateDeviceID(got DeviceIdentity, want string) error {
	if err := ValidateIdentityStrings(got.Identity(), want); err != nil {
		return fmt.Errorf("%s: %w", got.Path, err)
	}
	return nil
}

// ValidateIdentityStrings compares a live identity string
// (DeviceIdentity.Identity(), "" when unreadable) against a pin. A pin
// of "" (or the bare device-node label prefix "path", which is
// informational only) always passes; a legacy raw fs-UUID pin is
// understood as "filesystem=<uuid>"; a non-empty pin against an empty
// live identity fails: the identity cannot be verified, which is not a
// licence to serve foreign data.
func ValidateIdentityStrings(gotID, want string) error {
	want = strings.TrimSpace(want)
	if want == "" || want == "path" || strings.HasPrefix(want, "path=") {
		return nil
	}
	if !strings.Contains(want, "=") { // legacy raw fs-UUID pin
		want = "filesystem=" + want
	}
	if gotID == "" {
		return identityError("cannot verify the pinned device identity %q: this platform reads no filesystem UUID/serial (run the daemon on Linux and mount the disk by UUID=... in fstab)", want)
	}
	if gotID != want {
		return fmt.Errorf("%w: live identity %s, pinned as %s - the mount points likely swapped across the restart (mount disks by UUID=... in fstab, not /dev/sdX)", errIdentityMismatch, gotID, want)
	}
	return nil
}

// IdentityMismatch reports whether err is the mismatch raised by
// ValidateDeviceID. The caller may downgrade it to a warning when
// meta - the cluster-wide authority - proves the id belongs on this
// device (an operator who deliberately moved an OSD directory updates
// the meta record, not the pin file).
func IdentityMismatch(err error) bool {
	return errors.Is(err, errIdentityMismatch)
}

var errIdentityMismatch = errors.New("store: device identity mismatch")

// identityError wraps a mismatch with the sentinel, keeping the
// formatted message for the operator.
func identityError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errIdentityMismatch, fmt.Sprintf(format, args...))
}

// parseMountinfo maps a "major:minor" st_dev to its /proc/self/mountinfo
// entry: the source device, the mount point, the mount options and the
// filesystem type. After the " - " separator mountinfo lists fstype,
// source, super-options - so the source follows the fstype (the longest
// mount point wins for stacked mounts).
func parseMountinfo(content, dev string) (source, mount, options, fstype string, ok bool) {
	bestLen := -1
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 || fields[2] != dev {
			continue
		}
		m := fields[4]
		if len(m) <= bestLen {
			continue
		}
		sep := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				sep = i
				break
			}
		}
		if sep < 0 || sep+2 >= len(fields) {
			continue
		}
		bestLen = len(m)
		mount, fstype, source, options = m, fields[sep+1], fields[sep+2], fields[5]
		ok = true
	}
	return source, mount, options, fstype, ok
}

// scanByLinks inspects the /dev/disk/by-<kind> symlink directory (uuid,
// id, wwid, ...) and returns the first link, in sorted order, whose
// target is one of the canonical devices in devs. Sorted order keeps
// the result deterministic when several links point at one device
// (by-id lists the whole disk and its partitions: the alphabetically
// first, the whole-disk id, wins).
func scanByLinks(kind string, devs map[string]bool) (name, target string, ok bool) {
	dir := filepath.Join("/dev/disk/by-" + kind)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return "", "", false
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, n := range names {
		t, err := os.Readlink(filepath.Join(dir, n))
		if err != nil {
			continue
		}
		if !filepath.IsAbs(t) {
			t = filepath.Join(dir, t)
		}
		t = filepath.Clean(t)
		if devs[t] {
			return n, t, true
		}
	}
	return "", "", false
}

// fstabEntry is the slice of /etc/fstab that says where a data
// directory's disk is supposed to come from.
type fstabEntry struct {
	device  string // column 1: UUID=..., /dev/disk/by-id/..., /dev/sdb1
	mount   string // column 2: the mount point
	fstype  string // column 3
	options string // column 4
}

// fstabEntryFor returns the fstab entry whose mount point is the
// longest prefix of path. It labels a mount for operators (a mount by
// /dev/sdX is the root cause of the swap this file is about); no
// identity machinery depends on it.
func fstabEntryFor(path string) (fstabEntry, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fstabEntry{}, false
	}
	content, err := fstabReader()
	if err != nil {
		return fstabEntry{}, false
	}
	best := fstabEntry{}
	for _, e := range parseFstab(content) {
		if e.mount == "" || e.mount[0] != '/' {
			continue
		}
		if mountsUnder(abs, e.mount) && (best.mount == "" || len(e.mount) > len(best.mount)) {
			best = e
		}
	}
	return best, best.mount != ""
}

// fstabReader is swapped by tests.
var fstabReader = func() (string, error) {
	b, err := os.ReadFile("/etc/fstab")
	return string(b), err
}

func parseFstab(content string) []fstabEntry {
	var out []fstabEntry
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) < 3 {
			continue
		}
		e := fstabEntry{device: f[0], mount: f[1], fstype: f[2]}
		if len(f) > 3 {
			e.options = f[3]
		}
		out = append(out, e)
	}
	return out
}

// mountsUnder reports whether p lies at or below the mount point m.
func mountsUnder(p, m string) bool {
	m = filepath.Clean(m)
	p = filepath.Clean(p)
	if m == "/" {
		return true
	}
	return p == m || strings.HasPrefix(p, m+"/")
}

// canonicalDevice resolves a device node through the /dev symlinks
// (/dev/mapper/vg-lv -> /dev/dm-0) so mount sources and by-* link
// targets can be compared.
func canonicalDevice(dev string) string {
	if dev == "" {
		return ""
	}
	if t, err := filepath.EvalSymlinks(dev); err == nil {
		return filepath.Clean(t)
	}
	return filepath.Clean(dev)
}
