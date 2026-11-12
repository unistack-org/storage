package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeviceIdentityString(t *testing.T) {
	full := DeviceIdentity{Filesystem: "abcd", Serial: "s1", WWID: "w1"}
	if got := full.Identity(); got != "filesystem=abcd" {
		t.Errorf("Identity = %q, want filesystem=abcd (fs uuid wins)", got)
	}
	serial := DeviceIdentity{Serial: "s1", WWID: "w1"}
	if got := serial.Identity(); got != "serial=s1" {
		t.Errorf("Identity = %q, want serial=s1 (serial beats wwid)", got)
	}
	wwid := DeviceIdentity{WWID: "w1"}
	if got := wwid.Identity(); got != "wwid=w1" {
		t.Errorf("Identity = %q", got)
	}
	node := DeviceIdentity{Device: "/dev/sda1", MountSource: "/dev/sda1"}
	if got := node.Identity(); got != "" {
		t.Errorf("a bare kernel node name is not a stable identity: %q", got)
	}
	if got := node.IdentityLabel(); got != "/dev/sda1" {
		t.Errorf("IdentityLabel = %q, want the node as the human label", got)
	}
	if full.IdentityLabel() != "filesystem=abcd" {
		t.Errorf("IdentityLabel = %q", full.IdentityLabel())
	}
	if !full.Stable() || node.Stable() {
		t.Error("Stable: fs uuid is, a kernel node name is not")
	}
}

func TestNormalizeIdentity(t *testing.T) {
	for in, want := range map[string]string{
		"":                "",
		"abcd-1234":       "filesystem=abcd-1234", // legacy raw fs uuid
		"filesystem=abcd": "filesystem=abcd",
		"serial=s1":       "serial=s1",
		"/dev/sdb1":       "filesystem=/dev/sdb1", // not a legal stored pin; label rules live elsewhere
		"  abcd  ":        "filesystem=abcd",
		"uuid=550e8400":   "uuid=550e8400", // prefixed forms are returned as-is
	} {
		if got := NormalizeIdentity(in); got != want {
			t.Errorf("NormalizeIdentity(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestValidateIdentityStrings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		got, kw string
		bad     bool
	}{
		{"nothing pinned", "filesystem=a", "", false},
		{"path label pins document, never constrain", "filesystem=a", "path=/dev/sdb1", false},
		{"match", "filesystem=a", "filesystem=a", false},
		{"legacy raw uuid match", "filesystem=a", "a", false},
		{"legacy raw uuid mismatch", "filesystem=a", "b", true},
		{"swap", "filesystem=a", "filesystem=b", true},
		{"kind change serial vs fs", "serial=a", "filesystem=a", true},
		{"live unreadable, pin set", "", "filesystem=a", true},
		{"live unreadable, nothing pinned", "", "", false},
	} {
		err := ValidateIdentityStrings(tc.got, tc.kw)
		if tc.bad {
			if err == nil {
				t.Errorf("%s: expected error", tc.name)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	// The mismatch marker must survive for callers that downgrade it.
	err := ValidateIdentityStrings("filesystem=a", "filesystem=b")
	if !IdentityMismatch(err) {
		t.Errorf("mismatch must carry the marker: %v", err)
	}
	if IdentityMismatch(ValidateIdentityStrings("filesystem=a", "filesystem=a")) {
		t.Error("nil must not match the marker")
	}
}

func TestValidateDeviceID(t *testing.T) {
	live := DeviceIdentity{Path: "/d1", Device: "/dev/sda1", Filesystem: "aaaa"}
	if err := ValidateDeviceID(live, "filesystem=aaaa"); err != nil {
		t.Fatalf("match: %v", err)
	}
	err := ValidateDeviceID(live, "filesystem=bbbb")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "/d1") || !strings.Contains(err.Error(), "filesystem=bbbb") {
		t.Errorf("error must name the dir and the pin: %v", err)
	}
	if !IdentityMismatch(err) {
		t.Errorf("wrapped mismatch must keep the marker: %v", err)
	}
}

func TestParseMountinfo(t *testing.T) {
	// A real-shaped mountinfo: overlay/tmpfs noise, a stacked mount,
	// and the " - fstype source options" tail (the old hand-rolled
	// parser returned fields[sep+1], the fstype, as the device).
	content := strings.Join([]string{
		`18 25 0:18 / /sys rw - sysfs sysfs rw`,
		`30 24 0:29 / /tmp rw - tmpfs tmpfs rw`,
		`22 23 8:1 / /var/lib/unistord rw,relatime shared:2 - ext4 /dev/sda1 rw`,
		`24 22 8:1 /osd2 /var/lib/unistord/osd2 rw - ext4 /dev/sda1 rw`, // stacked, longer prefix wins
		`26 23 253:0 / /mnt/lv rw - xfs /dev/mapper/vg-lv rw`,
	}, "\n")
	source, mount, options, fstype, ok := parseMountinfo(content, "8:1")
	if !ok {
		t.Fatal("8:1 not found")
	}
	if source != "/dev/sda1" {
		t.Fatalf("source = %q, want /dev/sda1", source)
	}
	if mount != "/var/lib/unistord/osd2" {
		t.Fatalf("mount = %q, want the longest match", mount)
	}
	if fstype != "ext4" {
		t.Fatalf("fstype = %q", fstype)
	}
	if options != "rw" && options != "rw,relatime" {
		t.Fatalf("options = %q", options)
	}
	source, _, _, fstype, ok = parseMountinfo(content, "253:0")
	if !ok || source != "/dev/mapper/vg-lv" || fstype != "xfs" {
		t.Fatalf("dm line: %q %q %v", source, fstype, ok)
	}
	if _, _, _, _, ok := parseMountinfo(content, "9:9"); ok {
		t.Fatal("unknown dev must not match")
	}
}

func TestMountedByKernelName(t *testing.T) {
	for _, tc := range []struct {
		fstab, want string
	}{
		{"/dev/sda1", "/dev/sda1"},
		{"/dev/nvme0n1p2", "/dev/nvme0n1p2"},
		{"/dev/vdb", "/dev/vdb"},
		{"/dev/disk/by-id/ata-X", ""},
		{"UUID=abcd-1234", ""},
		{"/dev/mapper/vg-lv", ""},
		{"/dev/md0", ""},
		{"", ""},
	} {
		id := DeviceIdentity{FstabDevice: tc.fstab}
		if got := id.MountedByKernelName(); got != tc.want {
			t.Errorf("MountedByKernelName(%q) = %q, want %q", tc.fstab, got, tc.want)
		}
	}
}

func TestParseFstab(t *testing.T) {
	content := "# comment\n\nUUID=abcd-1234 /var/lib/unistord ext4 defaults,noatime 0 2\n/dev/sdb1 /data xfs rw 0 0\nbadline\n"
	ents := parseFstab(content)
	if len(ents) != 2 {
		t.Fatalf("entries = %v", ents)
	}
	if ents[0].device != "UUID=abcd-1234" || ents[0].mount != "/var/lib/unistord" || ents[0].fstype != "ext4" || ents[0].options != "defaults,noatime" {
		t.Errorf("entry 0 = %+v", ents[0])
	}
	if ents[1].device != "/dev/sdb1" {
		t.Errorf("entry 1 = %+v", ents[1])
	}
}

func TestFstabEntryFor(t *testing.T) {
	old := fstabReader
	t.Cleanup(func() { fstabReader = old })
	fstabReader = func() (string, error) {
		return "/dev/sda1 /data ext4 defaults 0 2\n/dev/sdb1 /data/osd2 ext4 defaults 0 2\n", nil
	}
	// The deepest mount point wins, so a bind mount under the main
	// data mount reports its own fstab device.
	dir := t.TempDir()
	sub := filepath.Join(dir, "osd2")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	// Paths here are under TempDir, not under /data: no match.
	if _, ok := fstabEntryFor(sub); ok {
		t.Fatal("tempdir must not match the fake fstab mounts")
	}
	fstabReader = func() (string, error) {
		return "/dev/sda1 " + dir + " ext4 defaults 0 2\n/dev/sdb1 " + sub + " ext4 defaults 0 2\n", nil
	}
	ent, ok := fstabEntryFor(sub)
	if !ok || ent.device != "/dev/sdb1" {
		t.Fatalf("ent = %+v ok=%v, want /dev/sdb1", ent, ok)
	}
	ent, ok = fstabEntryFor(dir)
	if !ok || ent.device != "/dev/sda1" {
		t.Fatalf("ent = %+v ok=%v, want /dev/sda1", ent, ok)
	}
}

func TestDeviceIDPortableParts(t *testing.T) {
	dir := t.TempDir()
	id, err := DeviceID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if id.Path != dir {
		t.Errorf("Path = %q, want %q", id.Path, dir)
	}
	// The identity itself is platform-specific (Linux fills it from
	// mountinfo + /dev/disk/by-*); off Linux the strict checks must be
	// inert, which the Validate* tests cover directly.
	if id.Identity() != "" {
		if !strings.HasPrefix(id.Identity(), "filesystem=") && !strings.HasPrefix(id.Identity(), "serial=") && !strings.HasPrefix(id.Identity(), "wwid=") {
			t.Errorf("unexpected identity form %q", id.Identity())
		}
	}
	if _, err := DeviceID(filepath.Join(dir, "missing")); err == nil {
		t.Error("missing path must be an error")
	}
}
