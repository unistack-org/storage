package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/store"
)

// Device-identity pinning for -data-dir entries (see pkg/store
// deviceid.go for what an identity is).
//
// An OSD id lives inside the data directory (<dir>/id), so an operator
// who mounts disks by kernel name (/dev/sda, /dev/sdb) and restarts
// the host after the enumeration changed (disks rewired, a disk added)
// trades the disks between the mount points: every id then boots on
// foreign data and the object layout of the whole cluster drifts. The
// boot sequence here records the stable identity of the backing device
// (filesystem UUID, else serial, else WWID) in three places -
// <dir>/device (travels with the data), <meta-dir>/devices.json (this
// node's view) and OSDInfo.Device (the cluster's view) - and
// re-validates all three on every start. A swap becomes a fatal
// startup error with an actionable message, never silent corruption.

const (
	idPinFile      = "id"
	devicePinFile  = "device"
	deviceMapFile  = "devices.json"
	uuidSpecPrefix = "uuid="
)

// dataDirSpec is one parsed -data-dir entry.
type dataDirSpec struct {
	dir string
	// wantDev is the device identity pinned by the explicit
	// "uuid=<identity>@<dir>" form, "" for a bare directory.
	wantDev string
}

// parseDataDirSpec parses one -data-dir entry: a bare data directory
// (id auto-assigned), or the explicit form "uuid=<identity>@<dir>"
// that pins the directory to a device identity. The identity is the
// canonical form reported by `unistorctl disk probe` / the startup
// logs: "filesystem=<uuid>", "serial=<id>" or "wwid=<id>".
func parseDataDirSpec(spec string) (dataDirSpec, error) {
	if spec == "" {
		return dataDirSpec{}, fmt.Errorf("empty data dir")
	}
	if !strings.HasPrefix(spec, uuidSpecPrefix) {
		return dataDirSpec{dir: filepath.Clean(spec)}, nil
	}
	at := strings.IndexByte(spec, '@')
	if at < 0 {
		return dataDirSpec{}, fmt.Errorf(`expected "uuid=<identity>@<data-dir>"`)
	}
	ident := spec[len(uuidSpecPrefix):at]
	if ident == "" {
		return dataDirSpec{}, fmt.Errorf("empty identity in %q", spec)
	}
	for i := 0; i < len(ident); i++ {
		c := ident[i]
		ok := c == '-' || c == '_' || c == '.' || c == '=' ||
			(c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !ok {
			return dataDirSpec{}, fmt.Errorf("identity %q: unexpected character %q", ident, c)
		}
	}
	dir := spec[at+1:]
	if dir == "" {
		return dataDirSpec{}, fmt.Errorf("empty data dir in %q", spec)
	}
	return dataDirSpec{dir: filepath.Clean(dir), wantDev: ident}, nil
}

// osdPin is what a data directory says about itself: the id pinned in
// <dir>/id and (for directories booted by a device-aware daemon) the
// device identity pinned in <dir>/device. A missing device file is a
// legacy pin: the id is honoured, the identity gets written this boot.
type osdPin struct {
	id     int32
	device string
}

// readOSDPins reads the self-description of every -data-dir entry. A
// missing id file means the directory is new (its id is still to be
// assigned); any other read/parse failure is an operator-visible
// error, since silently re-assigning an id would orphan the data.
func readOSDPins(specs []dataDirSpec) (map[string]osdPin, error) {
	pins := map[string]osdPin{}
	seen := map[int32]string{}
	for _, sp := range specs {
		var p osdPin
		b, err := os.ReadFile(filepath.Join(sp.dir, idPinFile))
		if err == nil {
			id, err := strconv.Atoi(strings.TrimSpace(string(b)))
			if err != nil || id <= 0 {
				return nil, fmt.Errorf("%s: id pin %q is not a positive id", sp.dir, strings.TrimSpace(string(b)))
			}
			p.id = int32(id)
			if other, dup := seen[p.id]; dup {
				return nil, fmt.Errorf("%s: id pin %d collides with %s", sp.dir, p.id, other)
			}
			seen[p.id] = sp.dir
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("%s: read id pin: %w", sp.dir, err)
		}
		b, err = os.ReadFile(filepath.Join(sp.dir, devicePinFile))
		if err == nil {
			p.device = strings.TrimSpace(string(b))
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("%s: read device pin: %w", sp.dir, err)
		}
		pins[sp.dir] = p
	}
	return pins, nil
}

// writeOSDPin writes the directory's self-description. The id pin goes
// out on every platform (it is the directory's memory of who it is);
// the device pin only when a stable identity was readable - writing a
// path label as the identity would pin a fact a restart can change.
func writeOSDPin(dir string, id int32, device string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, idPinFile), []byte(strconv.Itoa(int(id))+"\n"), 0o644); err != nil {
		return err
	}
	if device == "" {
		return nil
	}
	return os.WriteFile(filepath.Join(dir, devicePinFile), []byte(device+"\n"), 0o644)
}

// readDevicePin reads only the device pin of a directory ("" when
// absent); used by the -osd boot loop, whose directories carry no id
// pin (their id is on the command line).
func readDevicePin(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, devicePinFile))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// readDeviceMap loads <meta-dir>/devices.json: the map of data dir ->
// device identity this node pinned at its last boot. A missing file is
// not an error (fresh node); a corrupt one is surfaced to the log.
func readDeviceMap(metaDir string) (map[string]string, error) {
	if metaDir == "" {
		return nil, nil
	}
	b, err := os.ReadFile(filepath.Join(metaDir, deviceMapFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	m := map[string]string{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", deviceMapFile, err)
	}
	return m, nil
}

// writeDeviceMap persists the node's dir -> identity map. It is node
// bookkeeping (like the id pins), not cluster state, and it can be
// rebuilt from the directories, so 0600 is enough.
func writeDeviceMap(metaDir string, m map[string]string) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp := filepath.Join(metaDir, deviceMapFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(metaDir, deviceMapFile))
}

// mapKey is the devices.json key for a directory: absolute and
// cleaned, so the same disk mounted at the same path maps to one
// entry however the operator spelled it.
func mapKey(dir string) string {
	if abs, err := filepath.Abs(dir); err == nil {
		return filepath.Clean(abs)
	}
	return filepath.Clean(dir)
}

// dirPlan is the boot decision's working record for one -data-dir
// entry: the parsed spec, the pins the directory carries, the identity
// the node map recorded for the path, and the identity detected live.
type dirPlan struct {
	spec    dataDirSpec
	pin     osdPin
	nodeDev string
	live    store.DeviceIdentity
}

// detectDataDirs probes the live identity of every -data-dir entry.
// The probe is read-only; it runs before any id is assigned, so a
// swap is caught while it can still be refused.
func detectDataDirs(specs []dataDirSpec, pins map[string]osdPin, nodeMap map[string]string) ([]*dirPlan, error) {
	seen := map[string]bool{}
	plans := make([]*dirPlan, 0, len(specs))
	for _, sp := range specs {
		if seen[sp.dir] {
			return nil, fmt.Errorf("duplicate -data-dir %s", sp.dir)
		}
		seen[sp.dir] = true
		live, err := store.DeviceID(sp.dir)
		if err != nil {
			return nil, fmt.Errorf("%s: device identity: %w", sp.dir, err)
		}
		plans = append(plans, &dirPlan{
			spec:    sp,
			pin:     pins[sp.dir],
			nodeDev: nodeMap[mapKey(sp.dir)],
			live:    live,
		})
	}
	return plans, nil
}

// checkDataDirSwaps is the swap detector. For every directory it
// checks the live identity against (a) the explicit uuid= pin, (b) the
// identity pinned by the directory itself (and, when the directory
// predates device pinning, the node map), and (c) the device the OSD
// is registered on in meta. A mismatch is returned as a fatal error
// unless the evidence says the operator moved the directory on
// purpose: the data dir carries a pin that matches the live device and
// meta alone is stale - then the meta record is refreshed at
// registration and the boot continues with a warning.
func checkDataDirSwaps(plans []*dirPlan, registered map[int32]meta.OSDInfo, warn func(string, ...any)) error {
	for _, p := range plans {
		if p.spec.wantDev != "" {
			if err := store.ValidateDeviceID(p.live, p.spec.wantDev); err != nil {
				return fmt.Errorf("-data-dir %q: %w", p.spec.dir, err)
			}
		}
		recorded := p.pin.device
		if recorded == "" {
			recorded = p.nodeDev
		}
		if recorded != "" {
			if err := store.ValidateDeviceID(p.live, recorded); err != nil {
				reg, hasReg := registered[p.pin.id]
				moved := p.pin.device != "" && hasReg && reg.Device != "" &&
					store.ValidateDeviceID(p.live, reg.Device) == nil
				if !moved {
					return fmt.Errorf("-data-dir %s: %w", p.spec.dir, err)
				}
				warn("-data-dir %s: the directory moved to %s (pinned as %s): it carries its own device pin and meta agrees, continuing",
					p.spec.dir, p.live.IdentityLabel(), recorded)
			}
		}
		if p.pin.id == 0 {
			continue
		}
		reg, hasReg := registered[p.pin.id]
		if !hasReg || reg.Device == "" || p.live.Identity() == "" {
			continue
		}
		if err := store.ValidateDeviceID(p.live, reg.Device); err != nil {
			if p.pin.device != "" && store.ValidateDeviceID(p.live, p.pin.device) == nil {
				warn("osd %d: meta knows device %s, the directory %s is now on %s: the data dir was moved on purpose, meta updates at registration",
					p.pin.id, store.NormalizeIdentity(reg.Device), p.spec.dir, p.live.Identity())
				continue
			}
			return fmt.Errorf("-data-dir %s: osd %d is registered on %s, but the directory is on %s - the mount points likely swapped across the restart. Mount disks by UUID=... in fstab (not /dev/sdX); if the move was deliberate, pin the directory with \"-data-dir uuid=%s@%s\"",
				p.spec.dir, p.pin.id, store.NormalizeIdentity(reg.Device), p.live.IdentityLabel(), p.live.Identity(), p.spec.dir)
		}
	}
	return nil
}

// bootOSD is one assigned -data-dir OSD: the id and the directory.
type bootOSD struct {
	id  int32
	dir string
}

// liveIdentity returns the stable identity detected for dir ("" when
// none is readable, e.g. off Linux, so no device pin is written).
func liveIdentity(plans []*dirPlan, dir string) string {
	for _, p := range plans {
		if p.spec.dir == dir {
			return p.live.Identity()
		}
	}
	return ""
}

// assignDataDirIDs gives every -data-dir entry its id.
//
// Rules (the user-visible contract of -data-dir):
//   - a directory with an id pin keeps it;
//   - a new directory gets the lowest id free of -osd specs, id pins
//     and meta registrations, and the id is pinned in <dir>/id
//     together with the device identity in <dir>/device;
//   - several directories on one block device is fine: each gets its
//     own id, and because they share the device a mount swap moves
//     them together, where every <dir>/device pin refuses the boot;
//   - two or more directories on device A plus two or more on device B
//     (none pinned) is refused: the order of -data-dir entries cannot
//     say which pair belongs to which disk, so the ids assigned now
//     would trade places with the first swap and every pin would agree
//     with the swap. The operator breaks the tie with
//     "-data-dir uuid=<identity>@<dir>";
//   - off Linux no identity is readable, so every directory is one
//     unknown device: no ambiguity is possible and ids are assigned in
//     the order given (the pre-pinning behaviour).
//
// Directories are processed in command-line order, so a fresh cluster
// numbers them 1,2,3... in the order the operator listed them.
func assignDataDirIDs(plans []*dirPlan, used map[int32]bool, warn func(string, ...any)) ([]bootOSD, error) {
	// Pinned ids are taken before any fresh dir draws one, whatever
	// the caller seeded into used.
	for _, p := range plans {
		if p.pin.id != 0 {
			used[p.pin.id] = true
		}
	}
	byIdent := map[string][]*dirPlan{}
	for _, p := range plans {
		byIdent[p.live.Identity()] = append(byIdent[p.live.Identity()], p)
	}
	// Count the new (unpinned) directories per known device.
	fresh, known := 0, []string{}
	for ident, g := range byIdent {
		n := 0
		for _, p := range g {
			if p.pin.id == 0 {
				n++
			}
		}
		if ident != "" && n >= 2 {
			fresh++
			known = append(known, ident)
		}
	}
	if fresh >= 2 {
		sort.Strings(known)
		var dirs []string
		for _, p := range plans {
			if p.pin.id == 0 && p.live.Identity() != "" {
				dirs = append(dirs, fmt.Sprintf("%s on %s", p.spec.dir, p.live.Identity()))
			}
		}
		return nil, fmt.Errorf("the new data directories span %d devices (%s) with more than one directory on each: %s. The -data-dir order cannot say which directories belong to which disk, and the ids assigned now would trade places with the first mount swap. Bind each directory to its device: -data-dir uuid=<identity>@<data-dir> (`unistorctl disk probe <dir>` prints the identity)",
			fresh, strings.Join(known, ", "), strings.Join(dirs, "; "))
	}

	out := make([]bootOSD, 0, len(plans))
	for _, p := range plans {
		if p.pin.id != 0 {
			out = append(out, bootOSD{id: p.pin.id, dir: p.spec.dir})
			continue
		}
		id := firstFreeOSDID(used)
		used[id] = true
		out = append(out, bootOSD{id: id, dir: p.spec.dir})
		if len(byIdent[p.live.Identity()]) > 1 && p.live.Identity() != "" {
			warn("-data-dir %s: osd %d shares device %s with the other listed directories; each directory keeps its own id, the device pin ties them together",
				p.spec.dir, id, p.live.Identity())
		}
	}
	return out, nil
}
