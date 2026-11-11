package main

import (
	"errors"
	"path/filepath"
	"strings"
)

// vubDeviceInfo is the admin-facing view of one live vhost-user-blk
// export (the virtio-blk disk QEMU/Dragonball attaches over a unix
// socket).
type vubDeviceInfo struct {
	Name     string `json:"name"`   // export name (unique)
	Socket   string `json:"socket"` // unix socket path QEMU connects to
	Volume   string `json:"volume"` // backing volume ("" for boot file exports)
	ReadOnly bool   `json:"read_only"`
	Size     uint64 `json:"size"`
	Attached bool   `json:"attached"` // a VM session is live (queues active)
}

// vubFlags carries the daemon-wide virtio-blk feature flags decided at
// boot (-trim / -write-zeroes); per-device flags ride in the options.
type vubFlags struct {
	Trim        bool
	WriteZeroes bool
	Queues      int
	Workers     int
}

// parseVUBSpec splits "export" / "export=socket" / "export=socket:ro".
// The socket path is the devname of a vub export; a bare export name
// derives one under /var/run/unistord.
func parseVUBSpec(spec string) (exp, sock string, readOnly bool) {
	exp, rest, found := strings.Cut(spec, "=")
	if !found {
		return exp, defaultVUBSocket(exp), false
	}
	if strings.HasSuffix(rest, ":ro") {
		readOnly = true
		rest = strings.TrimSuffix(rest, ":ro")
	}
	if rest == "" {
		rest = defaultVUBSocket(exp)
	}
	return exp, rest, readOnly
}

// runtimeVUBSocketDir is the directory a runtime (admin API) vub export
// derives its socket from. Tests point it at a temp dir.
var runtimeVUBSocketDir = "/var/run/unistord"

func defaultVUBSocket(exp string) string {
	return filepath.Join(runtimeVUBSocketDir, exp+".sock")
}

// validateVUBSocket keeps socket paths out of surprising territory:
// absolute, no traversal past cleaning.
func validateVUBSocket(p string) error {
	if p == "" {
		return errors.New("vhost-user-blk: socket path is empty")
	}
	if !filepath.IsAbs(p) {
		return errors.New("vhost-user-blk: socket path must be absolute")
	}
	if len(p) > 107 {
		// The unix socket address limit (108 bytes incl. NUL).
		return errors.New("vhost-user-blk: socket path too long (>107 bytes)")
	}
	return nil
}
