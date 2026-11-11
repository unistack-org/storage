package main

import (
	"errors"
	"fmt"
	"strings"
)

// vduseDeviceInfo is the admin-facing view of one live VDUSE device.
type vduseDeviceInfo struct {
	Name     string `json:"name"` // kernel vduse device name (devname)
	Volume   string `json:"volume"`
	ReadOnly bool   `json:"read_only"`
	Size     uint64 `json:"size"`
	Attached bool   `json:"attached"` // a driver (virtio-vdpa) claimed it
}

// vduseFlags carries the daemon-wide virtio-blk feature flags decided
// at boot (-trim / -write-zeroes); per-device flags ride in spec/args.
type vduseFlags struct {
	Trim        bool
	WriteZeroes bool
	ReadAhead   bool
	// ReadAheadWindow bounds the per-queue prefetch window in bytes;
	// 0 selects the package default (256 KiB).
	ReadAheadWindow int
	// Queues is the number of virtqueues per device (VIRTIO_BLK_F_MQ);
	// 0/1 means a single queue, the historical behaviour.
	Queues int
}

// parseVDUSESpec splits "export" / "export=devname".
func parseVDUSESpec(spec string) (exp, dev string) {
	exp, dev, found := strings.Cut(spec, "=")
	if !found {
		dev = "sds-" + strings.ReplaceAll(exp, "/", "-")
	}
	return exp, dev
}

// validateDevName keeps kernel device names to a conservative alphabet.
func validateDevName(name string) error {
	if name == "" || len(name) > 255 {
		return fmt.Errorf("device name %q: must be 1..255 bytes", name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !ok {
			return fmt.Errorf("device name %q: only [A-Za-z0-9-_] allowed", name)
		}
	}
	return nil
}

// errVDUSEUnsupported answers VDUSE operations on platforms without the
// kernel interface; the linux manager never returns it.
var errVDUSEUnsupported = errors.New("vduse requires Linux (vduse kernel module); this binary was built for a non-Linux platform")

// errVDUSEKernelAbsent answers VDUSE operations on Linux when the kernel
// interface is missing: /dev/vduse/control does not exist (the vduse
// module is not loaded). The admin API maps it to 501, like the
// cross-platform stub, so a control plane sees "unsupported here", not
// an internal error.
var errVDUSEKernelAbsent = errors.New("vduse requires the vduse kernel module on Linux (/dev/vduse/control missing; load it with `modprobe vduse`)")
