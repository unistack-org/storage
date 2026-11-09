//go:build linux

package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/unistack-org/sds-nbd/nbd"
	"github.com/unistack-org/storage/pkg/vduse"
)

// vduseDevice is one running VDUSE export.
type vduseDevice struct {
	name string
	dev  *vduse.Device
}

// startVDUSE creates and serves a VDUSE virtio-blk device for each
// -vduse spec (export name, optionally "export=devname").
//
// On the host the device is attached with:
//
//	modprobe vdpa virtio_vdpa
//	vdpa dev add name <devname> mgmtdev vduse   -> /dev/vdX
//
// (the vduse bus auto-registers the "vduse" mgmtdev; a VM uses the
// device via `-chardev vdpa,name=<devname>,id=vdpa0 -device
// vhost-vdpa-pci,chardev=vdpa0`).
func startVDUSE(ctx context.Context, logger *log.Logger, es []*nbd.Export, specs []string, trim, writeZeroes bool) ([]*vduseDevice, error) {
	byName := map[string]*nbd.Export{}
	for _, e := range es {
		byName[e.Name] = e
	}
	var devs []*vduseDevice
	for _, spec := range specs {
		expName, devName := parseVDUSESpec(spec)
		e, ok := byName[expName]
		if !ok {
			return nil, fmt.Errorf("-vduse %q: no such -export", spec)
		}
		if err := validateDevName(devName); err != nil {
			return nil, fmt.Errorf("-vduse %q: %w", spec, err)
		}
		// nbd.Backend and vduse.Backend are the same method set, so the
		// export backend satisfies the VDUSE interface directly.
		dev, err := vduse.NewDevice(devName, vduse.Options{
			NumQueues:   1,
			Trim:        trim,
			WriteZeroes: writeZeroes,
		}, e.Backend)
		if err != nil {
			return nil, fmt.Errorf("-vduse %q: %w", spec, err)
		}
		go dev.Serve()
		logger.Printf("vduse %q -> export %q (%s): attach with `vdpa dev add name %s mgmtdev vduse`",
			devName, e.Name, humanSize(e.Backend.Size()), devName)
		devs = append(devs, &vduseDevice{name: devName, dev: dev})
	}
	go func() {
		<-ctx.Done()
		for _, d := range devs {
			if err := d.dev.Close(); err != nil {
				logger.Printf("vduse %s close: %v", d.name, err)
			}
		}
	}()
	return devs, nil
}

// parseVDUSESpec splits "export" / "export=devname".
func parseVDUSESpec(spec string) (exp, dev string) {
	exp, dev, found := strings.Cut(spec, "=")
	if !found {
		dev = "sds-" + strings.ReplaceAll(exp, "/", "-")
	}
	return exp, dev
}

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
