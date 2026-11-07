//go:build !linux

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/unistack-org/sds-nbd/nbd"
)

type vduseDevice struct {
	name string
}

// startVDUSE: VDUSE is a Linux kernel interface (CONFIG_VDUSE, /dev/vduse);
// it is not available on this platform.
func startVDUSE(ctx context.Context, logger *log.Logger, es []*nbd.Export, specs []string, trim, writeZeroes bool) ([]*vduseDevice, error) {
	return nil, fmt.Errorf("-vduse requires Linux (vduse kernel module); this binary was built for a non-Linux platform")
}
