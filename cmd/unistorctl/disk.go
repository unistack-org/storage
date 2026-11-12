package main

// disk probe: report the atomic-write capability of the disk (block
// device) backing a path - the data an operator needs before enabling
// unistord -atomic-writes (vitastor-ideas §2.2): does the device
// publish an atomic write unit, is the kernel new enough to expose
// RWF_ATOMIC, and what are the size/alignment rules.

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/unistack-org/storage/pkg/store"
)

func diskCmd(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: unistorctl disk probe <path> [-json]")
	}
	switch args[0] {
	case "probe":
		return diskProbeCmd(args[1:])
	default:
		return fmt.Errorf("unknown disk command %q (probe)", args[0])
	}
}

// reorderFlags moves flags ahead of positionals: Go's flag package
// stops at the first non-flag, and operators type "probe /path -json".
func reorderFlags(args []string) []string {
	var flags, rest []string
	for _, a := range args {
		if len(a) > 1 && a[0] == '-' {
			flags = append(flags, a)
		} else {
			rest = append(rest, a)
		}
	}
	return append(flags, rest...)
}

func diskProbeCmd(args []string) error {
	fs := flag.NewFlagSet("disk probe", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(reorderFlags(args)); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: unistorctl disk probe <path> [-json]")
	}
	info, err := store.ProbeDisk(fs.Arg(0))
	if err != nil {
		return err
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(info)
	}
	fmt.Printf("path:                %s\n", info.Path)
	if info.Device == "" {
		fmt.Println("device:              (not resolved on this platform)")
	} else {
		fmt.Printf("device:              %s\n", info.Device)
	}
	fmt.Printf("logical block size:  %d\n", info.LogicalBlockSize)
	fmt.Printf("physical block size: %d\n", info.PhysicalBlockSize)
	fmt.Printf("atomic_write_unit_max: %d\n", info.AtomicMax)
	fmt.Printf("atomic_write_unit_min: %d\n", info.AtomicMin)
	fmt.Printf("kernel RWF_ATOMIC:   %v\n", info.RWFAtomic)
	if info.RWFAtomic {
		fmt.Printf("\nOK: unistord -atomic-writes will use RWF_ATOMIC writes up to %d bytes\n", info.AtomicMax)
	} else if info.Device != "" && !info.KernelSupported {
		fmt.Println("\nno: the kernel exposes no atomic-write interface (Linux 6.11+ is required)")
	} else {
		fmt.Println("\nno: the device publishes no atomic write unit; -atomic-writes stays inert on this disk")
	}
	return nil
}
