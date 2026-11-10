package nbd

import (
	"testing"

	sdsclient "github.com/unistack-org/storage/pkg/client"
)

// TestErrnoToNBDNoSpace: the cluster client's ErrNoSpace (which wraps
// syscall.ENOSPC) must reach the NBD guest as -ENOSPC, not -EIO: the VM's
// filesystem needs to tell "disk full" from "disk broken" (it stops
// writing on ENOSPC, panics/remounts read-only on EIO).
func TestErrnoToNBDNoSpace(t *testing.T) {
	if got := errnoToNBD(sdsclient.ErrNoSpace); got != ErrENOSPC {
		t.Fatalf("ErrNoSpace mapped to %#x, want ErrENOSPC (%#x)", got, ErrENOSPC)
	}
}
