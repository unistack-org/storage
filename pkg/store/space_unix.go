//go:build !windows

package store

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// ErrNoSpace means the filesystem backing the store cannot take the write
// (ENOSPC, and EDQUOT: a user quota on the store directory is the cheapest
// way to cap a tenant on shared hardware — both are reported as ENOSPC,
// the guest-visible meaning is identical). It wraps the raw errno, so
// callers can classify it with errors.Is(err, syscall.ENOSPC) — the OSD
// server maps exactly that onto StatusNoSpace — and the store's own
// pre-flight check raises it too. It means "the device said no", not
// "the device is broken": an OSD answering ErrNoSpace is healthy.
var ErrNoSpace = fmt.Errorf("store: no space left on device: %w", syscall.ENOSPC)

// isNoSpace classifies the OS errors that mean "you cannot write more".
func isNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

// fileAvail reports the bytes a non-privileged writer may still allocate
// on the filesystem holding f (statfs: free blocks * block size, using the
// unprivileged free-block count, which already respects reserved blocks).
// It is a point-in-time hint: concurrent writers race with us, which is
// why it guards the *start* of an allocation, and the real ENOSPC from the
// kernel is still classified above.
func fileAvail(f *os.File) (int64, bool) {
	avail, _, ok := fileSpace(f)
	return avail, ok
}

// storeDirStat opens the store directory for statfs purposes.
func storeDirStat(dir string) (*os.File, error) { return os.Open(dir) }

// fileSpace reports the bytes a non-privileged writer may still allocate
// and the total filesystem size (point-in-time statfs hints).
func fileSpace(f *os.File) (avail, total int64, ok bool) {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(f.Fd()), &st); err != nil {
		return 0, 0, false
	}
	bs := int64(st.Bsize)
	return int64(st.Bavail) * bs, int64(st.Blocks) * bs, true
}
