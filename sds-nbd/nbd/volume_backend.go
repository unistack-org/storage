package nbd

import (
	"context"
	"errors"
	"io"

	sdsclient "github.com/unistack-org/storage/pkg/client"
)

// VolumeBackend exposes a clustered client.Volume as an NBD Backend.
// It is the phase-3 data path: NBD requests are fanned out to OSDs by
// the cluster client (CRUSH placement + quorum writes). All placement,
// replication and epoch-fencing logic lives in pkg/client; this file is
// a thin protocol adapter.
type VolumeBackend struct {
	vol  *sdsclient.Volume
	size uint64
}

// NewVolumeBackend wraps an open cluster volume.
func NewVolumeBackend(vol *sdsclient.Volume) *VolumeBackend {
	return &VolumeBackend{vol: vol, size: vol.Size()}
}

func (b *VolumeBackend) Size() uint64 { return b.size }

func (b *VolumeBackend) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	n, err := b.vol.ReadAt(context.Background(), p, uint64(off))
	// The volume zero-fills thin (unwritten) regions and reports io.EOF
	// only for reads past the volume end, which NBD treats as invalid
	// requests; both are normal for a sparse export, so hide EOF.
	if errors.Is(err, io.EOF) && n > 0 {
		err = nil
	}
	return n, err
}

func (b *VolumeBackend) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("negative offset")
	}
	return b.vol.WriteAt(context.Background(), p, uint64(off))
}

func (b *VolumeBackend) Flush() error {
	return b.vol.Flush(context.Background())
}

func (b *VolumeBackend) Trim(off int64, length uint32) error {
	return b.WriteZeroes(off, length, true)
}

func (b *VolumeBackend) WriteZeroes(off int64, length uint32, _ bool) error {
	if off < 0 {
		return errors.New("negative offset")
	}
	return b.vol.WriteZeroes(context.Background(), uint64(off), uint64(length))
}

func (b *VolumeBackend) Close() error { return nil }

var _ Backend = (*VolumeBackend)(nil)
