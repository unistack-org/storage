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
	vol *sdsclient.Volume
}

// NewVolumeBackend wraps an open cluster volume.
func NewVolumeBackend(vol *sdsclient.Volume) *VolumeBackend {
	return &VolumeBackend{vol: vol}
}

// Size reports the volume's live size: online resize (unistorctl vol
// resize / PUT /v1/volumes/{name}/resize) is picked up here without
// restarting the export. The NBD server reads Size() per connection at
// handshake, so a guest sees the new capacity after reconnecting the
// device (or an nbd size rescan); in-flight connections keep the size
// they negotiated.
func (b *VolumeBackend) Size() uint64 { return b.vol.Size() }

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

// WriteZeroes implements the NBD Write_Zeroes request. punchHole is the
// negation of the NBD_FLAG_SEND_WRITE_ZEROES NO_HOLE request flag: when the
// guest asks for a sparse zeroing we forward it to the volume, which punches
// a real hole on every replica; NO_HOLE keeps the allocation and stores
// literal zeroes.
func (b *VolumeBackend) WriteZeroes(off int64, length uint32, punchHole bool) error {
	if off < 0 {
		return errors.New("negative offset")
	}
	return b.vol.WriteZeroes(context.Background(), uint64(off), uint64(length), punchHole)
}

// Close releases the volume's QoS watcher. The cluster connection
// pools are shared per target and stay open for other users.
func (b *VolumeBackend) Close() error { return b.vol.Close() }

var _ Backend = (*VolumeBackend)(nil)
