package vduse

// Backend is the block device a VDUSE virtio-blk device exports. It is
// deliberately identical in shape to sds-nbd's nbd.Backend, so the same
// volume adapter (sdsclient.Volume, FileBackend, ...) plugs into both.
//
// ReadAt/WriteAt must be safe for concurrent use (the runtime services
// every virtqueue with its own worker goroutine).
type Backend interface {
	// Size returns the export size in bytes.
	Size() uint64
	// ReadAt reads len(p) bytes at byte offset off.
	ReadAt(p []byte, off int64) (int, error)
	// WriteAt writes len(p) bytes at byte offset off.
	WriteAt(p []byte, off int64) (int, error)
	// Flush persists all previously written data.
	Flush() error
	// Trim marks [off, off+length) as sparse/unwritten.
	Trim(off int64, length uint32) error
	// WriteZeroes makes [off, off+length) read as zeroes; if punchHole is
	// set the backend may punch a sparse hole, otherwise it must write
	// actual zeroes.
	WriteZeroes(off int64, length uint32, punchHole bool) error
	// Close releases resources.
	Close() error
}

// maxReqBytes caps the data of a single virtio-blk request. The config
// advertises size_max 512 KiB per segment and seg_max 256 segments; we
// additionally cap total request size so a broken guest cannot make us
// map/allocate huge buffers.
const maxReqBytes = 32 << 20 // 32 MiB
