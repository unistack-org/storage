// Package vduse implements a VDUSE (vDPA Device in Userspace) virtio-blk
// device: it creates a kernel VDUSE device (/dev/vduse/control), serves
// its virtqueues from userspace, and maps them onto an sd-storage volume.
// Inside the VM the device appears as a native /dev/vdX (virtio-blk) disk,
// which is the recommended way Vitastor itself exposes NBD-style exports
// on Linux (see docs/intro/architecture.ru.md: "VDUSE is recommended,
// nbd is legacy").
//
// The package is split in two halves:
//
//   - Portable logic (this file set): virtio-blk request parsing, config
//     space layout, split-virtqueue arithmetic. Pure functions over byte
//     slices, unit-tested everywhere (including macOS dev hosts).
//   - Linux runtime (files tagged go:linux): the /dev/vduse ioctl +
//     message protocol, guest-memory (IOTLB) mapping, kick eventfds and
//     the per-queue worker loop. Requires Linux >= 5.15 with the vduse
//     kernel module loaded.
//
// Runtime validation of the Linux half is done on a Linux host; on macOS
// `go vet`/build of the portable half plus the logic tests are the gate.
package vduse

import "encoding/binary"

// virtio device ids.
const (
	// DevIDBlk is the virtio device id for a block device.
	DevIDBlk = 2
	// VendorID matches QEMU's, which keeps guest tooling happy.
	VendorID = 0x1af4
)

// virtio feature bits we advertise (kept to what we actually implement).
const (
	featRingIndirect = 1 << 28 // indirect descriptor tables allowed
	// VIRTIO_RING_F_EVENT_IDX is deliberately NOT advertised: it lets the
	// driver suppress kicks using used.avail_event, a field a split-ring
	// device must write. We never do, so the threshold computed from the
	// stale zero makes vring_need_event() false and the driver publishes
	// requests without ever kicking the daemon (the request sits in the
	// avail ring forever). Kicking on every publish is the correct
	// behaviour for a device that does not maintain event indices.
	featVersion1      = 1 << 32 // virtio 1.0 device, legacy not offered
	featBlkSizeMax    = 1 << 1  // config.size_max valid
	featBlkSegMax     = 1 << 2  // config.seg_max valid
	featBlkBlkSize    = 1 << 10 // config.blk_size valid
	featBlkFlush      = 1 << 9  // REQ_FLUSH supported
	featBlkDiscard    = 1 << 13 // REQ_DISCARD supported
	featBlkTopology   = 1 << 14 // config topology fields valid
	featBlkWriteZeroe = 1 << 15 // REQ_WRITE_ZEROES supported
	// config.wce is writable only with VIRTIO_BLK_F_CONFIG_WCE, which
	// the VDUSE kernel driver forbids (read-only config space), so we
	// advertise read-only VIRTIO_BLK_F_FLUSH and keep wce=1 fixed.
	featAccessPlatform = 1 << 33 // VIRTIO_F_ACCESS_PLATFORM: required by VDUSE
	featBlkRO          = 1 << 6  // VIRTIO_BLK_F_RO: read-only device
)

// Features is the feature word offered to the guest (discard and
// write-zeroes included; see Options to disable them per device).
// VIRTIO_F_ACCESS_PLATFORM is mandatory (vduse features_is_valid()) and
// CONFIG_WCE must stay off for the same reason.
const Features = uint64(featVersion1 | featRingIndirect |
	featAccessPlatform |
	featBlkSizeMax | featBlkSegMax | featBlkBlkSize | featBlkFlush |
	featBlkDiscard | featBlkTopology | featBlkWriteZeroe)

// FeatureMask returns the feature word for a device, dropping the
// discard / write-zeroes advertisement when the backend cannot honour
// them (same rationale as sdsd's -trim/-write-zeroes flags), and adding
// VIRTIO_BLK_F_RO for a read-only device.
func FeatureMask(trim, writeZeroes, readOnly bool) uint64 {
	f := Features
	if !trim {
		f &^= featBlkDiscard
	}
	if !writeZeroes {
		f &^= featBlkWriteZeroe
	}
	if readOnly {
		f |= featBlkRO
	}
	return f
}

// virtio-blk request types (virtio_blk_req.type).
const (
	ReqIn      = 0  // read: device -> driver
	ReqOut     = 1  // write: driver -> device
	ReqFlush   = 4  // flush volatile cache
	ReqGetID   = 8  // get device id (serial)
	ReqDiscard = 11 // discard segments
	ReqZeroOut = 18 // write zeroes segments
)

// virtio-blk status byte values.
const (
	StatusOK     = 0
	StatusIOErr  = 1
	StatusUnsupp = 2
)

// vqMaxSize is the virtqueue depth we offer the driver. It must
// comfortably exceed the descriptor count of the largest request
// the guest can build (a 1 MiB write at 4 KiB segments = 256 data
// descriptors + header + status): at the old 256-entry ring one
// 1 MiB request consumed the whole queue and serialised every
// large I/O behind it (measured 48 MB/s at qd1 for 1M seqwrite).
// The kernel caps this at 32768; 1024 fits four full-size requests
// and 4k workloads never notice.
const vqMaxSize = 1024

// ConfigLen is the virtio-blk configuration space size: struct
// virtio_blk_config (packed) up to unused1[3] = 60 bytes. Secure-erase
// and zoned fields beyond it are not advertised.
const ConfigLen = 60

// ConfigSpace builds the virtio-blk configuration space for a disk of
// sizeBytes: 512-byte logical / 4 KiB physical geometry, write cache on.
// Offsets follow packed struct virtio_blk_config (kernel uapi) exactly:
// capacity@0 size_max@8 seg_max@12 geometry@16 blk_size@20
// physical_block_exp@24 min_io_size@26 wce@32 num_queues@34(u16)
// max_discard_sectors@36 max_discard_seg@40 discard_sector_alignment@44
// max_write_zeroes_sectors@48 max_write_zeroes_seg@52
// write_zeroes_may_unmap@56.
func ConfigSpace(sizeBytes uint64, numQueues uint16) []byte {
	c := make([]byte, ConfigLen)
	le := binary.LittleEndian
	le.PutUint64(c[0:], sizeBytes/512) // capacity, 512-byte sectors
	le.PutUint32(c[8:], 512*1024)      // size_max per segment
	le.PutUint32(c[12:], vqMaxSize-2)  // seg_max data segments (u32)
	// geometry@16 (cylinders/heads/sectors): informational, left zero
	le.PutUint32(c[20:], 512) // blk_size (logical) = 512
	// topology@24: physical_block_exp=3 (4KiB), alignment_offset=0,
	// min_io_size=1, opt_io_size=0
	c[24] = 3
	le.PutUint16(c[26:], 1) // min_io_size
	c[32] = 1               // wce: write cache enabled; REQ_FLUSH flushes
	le.PutUint16(c[34:], numQueues)
	le.PutUint32(c[36:], 1<<20) // max_discard_sectors (512 MiB)
	le.PutUint32(c[40:], 256)   // max_discard_seg
	le.PutUint32(c[44:], 1)     // discard_sector_alignment
	le.PutUint32(c[48:], 1<<20) // max_write_zeroes_sectors
	le.PutUint32(c[52:], 256)   // max_write_zeroes_seg
	c[56] = 1                   // write_zeroes_may_unmap (thin volumes)
	return c
}

// BlkReqHeader is the 16-byte virtio_blk_req header.
type BlkReqHeader struct {
	Type   uint32
	Resv   uint32
	Sector uint64
}

// ParseBlkReqHeader decodes the header; ok=false when b is too short.
func ParseBlkReqHeader(b []byte) (h BlkReqHeader, ok bool) {
	if len(b) < 16 {
		return h, false
	}
	h.Type = binary.LittleEndian.Uint32(b[0:])
	h.Resv = binary.LittleEndian.Uint32(b[4:])
	h.Sector = binary.LittleEndian.Uint64(b[8:])
	return h, true
}

// DiscardSeg is one virtio_blk_discard_write_zeroes segment.
type DiscardSeg struct {
	Sector     uint64
	NumSectors uint32
	Flags      uint32
}

// ParseDiscardSeg decodes a 16-byte discard/write-zeroes segment.
func ParseDiscardSeg(b []byte) (s DiscardSeg, ok bool) {
	if len(b) < 16 {
		return s, false
	}
	s.Sector = binary.LittleEndian.Uint64(b[0:])
	s.NumSectors = binary.LittleEndian.Uint32(b[8:])
	s.Flags = binary.LittleEndian.Uint32(b[12:])
	return s, true
}
