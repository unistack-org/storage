// Package nbd implements an NBD (Network Block Device) server speaking the
// official NBD protocol (https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md).
//
// The server implements the new-style fixed handshake, structured replies and
// the READ / WRITE / FLUSH / TRIM / WRITE_ZEROES / BLOCK_STATUS transmission
// commands, which is the exact feature set QEMU's nbd block driver uses.
package nbd

// Global magic values (all big-endian on the wire).
const (
	// NBD_MAGIC is sent by the server at the start of the handshake.
	nbdMagic = 0x4e42444d41474943 // "NBDMAGIC"
	// IHAVEOPT_MAGIC follows NBD_MAGIC in a new-style handshake and marks
	// "options mode"; it is also the prefix of every option request.
	ihaveoptMagic = 0x49484156454f5054 // "IHAVEOPT"
	// NBD_REQUEST_MAGIC prefixes every transmission-phase request.
	requestMagic = 0x25609513
	// NBD_SIMPLE_REPLY_MAGIC prefixes a simple (legacy) reply.
	simpleReplyMagic = 0x67446698
	// NBD_STRUCTURED_REPLY_MAGIC prefixes a structured reply chunk.
	structuredReplyMagic = 0x668e33ef
	// NBD_REP_MAGIC prefixes every option reply from the server.
	repMagic = 0x3e889045565a9
	// ihaveoptHigh is the first 32 bits of IHAVEOPT on the wire (big-endian).
	ihaveoptHigh = uint32(ihaveoptMagic >> 32)
)

// Handshake flags (server -> client, u16).
const (
	handshakeFixedNewstyle = 1 << 0 // server supports only new-style
	handshakeNoZeroes      = 1 << 1 // client may request no 124-byte padding
)

// Client flags (client -> server, u32).
const (
	clientFixedNewstyle = 1 << 0
	clientNoZeroes      = 1 << 1
)

// Transmission flags (u16, sent with NBD_OPT_GO / NBD_OPT_INFO).
// Bit numbering is taken verbatim from the spec ("Transmission flags"):
// HAS_FLAGS=bit0 READ_ONLY=bit1 SEND_FLUSH=bit2 SEND_FUA=bit3
// ROTATIONAL=bit4 SEND_TRIM=bit5 SEND_WRITE_ZEROES=bit6 SEND_DF=bit7
// CAN_MULTI_CONN=bit8.
const (
	transmissionHasFlags        = 1 << 0  // mandatory, bit is always set
	transmissionReadOnly        = 1 << 1
	transmissionSendFlush       = 1 << 2
	transmissionSendFUA         = 1 << 3
	transmissionRotational      = 1 << 4
	transmissionSendTrim        = 1 << 5
	transmissionSendWriteZeroes = 1 << 6
	transmissionSendDF          = 1 << 7
	transmissionCanMultiConn    = 1 << 8
	transmissionSendCache       = 1 << 14 // bit 10, defined by the CACHE extension
)

// Option requests (u32).
const (
	optExportName      = 1
	optAbort           = 2
	optList            = 3
	optPeekExport      = 4
	optStartTLS        = 5
	optInfo            = 6
	optGo              = 7
	optStructuredReply = 8
	optListMetaContext = 9
	optSetMetaContext  = 10
)

// Reply types (u32).
const (
	repACK               = 1
	repServer            = 2
	repInfo              = 3
	repMetaContext       = 4
	repErrUnsup          = 0x80000001
	repErrPolicy         = 0x80000002
	repErrInvalid        = 0x80000003
	repErrPlatform       = 0x80000004
	repErrTLSReq         = 0x80000005
	repErrNotAvail       = 0x80000006
	repErrNetDown        = 0x80000007
	repErrNoSpace        = 0x80000008
	repErrPerm           = 0x80000009
	repErrExportExist    = 0x8000000a
	repErrNotFound       = 0x8000000b
	repErrInvalidReq     = 0x8000000d
	repErrTooManyExtents = 0x8000000e
)

// Info types used inside NBD_REP_INFO payloads (u16).
const (
	infoExport      = 0
	infoName        = 1
	infoDescription = 2
	infoBlockSize   = 3
)

// Structured reply chunk types (u16), per the spec.
const (
	chunkTypeNone        = 0
	chunkTypeOffsetData  = 1
	chunkTypeOffsetHole  = 2
	chunkTypeBlockStatus = 5
	chunkTypeError       = 1<<15 + 1
	chunkErrorFlag       = 1 << 15 // set on all error chunk types
)

// Structured reply chunk flags (u16).
const (
	chunkFlagDone = 1 << 0
)

// Transmission command types (u16).
const (
	CmdRead        = 0
	CmdWrite       = 1
	CmdDisc        = 2
	CmdFlush       = 3
	CmdTrim        = 4
	CmdCache       = 5 // cache/preflush hint (CACHE extension)
	CmdWriteZeroes = 6
	CmdBlockStatus = 7
)

// Transmission command flags (u16).
const (
	cmdFlagFUA    = 1 << 0
	cmdFlagNoHole = 1 << 1
	cmdFlagDF     = 1 << 2
	cmdFlagReqOne = 1 << 3
)

// NBD error codes used in replies (u32). Per the spec an error is "a negated
// errno value cast to uint32", i.e. two's complement. On Linux (the target
// platform) the errno values are the ones below; macOS errno values differ,
// so the wire constants are pinned to the Linux numbers.
const (
	ErrOK         uint32 = 0
	ErrEPERM      uint32 = ^uint32(1 - 1)   // -1  EPERM
	ErrENOENT     uint32 = ^uint32(2 - 1)   // -2  ENOENT
	ErrEIO        uint32 = ^uint32(5 - 1)   // -5  EIO
	ErrENXIO      uint32 = ^uint32(6 - 1)   // -6  ENXIO
	ErrE2BIG      uint32 = ^uint32(7 - 1)   // -7  E2BIG
	ErrEINVAL     uint32 = ^uint32(22 - 1)  // -22 EINVAL
	ErrENOSPC     uint32 = ^uint32(28 - 1)  // -28 ENOSPC
	ErrENOTSUP    uint32 = ^uint32(95 - 1)  // -95 EOPNOTSUPP
	ErrENOTCONN   uint32 = ^uint32(107 - 1) // -107 ENOTCONN
	ErrESHUTDOWN  uint32 = ^uint32(108 - 1) // -108 ESHUTDOWN
	ErrEOPNOTSUPP        = ErrENOTSUP
)

// MaxPayload is the maximum request length we accept. QEMU's NBD client can
// issue requests up to 2 MiB, so 4 MiB leaves headroom and avoids EINVAL
// replies to large guest I/O.
const MaxPayload = 4 << 20

// Metadata contexts. The server supports the single standardized context of
// the base namespace: "base:allocation", which feeds NBD_CMD_BLOCK_STATUS.
const (
	metaBaseAllocation = "base:allocation"
)

// Extent flags inside an NBD_CMD_BLOCK_STATUS reply (u32 per the spec):
// bit 0 NBD_STATE_HOLE (reads return zeroes / region is unwritten),
// bit 1 NBD_STATE_ZERO (reads return zeroes).
const (
	nbdStateHole = 1 << 0
	nbdStateZero = 1 << 1
)
