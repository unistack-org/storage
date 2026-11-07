// Package net implements the SDS data-path wire protocol: a fixed-size
// binary header (no protobuf/grpc), request pipelining, and a pooled
// client. It is the transport for replication traffic (phase 2) and for
// the eventual client protocol.
//
// Wire layout, all integers big-endian (network order, like NBD):
//
//	request  = 32-byte header [+ length bytes of payload for writes]
//	response = 16-byte header [+ length bytes of payload for reads]
//
// A connection is fully ordered: replies come back in request order, so
// the client can pipeline any number of requests. seq must match and is
// checked on every response to detect stream desync.
package sdsnet

import (
	"encoding/binary"
	"fmt"
)

// Protocol version. Bumped only for incompatible framing changes.
const Version = 1

// Header sizes (bytes).
const (
	ReqHeaderSize  = 32
	RespHeaderSize = 16
)

// MaxPayload bounds a single request/response payload so a peer cannot
// force huge allocations. 4 MiB matches the object size.
const MaxPayload = 4 << 20

// Op codes.
type Op uint8

const (
	OpPing Op = iota
	OpRead
	OpWrite
	OpWriteZeroes // zeroes [Offset, Offset+Length); FlagsHole requests sparse
	OpTrim        // punch a hole (sparse zeroes)
	OpSync        // persist everything prior to this request
)

func (o Op) String() string {
	switch o {
	case OpPing:
		return "ping"
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpWriteZeroes:
		return "write_zeroes"
	case OpTrim:
		return "trim"
	case OpSync:
		return "sync"
	}
	return fmt.Sprintf("op(%d)", uint8(o))
}

// Request flags.
const (
	FlagsHole = 1 << 0 // WriteZeroes: make the range sparse (punch hole)
	FlagsFUA  = 1 << 1 // persist this write before replying
)

// Status codes. Zero means success; others are stable wire values and
// map onto local error semantics at both ends.
type Status uint32

const (
	StatusOK       Status = iota
	StatusInvalid         // malformed request / bad range
	StatusReadOnly        // export opened read-only
	StatusEOF             // offset at/after end of volume
	StatusNotSupp         // operation not supported by this server
	StatusIO              // backend I/O failure
	StatusTooBig          // payload exceeds MaxPayload
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusInvalid:
		return "invalid"
	case StatusReadOnly:
		return "read-only"
	case StatusEOF:
		return "eof"
	case StatusNotSupp:
		return "not supported"
	case StatusIO:
		return "i/o"
	case StatusTooBig:
		return "too big"
	}
	return fmt.Sprintf("status(%d)", uint32(s))
}

// Error is a remote failure: the server's status for one request.
type Error struct {
	Seq    uint32
	Op     Op
	Status Status
}

func (e *Error) Error() string {
	return fmt.Sprintf("sdsnet: %s (seq %d) failed: %s", e.Op, e.Seq, e.Status)
}

// ReqHeader is one protocol request.
//
// Layout (32 B): ver u8 | op u8 | flags u16 | seq u32 | oid u64 |
//
//	offset u64 | length u32 | epoch u32
//
// OID addresses an object inside the export the connection is bound to
// (handshake); a server maps (oid, offset) onto its storage. epoch is a
// placement/membership epoch reserved for replication fencing (phase 2).
type ReqHeader struct {
	Op     Op
	Flags  uint16
	Seq    uint32
	OID    uint64
	Offset uint64 // in-object offset for Read/Write; byte 0..Size-1
	Length uint32
	Epoch  uint32
}

// Encode writes the header into b, which must be at least
// ReqHeaderSize long.
func (h *ReqHeader) Encode(b []byte) {
	_ = b[ReqHeaderSize-1]
	b[0] = Version
	b[1] = byte(h.Op)
	binary.BigEndian.PutUint16(b[2:4], h.Flags)
	binary.BigEndian.PutUint32(b[4:8], h.Seq)
	binary.BigEndian.PutUint64(b[8:16], h.OID)
	binary.BigEndian.PutUint64(b[16:24], h.Offset)
	binary.BigEndian.PutUint32(b[24:28], h.Length)
	binary.BigEndian.PutUint32(b[28:32], h.Epoch)
}

// DecodeReqHeader parses a request header.
func DecodeReqHeader(b []byte) (ReqHeader, error) {
	if len(b) < ReqHeaderSize {
		return ReqHeader{}, ErrShortHeader
	}
	if b[0] != Version {
		return ReqHeader{}, fmt.Errorf("sdsnet: unsupported version %d", b[0])
	}
	h := ReqHeader{
		Op:     Op(b[1]),
		Flags:  binary.BigEndian.Uint16(b[2:4]),
		Seq:    binary.BigEndian.Uint32(b[4:8]),
		OID:    binary.BigEndian.Uint64(b[8:16]),
		Offset: binary.BigEndian.Uint64(b[16:24]),
		Length: binary.BigEndian.Uint32(b[24:28]),
		Epoch:  binary.BigEndian.Uint32(b[28:32]),
	}
	if h.Length > MaxPayload {
		return h, ErrTooBig
	}
	return h, nil
}

// RespHeader is one protocol response.
//
// Layout (16 B): seq u32 | status u32 | value u64
//
// value carries the payload length for reads (and other future data).
type RespHeader struct {
	Seq    uint32
	Status Status
	Value  uint64
}

// Encode writes the header into b (at least RespHeaderSize bytes).
func (h *RespHeader) Encode(b []byte) {
	_ = b[RespHeaderSize-1]
	binary.BigEndian.PutUint32(b[0:4], h.Seq)
	binary.BigEndian.PutUint32(b[4:8], uint32(h.Status))
	binary.BigEndian.PutUint64(b[8:16], h.Value)
}

// DecodeRespHeader parses a response header.
func DecodeRespHeader(b []byte) (RespHeader, error) {
	if len(b) < RespHeaderSize {
		return RespHeader{}, ErrShortHeader
	}
	return RespHeader{
		Seq:    binary.BigEndian.Uint32(b[0:4]),
		Status: Status(binary.BigEndian.Uint32(b[4:8])),
		Value:  binary.BigEndian.Uint64(b[8:16]),
	}, nil
}

// Handshake: sent by the client right after connect, before any request.
//
// Layout (16 B): magic u64 | ver u32 | reserved u16 | export-name
// length u16, followed by that many bytes of export name. The server
// replies with a RespHeader (StatusOK on success, Value = 0).
const handshakeMagic = uint64(0x5344534E45540100) // "SDSNET" + version byte

// EncodeHandshake builds a handshake message.
func EncodeHandshake(export string) []byte {
	b := make([]byte, 16+len(export))
	binary.BigEndian.PutUint64(b[0:8], handshakeMagic)
	binary.BigEndian.PutUint32(b[8:12], Version)
	binary.BigEndian.PutUint16(b[12:14], 0) // reserved
	binary.BigEndian.PutUint16(b[14:16], uint16(len(export)))
	copy(b[16:], export)
	return b
}

// HandshakeFixed is the fixed 16-byte prefix of a handshake.
type HandshakeFixed struct {
	Version  uint32
	NameLen  uint16
	HasMagic bool
}

// DecodeHandshakeFixed parses the fixed prefix of a handshake message
// (the 16 bytes following the connection accept, before the name).
func DecodeHandshakeFixed(b []byte) (HandshakeFixed, error) {
	if len(b) < 16 {
		return HandshakeFixed{}, ErrShortHeader
	}
	return HandshakeFixed{
		Version:  binary.BigEndian.Uint32(b[8:12]),
		NameLen:  binary.BigEndian.Uint16(b[14:16]),
		HasMagic: binary.BigEndian.Uint64(b[0:8]) == handshakeMagic,
	}, nil
}

// DecodeHandshake parses a complete handshake message.
func DecodeHandshake(b []byte) (export string, err error) {
	f, err := DecodeHandshakeFixed(b)
	if err != nil {
		return "", err
	}
	if !f.HasMagic {
		return "", ErrProtocol
	}
	if f.Version != Version {
		return "", fmt.Errorf("sdsnet: unsupported version %d", f.Version)
	}
	if 16+int(f.NameLen) > len(b) {
		return "", ErrShortHeader
	}
	return string(b[16 : 16+f.NameLen]), nil
}
