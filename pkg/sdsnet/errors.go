package sdsnet

import "errors"

// Protocol-level sentinel errors.
var (
	// ErrShortHeader: buffer smaller than a fixed header.
	ErrShortHeader = errors.New("sds/net: short header")
	// ErrProtocol: peer violated framing (magic mismatch, seq gap).
	ErrProtocol = errors.New("sds/net: protocol violation")
	// ErrTooBig: length exceeds MaxPayload.
	ErrTooBig = errors.New("sds/net: payload too big")
	// ErrClosed: connection is closed.
	ErrClosed = errors.New("sds/net: connection closed")
)
