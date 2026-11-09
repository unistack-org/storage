//go:build !unix

package sdsnet

import (
	"errors"
	"net"
)

// Socketpair is not available on this platform; callers must fall back
// to a TCP transport.
func Socketpair() (a, b net.Conn, err error) {
	return nil, nil, errors.New("sdsnet: local socketpair transport not supported on this platform")
}
