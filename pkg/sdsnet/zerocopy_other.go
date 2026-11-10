//go:build !linux

package sdsnet

import "time"

// canZeroCopy reports MSG_ZEROCOPY availability: only Linux TCP
// sockets support it (see zerocopy_linux.go). TLS and other transports
// always copy.
func (cc *Conn) canZeroCopy() bool { return false }

// sendZeroCopy is a plain write off Linux.
func (cc *Conn) sendZeroCopy(b []byte, deadline time.Time) error {
	_, err := cc.c.Write(b)
	return err
}
