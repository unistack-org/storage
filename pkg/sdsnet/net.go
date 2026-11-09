package sdsnet

import "net"

// setNoDelay disables Nagle buffering. The protocol is strictly
// request/response with small messages: with Nagle enabled a response
// that follows a half-filled segment waits for the peer's ACK
// (delayed-ACK, ~40ms worst case), which on loopback shows up as
// inflated per-op latency. TCP_NODELAY is the standard fix and is
// never harmful for this traffic pattern.
func setNoDelay(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
}
