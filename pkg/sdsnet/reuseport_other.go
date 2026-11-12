//go:build !linux

package sdsnet

import "net"

// ListenReusePort has no SO_REUSEPORT fan-out off Linux: it binds one
// plain listener, which is exactly what n == 1 means. Callers must not
// rely on more than one listener being returned.
func ListenReusePort(addr string, n int) ([]net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return []net.Listener{ln}, nil
}
