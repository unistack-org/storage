package sdsnet

import (
	"context"
	"net"
	"syscall"
)

// ListenReusePort binds n independent listeners on the same address with
// SO_REUSEPORT set before bind: the kernel spreads incoming connections
// across the listeners by a flow hash of the 5-tuple, so every listener
// gets its own accept queue and its own lock. A single accept loop is a
// serialisation point at high connection rates (one goroutine wakes per
// connection, the accept queue lock is shared), and pinning an OSD's
// workers to a NUMA node makes the skew worse: the accept thread may
// live on the far node. One listener per pinned worker group keeps the
// whole path - accept, handshake, request - on one node.
//
// The kernel requires SO_REUSEADDR alongside SO_REUSEPORT and all n
// sockets must bind the exact same address: a caller that wants port 0
// resolves the port from the first listener and rebinds the rest to the
// explicit "host:port".
func ListenReusePort(addr string, n int) ([]net.Listener, error) {
	if n < 1 {
		n = 1
	}
	ln, err := listenReuse(addr)
	if err != nil {
		return nil, err
	}
	out := []net.Listener{ln}
	resolved := ln.Addr().String()
	for len(out) < n {
		ln, err := listenReuse(resolved)
		if err != nil {
			for _, l := range out {
				_ = l.Close()
			}
			return nil, err
		}
		out = append(out, ln)
	}
	return out, nil
}

func listenReuse(addr string) (net.Listener, error) {
	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var serr error
			if err := c.Control(func(fd uintptr) {
				if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
					serr = err
					return
				}
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unixSO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return serr
		},
	}
	return lc.Listen(context.Background(), "tcp", addr)
}

const unixSO_REUSEPORT = 15 // SOL_SOCKET/SO_REUSEPORT on Linux
