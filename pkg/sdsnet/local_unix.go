//go:build unix

package sdsnet

import (
	"net"
	"os"
	"syscall"
)

// localBufSize is the socket buffer we ask for on each endpoint. The
// 4 KiB ops of a block device fit with room to spare; a big buffer
// keeps the pair a lossless queue under deep queues.
const localBufSize = 4 << 20

// Socketpair returns two connected endpoints of an AF_UNIX
// SOCK_STREAM socket. It is the transport for a client and server in
// one process (the local-direct data path): requests carry the full
// wire protocol (handshake, headers, payloads) exactly as over TCP, so
// the server-side QoS, epoch fencing and metrics all stay in force,
// but no bytes traverse the TCP/IP stack — no checksums, no socket
// state machine, and (unix sockets have no Nagle) no delayed-ACK
// round-trips. Both endpoints are close-on-exec and sized with a large
// socket buffer.
func Socketpair() (a, b net.Conn, err error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}
	for i := range fds {
		_ = syscall.SetsockoptInt(fds[i], syscall.SOL_SOCKET, syscall.SO_SNDBUF, localBufSize)
		_ = syscall.SetsockoptInt(fds[i], syscall.SOL_SOCKET, syscall.SO_RCVBUF, localBufSize)
	}
	fa := os.NewFile(uintptr(fds[0]), "sdsnet-local-a")
	fb := os.NewFile(uintptr(fds[1]), "sdsnet-local-b")
	ca, err := net.FileConn(fa)
	fa.Close()
	if err != nil {
		syscall.Close(fds[1])
		return nil, nil, err
	}
	cb, err := net.FileConn(fb)
	fb.Close()
	if err != nil {
		ca.Close()
		syscall.Close(fds[1])
		return nil, nil, err
	}
	return ca, cb, nil
}
