//go:build linux

package sdsnet

import (
	"errors"
	"io"
	"net"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// soZerocopy is SO_ZEROCOPY (linux 2.14+); x/sys does not export it.
const soZerocopy = 61

// zcState values (Conn.zcState).
const (
	zcUnknown int32 = 0
	zcOK      int32 = 1
	zcNo      int32 = 2
)

// canZeroCopy reports that large writes on this connection may use
// MSG_ZEROCOPY. Only a plain TCP socket qualifies (TLS owns its socket
// writes, the in-process socketpair never hits a kernel copy path);
// support is probed once per connection by setting SO_ZEROCOPY.
func (cc *Conn) canZeroCopy() bool {
	tcp, ok := cc.c.(*net.TCPConn)
	if !ok {
		return false
	}
	switch cc.zcState.Load() {
	case zcOK:
		return true
	case zcNo:
		return false
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		cc.zcState.Store(zcNo)
		return false
	}
	var serr error
	if err := raw.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, soZerocopy, 1)
	}); err != nil || serr != nil {
		cc.zcState.Store(zcNo)
		return false
	}
	cc.zcState.Store(zcOK)
	return true
}

// sendZeroCopy sends b with MSG_ZEROCOPY and then waits for the
// kernel's completion notification on MSG_ERRQUEUE, which is the
// zero-copy contract: the kernel may still be reading b until the
// notification arrives, so the buffer is handed back to the caller
// only after it.
//
// Go sockets are non-blocking at the kernel level, so the send runs on
// a dup'ed descriptor (dup clears O_NONBLOCK) bounded by
// SO_SNDTIMEO/SO_RCVTIMEO from deadline. Sends on a connection are
// serialised (single writer goroutine) and completions arrive in send
// order, so the first completion drained after a send is this send's.
// If the kernel fell back to copying (memory pressure, pages not
// pinned) no notification is armed: the receive-timeout is reported as
// success - the data is already in kernel hands.
func (cc *Conn) sendZeroCopy(b []byte, deadline time.Time) error {
	tcp, ok := cc.c.(*net.TCPConn)
	if !ok {
		_, err := cc.c.Write(b)
		return err
	}
	var fd int
	var derr error
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	if err := raw.Control(func(f uintptr) { fd, derr = unix.Dup(int(f)) }); err != nil {
		return err
	}
	if derr != nil {
		return derr
	}
	defer unix.Close(fd)
	if !deadline.IsZero() {
		ns := time.Until(deadline).Nanoseconds()
		if ns < time.Millisecond.Nanoseconds() {
			ns = time.Millisecond.Nanoseconds()
		}
		tv := syscall.NsecToTimeval(ns)
		_ = syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_SNDTIMEO, &tv)
		_ = syscall.SetsockoptTimeval(fd, syscall.SOL_SOCKET, syscall.SO_RCVTIMEO, &tv)
	}
	total := 0
	for total < len(b) {
		iovs := unix.Iovec{Base: &b[total]}
		iovs.SetLen(len(b) - total)
		ms := unix.Msghdr{Iov: &iovs, Iovlen: 1}
		n, _, errno := unix.Syscall6(unix.SYS_SENDTO, uintptr(fd),
			uintptr(unsafe.Pointer(&ms)), uintptr(unix.MSG_ZEROCOPY), 0, 0, 0)
		if errno != 0 {
			return errno
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		total += int(n)
	}
	// Drain the errqueue until our completion (or the copy-fallback
	// timeout) shows up.
	oob := make([]byte, 256)
	buf := make([]byte, 1)
	for {
		_, oobn, _, _, err := unix.Recvmsg(fd, buf, oob, unix.MSG_ERRQUEUE)
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return nil // copy fallback: nothing will be notified
		}
		if err != nil {
			return err
		}
		if oobn > 0 && parseZCNotification(oob[:oobn]) {
			return nil
		}
	}
}

// parseZCNotification reports whether the control messages contain a
// zerocopy completion: SOL_SOCKET control data whose first byte is
// SO_EE_ORIGIN_ZEROCOPY (7) - struct sock_exterr_errqueue
// {origin u8; type u8; code u8; pad; info u32; data u32 (seq range)}.
func parseZCNotification(oob []byte) bool {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return false
	}
	for _, m := range msgs {
		if len(m.Data) >= 8 && m.Header.Level == syscall.SOL_SOCKET &&
			m.Data[0] == 7 /* SO_EE_ORIGIN_ZEROCOPY */ {
			return true
		}
	}
	return false
}
