package transport

import (
	"errors"
	"net"
	"time"
)

func DialTimeout(network string, address string, timeout time.Duration) (net.Conn, error) {
	switch network {
	//	case "utp":
	//	return utp.DialTimeout(address, timeout)
	case "tcp":
		return net.DialTimeout("tcp", address, timeout)
	}
	return nil, errors.New("unsupported network")
}

func Listen(network string, laddr string) (net.Listener, error) {
	switch network {
	//	case "utp":
	//	return utp.NewSocket("udp", laddr)
	case "tcp":
		return net.Listen(network, laddr)
	}
	return nil, errors.New("unsupported network")
}
