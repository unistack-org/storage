package vub

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"testing"

	"golang.org/x/sys/unix"
)

// socketPair returns two connected unix connections (socketpair +
// FileConn so SCM_RIGHTS works on both Linux and macOS).
func socketPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatalf("socketpair: %v", err)
	}
	fd1, fd2 := fds[0], fds[1]
	f1 := os.NewFile(uintptr(fd1), "sp1")
	defer f1.Close()
	f2 := os.NewFile(uintptr(fd2), "sp2")
	defer f2.Close()
	c1, err := net.FileConn(f1)
	if err != nil {
		t.Fatalf("FileConn: %v", err)
	}
	c2, err := net.FileConn(f2)
	if err != nil {
		t.Fatalf("FileConn: %v", err)
	}
	u1, ok1 := c1.(*net.UnixConn)
	u2, ok2 := c2.(*net.UnixConn)
	if !ok1 || !ok2 {
		t.Fatalf("not unix conns")
	}
	return u1, u2
}

func TestFramingRoundtrip(t *testing.T) {
	a, b := socketPair(t)
	defer a.Close()
	defer b.Close()
	cli, srv := WrapConn(a), WrapConn(b)

	payload := make([]byte, 40)
	for i := range payload {
		payload[i] = byte(i)
	}
	go func() {
		if err := cli.write(ReqSetVringNum, flagVersion|flagNeedReply, payload, nil); err != nil {
			t.Errorf("send: %v", err)
		}
	}()
	h, body, fds, err := srv.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if h.Request != ReqSetVringNum || h.Flags != flagVersion|flagNeedReply {
		t.Fatalf("header = %+v", h)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("body mismatch: %x", body)
	}
	if len(fds) != 0 {
		t.Fatalf("unexpected fds: %v", fds)
	}
}

func TestFDRoundtrip(t *testing.T) {
	a, b := socketPair(t)
	defer a.Close()
	defer b.Close()
	cli, srv := WrapConn(a), WrapConn(b)

	// Pass the pipe READ end over SCM_RIGHTS (kick/call fds are read by the
	// receiver); raw unix.Pipe fds avoid os.File lifetime races.
	var p [2]int
	if err := unix.Pipe(p[:]); err != nil {
		t.Fatal(err)
	}
	rfd, wfd := p[0], p[1]
	sent := make(chan error, 1)
	go func() {
		sent <- cli.write(ReqSetVringKick, flagVersion, []byte{1, 2, 3, 4}, []int{rfd})
	}()
	h, body, fds, err := srv.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if serr := <-sent; serr != nil {
		t.Fatalf("send: %v", serr)
	}
	unix.Close(rfd) // sender's copy: SCM_RIGHTS installed a fresh one
	if h.Request != ReqSetVringKick || !bytes.Equal(body, []byte{1, 2, 3, 4}) {
		t.Fatalf("msg = %+v %x", h, body)
	}
	if len(fds) != 1 {
		t.Fatalf("fds = %v", fds)
	}
	defer unix.Close(fds[0])
	if _, err := unix.Write(wfd, []byte("ping")); err != nil {
		t.Fatal(err)
	}
	unix.Close(wfd)
	buf := make([]byte, 4)
	if _, err := unix.Read(fds[0], buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("fd payload = %q", buf)
	}
}

func TestReplyAckEcho(t *testing.T) {
	a, b := socketPair(t)
	defer a.Close()
	defer b.Close()
	cli, srv := WrapConn(a), WrapConn(b)
	go func() {
		if err := srv.ReplyU64(ReqResetDevice, 0, true); err != nil {
			t.Errorf("ReplyU64: %v", err)
		}
	}()
	h, body, _, err := cli.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if h.Request != ReqResetDevice {
		t.Fatalf("request = %d", h.Request)
	}
	if h.Flags&(flagReply|flagVersion|flagNeedReply) == 0 {
		t.Fatalf("flags = %#x want reply|version|need_reply", h.Flags)
	}
	if binary.LittleEndian.Uint64(body) != 0 {
		t.Fatalf("ack payload = %x", body)
	}
}

func TestMemTableParse(t *testing.T) {
	// VhostUserMemory = {u32 nregions; u32 padding; regions[n]}
	body := make([]byte, 8+32*2)
	binary.LittleEndian.PutUint32(body, 2)
	put := func(off int, gpa, size, ua, mo uint64) {
		binary.LittleEndian.PutUint64(body[off:], gpa)
		binary.LittleEndian.PutUint64(body[off+8:], size)
		binary.LittleEndian.PutUint64(body[off+16:], ua)
		binary.LittleEndian.PutUint64(body[off+24:], mo)
	}
	put(8, 0x1000, 0x200000, 0x7f0000, 0)
	put(8+32, 0x9000_0000, 0x1000, 0x7f3000, 0x200000)
	regions, err := parseMemTable(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(regions) != 2 {
		t.Fatalf("regions = %d", len(regions))
	}
	if regions[0].GuestPhys != 0x1000 || regions[0].Size != 0x200000 || regions[0].MmapOffset != 0 {
		t.Fatalf("region0 = %+v", regions[0])
	}
	if regions[1].GuestPhys != 0x9000_0000 || regions[1].MmapOffset != 0x200000 {
		t.Fatalf("region1 = %+v", regions[1])
	}
	// Truncated region list must be rejected.
	if _, err := parseMemTable(body[:8+32*2-1]); err == nil {
		t.Fatal("truncated memtable accepted")
	}
}

func TestVringAddrParse(t *testing.T) {
	b := make([]byte, 40)
	binary.LittleEndian.PutUint32(b[0:], 3) // index
	binary.LittleEndian.PutUint32(b[4:], 256)
	binary.LittleEndian.PutUint64(b[8:], 0x1000)  // desc
	binary.LittleEndian.PutUint64(b[16:], 0x2000) // used
	binary.LittleEndian.PutUint64(b[24:], 0x3000) // avail
	idx, desc, used, avail, err := vringAddr(b)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 3 || desc != 0x1000 || used != 0x2000 || avail != 0x3000 {
		t.Fatalf("idx=%d desc=%#x used=%#x avail=%#x", idx, desc, used, avail)
	}
}

func TestKickFDParse(t *testing.T) {
	// VhostUserVringState {index=2, num=(2<<8)|fd1}
	b := make([]byte, 8)
	binary.LittleEndian.PutUint32(b[0:], 2)
	binary.LittleEndian.PutUint32(b[4:], (2<<8)|1)
	idx, noFD, err := kickFD(b)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 2 || noFD {
		t.Fatalf("idx=%d noFD=%v", idx, noFD)
	}
	// NOFD: index=3, num=(3<<8)|NOFD_MASK
	binary.LittleEndian.PutUint32(b[0:], 3)
	binary.LittleEndian.PutUint32(b[4:], (3<<8)|(1<<8))
	idx, noFD, err = kickFD(b)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 3 || !noFD {
		t.Fatalf("idx=%d noFD=%v", idx, noFD)
	}
}

func TestGetVringBaseReplyPacking(t *testing.T) {
	// VhostUserVringState reply: {u32 index; u32 num} little-endian,
	// which as a u64 puts index in the low half.
	v := getVringBaseReply(5, 0xABCD)
	if got := uint32(v); got != 5 {
		t.Fatalf("index = %d", got)
	}
	if got := uint32(v >> 32); got != 0xABCD {
		t.Fatalf("base = %#x", got)
	}
}

// cmsgSpace must be at least what the kernel needs for maxFDs fds.
func TestCmsgSpace(t *testing.T) {
	need := unix.CmsgSpace(maxFDs * 8)
	if cmsgSpace < need {
		t.Fatalf("cmsgSpace = %d, kernel needs %d", cmsgSpace, need)
	}
}
