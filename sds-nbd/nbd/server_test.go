package nbd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// client is a minimal in-process NBD client used by the tests. It is
// deliberately protocol-explicit (no helper library) so the tests validate
// the wire format of the server.
type client struct {
	conn       net.Conn
	br         *bufioReader
	structured bool
	size       uint64
	flags      uint16
	nextHandle uint64
	// pending reads: handle -> expected length (to consume payload).
	pending map[uint64]uint32
}

// bufioReader is a tiny wrapper so we can ReadFull from the conn.
type bufioReader struct{ c net.Conn }

func (r *bufioReader) ReadFull(p []byte) error {
	_, err := io.ReadFull(r.c, p)
	return err
}

func newClient(t testing.TB, addr string) *client {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	cl := &client{conn: c, br: &bufioReader{c: c}, pending: map[uint64]uint32{}}
	cl.handshake(t)
	return cl
}

func (cl *client) handshake(t testing.TB) {
	t.Helper()
	var b [18]byte
	if err := cl.br.ReadFull(b[:]); err != nil {
		t.Fatalf("handshake server hello: %v", err)
	}
	if got := binary.BigEndian.Uint64(b[:8]); got != nbdMagic {
		t.Fatalf("bad server magic 0x%x", got)
	}
	if got := binary.BigEndian.Uint64(b[8:16]); got != ihaveoptMagic {
		t.Fatalf("bad server opt magic 0x%x", got)
	}
	hsFlags := binary.BigEndian.Uint16(b[16:])
	if hsFlags&handshakeFixedNewstyle == 0 {
		t.Fatalf("server did not set FIXED_NEWSTYLE (flags 0x%x)", hsFlags)
	}
	// Client flags: request fixed newstyle (no NO_ZEROES so EXPORT_NAME padding exists).
	var cf [4]byte
	binary.BigEndian.PutUint32(cf[:], clientFixedNewstyle)
	if _, err := cl.conn.Write(cf[:]); err != nil {
		t.Fatalf("client flags: %v", err)
	}
}

// option sends an option request and reads all replies until the final one
// for that option (the spec forbids pipelining options, so the final reply
// is identified by: for LIST, a reply of type ACK/ERR; here we simply collect
// replies while collecting server entries for LIST).
func (cl *client) option(t testing.TB, option uint32, data []byte) (replies []optionReply) {
	t.Helper()
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[:8], ihaveoptMagic)
	binary.BigEndian.PutUint32(hdr[8:12], option)
	binary.BigEndian.PutUint32(hdr[12:16], uint32(len(data)))
	if _, err := cl.conn.Write(append(hdr[:], data...)); err != nil {
		t.Fatalf("option %d write: %v", option, err)
	}
	for {
		var rh [20]byte
		if err := cl.br.ReadFull(rh[:]); err != nil {
			t.Fatalf("option %d reply header: %v", option, err)
		}
		if got := binary.BigEndian.Uint64(rh[:8]); got != repMagic {
			t.Fatalf("bad reply magic 0x%x", got)
		}
		ropt := binary.BigEndian.Uint32(rh[8:12])
		typ := binary.BigEndian.Uint32(rh[12:16])
		length := binary.BigEndian.Uint32(rh[16:20])
		if ropt != option {
			t.Fatalf("reply for option %d, asked %d", ropt, option)
		}
		payload := make([]byte, length)
		if length > 0 {
			if err := cl.br.ReadFull(payload); err != nil {
				t.Fatalf("option %d reply data: %v", option, err)
			}
		}
		replies = append(replies, optionReply{typ: typ, data: payload})
		// Final reply for most options is ACK or an ERR; for LIST it is ACK
		// (after REP_SERVER entries); for INFO/GO the ACK closes it.
		if typ == repACK || typ&0x80000000 != 0 {
			return replies
		}
	}
}

type optionReply struct {
	typ  uint32
	data []byte
}

// goExport negotiates an export via OPT_GO (+ structured replies first) and
// enters transmission.
func (cl *client) goExport(t testing.TB, name string) {
	t.Helper()
	cl.requestStructured(t)
	data := make([]byte, 4+len(name)+2)
	binary.BigEndian.PutUint32(data[:4], uint32(len(name)))
	copy(data[4:], name)
	binary.BigEndian.PutUint16(data[4+len(name):], 0) // no extra info requests
	replies := cl.option(t, optGo, data)
	cl.applyExportReplies(t, replies)
}

func (cl *client) requestStructured(t testing.TB) {
	t.Helper()
	replies := cl.option(t, optStructuredReply, nil)
	if replies[len(replies)-1].typ != repACK {
		t.Fatalf("STRUCTURED_REPLY not acked: %+v", replies)
	}
	cl.structured = true
}

// applyExportReplies parses INFO replies (size + flags) out of GO/INFO replies.
func (cl *client) applyExportReplies(t testing.TB, replies []optionReply) {
	t.Helper()
	for _, r := range replies {
		if r.typ == repInfo {
			if len(r.data) < 2 {
				t.Fatalf("short info reply")
			}
			ityp := binary.BigEndian.Uint16(r.data[:2])
			switch ityp {
			case infoExport:
				if len(r.data) != 12 {
					t.Fatalf("INFO_EXPORT len %d", len(r.data))
				}
				cl.size = binary.BigEndian.Uint64(r.data[2:10])
				cl.flags = binary.BigEndian.Uint16(r.data[10:])
			}
		}
		if r.typ&0x80000000 != 0 {
			t.Fatalf("export rejected: type=0x%x msg=%q", r.typ, r.data)
		}
	}
	if cl.size == 0 {
		t.Fatalf("no INFO_EXPORT in replies")
	}
}

// exportNameLegacy performs the old OPT_EXPORT_NAME negotiation.
func (cl *client) exportNameLegacy(t testing.TB, name string) {
	t.Helper()
	replies := cl.option(t, optExportName, []byte(name)) // never returns: success has no reply
	_ = replies
}

// readExportNameLegacyResponse reads the size+flags(+124 zeros) reply.
func (cl *client) readExportNameLegacyResponse(t testing.TB, noZeroes bool) {
	t.Helper()
	var b [10]byte
	if err := cl.br.ReadFull(b[:]); err != nil {
		t.Fatalf("EXPORT_NAME reply: %v", err)
	}
	cl.size = binary.BigEndian.Uint64(b[:8])
	cl.flags = binary.BigEndian.Uint16(b[8:])
	if !noZeroes {
		var pad [124]byte
		if err := cl.br.ReadFull(pad[:]); err != nil {
			t.Fatalf("EXPORT_NAME padding: %v", err)
		}
	}
}

func (cl *client) request(t testing.TB, typ uint16, flags uint16, offset uint64, length uint32, payload []byte) uint64 {
	t.Helper()
	var hdr [28]byte
	binary.BigEndian.PutUint32(hdr[:4], requestMagic)
	binary.BigEndian.PutUint16(hdr[4:6], flags)
	binary.BigEndian.PutUint16(hdr[6:8], typ)
	h := cl.nextHandle
	cl.nextHandle++
	binary.BigEndian.PutUint64(hdr[8:16], h)
	binary.BigEndian.PutUint64(hdr[16:24], offset)
	binary.BigEndian.PutUint32(hdr[24:28], length)
	msg := append(hdr[:], payload...)
	if _, err := cl.conn.Write(msg); err != nil {
		t.Fatalf("request %d write: %v", typ, err)
	}
	return h
}

// reply is a decoded transmission reply.
type reply struct {
	handle uint64
	err    uint32
	data   []byte
	// structured chunks
	chunks []chunk
}

type chunk struct {
	flags uint16
	typ   uint16
	data  []byte // payload after the fixed chunk fields
}

// readSimpleReply reads one simple reply.
func (cl *client) readSimpleReply(t testing.TB) reply {
	t.Helper()
	var hdr [16]byte
	if err := cl.br.ReadFull(hdr[:]); err != nil {
		t.Fatalf("simple reply: %v", err)
	}
	if got := binary.BigEndian.Uint32(hdr[:4]); got != simpleReplyMagic {
		t.Fatalf("bad simple reply magic 0x%x", got)
	}
	return reply{
		err:    binary.BigEndian.Uint32(hdr[4:8]),
		handle: binary.BigEndian.Uint64(hdr[8:16]),
	}
}

// readCommandReply reads a reply to a non-read transmission command in
// whichever format was negotiated (simple 16B, or structured chunks).
func (cl *client) readCommandReply(t testing.TB, handle uint64) reply {
	t.Helper()
	if !cl.structured {
		r := cl.readSimpleReply(t)
		if r.handle != handle {
			t.Fatalf("reply handle %d want %d", r.handle, handle)
		}
		return r
	}
	r, _ := cl.readReadReplyData(t, handle)
	return r
}

// readReadReply reads the reply to a READ (payload follows simple reply;
// structured chunks until DONE).
func (cl *client) readReadReply(t testing.TB, handle uint64, length uint32) reply {
	t.Helper()
	if cl.structured {
		r, _ := cl.readReadReplyData(t, handle)
		return r
	}
	{
		r := cl.readSimpleReply(t)
		if r.handle != handle {
			t.Fatalf("reply handle %d want %d", r.handle, handle)
		}
		if r.err == 0 && length > 0 {
			r.data = make([]byte, length)
			if err := cl.br.ReadFull(r.data); err != nil {
				t.Fatalf("read payload: %v", err)
			}
		}
		return r
	}
}

// readReadReplyData consumes structured chunks for handle and returns the
// assembled reply.
func (cl *client) readReadReplyData(t testing.TB, handle uint64) (reply, []byte) {
	t.Helper()
	var out reply
	out.handle = handle
	for {
		var hdr [20]byte
		if err := cl.br.ReadFull(hdr[:]); err != nil {
			t.Fatalf("structured reply: %v", err)
		}
		if got := binary.BigEndian.Uint32(hdr[:4]); got != structuredReplyMagic {
			t.Fatalf("bad structured magic 0x%x", got)
		}
		flags := binary.BigEndian.Uint16(hdr[4:6])
		typ := binary.BigEndian.Uint16(hdr[6:8])
		h := binary.BigEndian.Uint64(hdr[8:16])
		length := binary.BigEndian.Uint32(hdr[16:20])
		if h != handle {
			t.Fatalf("chunk handle %d want %d", h, handle)
		}
		payload := make([]byte, length)
		if length > 0 {
			if err := cl.br.ReadFull(payload); err != nil {
				t.Fatalf("chunk payload: %v", err)
			}
		}
		switch {
		case typ == chunkTypeNone:
			// success terminator
		case typ == chunkTypeOffsetData:
			if len(payload) < 8 {
				t.Fatalf("offset-data chunk too short")
			}
			out.data = append(out.data, payload[8:]...)
		case typ&chunkErrorFlag != 0:
			if len(payload) >= 4 {
				out.err = binary.BigEndian.Uint32(payload[:4])
			} else {
				out.err = ErrEIO
			}
		default:
			t.Fatalf("unexpected chunk type %d", typ)
		}
		out.chunks = append(out.chunks, chunk{flags: flags, typ: typ, data: payload})
		if flags&chunkFlagDone != 0 {
			return out, out.data
		}
	}
}

// cmd helpers -------------------------------------------------------------

func (cl *client) read(t testing.TB, offset uint64, length uint32) ([]byte, uint32) {
	t.Helper()
	h := cl.request(t, CmdRead, 0, offset, length, nil)
	r := cl.readReadReply(t, h, length)
	return r.data, r.err
}

func (cl *client) write(t testing.TB, offset uint64, data []byte) uint32 {
	t.Helper()
	h := cl.request(t, CmdWrite, 0, offset, uint32(len(data)), data)
	r := cl.readCommandReply(t, h)
	return r.err
}

func (cl *client) flush(t testing.TB) uint32 {
	t.Helper()
	h := cl.request(t, CmdFlush, 0, 0, 0, nil)
	r := cl.readCommandReply(t, h)
	return r.err
}

func (cl *client) trim(t testing.TB, offset uint64, length uint32) uint32 {
	t.Helper()
	h := cl.request(t, CmdTrim, 0, offset, length, nil)
	r := cl.readCommandReply(t, h)
	return r.err
}

func (cl *client) writeZeroes(t testing.TB, offset uint64, length uint32, noHole bool) uint32 {
	t.Helper()
	var flags uint16
	if noHole {
		flags |= cmdFlagNoHole
	}
	h := cl.request(t, CmdWriteZeroes, flags, offset, length, nil)
	r := cl.readCommandReply(t, h)
	return r.err
}

func (cl *client) disconnect(t testing.TB) {
	t.Helper()
	cl.request(t, CmdDisc, 0, 0, 0, nil)
}

// testServer starts a Server on a temp dir with one export "vol0".
func testServer(t testing.TB, size uint64, opts ServerOptions) (addr string, file string) {
	t.Helper()
	dir := t.TempDir()
	file = filepath.Join(dir, "vol0.img")
	b, err := NewFileBackend(file, size)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	srv := NewServer([]*Export{{Name: "vol0", Backend: b}}, opts)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) {
			// server closed
		}
	}()
	t.Cleanup(func() { srv.Close() })
	return ln.Addr().String(), file
}

// tests -------------------------------------------------------------------

func TestHandshakeListGoReadWriteFlush(t *testing.T) {
	addr, _ := testServer(t, 1<<20, ServerOptions{})
	cl := newClient(t, addr)

	// LIST must contain vol0.
	replies := cl.option(t, optList, nil)
	var names []string
	for _, r := range replies {
		if r.typ == repServer && len(r.data) >= 4 {
			n := binary.BigEndian.Uint32(r.data[:4])
			names = append(names, string(r.data[4:4+n]))
		}
	}
	if len(names) != 1 || names[0] != "vol0" {
		t.Fatalf("LIST = %v, want [vol0]", names)
	}

	cl.goExport(t, "vol0")
	if cl.size != 1<<20 {
		t.Fatalf("size = %d, want %d", cl.size, 1<<20)
	}
	if cl.flags&transmissionHasFlags == 0 {
		t.Fatal("HAS_FLAGS not set")
	}
	if cl.flags&transmissionSendFlush == 0 {
		t.Fatal("SEND_FLUSH not set")
	}

	// WRITE then READ roundtrip.
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	if e := cl.write(t, 4096, payload); e != ErrOK {
		t.Fatalf("write err = %d", e)
	}
	data, e := cl.read(t, 4096, 4096)
	if e != ErrOK {
		t.Fatalf("read err = %d", e)
	}
	if len(data) != 4096 {
		t.Fatalf("read len = %d, want 4096", len(data))
	}
	for i := range data {
		if data[i] != payload[i] {
			t.Fatalf("read data mismatch at %d", i)
		}
	}

	if e := cl.flush(t); e != ErrOK {
		t.Fatalf("flush err = %d", e)
	}

	// Read past EOF must be EINVAL.
	_, e = cl.read(t, cl.size-10, 20)
	if e != ErrEINVAL {
		t.Fatalf("read past EOF err = %d, want EINVAL", e)
	}

	cl.disconnect(t)
}

func TestExportNameLegacy(t *testing.T) {
	addr, _ := testServer(t, 64<<20, ServerOptions{})
	cl := newClient(t, addr)
	// OPT_EXPORT_NAME success has no structured reply: read raw size+flags+pad.
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[:8], ihaveoptMagic)
	binary.BigEndian.PutUint32(hdr[8:12], optExportName)
	binary.BigEndian.PutUint32(hdr[12:16], 4)
	if _, err := cl.conn.Write(append(hdr[:], []byte("vol0")...)); err != nil {
		t.Fatalf("write: %v", err)
	}
	cl.readExportNameLegacyResponse(t, false)
	if cl.size != 64<<20 {
		t.Fatalf("size = %d, want %d", cl.size, 64<<20)
	}
	payload := []byte("hello-nbd")
	if e := cl.write(t, 0, payload); e != ErrOK {
		t.Fatalf("write: %d", e)
	}
	data, e := cl.read(t, 0, uint32(len(payload)))
	if e != ErrOK || string(data) != "hello-nbd" {
		t.Fatalf("read = %q, %d", data, e)
	}
	cl.disconnect(t)
}

func TestExportNotFound(t *testing.T) {
	addr, _ := testServer(t, 1<<20, ServerOptions{})
	cl := newClient(t, addr)
	data := make([]byte, 4+len("nope")+2)
	binary.BigEndian.PutUint32(data[:4], 4)
	copy(data[4:], "nope")
	replies := cl.option(t, optGo, data)
	last := replies[len(replies)-1]
	if last.typ != repErrNotFound {
		t.Fatalf("reply type = 0x%x, want ERR_NOT_FOUND 0x%x", last.typ, repErrNotFound)
	}
}

func TestTrimAndWriteZeroes(t *testing.T) {
	if os.Getenv("SDS_NBD_TRIM_TESTS") != "1" {
		t.Skip("hole punching requires Linux; set SDS_NBD_TRIM_TESTS=1 on Linux")
	}
	addr, _ := testServer(t, 1<<20, ServerOptions{EnableTrim: true, EnableWriteZeroes: true})
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	if cl.flags&transmissionSendTrim == 0 {
		t.Fatal("SEND_TRIM not advertised")
	}
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = 0xAB
	}
	if e := cl.write(t, 0, payload); e != ErrOK {
		t.Fatalf("write: %d", e)
	}
	if e := cl.trim(t, 4096, 4096); e != ErrOK {
		t.Fatalf("trim: %d", e)
	}
	data, e := cl.read(t, 4096, 4096)
	if e != ErrOK {
		t.Fatalf("read after trim: %d", e)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("trim left data at %d: 0x%02x", i, b)
		}
	}
	if e := cl.writeZeroes(t, 0, 4096, true); e != ErrOK {
		t.Fatalf("write zeroes: %d", e)
	}
	data, e = cl.read(t, 0, 4096)
	if e != ErrOK {
		t.Fatalf("read after zeroes: %d", e)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("write-zeroes left data at %d", i)
		}
	}
	cl.disconnect(t)
}

func TestReadOnlyExport(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "ro.img")
	b, err := NewFileBackend(file, 1<<20)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	// Pre-fill so READ has content.
	b.WriteAt([]byte("seed"), 0)
	srv := NewServer([]*Export{{Name: "ro", Backend: b, ReadOnly: true}}, ServerOptions{})
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	cl := newClient(t, ln.Addr().String())
	cl.goExport(t, "ro")
	if cl.flags&transmissionReadOnly == 0 {
		t.Fatal("READ_ONLY not advertised")
	}
	if e := cl.write(t, 0, []byte("x")); e != ErrEPERM {
		t.Fatalf("write on RO = %d, want EPERM", e)
	}
	data, e := cl.read(t, 0, 4)
	if e != ErrOK || string(data) != "seed" {
		t.Fatalf("read = %q %d", data, e)
	}
	cl.disconnect(t)
}

func TestServerCloseDuringTransmission(t *testing.T) {
	addr, _ := testServer(t, 1<<20, ServerOptions{})
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	// Just check the connection is alive; Close() runs via cleanup.
	cl.disconnect(t)
}

func TestConcurrentClients(t *testing.T) {
	addr, _ := testServer(t, 1<<20, ServerOptions{})
	const n = 4
	done := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				done <- err
				return
			}
			defer c.Close()
			// Minimal handshake: read hello, send flags, GO.
			var b [18]byte
			if _, err := io.ReadFull(c, b[:]); err != nil {
				done <- err
				return
			}
			var cf [4]byte
			binary.BigEndian.PutUint32(cf[:], clientFixedNewstyle)
			if _, err := c.Write(cf[:]); err != nil {
				done <- err
				return
			}
			tf := &testing.T{} // not used for Fatalf in goroutines; use returns
			_ = tf
			name := "vol0"
			data := make([]byte, 4+len(name)+2)
			binary.BigEndian.PutUint32(data[:4], uint32(len(name)))
			copy(data[4:], name)
			var hdr [16]byte
			binary.BigEndian.PutUint64(hdr[:8], ihaveoptMagic)
			binary.BigEndian.PutUint32(hdr[8:12], optGo)
			binary.BigEndian.PutUint32(hdr[12:16], uint32(len(data)))
			if _, err := c.Write(append(hdr[:], data...)); err != nil {
				done <- err
				return
			}
			// Drain replies until ACK/ERR.
			for {
				var rh [20]byte
				if _, err := io.ReadFull(c, rh[:]); err != nil {
					done <- err
					return
				}
				length := binary.BigEndian.Uint32(rh[16:20])
				payload := make([]byte, length)
				if length > 0 {
					if _, err := io.ReadFull(c, payload); err != nil {
						done <- err
						return
					}
				}
				typ := binary.BigEndian.Uint32(rh[12:16])
				if typ == repACK || typ&0x80000000 != 0 {
					break
				}
			}
			// Write + read.
			payload := []byte(fmt.Sprintf("client-%d", i))
			w := make([]byte, 28)
			binary.BigEndian.PutUint32(w[:4], requestMagic)
			binary.BigEndian.PutUint16(w[6:8], CmdWrite)
			binary.BigEndian.PutUint64(w[8:16], 1)
			copy(w[24:28], []byte{0, 0, 0, byte(len(payload))})
			binary.BigEndian.PutUint32(w[24:28], uint32(len(payload)))
			if _, err := c.Write(append(w, payload...)); err != nil {
				done <- err
				return
			}
			var rr [16]byte
			if _, err := io.ReadFull(c, rr[:]); err != nil {
				done <- err
				return
			}
			if binary.BigEndian.Uint32(rr[4:8]) != 0 {
				done <- fmt.Errorf("write failed: %d", binary.BigEndian.Uint32(rr[4:8]))
				return
			}
			done <- nil
		}(i)
	}
	for i := 0; i < n; i++ {
		if err := <-done; err != nil {
			t.Fatalf("client %d: %v", i, err)
		}
	}
}

// --- pipelined (structured-reply) concurrency tests ----------------------

// delayBackend slows reads at a designated offset so out-of-order reply
// behaviour is observable: with a pipelined server, fast reads issued
// after the slow one finish (and reply) first.
type delayBackend struct {
	Backend
	slowOffset int64
	slowDelay  time.Duration
}

func (d *delayBackend) ReadAt(p []byte, off int64) (int, error) {
	if off == d.slowOffset {
		time.Sleep(d.slowDelay)
	}
	return d.Backend.ReadAt(p, off)
}

// readAnyStructuredReply reads one complete structured reply (any handle)
// and returns its handle, assembled data and error code.
func (cl *client) readAnyStructuredReply(t testing.TB) (uint64, []byte, uint32) {
	t.Helper()
	var handle uint64
	var data []byte
	var errCode uint32
	for {
		var hdr [20]byte
		if err := cl.br.ReadFull(hdr[:]); err != nil {
			t.Fatalf("structured reply: %v", err)
		}
		if got := binary.BigEndian.Uint32(hdr[:4]); got != structuredReplyMagic {
			t.Fatalf("bad structured magic 0x%x", got)
		}
		flags := binary.BigEndian.Uint16(hdr[4:6])
		typ := binary.BigEndian.Uint16(hdr[6:8])
		h := binary.BigEndian.Uint64(hdr[8:16])
		length := binary.BigEndian.Uint32(hdr[16:20])
		if handle == 0 {
			handle = h
		} else if h != handle {
			t.Fatalf("mixed handles in one reply: %d then %d", handle, h)
		}
		payload := make([]byte, length)
		if length > 0 {
			if err := cl.br.ReadFull(payload); err != nil {
				t.Fatalf("chunk payload: %v", err)
			}
		}
		switch {
		case typ == chunkTypeOffsetData:
			data = append(data, payload[8:]...)
		case typ&chunkErrorFlag != 0:
			if len(payload) >= 4 {
				errCode = binary.BigEndian.Uint32(payload[:4])
			} else {
				errCode = ErrEIO
			}
		case typ == chunkTypeNone:
		default:
			t.Fatalf("unexpected chunk type %d", typ)
		}
		if flags&chunkFlagDone != 0 {
			return handle, data, errCode
		}
	}
}

func TestPipelinedReadsReplyOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	fb, err := NewFileBackend(filepath.Join(dir, "vol0.img"), 1<<20)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { fb.Close() })
	b := &delayBackend{Backend: fb, slowOffset: 4096, slowDelay: 150 * time.Millisecond}
	srv := NewServer([]*Export{{Name: "vol0", Backend: b}}, ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close() })
	addr := ln.Addr().String()
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")

	// Populate: block i at offset i*4096 filled with byte i.
	const n = 8
	for i := 0; i < n; i++ {
		data := make([]byte, 4096)
		for j := range data {
			data[j] = byte(i + 1)
		}
		if e := cl.write(t, uint64(i)*4096, data); e != 0 {
			t.Fatalf("write %d: err %d", i, e)
		}
	}
	if e := cl.flush(t); e != 0 {
		t.Fatalf("flush: %d", e)
	}

	// Pipeline: slow read first, then fast reads. The server must answer
	// the fast reads while the slow one is in flight.
	slowHandle := cl.request(t, CmdRead, 0, 4096, 4096, nil) // slow: 150ms
	fastHandles := []uint64{}
	for _, i := range []int{0, 2, 3, 4, 5, 6, 7} {
		fastHandles = append(fastHandles, cl.request(t, CmdRead, 0, uint64(i)*4096, 4096, nil))
	}

	seen := map[uint64]bool{}
	gotFirst := uint64(0)
	for k := 0; k < len(fastHandles)+1; k++ {
		h, data, e := cl.readAnyStructuredReply(t)
		if e != 0 {
			t.Fatalf("read reply handle %d: err %d", h, e)
		}
		if len(data) != 4096 {
			t.Fatalf("read reply handle %d: len %d", h, len(data))
		}
		if k == 0 {
			gotFirst = h
		}
		if h == slowHandle {
			if data[0] != 2 {
				t.Fatalf("slow reply data[0]=%d want 2", data[0])
			}
		} else {
			idx := -1
			for i, fh := range fastHandles {
				if fh == h {
					idx = i
					break
				}
			}
			if idx < 0 {
				t.Fatalf("unexpected handle %d", h)
			}
			want := byte([]int{1, 3, 4, 5, 6, 7, 8}[idx])
			if data[0] != want {
				t.Fatalf("fast reply %d: data[0]=%d want %d", h, data[0], want)
			}
		}
		seen[h] = true
	}
	if len(seen) != len(fastHandles)+1 {
		t.Fatalf("saw %d replies, want %d", len(seen), len(fastHandles)+1)
	}
	if gotFirst == slowHandle {
		t.Fatalf("server serialised: slow reply arrived first")
	}
}

func TestPipelinedWritesFlushBarrier(t *testing.T) {
	addr, _ := testServer(t, 1<<20, ServerOptions{})
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")

	// Pipeline 16 writes with interleaved reads (reads may land before
	// the matching write and are not checked), then FLUSH: by spec FLUSH
	// must cover all previous writes, so the final reads must see all
	// written data.
	const n = 16
	handles := make([]uint64, n)
	for i := 0; i < n; i++ {
		data := make([]byte, 1024)
		for j := range data {
			data[j] = byte(i)
		}
		handles[i] = cl.request(t, CmdWrite, 0, uint64(i)*1024, uint32(len(data)), data)
	}
	flushHandle := cl.request(t, CmdFlush, 0, 0, 0, nil)
	for i := 0; i < n; i++ {
		h, _, e := cl.readAnyStructuredReply(t)
		if e != 0 {
			t.Fatalf("write reply %d (handle %d): %d", i, h, e)
		}
	}
	// The FLUSH reply is the n+1-th (may be interleaved: read exactly n replies first).
	for {
		h, _, e := cl.readAnyStructuredReply(t)
		if e != 0 {
			t.Fatalf("flush reply: %d", e)
		}
		if h == flushHandle {
			break
		}
	}

	// Verify all writes landed.
	for i := 0; i < n; i++ {
		data, e := cl.read(t, uint64(i)*1024, 1024)
		if e != 0 {
			t.Fatalf("read %d: %d", i, e)
		}
		if data[0] != byte(i) || data[1023] != byte(i) {
			t.Fatalf("block %d: got [%d..%d]", i, data[0], data[1023])
		}
	}
}
