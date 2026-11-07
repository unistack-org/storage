package nbd

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"syscall"
)

// Export is one named NBD export: a backend plus its access flags.
type Export struct {
	Name     string
	Backend  Backend
	ReadOnly bool
}

// ServerOptions configures a Server.
type ServerOptions struct {
	// MaxConnection is the number of concurrent client connections.
	// Zero means unlimited.
	MaxConnections int
	// Logger receives operational messages; nil disables logging.
	Logger *log.Logger
	// EnableTrim / EnableWriteZeroes advertise (and accept) TRIM and
	// WRITE_ZEROES. Disable on platforms where hole punching is absent
	// (e.g. the darwin dev host) so clients never send those commands.
	EnableTrim        bool
	EnableWriteZeroes bool
}

// Server is a concurrent NBD server serving a set of named exports over
// TCP. It implements the fixed-newstyle handshake, NBD_OPT_GO (with
// INFO/LIST/ABORT), structured replies, and the READ/WRITE/FLUSH/TRIM/
// WRITE_ZEROES transmission commands.
type Server struct {
	mu      sync.RWMutex
	exports map[string]*Export
	conns   map[net.Conn]struct{}
	opts    ServerOptions
	ln      net.Listener
	wg      sync.WaitGroup
	closing bool
}

// NewServer creates a server for the given exports.
func NewServer(exports []*Export, opts ServerOptions) *Server {
	s := &Server{exports: map[string]*Export{}, conns: map[net.Conn]struct{}{}, opts: opts}
	for _, e := range exports {
		s.exports[e.Name] = e
	}
	return s
}

// AddExport registers (or replaces) an export at runtime.
func (s *Server) AddExport(e *Export) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exports[e.Name] = e
}

// getExport looks up an export by name.
func (s *Server) getExport(name string) (*Export, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.exports[name]
	return e, ok
}

// listExports returns export names.
func (s *Server) listExports() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.exports))
	for n := range s.exports {
		names = append(names, n)
	}
	return names
}

func (s *Server) logf(format string, args ...any) {
	if s.opts.Logger != nil {
		s.opts.Logger.Printf(format, args...)
	}
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.ln != nil {
		s.mu.Unlock()
		return errors.New("nbd: server already serving")
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.RLock()
			closing := s.closing
			s.mu.RUnlock()
			if closing {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			conn.Close()
			return nil
		}
		n := len(s.conns)
		s.mu.Unlock()
		if s.opts.MaxConnections > 0 && n >= s.opts.MaxConnections {
			s.logf("nbd: refusing %s: connection limit reached", conn.RemoteAddr())
			conn.Close()
			continue
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
			s.mu.Lock()
			delete(s.conns, conn)
			s.mu.Unlock()
		}()
	}
}

// Close stops accepting, force-closes active connections and the listener.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	var errs []error
	if s.ln != nil {
		errs = append(errs, s.ln.Close())
	}
	for c := range s.conns {
		errs = append(errs, c.Close())
	}
	s.mu.Unlock()
	s.wg.Wait()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// connState is per-connection protocol state.
type connState struct {
	conn       net.Conn
	br         *bufio.Reader
	bw         *bufio.Writer
	noZeroes   bool
	structured bool // NBD_OPT_STRUCTURED_REPLY negotiated
	multiCon   bool // CAN_MULTI_CONN advertised
	readOnly   bool
	backend    Backend
	size       uint64
	shutdown   bool
	// ioBuf is the connection's single reusable I/O buffer. The
	// transmission loop is sequential per connection, so no pool is
	// needed: growing this slice on demand and never releasing it
	// makes the warm data path allocation-free (a sync.Pool Put of a
	// []byte allocates via interface boxing; this does not).
	ioBuf []byte
	// reqHdrBuf and scratch are per-connection preallocated wire buffers.
	// Stack arrays handed to bufio/io.Writer calls escape to the heap,
	// which would cost one allocation per request; these fields are
	// allocated once per connection and reused on the hot path.
	reqHdrBuf [28]byte
	scratch   [32]byte // largest header we emit: 20B chunk hdr + 8B offset
}

func (cs *connState) getBuf(n int) []byte {
	if cap(cs.ioBuf) >= n {
		return cs.ioBuf[:n]
	}
	cs.ioBuf = make([]byte, n)
	return cs.ioBuf
}

func (cs *connState) putBuf([]byte) {}

// traceConn wraps a net.Conn with hex logging (SDS_NBD_TRACE=1).
type traceConn struct {
	net.Conn
}

func (t traceConn) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	if n > 0 {
		log.Printf("TRACE %s RECV % x", t.Conn.RemoteAddr(), p[:n])
	}
	return n, err
}

func (t traceConn) Write(p []byte) (int, error) {
	n, err := t.Conn.Write(p)
	if n > 0 {
		b := p
		if n < len(b) {
			b = b[:n]
		}
		log.Printf("TRACE %s SEND % x", t.Conn.RemoteAddr(), b)
	}
	return n, err
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	if os.Getenv("SDS_NBD_TRACE") == "1" {
		conn = traceConn{conn}
	}
	cs := &connState{
		conn: conn,
		br:   bufio.NewReaderSize(conn, 64*1024),
		bw:   bufio.NewWriterSize(conn, 128*1024),
	}
	// TCP_NODELAY matters for small sync-heavy I/O.
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	if err := s.handshake(cs); err != nil {
		s.logf("nbd: handshake with %s failed: %v", conn.RemoteAddr(), err)
		return
	}
	s.logf("nbd: %s transmitting export (size=%d)", conn.RemoteAddr(), cs.size)
	err := s.transmission(cs)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		s.logf("nbd: %s transmission ended: %v", conn.RemoteAddr(), err)
	}
}

// readn is a helper reading exactly len(p) bytes.
func (cs *connState) readn(p []byte) error {
	_, err := io.ReadFull(cs.br, p)
	return err
}

// handshake performs the new-style fixed handshake and options negotiation,
// ending with the connection in transmission phase (or an error).
func (s *Server) handshake(cs *connState) error {
	// Newstyle hello: u64 NBDMAGIC, u64 IHAVEOPT, u16 handshake flags.
	var b [18]byte
	binary.BigEndian.PutUint64(b[:8], nbdMagic)
	binary.BigEndian.PutUint64(b[8:16], ihaveoptMagic)
	binary.BigEndian.PutUint16(b[16:], handshakeFixedNewstyle|handshakeNoZeroes)
	if _, err := cs.bw.Write(b[:]); err != nil {
		return err
	}
	if err := cs.bw.Flush(); err != nil {
		return err
	}
	// Client flags (u32).
	if err := cs.readn(b[:4]); err != nil {
		return err
	}
	cf := binary.BigEndian.Uint32(b[:4])
	if cf & ^uint32(clientFixedNewstyle|clientNoZeroes) != 0 {
		return fmt.Errorf("unknown client flags 0x%x", cf)
	}
	cs.noZeroes = cf&clientNoZeroes != 0

	// Option loop. Each option request is: u64 IHAVEOPT, u32 option,
	// u32 length, data.
	var hdr [16]byte
	for {
		if err := cs.readn(hdr[:]); err != nil {
			return err
		}
		if binary.BigEndian.Uint64(hdr[:8]) != ihaveoptMagic {
			return fmt.Errorf("bad option magic 0x%x", binary.BigEndian.Uint64(hdr[:8]))
		}
		option := binary.BigEndian.Uint32(hdr[8:12])
		length := binary.BigEndian.Uint32(hdr[12:16])
		if length > 1<<20 {
			return fmt.Errorf("option length %d too large", length)
		}
		var data []byte
		if length > 0 {
			data = make([]byte, length)
			if err := cs.readn(data); err != nil {
				return err
			}
		}
		switch option {
		case optExportName:
			// Old-style option: no reply header, success reply is
			// size + transmission flags (+ 124 zero padding).
			e, ok := s.getExport(string(data))
			if !ok {
				// Spec: server MUST close the connection on refusal.
				return fmt.Errorf("export %q not found (EXPORT_NAME)", string(data))
			}
			if err := s.enterTransmission(cs, e, true); err != nil {
				return err
			}
			return nil
		case optInfo:
			name, infoTypes, err := parseGoRequest(data)
			if err != nil {
				if err := s.replyOptionErr(cs, optInfo, repErrInvalid, err.Error()); err != nil {
					return err
				}
				continue
			}
			if err := s.handleInfo(cs, optInfo, name, infoTypes); err != nil {
				return err
			}
		case optGo:
			e, infoTypes, ok := s.lookupGoExport(cs, data)
			if !ok {
				continue // error reply already sent
			}
			if err := s.handleInfo(cs, optGo, e.Name, infoTypes); err != nil {
				return err
			}
			if err := s.enterTransmission(cs, e, false); err != nil {
				return err
			}
			return nil
		case optList:
			if err := s.handleList(cs); err != nil {
				return err
			}
		case optAbort:
			if err := s.replyOption(cs, optAbort, repACK, nil); err != nil {
				return err
			}
			return errAborted
		case optStructuredReply:
			if length != 0 {
				if err := s.replyOptionErr(cs, option, repErrInvalid, "STRUCTURED_REPLY with data"); err != nil {
					return err
				}
				continue
			}
			if err := s.replyOption(cs, option, repACK, nil); err != nil {
				return err
			}
			cs.structured = true
		case optStartTLS:
			if err := s.replyOptionErr(cs, option, repErrPolicy, "TLS not supported"); err != nil {
				return err
			}
		case optListMetaContext:
			// We support no meta contexts; reply with ACK (no REP_META_CONTEXT).
			if err := s.replyOption(cs, option, repACK, nil); err != nil {
				return err
			}
		case optSetMetaContext:
			// Requesting any context must fail (we have none).
			if err := s.replyOptionErr(cs, option, repErrInvalid, "no meta contexts"); err != nil {
				return err
			}
		case optPeekExport:
			if err := s.replyOptionErr(cs, option, repErrUnsup, "not supported"); err != nil {
				return err
			}
		default:
			if err := s.replyOptionErr(cs, option, repErrUnsup, fmt.Sprintf("option %d", option)); err != nil {
				return err
			}
		}
	}
}

// lookupGoExport parses NBD_OPT_GO's length-prefixed export name.
func (s *Server) lookupGoExport(cs *connState, data []byte) (*Export, []uint16, bool) {
	name, infoRequests, err := parseGoRequest(data)
	if err != nil {
		s.replyOptionErr(cs, optGo, repErrInvalid, err.Error())
		return nil, nil, false
	}
	e, ok := s.getExport(name)
	if !ok {
		s.replyOptionErr(cs, optGo, repErrNotFound, fmt.Sprintf("export %q not found", name))
		return nil, nil, false
	}
	return e, infoRequests, true
}

// parseGoRequest parses OPT_GO/OPT_INFO request data:
// u32 export-name length, name, u16 info-request count, u16 types...
func parseGoRequest(data []byte) (name string, infoTypes []uint16, err error) {
	if len(data) < 4 {
		return "", nil, errors.New("short GO request")
	}
	nlen := int(binary.BigEndian.Uint32(data[:4]))
	if 4+nlen > len(data) {
		return "", nil, errors.New("short GO export name")
	}
	name = string(data[4 : 4+nlen])
	rest := data[4+nlen:]
	if len(rest) == 0 {
		return name, nil, nil
	}
	if len(rest) < 2 {
		return "", nil, errors.New("short GO info count")
	}
	cnt := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	if len(rest) < 2*cnt {
		return "", nil, errors.New("short GO info types")
	}
	for i := 0; i < cnt; i++ {
		infoTypes = append(infoTypes, binary.BigEndian.Uint16(rest[2*i:2*i+2]))
	}
	return name, infoTypes, nil
}

// handleInfo replies with NBD_REP_INFO(export) (+ optional name/desc/block-size)
// followed by NBD_REP_ACK; for OPT_GO the ACK is what moves to transmission.
func (s *Server) handleInfo(cs *connState, option uint32, name string, infoTypes []uint16) error {
	e, ok := s.getExport(name)
	if !ok {
		return s.replyOptionErr(cs, option, repErrNotFound, fmt.Sprintf("export %q not found", name))
	}
	flags := s.transmissionFlags(cs, e)
	// NBD_INFO_EXPORT (always, per spec).
	var info [12]byte
	binary.BigEndian.PutUint16(info[:2], infoExport)
	binary.BigEndian.PutUint64(info[2:10], e.Backend.Size())
	binary.BigEndian.PutUint16(info[10:], flags)
	if err := s.replyOptionInfo(cs, option, info[:]); err != nil {
		return err
	}
	want := func(t uint16) bool {
		for _, x := range infoTypes {
			if x == t {
				return true
			}
		}
		return false
	}
	if want(infoName) {
		payload := make([]byte, 2+len(name))
		binary.BigEndian.PutUint16(payload[:2], infoName)
		copy(payload[2:], name)
		if err := s.replyOptionInfo(cs, option, payload); err != nil {
			return err
		}
	}
	if want(infoBlockSize) {
		payload := make([]byte, 2+12)
		binary.BigEndian.PutUint16(payload[:2], infoBlockSize)
		binary.BigEndian.PutUint32(payload[2:6], 512)          // minimum block size
		binary.BigEndian.PutUint32(payload[6:10], 4096)        // preferred block size
		binary.BigEndian.PutUint32(payload[10:14], MaxPayload) // maximum payload
		if err := s.replyOptionInfo(cs, option, payload); err != nil {
			return err
		}
	}
	if want(infoDescription) {
		desc := "sds-nbd"
		payload := make([]byte, 2+len(desc))
		binary.BigEndian.PutUint16(payload[:2], infoDescription)
		copy(payload[2:], desc)
		if err := s.replyOptionInfo(cs, option, payload); err != nil {
			return err
		}
	}
	return s.replyOption(cs, option, repACK, nil)
}

// handleList replies REP_SERVER per export + final ACK.
func (s *Server) handleList(cs *connState) error {
	for _, name := range s.listExports() {
		// payload: u32 name length + name
		p := make([]byte, 4+len(name))
		binary.BigEndian.PutUint32(p[:4], uint32(len(name)))
		copy(p[4:], name)
		if err := s.replyOption(cs, optList, repServer, p); err != nil {
			return err
		}
	}
	return s.replyOption(cs, optList, repACK, nil)
}

// replyOption writes an option-reply packet:
// u64 NBD_REP_MAGIC, u32 option, u32 reply type, u32 length, data.
func (s *Server) replyOption(cs *connState, option, typ uint32, data []byte) error {
	var hdr [20]byte
	binary.BigEndian.PutUint64(hdr[:8], repMagic)
	binary.BigEndian.PutUint32(hdr[8:12], option)
	binary.BigEndian.PutUint32(hdr[12:16], typ)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(data)))
	if _, err := cs.bw.Write(hdr[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := cs.bw.Write(data); err != nil {
			return err
		}
	}
	return cs.bw.Flush()
}

// replyOptionInfo sends an NBD_REP_INFO packet. The packet's 32-bit length
// field already covers the info structure, so the payload (2-byte info type
// + info-specific fields) is the whole option-data with no extra prefix.
func (s *Server) replyOptionInfo(cs *connState, option uint32, payload []byte) error {
	return s.replyOption(cs, option, repInfo, payload)
}

func (s *Server) replyOptionErr(cs *connState, option, code uint32, msg string) error {
	return s.replyOption(cs, option, code, []byte(msg))
}

// transmissionFlags computes the u16 transmission flags for an export.
func (s *Server) transmissionFlags(cs *connState, e *Export) uint16 {
	flags := uint16(transmissionHasFlags | transmissionCanMultiConn)
	if e.ReadOnly {
		flags |= transmissionReadOnly
	} else {
		flags |= transmissionSendFlush
		flags |= transmissionSendFUA
	}
	if s.opts.EnableTrim && !e.ReadOnly {
		flags |= transmissionSendTrim
	}
	if s.opts.EnableWriteZeroes && !e.ReadOnly {
		flags |= transmissionSendWriteZeroes
	}
	if cs.structured {
		flags |= transmissionSendDF
	}
	return flags
}

// enterTransmission sends the final OPT_EXPORT_NAME reply when needed and
// stores export state on the connection.
func (s *Server) enterTransmission(cs *connState, e *Export, exportNameStyle bool) error {
	if exportNameStyle {
		// Legacy reply: u64 size, u16 flags, 124 zero bytes (unless NO_ZEROES).
		var b [10]byte
		binary.BigEndian.PutUint64(b[:8], e.Backend.Size())
		binary.BigEndian.PutUint16(b[8:], s.transmissionFlags(cs, e))
		if _, err := cs.bw.Write(b[:]); err != nil {
			return err
		}
		if !cs.noZeroes {
			var pad [124]byte
			if _, err := cs.bw.Write(pad[:]); err != nil {
				return err
			}
		}
		if err := cs.bw.Flush(); err != nil {
			return err
		}
	}
	cs.backend = e.Backend
	cs.size = e.Backend.Size()
	cs.readOnly = e.ReadOnly
	return nil
}

// errAborted marks a clean OPT_ABORT client disconnect.
var errAborted = errors.New("nbd: client aborted")

// reqHdr is a decoded transmission request.
type reqHdr struct {
	flags  uint16
	typ    uint16
	handle uint64
	offset uint64
	length uint32
}

// transmission is the request/reply loop.
func (s *Server) transmission(cs *connState) error {
	for {
		hdr := cs.reqHdrBuf[:]
		if err := cs.readn(hdr); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(hdr[:4]) != requestMagic {
			return fmt.Errorf("bad request magic 0x%x", binary.BigEndian.Uint32(hdr[:4]))
		}
		req := reqHdr{
			flags:  binary.BigEndian.Uint16(hdr[4:6]),
			typ:    binary.BigEndian.Uint16(hdr[6:8]),
			handle: binary.BigEndian.Uint64(hdr[8:16]),
			offset: binary.BigEndian.Uint64(hdr[16:24]),
			length: binary.BigEndian.Uint32(hdr[24:28]),
		}
		s.mu.RLock()
		shutdown := s.closing
		s.mu.RUnlock()
		if shutdown {
			// Drain DISC politely; everything else gets ESHUTDOWN.
			if req.typ == CmdDisc {
				return nil
			}
			if err := s.replyErr(cs, req.handle, ErrESHUTDOWN, "request failed"); err != nil {
				return err
			}
			continue
		}
		switch req.typ {
		case CmdDisc:
			s.logf("nbd: %s disconnect", cs.conn.RemoteAddr())
			return nil
		case CmdRead:
			if err := s.handleRead(cs, req); err != nil {
				return err
			}
		case CmdWrite:
			if err := s.handleWrite(cs, req); err != nil {
				return err
			}
		case CmdFlush:
			// Flush takes no payload; length/offset are ignored.
			if err := cs.backend.Flush(); err != nil {
				if err2 := s.replyErr(cs, req.handle, errnoToNBD(err), err.Error()); err2 != nil {
					return err2
				}
				continue
			}
			if err := s.replyOK(cs, req.handle); err != nil {
				return err
			}
		case CmdTrim:
			if cs.readOnly {
				if err := s.replyErr(cs, req.handle, ErrEPERM, "request failed"); err != nil {
					return err
				}
				continue
			}
			if !s.opts.EnableTrim {
				if err := s.replyErr(cs, req.handle, ErrEOPNOTSUPP, "request failed"); err != nil {
					return err
				}
				continue
			}
			if err := validRange(cs.size, req.offset, req.length); err != nil {
				if err2 := s.replyErr(cs, req.handle, ErrEINVAL, "request failed"); err2 != nil {
					return err2
				}
				continue
			}
			if err := cs.backend.Trim(int64(req.offset), req.length); err != nil {
				if err2 := s.replyErr(cs, req.handle, errnoToNBD(err), err.Error()); err2 != nil {
					return err2
				}
				continue
			}
			if err := s.replyOK(cs, req.handle); err != nil {
				return err
			}
		case CmdWriteZeroes:
			if cs.readOnly {
				if err := s.replyErr(cs, req.handle, ErrEPERM, "request failed"); err != nil {
					return err
				}
				continue
			}
			if !s.opts.EnableWriteZeroes {
				if err := s.replyErr(cs, req.handle, ErrEOPNOTSUPP, "request failed"); err != nil {
					return err
				}
				continue
			}
			if err := validRange(cs.size, req.offset, req.length); err != nil {
				if err2 := s.replyErr(cs, req.handle, ErrEINVAL, "request failed"); err2 != nil {
					return err2
				}
				continue
			}
			punch := req.flags&cmdFlagNoHole == 0
			if err := cs.backend.WriteZeroes(int64(req.offset), req.length, punch); err != nil {
				if err2 := s.replyErr(cs, req.handle, errnoToNBD(err), err.Error()); err2 != nil {
					return err2
				}
				continue
			}
			if err := s.replyOK(cs, req.handle); err != nil {
				return err
			}
		case CmdBlockStatus:
			// We advertise no meta contexts, so per spec we must refuse.
			if err := s.replyStructuredError(cs, req.handle, ErrEOPNOTSUPP, "no meta contexts"); err != nil {
				return err
			}
		default:
			s.logf("nbd: %s unsupported command %d", cs.conn.RemoteAddr(), req.typ)
			if err := s.replyErr(cs, req.handle, ErrEOPNOTSUPP, "request failed"); err != nil {
				return err
			}
		}
	}
}

// validRange checks offset+length lies inside the export.
func validRange(size, offset uint64, length uint32) error {
	if offset >= size {
		return fmt.Errorf("offset past EOF")
	}
	if uint64(length) > size-offset {
		return fmt.Errorf("length past EOF")
	}
	return nil
}

// handleRead performs a READ and replies (structured chunks when negotiated).
func (s *Server) handleRead(cs *connState, req reqHdr) error {
	if req.length == 0 {
		// Zero-length read: legal, replies empty.
		return s.replyReadDone(cs, req.handle)
	}
	if req.length > MaxPayload || req.flags&cmdFlagDF != 0 && req.length > MaxPayload {
		if cs.structured {
			return s.replyStructuredError(cs, req.handle, ErrEINVAL, fmt.Sprintf("read length %d exceeds max payload %d", req.length, MaxPayload))
		}
		return s.replyErr(cs, req.handle, ErrEINVAL, "request failed")
	}
	if err := validRange(cs.size, req.offset, req.length); err != nil {
		return s.replyReadError(cs, req.handle, ErrEINVAL)
	}
	buf := cs.getBuf(int(req.length))
	defer cs.putBuf(buf)
	n, err := cs.backend.ReadAt(buf, int64(req.offset))
	if err != nil && (n != int(req.length) || err != io.EOF) {
		return s.replyReadError(cs, req.handle, errnoToNBD(err))
	}
	buf = buf[:n]
	if !cs.structured {
		return s.replySimple(cs, req.handle, ErrOK, buf)
	}
	// Structured: OFFSET_DATA chunks (<= MaxPayload each; length <= MaxPayload so one chunk) + DONE.
	if n > 0 {
		hdr := cs.scratch[:20]
		binary.BigEndian.PutUint32(hdr[:4], structuredReplyMagic)
		binary.BigEndian.PutUint16(hdr[4:6], 0) // no flags on data chunk
		binary.BigEndian.PutUint16(hdr[6:8], chunkTypeOffsetData)
		binary.BigEndian.PutUint64(hdr[8:16], req.handle)
		binary.BigEndian.PutUint32(hdr[16:20], uint32(n)+8)
		if _, err := cs.bw.Write(hdr); err != nil {
			return err
		}
		off := cs.scratch[:8]
		binary.BigEndian.PutUint64(off, req.offset)
		if _, err := cs.bw.Write(off); err != nil {
			return err
		}
		if _, err := cs.bw.Write(buf); err != nil {
			return err
		}
	}
	return s.replyStructuredDone(cs, req.handle)
}

// replyOK replies success for a non-read transmission command in whichever
// reply format (simple vs structured) the client negotiated. Sending a simple
// reply after STRUCTURED_REPLY was acknowledged breaks QEMU's block layer
// (it treats the unexpected magic as a protocol error and fails all I/O).
func (s *Server) replyOK(cs *connState, handle uint64) error {
	if cs.structured {
		return s.replyStructuredDone(cs, handle)
	}
	return s.replySimple(cs, handle, ErrOK, nil)
}

// replyErr replies a transmission failure in the negotiated reply format.
func (s *Server) replyErr(cs *connState, handle uint64, code uint32, msg string) error {
	s.logf("nbd: %s request error handle=%d code=%d: %s", cs.conn.RemoteAddr(), handle, code, msg)
	if cs.structured {
		return s.replyStructuredError(cs, handle, code, msg)
	}
	return s.replySimple(cs, handle, code, nil)
}

// replyReadError replies a read failure in structured or simple form.
func (s *Server) replyReadError(cs *connState, handle uint64, code uint32) error {
	return s.replyErr(cs, handle, code, "read failed")
}

// replyReadDone replies an empty successful read.
func (s *Server) replyReadDone(cs *connState, handle uint64) error {
	if cs.structured {
		return s.replyStructuredDone(cs, handle)
	}
	return s.replySimple(cs, handle, ErrOK, nil)
}

// handleWrite reads payload from the wire then writes it.
func (s *Server) handleWrite(cs *connState, req reqHdr) error {
	if req.length > MaxPayload {
		return s.replyErr(cs, req.handle, ErrEINVAL, "request failed")
	}
	if cs.readOnly {
		// Must drain payload before replying to keep framing.
		if _, err := io.CopyN(io.Discard, cs.br, int64(req.length)); err != nil {
			return err
		}
		return s.replyErr(cs, req.handle, ErrEPERM, "request failed")
	}
	if err := validRange(cs.size, req.offset, req.length); err != nil {
		if _, err := io.CopyN(io.Discard, cs.br, int64(req.length)); err != nil {
			return err
		}
		return s.replyErr(cs, req.handle, ErrEINVAL, "request failed")
	}
	buf := cs.getBuf(int(req.length))
	defer cs.putBuf(buf)
	if err := cs.readn(buf); err != nil {
		return err
	}
	// FUA: spec allows replying ok after write; we flush to honour it.
	var werr error
	n, werr := cs.backend.WriteAt(buf, int64(req.offset))
	if werr != nil || n != int(req.length) {
		if werr == nil {
			werr = io.ErrShortWrite
		}
		return s.replyErr(cs, req.handle, errnoToNBD(werr), "request failed")
	}
	if req.flags&cmdFlagFUA != 0 {
		if err := cs.backend.Flush(); err != nil {
			return s.replyErr(cs, req.handle, errnoToNBD(err), err.Error())
		}
	}
	return s.replyOK(cs, req.handle)
}

// replySimple writes a simple reply: magic + err + handle (+ optional data).
func (s *Server) replySimple(cs *connState, handle uint64, errCode uint32, data []byte) error {
	hdr := cs.scratch[:16]
	binary.BigEndian.PutUint32(hdr[:4], simpleReplyMagic)
	binary.BigEndian.PutUint32(hdr[4:8], errCode)
	binary.BigEndian.PutUint64(hdr[8:16], handle)
	if _, err := cs.bw.Write(hdr); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := cs.bw.Write(data); err != nil {
			return err
		}
	}
	return cs.bw.Flush()
}

// replyStructuredDone writes a NONE chunk with DONE flag.
func (s *Server) replyStructuredDone(cs *connState, handle uint64) error {
	b := cs.scratch[:20]
	binary.BigEndian.PutUint32(b[:4], structuredReplyMagic)
	binary.BigEndian.PutUint16(b[4:6], chunkFlagDone)
	binary.BigEndian.PutUint16(b[6:8], chunkTypeNone)
	binary.BigEndian.PutUint64(b[8:16], handle)
	binary.BigEndian.PutUint32(b[16:20], 0)
	if _, err := cs.bw.Write(b); err != nil {
		return err
	}
	return cs.bw.Flush()
}

// replyStructuredError writes an ERROR chunk (with DONE) carrying the code.
func (s *Server) replyStructuredError(cs *connState, handle uint64, code uint32, msg string) error {
	// chunk payload: u32 error + message bytes
	p := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(p[:4], code)
	copy(p[4:], msg)
	var hdr [20]byte
	binary.BigEndian.PutUint32(hdr[:4], structuredReplyMagic)
	binary.BigEndian.PutUint16(hdr[4:6], chunkFlagDone)
	binary.BigEndian.PutUint16(hdr[6:8], chunkTypeError)
	binary.BigEndian.PutUint64(hdr[8:16], handle)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(p)))
	if _, err := cs.bw.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := cs.bw.Write(p); err != nil {
		return err
	}
	return cs.bw.Flush()
}

// errnoToNBD maps a Go/os error to an NBD wire error code.
func errnoToNBD(err error) uint32 {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		// On Linux the wire code is simply uint32(-errno). On darwin our
		// syscall numbers differ from Linux errno values; the MVP dev host
		// is darwin, so map common codes explicitly.
		return errnoToNBDLinux(errno)
	}
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return ErrEIO
	case errors.Is(err, os.ErrPermission):
		return ErrEPERM
	case errors.Is(err, os.ErrNotExist):
		return ErrENOENT
	default:
		return ErrEIO
	}
}
