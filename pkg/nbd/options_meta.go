package nbd

// Meta-context negotiation (NBD_OPT_LIST_META_CONTEXT /
// NBD_OPT_SET_META_CONTEXT), NBD_OPT_START_TLS, and the
// NBD_CMD_BLOCK_STATUS command live here. We support exactly one
// context, base:allocation, and only on exports whose backend
// implements StatusReporter.
//
// Wire formats (verified against the NBD protocol spec):
//
//	LIST/SET request: u32 export-name length, name, u32 query count,
//	  then per query: u32 length + string. (The query count is u32,
//	  not u16.)
//	REP_META_CONTEXT data: u32 context ID + string context name.
//	  The ID is 0 in LIST replies and a server-chosen unique value in
//	  SET replies; clients treat it as opaque.
//	BLOCK_STATUS reply chunk payload: u32 context ID, then one or
//	  more descriptors of u32 length + u32 flags. Exactly one chunk
//	  per selected context, followed by the structured DONE chunk.

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// errTLSUpgraded is the internal signal that START_TLS completed: the
// options loop must stop parsing plaintext and return so the upgraded
// connection can be re-handshaked from scratch (spec: START_TLS resets
// all negotiation state).
var errTLSUpgraded = errors.New("nbd: connection upgraded to TLS")

// statusReporter returns the export backend's status reporter, if the
// backend can answer block-status queries.
func statusReporter(b Backend) (StatusReporter, bool) {
	sr, ok := b.(StatusReporter)
	return sr, ok
}

// metaContextAvailable reports whether base:allocation can be listed
// / selected for the given export on this connection: the export must
// exist and its backend must implement StatusReporter.
func (s *Server) metaContextAvailable(name string) bool {
	e, ok := s.getExport(name)
	if !ok {
		return false
	}
	_, ok = statusReporter(e.Backend)
	return ok
}

// parseMetaContextRequest decodes the LIST/SET_META_CONTEXT payload.
func parseMetaContextRequest(data []byte) (exportName string, queries []string, err error) {
	if len(data) < 8 {
		return "", nil, fmt.Errorf("meta context request too short")
	}
	n := binary.BigEndian.Uint32(data[:4])
	if uint64(n) > uint64(len(data)-4) {
		return "", nil, fmt.Errorf("export name length beyond payload")
	}
	exportName = string(data[4 : 4+n])
	rest := data[4+n:]
	if len(rest) < 4 {
		return "", nil, fmt.Errorf("missing query count")
	}
	q := binary.BigEndian.Uint32(rest[:4])
	rest = rest[4:]
	if q > uint32(len(rest)) { // each query costs >= 4 length bytes
		return "", nil, fmt.Errorf("query count beyond payload")
	}
	for i := uint32(0); i < q; i++ {
		if len(rest) < 4 {
			return "", nil, fmt.Errorf("query length beyond payload")
		}
		l := binary.BigEndian.Uint32(rest[:4])
		if uint64(l) > uint64(len(rest)-4) {
			return "", nil, fmt.Errorf("query length beyond payload")
		}
		queries = append(queries, string(rest[4:4+l]))
		rest = rest[4+l:]
	}
	return exportName, queries, nil
}

// querySelectsContext applies the base: namespace rules: the empty
// leaf-name "base:" is a wildcard during LIST and MUST be ignored
// during SET; "base:allocation" selects our one context; anything in
// an unknown namespace MUST be ignored.
func querySelectsContext(q string, wildcardSelects bool) bool {
	if q == metaBaseAllocation {
		return true
	}
	if q == "base:" {
		return wildcardSelects
	}
	return false // unknown leaf/namespace: silently ignored
}

// metaContextReplyData builds REP_META_CONTEXT payload: u32 id + name.
func metaContextReplyData(id uint32, name string) []byte {
	b := make([]byte, 4+len(name))
	binary.BigEndian.PutUint32(b[:4], id)
	copy(b[4:], name)
	return b
}

func (s *Server) handleListMetaContext(cs *connState, option uint32, data []byte) error {
	exportName, queries, err := parseMetaContextRequest(data)
	if err != nil {
		return s.replyOptionErr(cs, option, repErrInvalid, err.Error())
	}
	// Zero queries mean "all available contexts" for this export.
	if len(queries) == 0 {
		queries = []string{"base:"}
	}
	if s.metaContextAvailable(exportName) {
		for _, q := range queries {
			if querySelectsContext(q, true) {
				if err := s.replyOption(cs, option, repMetaContext, metaContextReplyData(0, metaBaseAllocation)); err != nil {
					return err
				}
				break // one context max; never reply with duplicates
			}
		}
	}
	return s.replyOption(cs, option, repACK, nil)
}

func (s *Server) handleSetMetaContext(cs *connState, option uint32, data []byte) error {
	// Structured replies are a hard prerequisite (spec).
	if !cs.structured {
		return s.replyOptionErr(cs, option, repErrInvalid, "SET_META_CONTEXT requires structured replies")
	}
	exportName, queries, err := parseMetaContextRequest(data)
	if err != nil {
		return s.replyOptionErr(cs, option, repErrInvalid, err.Error())
	}
	// This call REPLACES the selection, even on parse-level failure
	// below; a fresh SET starts from zero contexts.
	cs.metaCtxs = nil
	cs.metaExport = ""
	var selected []string
	if s.metaContextAvailable(exportName) {
		for _, q := range queries {
			// Wildcard "base:" MUST be ignored during SET.
			if querySelectsContext(q, false) {
				selected = append(selected, metaBaseAllocation)
				break
			}
		}
	}
	// Context ID: index+1, unique per SET call, opaque to the client.
	for i, c := range selected {
		if err := s.replyOption(cs, option, repMetaContext, metaContextReplyData(uint32(i+1), c)); err != nil {
			return err
		}
	}
	if err := s.replyOption(cs, option, repACK, nil); err != nil {
		return err
	}
	cs.metaCtxs = selected
	if len(selected) > 0 {
		cs.metaExport = exportName
	}
	return nil
}

// handleStartTLS implements NBD_OPT_START_TLS. The server must reply
// (REP_ACK) before switching the socket to TLS; after a successful
// handshake every negotiation state is reset and the (now encrypted)
// connection goes through a fresh newstyle handshake.
func (s *Server) handleStartTLS(cs *connState, option uint32, data []byte) (bool, error) {
	if len(data) != 0 {
		return false, s.replyOptionErr(cs, option, repErrInvalid, "START_TLS must not include additional data")
	}
	if cs.tlsOn {
		return false, s.replyOptionErr(cs, option, repErrInvalid, "TLS already active")
	}
	cfg := s.opts.TLSServerConfig
	if cfg == nil {
		// Policy refusal: stay in options mode (plain client keeps working).
		return false, s.replyOptionErr(cs, option, repErrPolicy, "TLS not configured on this server")
	}
	// Flush our ACK before the client starts its TLS handshake.
	if err := s.replyOption(cs, option, repACK, nil); err != nil {
		return false, err
	}
	tc := tls.Server(cs.conn, cfg)
	if err := tc.Handshake(); err != nil {
		s.logf("nbd: %s START_TLS handshake failed: %v", cs.conn.RemoteAddr(), err)
		// Spec: failed handshake closes the connection.
		return false, fmt.Errorf("nbd: TLS handshake: %w", err)
	}
	// Swap in the encrypted connection and reset negotiation state:
	// spec requires the client to re-negotiate everything post-TLS.
	cs.conn = tc
	cs.br = bufio.NewReaderSize(tc, 64*1024)
	cs.bw = bufio.NewWriterSize(tc, 128*1024)
	cs.structured = false
	cs.metaCtxs = nil
	cs.metaExport = ""
	cs.tlsOn = true
	return true, nil
}

// computeBlockStatus validates an NBD_CMD_BLOCK_STATUS request and
// builds the BLOCK_STATUS chunk payload for the selected contexts. It
// is shared by the sequential and pipelined loops; on failure it
// returns a non-OK code with a message (replies MUST be structured).
func (s *Server) computeBlockStatus(cs *connState, req reqHdr) (payload []byte, code uint32, msg string) {
	// Spec: clients must have selected >=1 context for THIS export via
	// SET_META_CONTEXT, with structured replies, and no TLS upgrade in
	// between (START_TLS clears metaCtxs). Servers SHOULD reply EINVAL.
	if !cs.structured || len(cs.metaCtxs) == 0 {
		return nil, ErrEINVAL, "no metadata context selected"
	}
	if req.length == 0 {
		return nil, ErrEINVAL, "zero-length block status request"
	}
	if err := cs.checkRange(req.offset, req.length); err != nil {
		return nil, ErrEINVAL, err.Error()
	}
	sr, ok := statusReporter(cs.backend)
	if !ok {
		return nil, ErrEOPNOTSUPP, "backend provides no block status"
	}
	// The backends clamp the walk to the requested length; REQ_ONE
	// keeps only the first descriptor (spec: it MUST NOT exceed the
	// request length and only one descriptor is allowed).
	extents, err := sr.BlockStatus(int64(req.offset), req.length)
	if err != nil {
		s.logf("nbd: %s block status off=%d len=%d: %v", cs.conn.RemoteAddr(), req.offset, req.length, err)
		return nil, ErrEOPNOTSUPP, "block status failed"
	}
	if len(extents) == 0 {
		// A descriptor must have nonzero length; a walk that yields
		// nothing at a valid in-bounds offset is a backend bug, so
		// report conservative all-allocated.
		extents = []Extent{{Length: req.length, Flags: 0}}
	}
	if req.flags&cmdFlagReqOne != 0 {
		first := extents[0]
		if first.Length > req.length {
			first.Length = req.length
		}
		extents = []Extent{first}
	}
	// One chunk per selected context; we select at most one
	// (base:allocation, ID 1 -- see handleSetMetaContext).
	payload = make([]byte, 4+8*len(extents))
	binary.BigEndian.PutUint32(payload[:4], 1) // context ID
	for j, e := range extents {
		binary.BigEndian.PutUint32(payload[4+8*j:], e.Length)
		binary.BigEndian.PutUint32(payload[4+8*j+4:], e.Flags)
	}
	return payload, ErrOK, ""
}

// writeBlockStatusReply sends payload as a BLOCK_STATUS chunk followed
// by the structured DONE chunk, under the reply mutex with pipelined
// bookkeeping (mirrors writeStructuredReply).
func (cs *connState) writeBlockStatusReply(handle uint64, payload []byte, code uint32, msg string) error {
	cs.replyMu.Lock()
	defer cs.replyMu.Unlock()
	if cs.wErr != nil {
		return cs.wErr
	}
	err := func() error {
		if code != ErrOK {
			return writeStructuredErrorChunk(cs.bw, handle, code, msg)
		}
		var hdr [20]byte
		binary.BigEndian.PutUint32(hdr[:4], structuredReplyMagic)
		binary.BigEndian.PutUint16(hdr[4:6], 0) // more chunks follow
		binary.BigEndian.PutUint16(hdr[6:8], chunkTypeBlockStatus)
		binary.BigEndian.PutUint64(hdr[8:16], handle)
		binary.BigEndian.PutUint32(hdr[16:20], uint32(len(payload)))
		if _, err := cs.bw.Write(hdr[:]); err != nil {
			return err
		}
		if _, err := cs.bw.Write(payload); err != nil {
			return err
		}
		return writeStructuredDoneChunk(cs.bw, handle)
	}()
	cs.outstand--
	if cs.outstand <= 0 {
		cs.outstand = 0
		if ferr := cs.bw.Flush(); ferr != nil && err == nil {
			err = ferr
		}
	}
	if err != nil && cs.wErr == nil {
		cs.wErr = err
	}
	return err
}

// drain discards n payload bytes; used when replying to a command that
// carries data we do not consume (CACHE with a length, defensively).
func drain(r io.Reader, n int64) error {
	if n == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, n)
	return err
}
