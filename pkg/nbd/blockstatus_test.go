package nbd

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// fakeStatusBackend is a RAM backend with a programmed allocation map
// (region-granular) so block-status tests are deterministic on every
// platform (darwin cannot probe real holes reliably). Unlike memBackend
// (which discards writes) it stores data, so write-then-read works.
type fakeStatusBackend struct {
	size      uint64
	region    uint64 // bytes per region
	allocated []bool

	mu    sync.Mutex
	pages map[uint64][]byte // 4096-byte pages, lazily allocated
}

const fakeStatusPage = 4096

func (f *fakeStatusBackend) Size() uint64 { return f.size }

func (f *fakeStatusBackend) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(f.size) {
		return 0, io.EOF
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for done := 0; done < len(p); {
		o := uint64(off) + uint64(done)
		page := o / fakeStatusPage
		data := f.pages[page]
		if data == nil {
			data = make([]byte, fakeStatusPage)
			f.pages[page] = data
		}
		done += copy(p[done:], data[o%fakeStatusPage:])
	}
	return len(p), nil
}

func (f *fakeStatusBackend) WriteAt(p []byte, off int64) (int, error) {
	if off >= int64(f.size) {
		return 0, io.EOF
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for done := 0; done < len(p); {
		o := uint64(off) + uint64(done)
		page := o / fakeStatusPage
		data := f.pages[page]
		if data == nil {
			data = make([]byte, fakeStatusPage)
			f.pages[page] = data
		}
		done += copy(data[o%fakeStatusPage:], p[done:])
	}
	return len(p), nil
}

func (f *fakeStatusBackend) Flush() error                          { return nil }
func (f *fakeStatusBackend) Trim(int64, uint32) error              { return nil }
func (f *fakeStatusBackend) WriteZeroes(int64, uint32, bool) error { return nil }
func (f *fakeStatusBackend) Close() error                          { return nil }

func (f *fakeStatusBackend) BlockStatus(off int64, length uint32) ([]Extent, error) {
	var ext []Extent
	remain := int64(length)
	pos := off
	for remain > 0 {
		r := pos / int64(f.region)
		alloc := false
		if r < int64(len(f.allocated)) {
			alloc = f.allocated[r]
		}
		// Extend over a run of identical (hole) regions.
		run := int64(f.region) - pos%int64(f.region)
		for !alloc && r+run/int64(f.region) < int64(len(f.allocated)) && !f.allocated[r+run/int64(f.region)] {
			run += int64(f.region)
		}
		if run > remain {
			run = remain
		}
		var flags uint32
		if !alloc {
			flags = nbdStateHole | nbdStateZero
		}
		ext = append(ext, Extent{Length: uint32(run), Flags: flags})
		pos += run
		remain -= run
	}
	return ext, nil
}

func newFakeStatus(size, region uint64) *fakeStatusBackend {
	return &fakeStatusBackend{size: size, region: region, allocated: make([]bool, size/region), pages: map[uint64][]byte{}}
}

// statusServer starts a server with exports: "vol0" (fake status
// backend, 256MiB / 4MiB regions), "plain" (mem backend, no status).
func statusServer(t testing.TB, opts ServerOptions) (string, *fakeStatusBackend) {
	t.Helper()
	fb := newFakeStatus(256<<20, 4<<20)
	plain := &memBackend{size: 64 << 20}
	srv := NewServer([]*Export{
		{Name: "vol0", Backend: fb},
		{Name: "plain", Backend: plain},
	}, opts)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close(); ln.Close() })
	return ln.Addr().String(), fb
}

// metaReq builds a LIST/SET_META_CONTEXT payload.
func metaReq(export string, queries ...string) []byte {
	b := make([]byte, 4+len(export)+4)
	binary.BigEndian.PutUint32(b[:4], uint32(len(export)))
	copy(b[4:], export)
	binary.BigEndian.PutUint32(b[4+len(export):], uint32(len(queries)))
	for _, q := range queries {
		hdr := make([]byte, 4)
		binary.BigEndian.PutUint32(hdr, uint32(len(q)))
		b = append(b, hdr...)
		b = append(b, q...)
	}
	return b
}

// parseMetaReplies returns context names from REP_META_CONTEXT replies
// and the final reply type.
func parseMetaReplies(t testing.TB, replies []optionReply) (names []string, final uint32) {
	t.Helper()
	final = replies[len(replies)-1].typ
	for _, r := range replies[:len(replies)-1] {
		if r.typ != repMetaContext {
			t.Fatalf("unexpected meta reply type 0x%x", r.typ)
		}
		if len(r.data) < 4 {
			t.Fatalf("meta reply too short")
		}
		names = append(names, string(r.data[4:]))
	}
	return names, final
}

// goExportReused enters transmission on a connection whose structured
// replies option was already negotiated (OPT_GO with a zero-length
// info-request list).
func (cl *client) goExportReused(t testing.TB, name string) {
	t.Helper()
	data := make([]byte, 4+len(name)+2)
	binary.BigEndian.PutUint32(data[:4], uint32(len(name)))
	copy(data[4:], name)
	binary.BigEndian.PutUint16(data[4+len(name):], 0)
	replies := cl.option(t, optGo, data)
	cl.applyExportReplies(t, replies)
}

// readBlockStatus parses the structured reply to a BLOCK_STATUS:
// REP_BLOCK_STATUS chunks (u32 ctx id + descriptors) until DONE, plus
// any structured error code.
func (cl *client) readBlockStatus(t testing.TB, handle uint64) (descs []Extent, errCode uint32) {
	t.Helper()
	for {
		var hdr [20]byte
		if err := cl.br.ReadFull(hdr[:]); err != nil {
			t.Fatalf("block status reply: %v", err)
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
		case typ == chunkTypeBlockStatus:
			if len(payload) < 4 || (len(payload)-4)%8 != 0 {
				t.Fatalf("block status chunk len %d", len(payload))
			}
			if id := binary.BigEndian.Uint32(payload[:4]); id != 1 {
				t.Fatalf("context id %d", id)
			}
			for i := 4; i+8 <= len(payload); i += 8 {
				descs = append(descs, Extent{Length: binary.BigEndian.Uint32(payload[i:]), Flags: binary.BigEndian.Uint32(payload[i+4:])})
			}
		case typ == chunkTypeNone:
		case typ&chunkErrorFlag != 0:
			if len(payload) >= 4 {
				errCode = binary.BigEndian.Uint32(payload[:4])
			}
		default:
			t.Fatalf("unexpected chunk type %d", typ)
		}
		if flags&chunkFlagDone != 0 {
			return descs, errCode
		}
	}
}

func TestMetaContextListSet(t *testing.T) {
	addr, fb := statusServer(t, ServerOptions{})
	cl := newClient(t, addr)

	// LIST on an export with a StatusReporter advertises base:allocation.
	names, final := parseMetaReplies(t, cl.option(t, optListMetaContext, metaReq("vol0", "base:")))
	if final != repACK || len(names) != 1 || names[0] != metaBaseAllocation {
		t.Fatalf("LIST base: = %v final=%d", names, final)
	}
	// Zero queries = all contexts (wildcard).
	names, final = parseMetaReplies(t, cl.option(t, optListMetaContext, metaReq("vol0")))
	if final != repACK || len(names) != 1 {
		t.Fatalf("LIST zero-queries = %v final=%d", names, final)
	}
	// Unknown namespaces are ignored (spec), known ones answered.
	names, final = parseMetaReplies(t, cl.option(t, optListMetaContext, metaReq("vol0", "x-foo:bar", "base:allocation")))
	if final != repACK || len(names) != 1 || names[0] != metaBaseAllocation {
		t.Fatalf("LIST mixed = %v final=%d", names, final)
	}
	// Export without a reporter: no contexts.
	names, final = parseMetaReplies(t, cl.option(t, optListMetaContext, metaReq("plain", "base:")))
	if final != repACK || len(names) != 0 {
		t.Fatalf("LIST plain = %v final=%d", names, final)
	}
	// Unknown export: ACK with nothing (spec: ignore, do not error).
	names, final = parseMetaReplies(t, cl.option(t, optListMetaContext, metaReq("nope", "base:")))
	if final != repACK || len(names) != 0 {
		t.Fatalf("LIST nope = %v final=%d", names, final)
	}

	// SET before structured replies must be refused.
	replies := cl.option(t, optSetMetaContext, metaReq("vol0", "base:allocation"))
	if replies[len(replies)-1].typ != repErrInvalid {
		t.Fatalf("SET before structured: 0x%x", replies[len(replies)-1].typ)
	}

	cl.requestStructured(t)
	names, final = parseMetaReplies(t, cl.option(t, optSetMetaContext, metaReq("vol0", "base:allocation")))
	if final != repACK || len(names) != 1 || names[0] != metaBaseAllocation {
		t.Fatalf("SET = %v final=%d", names, final)
	}
	// Wildcard "base:" MUST be ignored during SET.
	names, final = parseMetaReplies(t, cl.option(t, optSetMetaContext, metaReq("vol0", "base:")))
	if final != repACK || len(names) != 0 {
		t.Fatalf("SET wildcard = %v final=%d", names, final)
	}
	// Re-select base:allocation (the wildcard SET cleared the selection).
	if _, final := parseMetaReplies(t, cl.option(t, optSetMetaContext, metaReq("vol0", "base:allocation"))); final != repACK {
		t.Fatal("re-SET not acked")
	}
	cl.goExportReused(t, "vol0")

	// BLOCK_STATUS: region 0 hole, region 1 allocated.
	fb.allocated[1] = true
	h := cl.request(t, CmdBlockStatus, 0, 0, 8<<20, nil)
	descs, errCode := cl.readBlockStatus(t, h)
	if errCode != 0 {
		t.Fatalf("block status err=%d", errCode)
	}
	if len(descs) != 2 || descs[0].Flags != nbdStateHole|nbdStateZero || descs[0].Length != 4<<20 ||
		descs[1].Flags != 0 || descs[1].Length != 4<<20 {
		t.Fatalf("extents = %+v", descs)
	}

	// REQ_ONE: exactly one descriptor, clamped to the request length.
	h = cl.request(t, CmdBlockStatus, cmdFlagReqOne, 0, 1<<20, nil)
	descs, errCode = cl.readBlockStatus(t, h)
	if errCode != 0 || len(descs) != 1 || descs[0].Length > 1<<20 || descs[0].Flags != nbdStateHole|nbdStateZero {
		t.Fatalf("REQ_ONE = %+v err=%d", descs, errCode)
	}
	// REQ_ONE over the allocated region, request crossing the boundary.
	h = cl.request(t, CmdBlockStatus, cmdFlagReqOne, 4<<20, 8<<20, nil)
	descs, errCode = cl.readBlockStatus(t, h)
	if errCode != 0 || len(descs) != 1 || descs[0].Length > 8<<20 || descs[0].Flags != 0 {
		t.Fatalf("REQ_ONE alloc = %+v err=%d", descs, errCode)
	}

	// Range past EOF: EINVAL.
	h = cl.request(t, CmdBlockStatus, 0, 256<<20, 1<<20, nil)
	_, errCode = cl.readBlockStatus(t, h)
	if errCode != ErrEINVAL {
		t.Fatalf("past EOF err=%d", errCode)
	}
}

func TestBlockStatusRefusals(t *testing.T) {
	addr, _ := statusServer(t, ServerOptions{})
	// Structured, GO, but NO SET_META_CONTEXT: EINVAL.
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	h := cl.request(t, CmdBlockStatus, 0, 0, 1<<20, nil)
	_, code := cl.readBlockStatus(t, h)
	if code != ErrEINVAL {
		t.Fatalf("no-context code=%d", code)
	}
}

func TestBlockStatusContextScope(t *testing.T) {
	addr, _ := statusServer(t, ServerOptions{})
	cl := newClient(t, addr)
	cl.requestStructured(t)
	if _, final := parseMetaReplies(t, cl.option(t, optSetMetaContext, metaReq("vol0", "base:allocation"))); final != repACK {
		t.Fatal("SET not acked")
	}
	// Contexts selected for "vol0" must not authorise BLOCK_STATUS on
	// the "plain" export (no reporter / selection reset on export
	// change).
	cl.goExportReused(t, "plain")
	h := cl.request(t, CmdBlockStatus, 0, 0, 1<<20, nil)
	_, code := cl.readBlockStatus(t, h)
	if code != ErrEINVAL {
		t.Fatalf("export-switch code=%d", code)
	}
}

func TestBlockStatusFileBackend(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "vol0.img")
	b, err := NewFileBackend(file, 64<<20)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	srv := NewServer([]*Export{{Name: "vol0", Backend: b}}, ServerOptions{EnableTrim: true})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { srv.Close(); ln.Close() })

	cl := newClient(t, ln.Addr().String())
	cl.goExport(t, "vol0")
	if cl.flags&transmissionSendCache == 0 {
		t.Fatal("SEND_CACHE not advertised")
	}
	// Fresh sparse file: Linux reports a hole; other platforms have no
	// reliable hole probing and must report conservatively allocated.
	ext, err := b.BlockStatus(0, 64<<20)
	if err != nil {
		t.Fatalf("BlockStatus: %v", err)
	}
	if len(ext) == 0 {
		t.Fatal("no extents")
	}
	var sum uint64
	for _, e := range ext {
		sum += uint64(e.Length)
	}
	if sum != 64<<20 {
		t.Fatalf("extents sum %d", sum)
	}
	if runtime.GOOS == "linux" {
		if ext[0].Flags&(nbdStateHole|nbdStateZero) != nbdStateHole|nbdStateZero {
			t.Fatalf("fresh sparse file: ext[0]=%+v want HOLE|ZERO", ext[0])
		}
	}
	// After a write the region is allocated everywhere.
	if n, err := b.WriteAt(make([]byte, 4096), 0); err != nil || n != 4096 {
		t.Fatalf("write: %v", err)
	}
	ext, err = b.BlockStatus(0, 4096)
	if err != nil || len(ext) == 0 || ext[0].Flags != 0 {
		t.Fatalf("after write: ext=%+v err=%v", ext, err)
	}
	// Protocol round trip: LIST then SET, GO on a fresh client.
	cl2 := newClient(t, ln.Addr().String())
	cl2.requestStructured(t)
	if _, final := parseMetaReplies(t, cl2.option(t, optListMetaContext, metaReq("vol0", "base:allocation"))); final != repACK {
		t.Fatal("LIST not acked")
	}
	if _, final := parseMetaReplies(t, cl2.option(t, optSetMetaContext, metaReq("vol0", "base:allocation"))); final != repACK {
		t.Fatal("SET not acked")
	}
	cl2.goExportReused(t, "vol0")
	h := cl2.request(t, CmdBlockStatus, 0, 0, 4096, nil)
	descs, code := cl2.readBlockStatus(t, h)
	if code != 0 || len(descs) != 1 || descs[0].Flags != 0 || descs[0].Length != 4096 {
		t.Fatalf("file backend block status: %+v code=%d", descs, code)
	}
}

func TestCacheNoop(t *testing.T) {
	addr, _ := statusServer(t, ServerOptions{})
	// Structured mode.
	cl := newClient(t, addr)
	cl.goExport(t, "vol0")
	if cl.flags&transmissionSendCache == 0 {
		t.Fatal("SEND_CACHE not advertised (structured)")
	}
	// CACHE carries a payload (like WRITE) that the server must drain.
	h := cl.request(t, CmdCache, 0, 0, 1<<20, make([]byte, 1<<20))
	if r := cl.readCommandReply(t, h); r.err != 0 {
		t.Fatalf("CACHE structured err=%d", r.err)
	}
	// Export stays usable after CACHE.
	if _, code := cl.read(t, 0, 4096); code != 0 {
		t.Fatalf("read after CACHE: %d", code)
	}
	cl.disconnect(t)

	// Legacy (simple replies) mode.
	cl2 := newClient(t, addr)
	var hdr [16]byte
	binary.BigEndian.PutUint64(hdr[:8], ihaveoptMagic)
	binary.BigEndian.PutUint32(hdr[8:12], optExportName)
	binary.BigEndian.PutUint32(hdr[12:16], 4)
	if _, err := cl2.conn.Write(append(hdr[:], []byte("vol0")...)); err != nil {
		t.Fatalf("write: %v", err)
	}
	cl2.readExportNameLegacyResponse(t, false)
	if cl2.flags&transmissionSendCache == 0 {
		t.Fatal("SEND_CACHE not advertised (simple)")
	}
	h = cl2.request(t, CmdCache, 0, 0, 1<<20, make([]byte, 1<<20))
	if r := cl2.readCommandReply(t, h); r.err != 0 {
		t.Fatalf("CACHE simple err=%d", r.err)
	}
	if _, code := cl2.read(t, 0, 4096); code != 0 {
		t.Fatalf("read after CACHE (simple): %d", code)
	}
	cl2.disconnect(t)
}

func TestPeekExport(t *testing.T) {
	addr, _ := statusServer(t, ServerOptions{})
	cl := newClient(t, addr)
	// PEEK_EXPORT takes the raw export name; replies INFO + ACK.
	replies := cl.option(t, optPeekExport, []byte("vol0"))
	var info *optionReply
	for i := range replies {
		if replies[i].typ == repInfo {
			info = &replies[i]
		}
	}
	if info == nil {
		t.Fatalf("no INFO reply: %+v", replies)
	}
	if ityp := binary.BigEndian.Uint16(info.data[:2]); ityp != infoExport {
		t.Fatalf("info type %d", ityp)
	}
	if got := binary.BigEndian.Uint64(info.data[2:10]); got != 256<<20 {
		t.Fatalf("peek size %d", got)
	}
	if replies[len(replies)-1].typ != repACK {
		t.Fatal("peek not acked")
	}
	// Unknown export: error reply, connection stays in options mode.
	replies = cl.option(t, optPeekExport, []byte("nope"))
	if replies[len(replies)-1].typ&0x80000000 == 0 {
		t.Fatalf("peek nope: 0x%x", replies[len(replies)-1].typ)
	}
	cl.goExport(t, "vol0")
}

func TestStartTLSPolicy(t *testing.T) {
	addr, _ := statusServer(t, ServerOptions{}) // no TLS config
	cl := newClient(t, addr)
	replies := cl.option(t, optStartTLS, nil)
	if replies[len(replies)-1].typ != repErrPolicy {
		t.Fatalf("expected ERR_POLICY, got %+v", replies)
	}
	// Connection remains usable for plain negotiation after refusal.
	cl.goExport(t, "vol0")
	if code := cl.write(t, 0, []byte("plain")); code != 0 {
		t.Fatalf("write: %d", code)
	}
}

func TestStartTLSHandshake(t *testing.T) {
	scfg, ccfg := tlsMaterial(t)
	addr, fb := statusServer(t, ServerOptions{TLSServerConfig: scfg})
	cl := newClient(t, addr)
	replies := cl.option(t, optStartTLS, nil)
	if replies[len(replies)-1].typ != repACK {
		t.Fatalf("expected ACK, got %+v", replies)
	}
	ccfg.ServerName = "127.0.0.1"
	tc := tls.Client(cl.conn, ccfg)
	if err := tc.Handshake(); err != nil {
		t.Fatalf("client TLS handshake: %v", err)
	}
	// Post-TLS: full renegotiation on the encrypted connection.
	tlsCl := &client{conn: tc, br: &bufioReader{c: tc}, pending: map[uint64]uint32{}}
	tlsCl.handshake(t)
	// A second START_TLS (options phase) is refused and the connection
	// keeps working.
	replies = tlsCl.option(t, optStartTLS, nil)
	if replies[len(replies)-1].typ&0x80000000 == 0 {
		t.Fatalf("second START_TLS: 0x%x", replies[len(replies)-1].typ)
	}
	tlsCl.requestStructured(t)
	// Meta state was reset by START_TLS: SET must be re-sent to work.
	if _, final := parseMetaReplies(t, tlsCl.option(t, optSetMetaContext, metaReq("vol0", "base:allocation"))); final != repACK {
		t.Fatal("post-TLS SET not acked")
	}
	tlsCl.goExportReused(t, "vol0")
	// I/O works over TLS.
	if code := tlsCl.write(t, 0, []byte("hello")); code != 0 {
		t.Fatalf("write over TLS: %d", code)
	}
	data, code := tlsCl.read(t, 0, 5)
	if code != 0 || string(data) != "hello" {
		t.Fatalf("read over TLS: %q %d", data, code)
	}
	// Block status over TLS.
	fb.allocated[0] = true
	h := tlsCl.request(t, CmdBlockStatus, 0, 0, 4<<20, nil)
	descs, code := tlsCl.readBlockStatus(t, h)
	if code != 0 || len(descs) != 1 || descs[0].Length != 4<<20 || descs[0].Flags != 0 {
		t.Fatalf("block status over TLS: %+v %d", descs, code)
	}
	if _, code := tlsCl.read(t, 0, 5); code != 0 {
		t.Fatalf("read after TLS: %d", code)
	}
}

func TestStartTLSRejectsData(t *testing.T) {
	scfg, _ := tlsMaterial(t)
	addr, _ := statusServer(t, ServerOptions{TLSServerConfig: scfg})
	cl := newClient(t, addr)
	// START_TLS takes no data; sending any is a protocol violation.
	replies := cl.option(t, optStartTLS, []byte("x"))
	if replies[len(replies)-1].typ != repErrInvalid {
		t.Fatalf("expected ERR_INVALID, got %+v", replies)
	}
}

// tlsMaterial writes a CA + server/client leafs (127.0.0.1) to a temp
// dir and returns mutual-TLS configs for both sides (same scheme as
// pkg/tlsconf's test fixture).
func tlsMaterial(t testing.TB) (server, clientCfg *tls.Config) {
	t.Helper()
	dir := t.TempDir()
	writePEM := func(path, typ string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	newCA := func(name string) (*x509.Certificate, *ecdsa.PrivateKey, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			BasicConstraintsValid: true,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name+"-ca.pem")
		writePEM(path, "CERTIFICATE", der)
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		return cert, key, path
	}
	leaf := func(ca *x509.Certificate, caKey *ecdsa.PrivateKey, name string) (string, string) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kder, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPath, keyPath := filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
		writePEM(certPath, "CERTIFICATE", der)
		writePEM(keyPath, "EC PRIVATE KEY", kder)
		return certPath, keyPath
	}
	ca, caKey, caPath := newCA("nbd-test-ca")
	sCert, sKey := leaf(ca, caKey, "nbd-server")
	cCert, cKey := leaf(ca, caKey, "nbd-client")
	loadPool := func(path string) *x509.CertPool {
		p := x509.NewCertPool()
		pemData, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !p.AppendCertsFromPEM(pemData) {
			t.Fatal("append ca")
		}
		return p
	}
	s := &tls.Config{MinVersion: tls.VersionTLS12}
	sc, err := tls.LoadX509KeyPair(sCert, sKey)
	if err != nil {
		t.Fatal(err)
	}
	s.Certificates = []tls.Certificate{sc}
	s.ClientCAs = loadPool(caPath)
	s.ClientAuth = tls.RequireAndVerifyClientCert
	c := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: loadPool(caPath)}
	cc, err := tls.LoadX509KeyPair(cCert, cKey)
	if err != nil {
		t.Fatal(err)
	}
	c.Certificates = []tls.Certificate{cc}
	return s, c
}
