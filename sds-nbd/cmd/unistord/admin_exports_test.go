package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/sds-nbd/nbd"
	"github.com/unistack-org/storage/pkg/meta"
)

// hubEnv extends the vacuum harness with a runtime export hub and a
// live (initially empty) NBD server, mirroring how main() boots cluster
// mode: hub first, admin second (the hub borrows the admin's cluster
// client), the NBD server bound late via setNBD.
type hubEnv struct {
	*vacuumEnv
	nbdAddr string
}

func startHubEnv(t *testing.T) *hubEnv {
	t.Helper()
	env := startVacuumEnv(t)
	hub := newExportHub(nil, log.New(io.Discard, "", 0),
		newVDUSEManager(log.New(io.Discard, "", 0), vduseFlags{}))
	hub.setCluster(env.admin.cluster())
	env.admin.exports = hub

	srv := nbd.NewServer(nil, nbd.ServerOptions{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close(); ln.Close() })
	hub.setNBD(srv)
	return &hubEnv{vacuumEnv: env, nbdAddr: ln.Addr().String()}
}

// TestAdminExportEndpoints walks the runtime export surface: add (201),
// idempotent re-add (200), the export readable over the wire, list,
// removal refused (409) while a client transmits and accepted once it
// is gone, double removal (404).
func TestAdminExportEndpoints(t *testing.T) {
	env := startHubEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if _, err := env.cl.CreateVolume(ctx, "vol-a", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	v, err := env.cl.OpenVolume(ctx, "vol-a")
	if err != nil {
		t.Fatal(err)
	}
	seed := make([]byte, 512)
	for i := range seed {
		seed[i] = byte(i ^ 0x5a)
	}
	if _, err := v.WriteAt(ctx, seed, 0); err != nil {
		t.Fatal(err)
	}
	v.Close()

	code, info := doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"vol-a"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST /v1/exports: %d %+v", code, info)
	}
	if info["name"] != "vol-a" || info["read_only"] != false {
		t.Fatalf("export info: %+v", info)
	}
	// Idempotent re-add answers 200 with the same registration.
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"vol-a"}`)
	if code != http.StatusOK {
		t.Fatalf("re-add: %d, want 200", code)
	}

	// The export is live on the NBD port: read the seeded bytes.
	cl := newExportNBDClient(t, env.nbdAddr)
	cl.goExportNBD(t, "vol-a")
	got := cl.readNBD(t, 0, 512)
	for i := range got {
		if got[i] != byte(i^0x5a) {
			t.Fatalf("nbd read mismatch at %d: %d", i, got[i])
		}
	}

	// List shows exactly this export.
	code, lst := doAdminJSON(t, h, http.MethodGet, "/v1/exports")
	if code != http.StatusOK {
		t.Fatalf("GET /v1/exports: %d", code)
	}
	nbds, _ := lst["nbd"].([]any)
	if len(nbds) != 1 {
		t.Fatalf("list: %+v", lst)
	}

	// Unknown volume -> 404; bad type -> 400.
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"ghost"}`)
	if code != http.StatusNotFound {
		t.Fatalf("add missing volume: %d, want 404", code)
	}
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"vol-a","type":"iscsi"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad type: %d, want 400", code)
	}

	// The connected client pins the export: removal answers 409.
	code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/exports/vol-a", "")
	if code != http.StatusConflict {
		t.Fatalf("remove while in use: %d, want 409", code)
	}
	cl.conn.Close()
	var last int
	for i := 0; i < 150; i++ {
		last, _ = doAdminBody(t, h, http.MethodDelete, "/v1/exports/vol-a", "")
		if last == http.StatusOK {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if last != http.StatusOK {
		t.Fatalf("remove after disconnect: %d, want 200", last)
	}
	code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/exports/vol-a", "")
	if code != http.StatusNotFound {
		t.Fatalf("remove twice: %d, want 404", code)
	}
}

// TestAdminExportReadOnlyPersisted: adding an export with read_only
// persists the flag on the volume record, so a daemon restart that
// rebuilds exports reproduces the access mode.
func TestAdminExportReadOnlyPersisted(t *testing.T) {
	env := startHubEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "ro-vol", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	code, _ := doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"ro-vol","read_only":true}`)
	if code != http.StatusCreated {
		t.Fatalf("add ro export: %d", code)
	}
	code, view := doAdminJSON(t, h, http.MethodGet, "/v1/volumes/ro-vol")
	if code != http.StatusOK {
		t.Fatalf("get volume: %d", code)
	}
	if view["read_only"] != true {
		t.Fatalf("read_only not persisted: %+v", view)
	}
	// The nbd registration carries the flag too.
	_, lst := doAdminJSON(t, h, http.MethodGet, "/v1/exports")
	nbds, _ := lst["nbd"].([]any)
	if len(nbds) != 1 {
		t.Fatalf("list: %+v", lst)
	}
	if e, _ := nbds[0].(map[string]any); e["read_only"] != true {
		t.Fatalf("export info ro flag: %+v", nbds[0])
	}
}

// TestAdminVDUSEUnsupported: input validation runs before the platform
// gate; on non-Linux the manager answers 501.
func TestAdminVDUSEUnsupported(t *testing.T) {
	env := startHubEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "vd-vol", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	code, _ := doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"vd-vol","type":"vduse"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("vduse without devname: %d, want 400", code)
	}
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"vd-vol","type":"vduse","devname":"bad/name"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad devname: %d, want 400", code)
	}
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"ghost","type":"vduse","devname":"ok"}`)
	if code != http.StatusNotFound {
		t.Fatalf("vduse unknown volume: %d, want 404", code)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/exports",
		strings.NewReader(`{"volume":"vd-vol","type":"vduse","devname":"sdsv-test"}`)))
	code = rec.Code
	if code == http.StatusNotImplemented {
		if !strings.Contains(rec.Body.String(), "Linux") {
			t.Fatalf("501 body: %q", rec.Body.String())
		}
	} else if code != http.StatusCreated && code != http.StatusOK {
		t.Fatalf("vduse add: %d %s", code, rec.Body.String())
	}
	// Removal routes to the manager as well (501 here).
	code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/exports/sdsv-test?type=vduse", "")
	if code != http.StatusNotImplemented && code != http.StatusOK {
		t.Fatalf("vduse remove: %d", code)
	}
}

// TestAdminBearerAuth: with a token configured every endpoint except
// /healthz demands the exact bearer header.
func TestAdminBearerAuth(t *testing.T) {
	dir := t.TempDir()
	ms, err := meta.OpenDir(dir + "/meta")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ms.Close() })
	a, err := newAdminServer(ms, "127.0.0.1:0", log.New(io.Discard, "", 0), nil, newDiskRegistry(), nil, nil, "s3cr3t", nil)
	if err != nil {
		t.Fatal(err)
	}
	h := a.srv.Handler

	req := httptest.NewRequest(http.MethodGet, "/v1/volumes", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d, want 401", rec.Code)
	}
	req.Header.Set("Authorization", "Bearer wr0ng")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad token: %d, want 401", rec.Code)
	}
	req.Header.Set("Authorization", "Bearer s3cr3t")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("good token: %d, want 200", rec.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz unauthenticated: %d, want 200", rec.Code)
	}
}

// TestAdminImportJobAsync covers the async URL ingest: 202 + job id,
// the bytes land in the volume, a matching sha256 completes the job, a
// mismatched sha256 fails it, sync mode answers inline.
func TestAdminImportJobAsync(t *testing.T) {
	env := startHubEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "img", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	sum := sha256.Sum256(payload)
	src := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer src.Close()

	code, resp := doAdminBody(t, h, http.MethodPost, "/v1/volumes/img/import",
		fmt.Sprintf(`{"url":%q,"sha256":%q}`, src.URL, hex.EncodeToString(sum[:])))
	if code != http.StatusAccepted {
		t.Fatalf("POST import: %d %+v", code, resp)
	}
	jobID, _ := resp["job_id"].(string)
	if jobID == "" {
		t.Fatalf("no job_id: %+v", resp)
	}
	job := waitJob(t, h, jobID, 30*time.Second)
	if job["state"] != "completed" || job["written"] != float64(len(payload)) {
		t.Fatalf("job: %+v", job)
	}
	v, err := env.cl.OpenVolume(ctx, "img")
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	got := make([]byte, len(payload))
	if _, err := v.ReadAt(ctx, got, 0); err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("imported data mismatch at %d", i)
		}
	}

	// A wrong checksum must fail the job (bytes stay written: the
	// ingest is idempotent per offset, the caller re-imports).
	bad := sha256.Sum256([]byte("not the payload"))
	code, resp = doAdminBody(t, h, http.MethodPost, "/v1/volumes/img/import",
		fmt.Sprintf(`{"url":%q,"sha256":%q}`, src.URL, hex.EncodeToString(bad[:])))
	if code != http.StatusAccepted {
		t.Fatalf("bad-checksum import: %d", code)
	}
	job = waitJob(t, h, resp["job_id"].(string), 30*time.Second)
	if job["state"] != "failed" || !strings.Contains(fmt.Sprint(job["error"]), "sha256 mismatch") {
		t.Fatalf("bad-checksum job: %+v", job)
	}

	// Synchronous mode answers inline with the byte count.
	off := uint64(2 << 20)
	code, resp = doAdminBody(t, h, http.MethodPost, "/v1/volumes/img/import",
		fmt.Sprintf(`{"url":%q,"offset":%d,"async":false}`, src.URL, off))
	if code != http.StatusOK || resp["written"] != float64(len(payload)) {
		t.Fatalf("sync import: %d %+v", code, resp)
	}
	head := make([]byte, 4096)
	if _, err := v.ReadAt(ctx, head, off); err != nil {
		t.Fatal(err)
	}
	for i := range head {
		if head[i] != payload[i] {
			t.Fatalf("sync import data mismatch at %d", i)
		}
	}

	// Import into an unknown volume -> 404; unknown job -> 404; the
	// url scheme is validated before the volume probe.
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/ghost/import", fmt.Sprintf(`{"url":%q}`, src.URL))
	if code != http.StatusNotFound {
		t.Fatalf("import ghost: %d, want 404", code)
	}
	code, _ = doAdminJSON(t, h, http.MethodGet, "/v1/jobs/nope")
	if code != http.StatusNotFound {
		t.Fatalf("job ghost: %d, want 404", code)
	}
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/img/import", `{"url":"ftp://x/y"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("ftp url: %d, want 400", code)
	}
}

func waitJob(t *testing.T, h http.Handler, id string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		code, job := doAdminJSON(t, h, http.MethodGet, "/v1/jobs/"+id)
		if code != http.StatusOK {
			t.Fatalf("GET job: %d", code)
		}
		if job["state"] != "running" {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %s did not finish in %s", id, timeout)
	return nil
}

// ---------- minimal NBD client for the hub tests ----------

// exportNBDClient speaks just enough fixed-newstyle NBD (OPT_GO +
// simple replies) to prove a runtime export is serving on the wire.
type exportNBDClient struct{ conn net.Conn }

func newExportNBDClient(t *testing.T, addr string) *exportNBDClient {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	cl := &exportNBDClient{conn: c}
	var b [18]byte
	if _, err := io.ReadFull(c, b[:]); err != nil {
		t.Fatalf("hello: %v", err)
	}
	var cf [4]byte
	binary.BigEndian.PutUint32(cf[:], 1) // NBD_FLAG_C_FIXED_NEWSTYLE
	if _, err := c.Write(cf[:]); err != nil {
		t.Fatal(err)
	}
	return cl
}

func (cl *exportNBDClient) goExportNBD(t *testing.T, name string) {
	t.Helper()
	payload := make([]byte, 4+len(name)+2)
	binary.BigEndian.PutUint32(payload[:4], uint32(len(name)))
	copy(payload[4:], name)
	hdr := make([]byte, 16)
	binary.BigEndian.PutUint64(hdr[:8], 0x49484156454f5054) // IHAVEOPT
	binary.BigEndian.PutUint32(hdr[8:12], 7)                // NBD_OPT_GO
	binary.BigEndian.PutUint32(hdr[12:16], uint32(len(payload)))
	if _, err := cl.conn.Write(append(hdr, payload...)); err != nil {
		t.Fatal(err)
	}
	// Option replies: 20-byte header (u64 rep magic, u32 option, u32
	// type, u32 length); the export is live once the ACK arrives.
	for {
		var rh [20]byte
		if _, err := io.ReadFull(cl.conn, rh[:]); err != nil {
			t.Fatalf("opt reply header: %v", err)
		}
		if m := binary.BigEndian.Uint64(rh[:8]); m != 0x3e889045565a9 {
			t.Fatalf("bad rep magic 0x%x", m)
		}
		typ := binary.BigEndian.Uint32(rh[12:16])
		body := make([]byte, binary.BigEndian.Uint32(rh[16:20]))
		if len(body) > 0 {
			if _, err := io.ReadFull(cl.conn, body); err != nil {
				t.Fatal(err)
			}
		}
		if typ == 1 { // NBD_REP_ACK -> transmission
			return
		}
		if typ&0x80000000 != 0 {
			t.Fatalf("export refused: type=0x%x msg=%q", typ, body)
		}
	}
}

func (cl *exportNBDClient) readNBD(t *testing.T, off uint64, length uint32) []byte {
	t.Helper()
	req := make([]byte, 28)
	binary.BigEndian.PutUint32(req[0:], 0x25609513) // request magic
	binary.BigEndian.PutUint16(req[6:], 0)          // READ
	binary.BigEndian.PutUint64(req[8:], 1)          // handle
	binary.BigEndian.PutUint64(req[16:], off)
	binary.BigEndian.PutUint32(req[24:], length)
	if _, err := cl.conn.Write(req); err != nil {
		t.Fatal(err)
	}
	var rh [16]byte
	if _, err := io.ReadFull(cl.conn, rh[:]); err != nil {
		t.Fatal(err)
	}
	if m := binary.BigEndian.Uint32(rh[:4]); m != 0x67446698 {
		t.Fatalf("bad reply magic 0x%x", m)
	}
	if e := binary.BigEndian.Uint32(rh[4:8]); e != 0 {
		t.Fatalf("read error %d", e)
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(cl.conn, data); err != nil {
		t.Fatal(err)
	}
	return data
}
