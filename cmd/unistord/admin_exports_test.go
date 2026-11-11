package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
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
		newVDUSEManager(log.New(io.Discard, "", 0), vduseFlags{}),
		newVUBManager(log.New(io.Discard, "", 0), vubFlags{}))
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

// TestAdminExportTypeAlias: the old shorthand "vub" is accepted on create
// and delete, while every reply and the list key use the canonical name
// "vhost-user-blk".
func TestAdminExportTypeAlias(t *testing.T) {
	dir := t.TempDir()
	runtimeVUBSocketDir = dir
	t.Cleanup(func() { runtimeVUBSocketDir = "/var/run/unistord" })

	env := startHubEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "alias-vol", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}

	code, info := doAdminBody(t, h, http.MethodPost, "/v1/exports", `{"volume":"alias-vol","type":"vub"}`)
	if code != http.StatusCreated {
		t.Fatalf("POST type=vub: %d %+v", code, info)
	}
	if info["type"] != "vhost-user-blk" || info["name"] != "alias-vol" {
		t.Fatalf("alias reply must carry the canonical type: %+v", info)
	}
	if info["socket"] != filepath.Join(dir, "alias-vol.sock") {
		t.Fatalf("socket: %+v", info)
	}
	_, lst := doAdminJSON(t, h, http.MethodGet, "/v1/exports")
	vubs, _ := lst["vhost-user-blk"].([]any)
	if len(vubs) != 1 {
		t.Fatalf("list: %+v", lst)
	}

	code, _ = doAdminBody(t, h, http.MethodDelete, "/v1/exports/alias-vol?type=vub", "")
	if code != http.StatusOK {
		t.Fatalf("DELETE type=vub: %d, want 200", code)
	}
	_, lst = doAdminJSON(t, h, http.MethodGet, "/v1/exports")
	vubs, _ = lst["vhost-user-blk"].([]any)
	if len(vubs) != 0 {
		t.Fatalf("list after alias delete: %+v", lst)
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

// ---------- checksum files ----------

// TestImportChecksumFile drives the sha256_url form of the import: the
// caller names a checksum file next to the image, the daemon fetches
// and parses it before the first byte is written, and the digest then
// verifies the transfer. A digest that cannot be resolved is a hard
// failure answered on the request — an import that cannot be verified
// must never be accepted.
func TestImportChecksumFile(t *testing.T) {
	env := startHubEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "chk", 8<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	sum := sha256.Sum256(payload)
	hexsum := hex.EncodeToString(sum[:])
	osum := sha256.Sum256([]byte("other image"))
	other := hex.EncodeToString(osum[:])
	msum := md5.Sum(payload)
	md5sum := hex.EncodeToString(msum[:])
	gsum := sha512.Sum512(payload)
	sha512sum := hex.EncodeToString(gsum[:])

	mux := http.NewServeMux()
	mux.HandleFunc("/img.iso", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(payload) })
	serve := func(body string) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }
	}
	// The formats publishers ship, all naming the digest of the image.
	mux.HandleFunc("/sums", serve(hexsum+"  img.iso\n"))
	mux.HandleFunc("/binary", serve(hexsum+" *img.iso\r\n"))
	mux.HandleFunc("/bare", serve(hexsum+"\n"))
	mux.HandleFunc("/prefixed", serve("sha256:"+hexsum+"\n"))
	mux.HandleFunc("/gostyle", serve("SHA256(img.iso)= "+hexsum+"\n"))
	mux.HandleFunc("/named-multi", serve(other+"  a.iso\n"+hexsum+"  img.iso\n"))
	mux.HandleFunc("/md5sums", serve(md5sum+"  img.iso\n"))
	mux.HandleFunc("/sha512sums", serve(sha512sum+"  img.iso\n"))
	// Unusable listings: the digest is not the image's, is not the
	// image's line, is ambiguous, or is not a digest at all.
	mux.HandleFunc("/wrong-file", serve(hexsum+"  other.iso\n"))
	mux.HandleFunc("/bare-multi", serve(hexsum+"\n"+other+"\n"))
	mux.HandleFunc("/conflict", serve(hexsum+"  img.iso\n"+other+"  img.iso\n"))
	mux.HandleFunc("/garbage", serve("see the project page for the digest\n"))
	mux.HandleFunc("/huge", func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte{'a'}, checksumMaxBytes+1))
	})
	src := httptest.NewServer(mux)
	defer src.Close()

	for _, tc := range []struct{ body, path, want string }{
		{"sha256_url", "/sums", ""}, {"sha256_url", "/binary", ""}, {"sha256_url", "/bare", ""},
		{"sha256_url", "/prefixed", ""}, {"sha256_url", "/gostyle", ""}, {"sha256_url", "/named-multi", ""},
		{"sha256_url", "/wrong-file", `no line for "img.iso"`},
		{"sha256_url", "/bare-multi", "several digests"},
		{"sha256_url", "/conflict", "several digests"},
		{"sha256_url", "/garbage", "no sha256 digest"},
		{"md5_url", "/sums", "no md5 digest"}, // a sha256 file cannot answer an md5 import
		{"sha512_url", "/md5sums", "no sha512 digest"},
		{"sha256_url", "/huge", "over"},
	} {
		code, msg := rawAdmin(t, h, http.MethodPost, "/v1/volumes/chk/import",
			fmt.Sprintf(`{"url":%q,%q:%q}`, src.URL+"/img.iso", tc.body, src.URL+tc.path))
		if tc.want == "" {
			if code != http.StatusAccepted {
				t.Fatalf("%s %s: import: %d %s", tc.body, tc.path, code, msg)
			}
			continue
		}
		if code != http.StatusBadGateway {
			t.Fatalf("%s %s: unusable checksum: %d %s, want 502", tc.body, tc.path, code, msg)
		}
		if !strings.Contains(msg, tc.want) {
			t.Fatalf("%s %s: error %q must mention %q", tc.body, tc.path, msg, tc.want)
		}
	}

	// The digest from the checksum file really verifies the bytes: a
	// listing that is valid but describes a different image fails the
	// job, and the job record shows where the digest came from.
	mux.HandleFunc("/stale", serve(other+"  img.iso\n"))
	code, resp := doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import",
		fmt.Sprintf(`{"url":%q,"sha256_url":%q,"offset":%d}`, src.URL+"/img.iso", src.URL+"/stale", 2<<20))
	if code != http.StatusAccepted {
		t.Fatalf("stale checksum: %d %+v", code, resp)
	}
	job := waitJob(t, h, resp["job_id"].(string), 30*time.Second)
	if job["state"] != "failed" || !strings.Contains(fmt.Sprint(job["error"]), "sha256 mismatch") {
		t.Fatalf("stale-checksum job: %+v", job)
	}
	if job["sha256_url"] != src.URL+"/stale" || job["sha256"] != other {
		t.Fatalf("job must record the resolved digest and its source: %+v", job)
	}

	// A checksum URL that is unreachable, or that answers an error,
	// fails the request: nothing is imported on trust.
	mux.HandleFunc("/missing", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	for _, bad := range []string{src.URL + "/missing", "http://127.0.0.1:1/none"} {
		code, msg := rawAdmin(t, h, http.MethodPost, "/v1/volumes/chk/import",
			fmt.Sprintf(`{"url":%q,"sha256_url":%q}`, src.URL+"/img.iso", bad))
		if code != http.StatusBadGateway {
			t.Fatalf("checksum %s: %d %s, want 502", bad, code, msg)
		}
	}

	// Inline digest and checksum file are alternatives, and the file
	// URL is validated with the rules of a URL the daemon will fetch.
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import",
		fmt.Sprintf(`{"url":%q,"sha256":%q,"sha256_url":%q}`, src.URL+"/img.iso", hexsum, src.URL+"/sums"))
	if code != http.StatusBadRequest {
		t.Fatalf("both digests: %d, want 400", code)
	}
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import",
		fmt.Sprintf(`{"url":%q,"sha256_url":"ftp://x/sums"}`, src.URL+"/img.iso"))
	if code != http.StatusBadRequest {
		t.Fatalf("ftp checksum: %d, want 400", code)
	}
	// The checksum URL must not be fetched for a volume that does not
	// exist (404, and "нельзя импортировать в никуда").
	code, _ = doAdminBody(t, h, http.MethodPost, "/v1/volumes/ghost/import",
		fmt.Sprintf(`{"url":%q,"sha256_url":%q}`, src.URL+"/img.iso", src.URL+"/sums"))
	if code != http.StatusNotFound {
		t.Fatalf("ghost volume: %d, want 404", code)
	}

	// Synchronous mode resolves the checksum file too.
	code, resp = doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import",
		fmt.Sprintf(`{"url":%q,"sha256_url":%q,"offset":%d,"async":false}`,
			src.URL+"/img.iso", src.URL+"/sums", 4<<20))
	if code != http.StatusOK || resp["written"] != float64(len(payload)) {
		t.Fatalf("sync import with a checksum file: %d %+v", code, resp)
	}

	// md5 and sha512 verify the same way, inline and by checksum file.
	for _, tc := range []struct{ field, want string }{
		{"md5", md5sum}, {"sha512", sha512sum},
	} {
		code, resp = doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import",
			fmt.Sprintf(`{"url":%q,%q:%q,"offset":%d,"async":false}`,
				src.URL+"/img.iso", tc.field, tc.want, 4<<20))
		if code != http.StatusOK || resp["written"] != float64(len(payload)) {
			t.Fatalf("sync import with inline %s: %d %+v", tc.field, code, resp)
		}
	}
	for _, tc := range []struct{ field, path string }{
		{"md5_url", "/md5sums"}, {"sha512_url", "/sha512sums"},
	} {
		code, resp = doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import",
			fmt.Sprintf(`{"url":%q,%q:%q,"offset":%d,"async":false}`,
				src.URL+"/img.iso", tc.field, src.URL+tc.path, 4<<20))
		if code != http.StatusOK || resp["written"] != float64(len(payload)) {
			t.Fatalf("sync import with %s: %d %+v", tc.field, code, resp)
		}
	}

	// The algorithms are alternatives: one import verifies one digest,
	// so any two digest fields in one request is a 400.
	for _, body := range []string{
		fmt.Sprintf(`{"url":%q,"sha256":%q,"md5":%q}`, src.URL+"/img.iso", hexsum, md5sum),
		fmt.Sprintf(`{"url":%q,"md5":%q,"md5_url":%q}`, src.URL+"/img.iso", md5sum, src.URL+"/md5sums"),
		fmt.Sprintf(`{"url":%q,"sha512_url":%q,"md5_url":%q}`, src.URL+"/img.iso", src.URL+"/sha512sums", src.URL+"/md5sums"),
	} {
		code, _ := doAdminBody(t, h, http.MethodPost, "/v1/volumes/chk/import", body)
		if code != http.StatusBadRequest {
			t.Fatalf("digest request %s: %d, want 400", body, code)
		}
	}
}

// TestImportChecksumFileGuarded proves the checksum fetch is served by
// the guarded client: an address the SSRF policy blocks is refused
// before the transfer, and the origin never sees the request.
func TestImportChecksumFileGuarded(t *testing.T) {
	env := startVacuumEnv(t)
	h := env.admin.srv.Handler
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := env.cl.CreateVolume(ctx, "guard-chk", 4<<20, 0, 3, 0, 64, "data"); err != nil {
		t.Fatal(err)
	}
	var hits int
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = io.WriteString(w, strings.Repeat("00", 32)+"\n")
	}))
	defer origin.Close()

	guard := newSSRFGuard(false, log.New(io.Discard, "", 0))
	env.admin.importHTTP = &http.Client{Transport: guard.importTransport(), CheckRedirect: guard.checkRedirect, Timeout: 0}

	body, _ := json.Marshal(map[string]any{
		"url": "https://mirror.example/img.iso", "sha256_url": origin.URL + "/SHA256SUMS", "async": false,
	})
	code, msg := rawAdmin(t, h, http.MethodPost, "/v1/volumes/guard-chk/import", string(body))
	if code != http.StatusBadGateway {
		t.Fatalf("guarded checksum fetch: %d %s, want 502", code, msg)
	}
	if !strings.Contains(msg, "loopback") {
		t.Fatalf("error %q must name the policy reason", msg)
	}
	if hits != 0 {
		t.Fatalf("guarded checksum fetch reached the origin (%d hits)", hits)
	}
}

// TestParseChecksum covers the checksum-file formats the parser
// accepts, and the listings it must refuse.
func TestParseChecksum(t *testing.T) {
	const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	const other = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	const md5sum = "0123456789abcdef0123456789abcdef"
	const md5other = "ffffffffffffffffffffffffffffffff"
	const sha512sum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name, algo, body, image, want, fail string
	}{
		{name: "bare", body: digest + "\n", image: "img.iso", want: digest},
		{name: "upper", body: strings.ToUpper(digest) + "\n", image: "img.iso", want: digest},
		{name: "prefixed", body: "sha256:" + digest, image: "img.iso", want: digest},
		{name: "coreutils", body: digest + "  img.iso\n", image: "img.iso", want: digest},
		{name: "binary", body: digest + " *img.iso\r\n", image: "img.iso", want: digest},
		{name: "spaces-in-name", body: digest + "  two words.iso\n", image: "two words.iso", want: digest},
		{name: "gostyle", body: "SHA256(img.iso)= " + digest + "\n", image: "img.iso", want: digest},
		{name: "multi-picks-image", body: other + "  a.iso\n" + digest + "  img.iso\n", image: "img.iso", want: digest},
		{name: "path-prefixed-name", body: digest + "  ./dir/img.iso\n", image: "img.iso", want: digest},
		{name: "escaped-name", body: digest + "  a%20b.iso\n", image: "a b.iso", want: digest},
		{name: "empty", image: "img.iso", fail: "no sha256 digest"},
		{name: "garbage", body: "nothing here\n", image: "img.iso", fail: "no sha256 digest"},
		{name: "short", body: digest[:63] + "\n", image: "img.iso", fail: "no sha256 digest"},
		{name: "non-hex", body: strings.Repeat("z", 64) + "\n", image: "img.iso", fail: "no sha256 digest"},
		{name: "other-file", body: digest + "  other.iso\n", image: "img.iso", fail: `no line for "img.iso"`},
		{name: "bare-ambiguous", body: digest + "\n" + other + "\n", image: "img.iso", fail: "several digests"},
		{name: "named-ambiguous", body: digest + "  img.iso\n" + other + "  img.iso\n", image: "img.iso", fail: "several digests"},
		{name: "mixed-forms", body: digest + "\n" + digest + "  img.iso\n", image: "img.iso", want: digest},
		{name: "bare-ignores-other-names", body: digest + "\n" + other + "  other.iso\n", image: "img.iso", want: digest},
		// The digest a listing carries must be of the requested
		// algorithm: an md5 file never satisfies a sha256 import.
		{name: "md5-listing-sha256-import", body: md5sum + "  img.iso\n", image: "img.iso", fail: "no sha256 digest"},
		{name: "md5-bare", algo: "md5", body: md5sum + "\n", image: "img.iso", want: md5sum},
		{name: "md5-prefixed", algo: "md5", body: "MD5:" + strings.ToUpper(md5sum), image: "img.iso", want: md5sum},
		{name: "md5-coreutils", algo: "md5", body: md5sum + "  img.iso\n", image: "img.iso", want: md5sum},
		{name: "md5-gostyle", algo: "md5", body: "MD5(img.iso)= " + md5sum + "\n", image: "img.iso", want: md5sum},
		{name: "md5-multi", algo: "md5", body: md5other + "  a.iso\n" + md5sum + "  img.iso\n", image: "img.iso", want: md5sum},
		{name: "md5-wrong-len", algo: "md5", body: digest + "  img.iso\n", image: "img.iso", fail: "no md5 digest"},
		{name: "sha512-bare", algo: "sha512", body: sha512sum + "\n", image: "img.iso", want: sha512sum},
		{name: "sha512-prefixed", algo: "sha512", body: "sha512:" + sha512sum, image: "img.iso", want: sha512sum},
		{name: "sha512-coreutils", algo: "sha512", body: sha512sum + " *img.iso\n", image: "img.iso", want: sha512sum},
		{name: "sha512-gostyle", algo: "sha512", body: "SHA512(img.iso)= " + sha512sum + "\n", image: "img.iso", want: sha512sum},
	} {
		algo := tc.algo
		if algo == "" {
			algo = "sha256"
		}
		got, fail := parseChecksum(tc.body, algo, tc.image)
		if tc.fail != "" {
			if fail == "" || !strings.Contains(fail, tc.fail) {
				t.Errorf("%s: got %q, %q; want error mentioning %q", tc.name, got, fail, tc.fail)
			}
			continue
		}
		if fail != "" {
			t.Errorf("%s: %v", tc.name, fail)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
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
