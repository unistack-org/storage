// Asynchronous admin jobs. A bulk ingest (currently the URL import)
// runs off the request: the handler answers 202 with a job id and the
// transfer continues in the background, so a control plane can drive a
// multi-gigabyte image upload without holding an HTTP connection (and
// without timing out a proxy).
//
// Job state lives in meta (meta.ImportJobRecord), mirrored in memory
// for the hot path: a daemon restart no longer loses the picture, and
// a "running" record owned by this daemon is resumed at boot from its
// persisted offset (HTTP Range). At most one ingest per volume may run
// (meta.ImportLock, cluster-visible): two interleaved writers would
// corrupt the image.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
)

type jobState string

const (
	jobRunning   jobState = "running"
	jobCompleted jobState = "completed"
	jobFailed    jobState = "failed"
)

const (
	// jobHeartbeat is how often a running job persists its progress
	// (written bytes, lock refresh). Cheap: one meta write per volume
	// per interval regardless of throughput.
	jobHeartbeat = 3 * time.Second
	// ingestAttempts is how many times a URL ingest re-issues the
	// request from the resume offset after a mid-stream failure.
	ingestAttempts = 4
	// ingestRetryPause is the base pause between attempts (doubled
	// per retry).
	ingestRetryPause = 2 * time.Second
	// maxJobResumes bounds boot resumes per job: a source that keeps
	// failing must not loop forever across restarts.
	maxJobResumes = 3
)

// importJob tracks one background ingest. The record is the source of
// truth (persisted to meta, throttled); mu guards the live mirror the
// job endpoints read.
type importJob struct {
	rec  meta.ImportJobRecord
	mu   sync.Mutex
	done chan struct{} // closed when the transfer finishes (waiters)
}

func (j *importJob) snapshot() meta.ImportJobRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rec
}

func (j *importJob) setWritten(n int64) {
	j.mu.Lock()
	j.rec.Written = n
	j.mu.Unlock()
}

func (j *importJob) finish(state jobState, reason string) {
	j.mu.Lock()
	j.rec.State = string(state)
	j.rec.Error = reason
	j.rec.FinishedAt = time.Now().Unix()
	j.mu.Unlock()
	close(j.done)
}

func (j *importJob) id() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rec.ID
}

func (j *importJob) volume() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.rec.Volume
}

// importView is the job JSON the API answers: the record plus progress
// (total/percent) so a control plane can render an install bar.
type importView struct {
	meta.ImportJobRecord
	Total   int64   `json:"total"`
	Percent float64 `json:"percent"`
}

func jobView(rec meta.ImportJobRecord) importView {
	v := importView{ImportJobRecord: rec, Total: rec.Size}
	if rec.Size > 0 {
		v.Percent = 100 * float64(rec.Written) / float64(rec.Size)
		if v.Percent > 100 {
			v.Percent = 100
		}
	}
	return v
}

// importRequest is the POST .../import body. Async defaults to true
// when the client asks for a checksum (a multi-GB ISO must not hold
// the connection); sha256 is verified over the streamed bytes. Size is
// the expected source length: when set, an early EOF is a failure (a
// truncated fetch must never be reported as a completed import).
type importRequest struct {
	URL    string `json:"url"`
	Offset int64  `json:"offset"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	Async  *bool  `json:"async"`
}

type jobRegistry struct {
	mu   sync.Mutex
	jobs map[string]*importJob
}

func newJobRegistry() *jobRegistry { return &jobRegistry{jobs: map[string]*importJob{}} }

func (jr *jobRegistry) add(j *importJob) {
	jr.mu.Lock()
	defer jr.mu.Unlock()
	jr.jobs[j.id()] = j
}

func (jr *jobRegistry) get(id string) (*importJob, bool) {
	jr.mu.Lock()
	defer jr.mu.Unlock()
	j, ok := jr.jobs[id]
	return j, ok
}

// owner identifies this daemon for the import lock and job records:
// the boot resume only takes over records owned by the same
// host+admin-address (a shared raft meta must not make every node
// resume every job).
func (a *adminAPI) owner() string {
	hn, err := os.Hostname()
	if err != nil || hn == "" {
		hn = "unknown"
	}
	return hn + "|" + a.srv.Addr
}

// ---------- URL import handler ----------

// handleImportURL launches a URL ingest: synchronously when the client
// asked for it (back-compat with the original synchronous endpoint),
// asynchronously (a background context — the request context dies with
// the response) otherwise. A read-only volume refuses the ingest (the
// flag is the export gate: clear it via PUT .../flags first), and one
// volume admits one ingest at a time (409 on a live lock, stale locks
// are taken over).
func (a *adminAPI) handleImportURL(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req importRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	a.startImport(w, r, name, req)
}

func (a *adminAPI) startImport(w http.ResponseWriter, r *http.Request, name string, req importRequest) {
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		jsonError(w, http.StatusBadRequest, "url must be http(s)")
		return
	}
	if req.Offset < 0 || req.Size < 0 {
		jsonError(w, http.StatusBadRequest, "bad offset/size")
		return
	}
	if req.SHA256 != "" {
		if _, derr := hex.DecodeString(req.SHA256); derr != nil {
			jsonError(w, http.StatusBadRequest, "sha256 must be hex")
			return
		}
	}
	spec, err := meta.GetVolume(r.Context(), a.ms, name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "volume: %v", err)
		return
	}
	if spec.ReadOnly {
		jsonError(w, http.StatusConflict,
			"volume %q is read-only (an export set it): clear the flag via PUT /v1/volumes/%s/flags first", name, name)
		return
	}
	async := req.Async == nil || *req.Async
	// Deliberately not openAdmin: an import must not saturate the OSDs,
	// so the ingest is throttled by the volume's own QoS class
	// (OpenVolume resolves it and the meta watch keeps it live). A
	// volume without a class imports at full speed.
	if _, err := a.cl.Refresh(r.Context()); err != nil {
		jsonError(w, http.StatusServiceUnavailable, "placement: %v", err)
		return
	}
	volume, err := a.cl.OpenVolume(r.Context(), name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "open: %v", err)
		return
	}
	if uint64(req.Offset) > volume.Size() {
		volume.Close()
		jsonError(w, http.StatusBadRequest, "offset %d beyond volume size %s", req.Offset, humanSize(volume.Size()))
		return
	}
	if req.Size > 0 && uint64(req.Offset)+uint64(req.Size) > volume.Size() {
		volume.Close()
		jsonError(w, http.StatusBadRequest, "import of %s at offset %d exceeds volume size %s",
			humanSize(uint64(req.Size)), req.Offset, humanSize(volume.Size()))
		return
	}
	if !async {
		defer volume.Close()
		var written int64
		_, err := a.ingestURL(r.Context(), volume, req.URL, req.Offset, req.Size, req.SHA256, nil, &written)
		if err != nil {
			jsonError(w, http.StatusBadGateway, "import: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"written": written, "offset": req.Offset})
		return
	}

	// One ingest per volume: take the cluster-visible lock (a fresh
	// lock held by another job answers 409; a stale one is taken over).
	job := &importJob{rec: meta.ImportJobRecord{
		ID: uuid.NewString(), Volume: name, Type: "import_url",
		State: string(jobRunning), URL: req.URL, Offset: req.Offset,
		Size: req.Size, SHA256: req.SHA256,
		Owner: a.owner(), CreatedAt: time.Now().Unix(),
	}, done: make(chan struct{})}
	now := time.Now().Unix()
	acquired, cur, err := meta.AcquireImportLock(r.Context(), a.ms, name,
		meta.ImportLock{JobID: job.rec.ID, Owner: job.rec.Owner, Heartbeat: now})
	if err != nil {
		volume.Close()
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "import lock: %v", err)
		return
	}
	if !acquired {
		volume.Close()
		if now-cur.Heartbeat < meta.ImportStaleSeconds {
			jsonError(w, http.StatusConflict,
				"volume %q is already being imported (job %s): check GET /v1/jobs/%s",
				name, cur.JobID, cur.JobID)
			return
		}
		acquired, _, err = meta.TakeoverImportLock(r.Context(), a.ms, name,
			meta.ImportLock{JobID: job.rec.ID, Owner: job.rec.Owner, Heartbeat: now}, now, meta.ImportStaleSeconds)
		if err != nil {
			volume.Close()
			jsonError(w, http.StatusInternalServerError, "import lock: %v", err)
			return
		}
		if !acquired {
			volume.Close()
			jsonError(w, http.StatusConflict, "volume %q is already being imported", name)
			return
		}
	}
	if _, err := meta.PutImportJob(r.Context(), a.ms, job.rec); err != nil {
		_ = meta.ReleaseImportLock(r.Context(), a.ms, name, job.rec.ID)
		volume.Close()
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "persist job: %v", err)
		return
	}
	a.jobs.add(job)
	// The request context dies with this response, so the transfer runs
	// on a fresh one; the volume handle moves into the goroutine.
	go a.runIngest(job, volume, 0)
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": job.rec.ID, "volume": name})
}

// runIngest drives a job's transfer to completion: heartbeats (written
// bytes + lock refresh) run on a ticker, the record is persisted when
// the job finishes, and the lock is released. seedWritten is the bytes
// already landed before a boot resume.
func (a *adminAPI) runIngest(job *importJob, volume *client.Volume, seedWritten int64) {
	defer volume.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()
	name := job.volume()
	owner := job.snapshot().Owner

	stop := make(chan struct{})
	var hbDone sync.WaitGroup
	hbDone.Add(1)
	go func() {
		defer hbDone.Done()
		tick := time.NewTicker(jobHeartbeat)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				rec := job.snapshot()
				_, _ = meta.UpdateImportJob(ctx, a.ms, rec.ID, func(cur *meta.ImportJobRecord) bool {
					if cur.State != meta.JobStateRunning {
						return false
					}
					cur.Written = rec.Written
					return true
				})
				_ = meta.RefreshImportLock(ctx, a.ms, name, rec.ID, owner, time.Now().Unix())
			}
		}
	}()

	a.logger.Printf("admin: import job %s (%s) started at offset %d", job.id(), name, job.snapshot().Offset)
	rec := job.snapshot()
	written := seedWritten
	_, err := a.ingestURL(ctx, volume, rec.URL, rec.Offset, rec.Size, rec.SHA256, job.setWritten, &written)
	close(stop)
	hbDone.Wait()

	state, reason := jobCompleted, ""
	if err != nil {
		state, reason = jobFailed, err.Error()
	}
	job.finish(state, reason)
	final := job.snapshot()
	final.Written = written
	_, perr := meta.UpdateImportJob(ctx, a.ms, final.ID, func(cur *meta.ImportJobRecord) bool {
		cur.State = final.State
		cur.Written = final.Written
		cur.Error = final.Error
		cur.FinishedAt = final.FinishedAt
		return true
	})
	if perr != nil {
		a.logger.Printf("admin: import job %s: persist final state: %v", final.ID, perr)
	}
	if rerr := meta.ReleaseImportLock(ctx, a.ms, name, final.ID); rerr != nil {
		a.logger.Printf("admin: import job %s: release lock: %v", final.ID, rerr)
	}
	a.logger.Printf("admin: import job %s (%s) finished: state=%s written=%d err=%v", final.ID, name, state, written, err)
}

// probeSource asks the origin for the source's size and Range support
// (HEAD, falling back to a one-byte ranged GET for servers that refuse
// HEAD). Returns size (-1 = unknown) and ranges ok.
func (a *adminAPI) probeSource(ctx context.Context, rawurl string) (int64, bool, error) {
	probe := func(method string, headers map[string]string) (int64, bool, bool, error) {
		req, err := http.NewRequestWithContext(ctx, method, rawurl, nil)
		if err != nil {
			return 0, false, false, fmt.Errorf("bad url: %w", err)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := a.importHTTP.Do(req)
		if err != nil {
			return 0, false, false, fmt.Errorf("probe: %w", err)
		}
		defer func() {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
			_ = resp.Body.Close()
		}()
		if resp.StatusCode == http.StatusNotImplemented || resp.StatusCode == http.StatusMethodNotAllowed {
			return 0, false, false, nil // HEAD not supported: retry as ranged GET
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return 0, false, false, fmt.Errorf("probe: %s", resp.Status)
		}
		ranges := strings.Contains(strings.ToLower(resp.Header.Get("Accept-Ranges")), "bytes")
		size := resp.ContentLength
		// A ranged GET answers the real total in Content-Range.
		if resp.StatusCode == http.StatusPartialContent {
			ranges = true
			if cr := resp.Header.Get("Content-Range"); cr != "" {
				if i := strings.IndexByte(cr, '/'); i >= 0 && i+1 < len(cr) {
					var total int64
					if _, perr := fmt.Sscanf(cr[i+1:], "%d", &total); perr == nil {
						size = total
					}
				}
			}
		}
		return size, ranges, true, nil
	}
	size, ranges, ok, err := probe(http.MethodHead, nil)
	if err != nil {
		return 0, false, err
	}
	if ok {
		if size <= 0 {
			size = -1
		}
		return size, ranges, nil
	}
	size, ranges, _, err = probe(http.MethodGet, map[string]string{"Range": "bytes=0-0"})
	if err != nil {
		return 0, false, err
	}
	if size <= 0 {
		size = -1
	}
	return size, ranges, nil
}

// handleProbe answers POST /v1/volumes/probe {"url": ...}: a control
// plane that must size a volume before importing (an ISO link gives
// no size locally) checks the origin without fetching the image.
func (a *adminAPI) handleProbe(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		jsonError(w, http.StatusBadRequest, "url must be http(s)")
		return
	}
	size, ranges, err := a.probeSource(r.Context(), req.URL)
	if err != nil {
		jsonError(w, http.StatusBadGateway, "%v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"size": size, "accept_ranges": ranges})
}

// handleVolumeFlags answers PUT /v1/volumes/{name}/flags
// {"read_only":bool,"shared":bool,"owner":string}: the control plane
// clears (or sets) the serving flags. Clearing read_only is what
// re-opens a volume that an export set read-only for re-import.
func (a *adminAPI) handleVolumeFlags(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req struct {
		ReadOnly *bool  `json:"read_only"`
		Shared   *bool  `json:"shared"`
		Owner    string `json:"owner"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	if req.ReadOnly == nil && req.Shared == nil && req.Owner == "" {
		jsonError(w, http.StatusBadRequest, "nothing to set: read_only, shared, owner")
		return
	}
	if _, err := a.cl.Refresh(r.Context()); err != nil {
		jsonError(w, http.StatusServiceUnavailable, "placement: %v", err)
		return
	}
	spec, err := meta.GetVolume(r.Context(), a.ms, name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "volume: %v", err)
		return
	}
	cur, err := a.cl.SetVolumeFlags(r.Context(), name,
		boolOr(req.ReadOnly, spec.ReadOnly), boolOr(req.Shared, spec.Shared),
		firstNonEmpty(req.Owner, spec.Owner))
	if err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "set flags: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q flags: read_only=%v shared=%v owner=%q", name, cur.ReadOnly, cur.Shared, cur.Owner)
	writeJSON(w, http.StatusOK, a.volumeView(r.Context(), cur))
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// ---------- resilient URL ingest ----------

// ingestURL fetches the URL and streams it into the volume, verifying
// the optional sha256 over exactly the bytes written. Progress lands
// in progress (when set) and written mirrors the landed total, so the
// heartbeat can persist it.
//
// Completion is strict (gap 1): when wantSize is known the stream must
// deliver exactly that many bytes — an early EOF (a proxy truncating
// the body, an origin dying mid-transfer) fails the job instead of
// reporting success. When wantSize is unknown a mid-stream ReadFull
// break is still an error; only a clean EOF ends the transfer.
//
// The fetch is resumable (gap 7): a mid-stream failure re-issues the
// request with a Range header from the landed offset (origins that
// ignore Range restart from scratch, which is safe: writes are
// idempotent per offset). On resume with a checksum the digest is
// rebuilt by re-reading the already-written volume range, so
// verification covers the whole image, not just the tail.
func (a *adminAPI) ingestURL(ctx context.Context, v *client.Volume, rawurl string,
	offset, wantSize int64, want string, progress func(int64), written *int64) (int64, error) {
	if want != "" {
		want = strings.ToLower(want)
	}
	attempt := 0
	var seed hash.Hash
	for {
		err := a.fetchInto(ctx, v, rawurl, offset, offset+*written, wantSize-*written, want, seed, progress, written)
		seed = nil
		if err == nil {
			return *written, nil
		}
		if ctx.Err() != nil {
			return *written, ctx.Err()
		}
		attempt++
		if attempt >= ingestAttempts {
			return *written, fmt.Errorf("import after %d attempts: %w", attempt, err)
		}
		pause := ingestRetryPause * time.Duration(1<<min(attempt-1, 4))
		a.logger.Printf("admin: ingest %s at offset %d: %v (resume in %s, attempt %d/%d)",
			rawurl, offset+*written, err, pause, attempt+1, ingestAttempts)
		select {
		case <-ctx.Done():
			return *written, ctx.Err()
		case <-time.After(pause):
		}
		if want != "" {
			h, rerr := a.hashVolumeRange(ctx, v, offset, *written)
			if rerr != nil {
				return *written, fmt.Errorf("rebuild sha256 after resume: %v (re-issue the import to redo it from scratch)", rerr)
			}
			seed = h
		}
	}
}

// hashVolumeRange reads [off, off+n) from the volume and returns a
// sha256 state that has already consumed those bytes, so a resumed
// ingest verifies the digest over the whole image.
func (a *adminAPI) hashVolumeRange(ctx context.Context, v *client.Volume, off, n int64) (hash.Hash, error) {
	h := sha256.New()
	buf := make([]byte, importChunk)
	var done int64
	for done < n {
		want := int64(len(buf))
		if n-done < want {
			want = n - done
		}
		rn, err := v.ReadAt(ctx, buf[:want], uint64(off+done))
		if rn > 0 {
			h.Write(buf[:rn])
			done += int64(rn)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if done != n {
		return nil, fmt.Errorf("volume returned %d of %d bytes at %d", done, n, off)
	}
	return h, nil
}

// fetchInto runs one GET (with a Range request when resume > base) and
// streams it into the volume. base is the volume offset of the start of
// the image at the origin; resume is the absolute volume offset the
// stream must supply; wantSize is the remaining expected bytes
// (0 = unknown); seed is the hash state rebuilt after a resume (nil
// when the stream starts from the beginning of the image).
func (a *adminAPI) fetchInto(ctx context.Context, v *client.Volume, rawurl string,
	base, resume, wantSize int64, want string, seed hash.Hash, progress func(int64), written *int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawurl, nil)
	if err != nil {
		return fmt.Errorf("bad url: %w", err)
	}
	if resume > base {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resume))
	}
	resp, err := a.importHTTP.Do(req)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return fmt.Errorf("fetch: origin refused Range at %d (416)", resume)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("fetch: %s", resp.Status)
	}
	if resume > base && resp.StatusCode != http.StatusPartialContent {
		// The origin ignored Range and is re-sending the whole image
		// from byte 0: the stream starts at base, not at resume. The
		// landed-count basis resets (rewriting the prefix is safe:
		// byte-identical data, idempotent per offset) and a rebuilt
		// hash seed is stale (the stream re-covers the prefix).
		a.logger.Printf("admin: origin ignored Range: restarting from %d", base)
		*written = 0
		resume = base
		seed = nil
		if progress != nil {
			progress(0)
		}
	}
	var h hash.Hash
	if want != "" {
		h = seed
		if h == nil {
			h = sha256.New()
		}
	}
	var hashOut io.Writer
	if h != nil {
		hashOut = h
	}
	if err := streamImport(ctx, v, resp.Body, resume, wantSize, hashOut, progress, written, a.importMaxSize); err != nil {
		return err
	}
	if want != "" {
		sum := hex.EncodeToString(h.Sum(nil))
		if sum != want {
			return fmt.Errorf("sha256 mismatch: got %s, want %s", sum, want)
		}
	}
	return nil
}

// streamImport writes src into vol from offset and advances *written
// (and calls progress) with every landed chunk.
//
// expected is the byte total this stream must deliver (0 = unknown):
// with a declared size, a stream that ends short — a clean EOF with
// bytes still missing, or a mid-chunk break — is an error, so a
// truncated fetch never reports success (gap 1); once the declared
// total has landed the stream is done and the rest goes unread.
// Without a declared size the boundary is EOF itself, like the
// body-push endpoint: a stream ending mid-chunk is that source's final
// partial chunk, not a break (a requested digest still covers the
// bytes that landed).
//
// limit caps how many bytes the volume may hold in total (offset
// included); 0 disables the cap. It is the -import-max-size guard
// against an unbounded source filling the cluster (gap 6).
func streamImport(ctx context.Context, vol *client.Volume, src io.Reader, offset int64,
	expected int64, hashOut io.Writer, progress func(int64), written *int64, limit int64) error {
	size := int64(vol.Size())
	buf := make([]byte, importChunk)
	total := offset
	landed := int64(0) // bytes this stream delivered (expected is per stream)
	report := func() {
		if progress != nil {
			progress(*written)
		}
	}
	for {
		if expected > 0 && landed >= expected {
			return nil
		}
		n, err := io.ReadFull(src, buf)
		if n > 0 {
			if limit > 0 && total+int64(n) > limit {
				report()
				return fmt.Errorf("input exceeds import size limit %s at offset %d", humanSize(uint64(limit)), offset)
			}
			if total+int64(n) > size {
				report()
				return fmt.Errorf("input exceeds volume size %s at offset %d", humanSize(uint64(size)), offset)
			}
			if _, werr := vol.WriteAt(ctx, buf[:n], uint64(total)); werr != nil {
				report()
				return fmt.Errorf("write at %d: %w", total, werr)
			}
			if hashOut != nil {
				if _, herr := hashOut.Write(buf[:n]); herr != nil {
					report()
					return fmt.Errorf("hash: %w", herr)
				}
			}
			total += int64(n)
			landed += int64(n)
			*written += int64(n)
			report()
		}
		if err == io.EOF {
			if expected > 0 && landed < expected {
				return fmt.Errorf("%w: stream ended at %s of expected %s",
					io.ErrUnexpectedEOF, humanSize(uint64(landed)), humanSize(uint64(expected)))
			}
			return nil
		}
		if err == io.ErrUnexpectedEOF {
			if expected <= 0 {
				// Undeclared size: the stream's end is the image's end.
				return nil
			}
			if landed < expected {
				return fmt.Errorf("stream broke at %s of expected %s: %w",
					humanSize(uint64(landed)), humanSize(uint64(expected)), err)
			}
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// ---------- job endpoints ----------

func (a *adminAPI) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if job, ok := a.jobs.get(id); ok {
		writeJSON(w, http.StatusOK, jobView(job.snapshot()))
		return
	}
	rec, err := meta.GetImportJob(r.Context(), a.ms, id)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "job %q not found", id)
			return
		}
		jsonError(w, http.StatusInternalServerError, "job: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, jobView(rec))
}

func (a *adminAPI) handleJobs(w http.ResponseWriter, r *http.Request) {
	recs, err := meta.ListImportJobs(r.Context(), a.ms)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "jobs: %v", err)
		return
	}
	// Overlay the live mirror: the persisted copy trails the in-memory
	// one by at most one heartbeat.
	out := make([]importView, 0, len(recs))
	for _, rec := range recs {
		if job, ok := a.jobs.get(rec.ID); ok {
			rec = job.snapshot()
		}
		out = append(out, jobView(rec))
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------- boot resume ----------

// resumeJobs picks up this daemon's "running" ingest jobs after a
// restart: the record was persisted with the landed offset, the volume
// lock (heartbeat) proves nobody else is writing, so the transfer
// re-issues from the resume point with a Range request. Records owned
// by other daemons are left alone (their lock goes stale and a fresh
// import on that volume takes it over); jobs resumed too many times
// fail with their last error visible.
func (a *adminAPI) resumeJobs(ctx context.Context) {
	recs, err := meta.ListImportJobs(ctx, a.ms)
	if err != nil {
		a.logger.Printf("admin: resume scan: %v", err)
		return
	}
	owner := a.owner()
	for _, rec := range recs {
		if rec.State != meta.JobStateRunning || rec.Owner != owner || rec.Type != "import_url" {
			continue
		}
		if rec.Resumed >= maxJobResumes {
			_, _ = meta.UpdateImportJob(ctx, a.ms, rec.ID, func(cur *meta.ImportJobRecord) bool {
				cur.State = meta.JobStateFailed
				cur.Error = fmt.Sprintf("gave up after %d resumes", cur.Resumed)
				cur.FinishedAt = time.Now().Unix()
				return true
			})
			_ = meta.ReleaseImportLock(ctx, a.ms, rec.Volume, rec.ID)
			a.logger.Printf("admin: import job %s (%s): giving up after %d resumes", rec.ID, rec.Volume, rec.Resumed)
			continue
		}
		// The lock must become ours: a fresh lock (an old process still
		// running) is left alone, a stale one (the restart we are part
		// of) is taken over. A lock that vanished (released by the
		// dying process) is created fresh.
		now := time.Now().Unix()
		acquired, cur, err := meta.TakeoverImportLock(ctx, a.ms, rec.Volume,
			meta.ImportLock{JobID: rec.ID, Owner: owner, Heartbeat: now}, now, meta.ImportStaleSeconds)
		if err != nil {
			a.logger.Printf("admin: import job %s: resume lock: %v", rec.ID, err)
			continue
		}
		if !acquired {
			a.logger.Printf("admin: import job %s (%s): lock held by %s (heartbeat %ds ago): leaving it",
				rec.ID, rec.Volume, cur.JobID, now-cur.Heartbeat)
			continue
		}
		if _, err := a.cl.Refresh(ctx); err != nil {
			a.logger.Printf("admin: import job %s: placement: %v", rec.ID, err)
			_ = meta.ReleaseImportLock(ctx, a.ms, rec.Volume, rec.ID)
			continue
		}
		volume, err := a.cl.OpenVolume(ctx, rec.Volume)
		if err != nil {
			a.logger.Printf("admin: import job %s: open volume: %v", rec.ID, err)
			_ = meta.ReleaseImportLock(ctx, a.ms, rec.Volume, rec.ID)
			continue
		}
		rec.Resumed++
		rec.Owner = owner
		if _, err := meta.PutImportJob(ctx, a.ms, rec); err != nil {
			a.logger.Printf("admin: import job %s: persist resume: %v", rec.ID, err)
			volume.Close()
			continue
		}
		job := &importJob{rec: rec, done: make(chan struct{})}
		a.jobs.add(job)
		seed := rec.Written // bytes landed before the restart
		a.logger.Printf("admin: resuming import job %s (%s) from offset %d+%d (attempt %d/%d)",
			rec.ID, rec.Volume, rec.Offset, seed, rec.Resumed, maxJobResumes)
		go a.runIngest(job, volume, seed)
	}
}
