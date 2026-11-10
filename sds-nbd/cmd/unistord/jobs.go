// Asynchronous admin jobs. A bulk ingest (currently the URL import)
// runs off the request: the handler answers 202 with a job id and the
// transfer continues in the background, so a control plane can drive a
// multi-gigabyte image upload without holding an HTTP connection (and
// without timing out a proxy). The registry is in-memory: a daemon
// restart drops job state, which is fine — the write itself is
// idempotent per offset and the caller can re-issue the import.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

// importJob tracks one background ingest.
type importJob struct {
	ID       string     `json:"id"`
	Volume   string     `json:"volume"`
	Type     string     `json:"type"` // "import_url"
	State    jobState   `json:"state"`
	Written  int64      `json:"written"`
	Offset   int64      `json:"offset"`
	Error    string     `json:"error,omitempty"`
	Created  time.Time  `json:"created"`
	Finished *time.Time `json:"finished,omitempty"`

	mu sync.Mutex
}

func (j *importJob) snapshot() *importJob {
	j.mu.Lock()
	defer j.mu.Unlock()
	return &importJob{ID: j.ID, Volume: j.Volume, Type: j.Type, State: j.State,
		Written: j.Written, Offset: j.Offset, Error: j.Error,
		Created: j.Created, Finished: j.Finished}
}

// importRequest is the POST .../import body. Async defaults to true
// when the client asks for a checksum (a multi-GB ISO must not hold
// the connection); sha256 is verified over the streamed bytes.
type importRequest struct {
	URL    string `json:"url"`
	Offset int64  `json:"offset"`
	SHA256 string `json:"sha256"`
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
	jr.jobs[j.ID] = j
}

func (jr *jobRegistry) get(id string) (*importJob, bool) {
	jr.mu.Lock()
	defer jr.mu.Unlock()
	j, ok := jr.jobs[id]
	return j, ok
}

// startImportURL launches a URL ingest: synchronously when the client
// asked for it (back-compat with the original synchronous endpoint),
// asynchronously (a background context — the request context dies with
// the response) otherwise.
func (a *adminAPI) handleImportURL(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	a.startImportURL(w, r, name)
}

func (a *adminAPI) startImportURL(w http.ResponseWriter, r *http.Request, name string) {
	var req importRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		jsonError(w, http.StatusBadRequest, "url must be http(s)")
		return
	}
	if req.Offset < 0 {
		jsonError(w, http.StatusBadRequest, "bad offset")
		return
	}
	if _, err := meta.GetVolume(r.Context(), a.ms, name); err != nil {
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
	if !async {
		defer volume.Close()
		n, err := a.ingestURL(r.Context(), volume, req.URL, req.Offset, req.SHA256)
		if err != nil {
			jsonError(w, http.StatusBadGateway, "import: %v", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"written": n, "offset": req.Offset})
		return
	}
	job := &importJob{ID: uuid.NewString(), Volume: name, Type: "import_url",
		State: jobRunning, Offset: req.Offset, Created: time.Now()}
	a.jobs.add(job)
	// The request context dies with this response, so the transfer runs
	// on a fresh one; the volume handle moves into the goroutine.
	go func() {
		defer volume.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
		defer cancel()
		n, err := a.ingestURL(ctx, volume, req.URL, req.Offset, req.SHA256)
		job.mu.Lock()
		job.Written = n
		if err != nil {
			job.State = jobFailed
			job.Error = err.Error()
		} else {
			job.State = jobCompleted
		}
		t := time.Now()
		job.Finished = &t
		job.mu.Unlock()
		a.logger.Printf("admin: import job %s (%s) finished: written=%d err=%v", job.ID, name, n, err)
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": job.ID, "volume": name})
}

// ingestURL fetches the URL and streams it into the volume, verifying
// the optional sha256 over exactly the bytes written.
func (a *adminAPI) ingestURL(ctx context.Context, v *client.Volume, url string, offset int64, want string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("bad url: %w", err)
	}
	resp, err := a.importHTTP.Do(req)
	if err != nil {
		return 0, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("fetch: %s", resp.Status)
	}
	var h io.Writer = io.Discard
	if want != "" {
		h = sha256.New()
	}
	n, err := streamImport(ctx, v, resp.Body, offset, h)
	if err != nil {
		return n, err
	}
	if want != "" {
		sum := hex.EncodeToString(h.(interface{ Sum([]byte) []byte }).Sum(nil))
		if !strings.EqualFold(sum, want) {
			return n, fmt.Errorf("sha256 mismatch: got %s, want %s", sum, want)
		}
	}
	return n, nil
}

func (a *adminAPI) handleJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, ok := a.jobs.get(id)
	if !ok {
		jsonError(w, http.StatusNotFound, "job %q not found", id)
		return
	}
	writeJSON(w, http.StatusOK, job.snapshot())
}

func (a *adminAPI) handleJobs(w http.ResponseWriter, r *http.Request) {
	a.jobs.mu.Lock()
	out := make([]*importJob, 0, len(a.jobs.jobs))
	for _, j := range a.jobs.jobs {
		out = append(out, j.snapshot())
	}
	a.jobs.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}
