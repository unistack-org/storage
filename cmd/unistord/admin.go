// Admin HTTP API for unistord (phase M2 of the test-VM plan): the
// daemon's control surface. Everything unistorctl does through the meta
// directory is reachable here over HTTP, plus the two byte-stream
// operations that need a live data path: blob import (octet-stream body,
// optionally fetched by the daemon from a URL) and export (octet-stream
// download of the whole volume).
//
// The caller of this API is the admin: volumes opened here are never
// throttled (the admin class), so backup/import of a limited volume
// runs at full speed.
//
// Routes (Go 1.22 method+wildcard patterns):
//
//	GET    /healthz
//	GET    /healthcheck                 orchestration verdict (200 ok / 503 fail)
//	GET    /v1/status
//	GET    /v1/osds
//	GET    /v1/volumes
//	POST   /v1/volumes                          {"name":"vm1","size":"40G",...}
//	GET    /v1/volumes/{name}
//	DELETE /v1/volumes/{name}
//	PUT    /v1/volumes/{name}/qos               {"read_iops":...,"qos_template":...}
//	PUT    /v1/volumes/{name}/quota             {"quota":"20G"}
//	PUT    /v1/volumes/{name}/resize            {"size":"40G"} (grow only)
//	POST   /v1/volumes/{name}/snapshot          create a read-only snapshot
//	GET    /v1/volumes/{name}/snapshots         list snapshots (oldest first)
//	DELETE /v1/volumes/{name}/snapshot/{gen}    drop one snapshot
//	POST   /v1/volumes/{name}/rollback          {"gen":N} (discards newer snapshots)
//	PUT    /v1/volumes/{name}/import?offset=N   body: application/octet-stream
//	POST   /v1/volumes/{name}/import            {"url":"http://...","offset":N}
//	GET    /v1/volumes/{name}/export            reply: application/octet-stream
//	GET    /v1/qos/classes
//	PUT    /v1/qos/classes/{name}               {"read_iops":...}
//	DELETE /v1/qos/classes/{name}
//	GET    /v1/vacuum                          pending vacuum tombstones
//	POST   /v1/vacuum                          run one vacuum pass now
//	GET    /v1/recovery                         check every volume
//	GET    /v1/recovery/{name}                  check one volume
//	POST   /v1/recovery/{name}                  check + repair one volume
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/qos"
)

// importChunk is the streaming granularity for import/export: big
// enough to keep the data path efficient, small enough that memory
// stays flat regardless of the image size.
const importChunk = 1 << 20 // 1 MiB

type adminAPI struct {
	cl     *client.Cluster
	ms     meta.Store
	raft   *meta.RaftStore // nil in dir-backed mode
	disks  *diskRegistry   // OSDs hosted in this process (disk stats)
	logger *log.Logger
	srv    *http.Server
	// token, when set (flag -admin-token), turns every endpoint except
	// /healthz behind "Authorization: Bearer <token>": the admin port
	// grows the power to attach volumes to VMs, so a control plane
	// talking to it over a shared network must authenticate.
	token string
	// exports is the runtime export hub (nbd/vduse add/remove/list);
	// nil in standalone mode. jobs tracks async ingests.
	exports *exportHub
	jobs    *jobRegistry
	// importHTTP is the client used by URL imports (tests swap in their
	// own transport).
	importHTTP *http.Client
	// importMaxSize caps how many bytes one import may land in a volume
	// (-import-max-size, 0 = unlimited); see streamImport.
	importMaxSize int64
	// recoveryRecord feeds the recovery counters when metrics are
	// enabled (nil = no-op); it never exposes metrics over REST.
	recoveryRecord func(copies int, bytes uint64)
}

// newAdminServer binds the routes on the process-wide meta store (in
// raft mode the store is a single instance shared with the exports, so
// the admin does not open its own). raft may be nil (dir-backed meta);
// when set, writes that hit a quorum loss answer 503 with the current
// leader hint and /v1/status reports the replication state. disks
// collects the OSDs hosted in this process (the admin starts before the
// daemons; they register into the registry as they come up). It does not
// start serving; call serve.
func newAdminServer(ms meta.Store, addr string, logger *log.Logger, raftSt *meta.RaftStore, disks *diskRegistry, tlsCfg *tls.Config, clientTLS *tls.Config, token string, exports *exportHub) (*adminAPI, error) {
	cl := client.NewCluster(ms)
	if clientTLS != nil {
		// Import/export touch the OSDs over the data plane: dial it the
		// same TLS way the exports do.
		cl.SetTLS(clientTLS)
	}
	a := &adminAPI{cl: cl, ms: ms, logger: logger, raft: raftSt, disks: disks, token: token, exports: exports,
		jobs: newJobRegistry(), importHTTP: &http.Client{Timeout: 0}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("GET /healthcheck", a.handleHealthcheck)
	mux.HandleFunc("GET /v1/exports", a.handleListExports)
	mux.HandleFunc("POST /v1/exports", a.handleAddExport)
	mux.HandleFunc("DELETE /v1/exports/{name}", a.handleDeleteExport)
	mux.HandleFunc("GET /v1/jobs", a.handleJobs)
	mux.HandleFunc("GET /v1/jobs/{id}", a.handleJob)
	mux.HandleFunc("GET /v1/status", a.handleStatus)
	mux.HandleFunc("GET /v1/osds", a.handleOSDs)
	mux.HandleFunc("GET /v1/volumes", a.handleListVolumes)
	mux.HandleFunc("POST /v1/volumes", a.handleCreateVolume)
	mux.HandleFunc("GET /v1/volumes/{name}", a.handleGetVolume)
	mux.HandleFunc("DELETE /v1/volumes/{name}", a.handleDeleteVolume)
	mux.HandleFunc("PUT /v1/volumes/{name}/qos", a.handleVolumeQoS)
	mux.HandleFunc("PUT /v1/volumes/{name}/quota", a.handleVolumeQuota)
	mux.HandleFunc("PUT /v1/volumes/{name}/resize", a.handleVolumeResize)
	mux.HandleFunc("POST /v1/volumes/{name}/snapshot", a.handleSnapshotCreate)
	mux.HandleFunc("GET /v1/volumes/{name}/snapshots", a.handleSnapshotList)
	mux.HandleFunc("DELETE /v1/volumes/{name}/snapshot/{gen}", a.handleSnapshotDelete)
	mux.HandleFunc("POST /v1/volumes/{name}/rollback", a.handleRollback)
	mux.HandleFunc("POST /v1/volumes/{name}/clone", a.handleClone)
	mux.HandleFunc("PUT /v1/volumes/{name}/flags", a.handleVolumeFlags)
	mux.HandleFunc("PUT /v1/volumes/{name}/import", a.handleImportBody)
	mux.HandleFunc("POST /v1/volumes/{name}/import", a.handleImportURL)
	mux.HandleFunc("POST /v1/volumes/probe", a.handleProbe)
	mux.HandleFunc("GET /v1/volumes/{name}/export", a.handleExport)
	mux.HandleFunc("GET /v1/qos/classes", a.handleListClasses)
	mux.HandleFunc("PUT /v1/qos/classes/{name}", a.handlePutClass)
	mux.HandleFunc("DELETE /v1/qos/classes/{name}", a.handleDeleteClass)
	mux.HandleFunc("GET /v1/vacuum", a.handleVacuumList)
	mux.HandleFunc("POST /v1/vacuum", a.handleVacuumRun)
	mux.HandleFunc("POST /v1/vacuum/{name}", a.handleVacuumVolume)
	mux.HandleFunc("GET /v1/recovery", a.handleRecoveryScan)
	mux.HandleFunc("GET /v1/recovery/{name}", a.handleRecoveryCheck)
	mux.HandleFunc("POST /v1/recovery/{name}", a.handleRecoveryRepair)
	var handler http.Handler = mux
	if token != "" {
		// /healthcheck, like /healthz, is a probe endpoint: probes run
		// inside trusted infrastructure and carry no secrets, so it
		// answers outside the token gate. bearerGate (exports.go) only
		// exempts /healthz, so mount /healthcheck on an outer mux.
		gated := a.bearerGate(mux)
		outer := http.NewServeMux()
		outer.HandleFunc("GET /healthcheck", a.handleHealthcheck)
		outer.Handle("/", gated)
		handler = outer
	}
	a.srv = &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 10 * time.Second, TLSConfig: tlsCfg}
	return a, nil
}

// serve runs the HTTP server until ctx is cancelled, then closes the
// listener and the meta handle.
func (a *adminAPI) serve(ctx context.Context) {
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.srv.Shutdown(shutCtx)
		// The store is owned by the caller (main closes it after the
		// exports), the admin only closes what it opened itself.
	}()
	a.logger.Printf("admin http on %s", a.srv.Addr)
	serve := func() error { return a.srv.ListenAndServe() }
	if a.srv.TLSConfig != nil {
		serve = func() error { return a.srv.ListenAndServeTLS("", "") }
	}
	if err := serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		a.logger.Printf("admin http: %v", err)
	}
}

// ---------- helpers ----------

func jsonError(w http.ResponseWriter, code int, format string, args ...any) {
	http.Error(w, fmt.Sprintf(format, args...), code)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// cluster exposes the admin's cluster client to the export hub (runtime
// exports open volumes through the same placement-refreshed handle).
func (a *adminAPI) cluster() *client.Cluster { return a.cl }

// readOnlyGate answers the request when err means "the meta cannot be
// written right now" (raft without a quorum / this node is a follower).
// It returns true when the request was answered, so callers return.
// Reads are never gated: M6 contract is read-only on quorum loss.
func (a *adminAPI) readOnlyGate(w http.ResponseWriter, err error) bool {
	if !meta.IsReadOnly(err) {
		return false
	}
	msg := "meta is read-only: no quorum"
	if a.raft != nil {
		if l := a.raft.Leader(); l != "" {
			msg = "meta is read-only here: leader is " + l
		}
	}
	jsonError(w, http.StatusServiceUnavailable, "%s", msg)
	return true
}

// volumeName validates the {name} path segment: it becomes a meta file
// name, so keep it to a conservative alphabet.
func volumeName(r *http.Request) string {
	name := r.PathValue("name")
	if name == "" || len(name) > 128 {
		return ""
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.'
		if !ok {
			return ""
		}
	}
	if name == "." || name == ".." || strings.HasSuffix(name, ".json") {
		return ""
	}
	return name
}

// openAdmin opens a volume for admin use: the returned handle has all
// limits removed (the admin class), regardless of the volume's QoS.
// Placement is refreshed first (cheap when unchanged: cached map bytes
// and registry cost two meta reads).
func (a *adminAPI) openAdmin(ctx context.Context, name string) (*client.Volume, error) {
	if _, err := a.cl.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("placement: %w", err)
	}
	vol, err := a.cl.OpenVolume(ctx, name)
	if err != nil {
		return nil, err
	}
	// Admin is unlimited: OpenVolume resolves the volume's own QoS (and
	// its class), which would throttle a backup of a limited volume.
	vol.SetQoS(meta.VolumeSpec{})
	return vol, nil
}

// ---------- meta routes ----------

func (a *adminAPI) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok\n"))
}

// diskStatView is one locally hosted OSD's disk accounting for
// /v1/status (see diskstats.go).
type diskStatView struct {
	OSD        int32  `json:"osd"`
	Usage      int64  `json:"usage"`
	Free       int64  `json:"free"`
	Total      int64  `json:"total"`
	OK         bool   `json:"ok"`
	UsageHuman string `json:"usage_human,omitempty"`
	FreeHuman  string `json:"free_human,omitempty"`
	TotalHuman string `json:"total_human,omitempty"`
	// Write amplification counters (lifetime sums since process
	// start): what the client offered, what the journal fsynced, what
	// the applier flattened into objects, fill/punch traffic, journal
	// syncs. sdsbench snapshots these before/after a run to compute
	// WA = (journal+flatten+fill) / client bytes.
	Writes writeStatView `json:"writes"`
	// Meta index population (in-memory object metadata).
	MetaObjects int64 `json:"meta_objects"`
	MetaRAM     int64 `json:"meta_ram"`
	// Intent is set only for OSDs running with the write intent log
	// (non-journal commit modes + -intent).
	Intent *intentStatView `json:"intent,omitempty"`
	// IntentWarnings is the recovery damage report (acknowledged
	// writes not durable on disk) from the last reopen; non-empty
	// means the OSD survived a power loss that damaged data.
	IntentWarnings []string `json:"intent_mismatches,omitempty"`
	// Journal is set only for OSDs running in journal commit mode:
	// the WAL footprint and the filesystem the journal grows into.
	Journal *journalStatView `json:"journal,omitempty"`
}

// journalStatView is the journal staging footprint of one locally
// hosted OSD (§4.2 ENOSPC): WAL file size, acknowledged-but-unapplied
// records, and the free/total of the filesystem the log lives on.
type journalStatView struct {
	LogBytes   int64  `json:"log_bytes"`
	Pending    int64  `json:"pending_records"`
	Free       int64  `json:"free"`
	Total      int64  `json:"total"`
	FreeHuman  string `json:"free_human,omitempty"`
	TotalHuman string `json:"total_human,omitempty"`
}

// writeStatView mirrors store.WriteStats in JSON form.
type writeStatView struct {
	ClientBytes  int64 `json:"client_bytes"`
	JournalBytes int64 `json:"journal_bytes"`
	JournalSyncs int64 `json:"journal_syncs"`
	FlattenBytes int64 `json:"flatten_bytes"`
	FillBytes    int64 `json:"fill_bytes"`
	PunchBytes   int64 `json:"punch_bytes"`
}

// intentStatView mirrors store.IntentStats: intent-log traffic and the
// recovery damage report (write intent journal, ideas §2.1).
type intentStatView struct {
	Records    int64 `json:"records"`
	Syncs      int64 `json:"syncs"`
	Checked    int64 `json:"checked"`
	Mismatches int64 `json:"mismatches"`
}

// statusProbeTimeout bounds the liveness fan-out in handleStatus. The
// polygon is a LAN; anything slower is as good as down for the status
// page's purposes.
const statusProbeTimeout = 2 * time.Second

// handleStatus answers the operational overview (plan M6.4): per-OSD
// liveness (fresh ping), weight (0 = evicted by out_time), map epoch,
// per-volume placement health, local disk capacity, and a flat list of
// warnings an operator (or a monitoring poll) can act on. It is
// machine-readable JSON; unistorctl status renders it for humans.
//
// Degraded volume = its placement rows resolve to fewer live targets
// than the volume's min_size: writes fail fast there, reads are served
// from the surviving replicas. Recovery is a phase M4 feature, so
// "recovery lag" is reported as placement gaps only (no backfill yet).
func (a *adminAPI) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var warnings []string

	vols, err := a.cl.ListVolumes(ctx)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "volumes: %v", err)
		return
	}
	epoch := uint32(0)
	if rec, err := meta.LatestMap(ctx, a.ms); err == nil {
		epoch = rec.Epoch
	}
	// Liveness: probe every registered OSD once.
	infos, probes, metaErr := a.cl.ProbeOSDs(ctx, statusProbeTimeout)
	if metaErr != nil {
		warnings = append(warnings, fmt.Sprintf("meta: OSD registry unreadable, liveness unknown: %v", metaErr))
	}
	type osdView struct {
		ID       int32   `json:"id"`
		Addr     string  `json:"addr"`
		CtrlAddr string  `json:"ctrl_addr,omitempty"`
		Weight   float64 `json:"weight"`
		Live     bool    `json:"live"`
		Evicted  bool    `json:"evicted"`
	}
	views := make([]osdView, 0, len(infos))
	liveCount := 0
	for _, info := range infos {
		v := osdView{ID: info.ID, Addr: info.Addr, CtrlAddr: info.CtrlAddr, Weight: info.Weight,
			Live: probes[info.ID] == nil, Evicted: info.Weight <= 0}
		if v.Live {
			liveCount++
		}
		views = append(views, v)
		if !v.Live && !v.Evicted {
			warnings = append(warnings, fmt.Sprintf("osd %d (%s) is not answering pings", info.ID, info.Addr))
		}
		if v.Evicted {
			warnings = append(warnings, fmt.Sprintf("osd %d evicted (weight %g): data needs recovery", info.ID, info.Weight))
		}
	}

	// Placement health per volume.
	type volHealthView struct {
		Name        string `json:"name"`
		LiveTargets int    `json:"live_targets"`
		MinSize     int    `json:"min_size"`
		Writable    bool   `json:"writable"`
	}
	healths := make([]volHealthView, 0, len(vols))
	for _, spec := range vols {
		h, err := a.cl.VolumeHealth(ctx, spec)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("volume %q: %v", spec.Name, err))
			continue
		}
		healths = append(healths, volHealthView{spec.Name, h.LiveTargets, h.MinSize, h.Writable})
		if !h.Writable {
			warnings = append(warnings, fmt.Sprintf("volume %q is degraded: %d live targets < min_size %d — writes fail, reads served", spec.Name, h.LiveTargets, h.MinSize))
		}
	}

	st := map[string]any{
		"volumes":   len(vols),
		"osds":      len(infos),
		"osds_live": liveCount,
		"map_epoch": epoch,
		"meta":      "dir",
		"healthy":   len(warnings) == 0,
		"time":      time.Now().UTC().Format(time.RFC3339),
	}
	if a.raft != nil {
		st["meta"] = "raft"
		st["raft_state"] = a.raft.RaftState()
		st["raft_leader"] = a.raft.Leader()
		st["writable"] = a.raft.RaftState() == "Leader"
		if a.raft.RaftState() != "Leader" && a.raft.RaftState() != "Follower" {
			warnings = append(warnings, "meta raft has no leader: control plane is read-only")
		}
	}
	if len(views) > 0 {
		st["osd_detail"] = views
	}
	if len(healths) > 0 {
		st["volume_health"] = healths
	}
	// Local disk capacity (OSDs hosted in this process only; see
	// diskstats.go). Free-space warnings fire below 10% of the fs.
	if a.disks != nil {
		if ds := a.disks.stats(); len(ds) > 0 {
			for i := range ds {
				d := &ds[i]
				if d.OK {
					d.UsageHuman, d.FreeHuman, d.TotalHuman = humanSize(uint64(d.Usage)), humanSize(uint64(d.Free)), humanSize(uint64(d.Total))
					if d.Total > 0 && d.Free*10 < d.Total {
						warnings = append(warnings, fmt.Sprintf("osd %d disk is low on space: %s free of %s", d.OSD, d.FreeHuman, d.TotalHuman))
					}
				}
				if d.Journal != nil {
					d.Journal.FreeHuman, d.Journal.TotalHuman = humanSize(uint64(d.Journal.Free)), humanSize(uint64(d.Journal.Total))
				}
				for _, w := range d.IntentWarnings {
					warnings = append(warnings, fmt.Sprintf("osd %d: %s", d.OSD, w))
				}
			}
			st["disks"] = ds
		}
	}
	st["healthy"] = len(warnings) == 0
	if len(warnings) > 0 {
		st["warnings"] = warnings
	}
	writeJSON(w, http.StatusOK, st)
}

func (a *adminAPI) handleOSDs(w http.ResponseWriter, r *http.Request) {
	osds, err := meta.ListOSDs(r.Context(), a.ms)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "osds: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, osds)
}

// volumeView is the JSON shape of a volume in API replies: the raw spec
// plus human-formatted size/quota and the current allocation (quota
// usage). A usage read failure degrades to zero rather than failing the
// listing — the spec is the important reply.
type volumeView struct {
	meta.VolumeSpec
	SizeHuman  string `json:"size_human"`
	QuotaHuman string `json:"quota_human,omitempty"`
	Usage      uint64 `json:"usage"`
	UsageHuman string `json:"usage_human"`
}

func (a *adminAPI) volumeView(ctx context.Context, spec meta.VolumeSpec) volumeView {
	nits := (spec.Size + object.Mask) >> object.SizeShift
	usage, err := a.allocUsage(ctx, spec.Name, int(nits))
	if err != nil {
		usage = 0
	}
	view := volumeView{
		VolumeSpec: spec,
		SizeHuman:  humanSize(spec.Size),
		Usage:      usage,
		UsageHuman: humanSize(usage),
	}
	if spec.Quota > 0 {
		view.QuotaHuman = humanSize(spec.Quota)
	}
	return view
}

// allocUsage reads a volume's allocation bitmap popcount (quota
// accounting: allocated objects x object size).
func (a *adminAPI) allocUsage(ctx context.Context, name string, nbits int) (uint64, error) {
	rec, err := meta.GetAllocMap(ctx, a.ms, name, nbits)
	if err != nil {
		return 0, err
	}
	return uint64(meta.AllocMapPopcount(rec.Bits)) * object.Size, nil
}

func (a *adminAPI) handleListVolumes(w http.ResponseWriter, r *http.Request) {
	vols, err := a.cl.ListVolumes(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "volumes: %v", err)
		return
	}
	out := make([]volumeView, 0, len(vols))
	for _, v := range vols {
		out = append(out, a.volumeView(r.Context(), v))
	}
	writeJSON(w, http.StatusOK, out)
}

type createVolumeRequest struct {
	Name     string `json:"name"`
	Size     string `json:"size"` // bytes or 40G form
	Quota    string `json:"quota,omitempty"`
	Replicas int    `json:"replicas,omitempty"`
	MinSize  int    `json:"min_size,omitempty"`
	PGNum    int    `json:"pg_num,omitempty"`
	Rule     string `json:"rule,omitempty"`
}

func (a *adminAPI) handleCreateVolume(w http.ResponseWriter, r *http.Request) {
	var req createVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	name := req.Name
	if name == "" || len(name) > 128 || strings.ContainsAny(name, `/\`) {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	size, err := sizeSuffix(req.Size)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "bad size: %v", err)
		return
	}
	var quota uint64
	if req.Quota != "" {
		if quota, err = sizeSuffix(req.Quota); err != nil {
			jsonError(w, http.StatusBadRequest, "bad quota: %v", err)
			return
		}
	}
	// Fill omitted placement fields from the compiled cluster defaults
	// (the same -pg-num/-replicas/-rule the daemon booted with).
	if req.Replicas == 0 || req.PGNum == 0 || req.Rule == "" {
		if snap, err := a.cl.Refresh(r.Context()); err == nil {
			if req.Replicas == 0 {
				req.Replicas = snap.Replicas
			}
			if req.PGNum == 0 {
				req.PGNum = int(snap.PGNum)
			}
			if req.Rule == "" {
				req.Rule = snap.Rule
			}
		}
	}
	if _, err := meta.GetVolume(r.Context(), a.ms, name); err == nil {
		jsonError(w, http.StatusConflict, "volume %q exists", name)
		return
	}
	spec, err := a.cl.CreateVolume(r.Context(), name, size, quota, req.Replicas, req.MinSize, req.PGNum, req.Rule)
	if err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "create: %v", err)
		return
	}
	a.logger.Printf("admin: created volume %q (%s)", name, humanSize(size))
	writeJSON(w, http.StatusCreated, a.volumeView(r.Context(), spec))
}

func (a *adminAPI) handleGetVolume(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	spec, err := meta.GetVolume(r.Context(), a.ms, name)
	if errors.Is(err, meta.ErrNotFound) {
		jsonError(w, http.StatusNotFound, "volume %q not found", name)
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "get: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, a.volumeView(r.Context(), spec))
}

func (a *adminAPI) handleDeleteVolume(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	if err := a.cl.DeleteVolume(r.Context(), name); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "delete: %v", err)
		return
	}
	a.logger.Printf("admin: deleted volume %q (objects stay on OSDs until vacuum)", name)
	w.WriteHeader(http.StatusNoContent)
}

func (a *adminAPI) handleVolumeQoS(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req meta.VolumeSpec
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	spec, err := a.cl.SetVolumeQoS(r.Context(), name, req)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "qos: %v", err)
		return
	}
	// Open exports of this volume in this process pick the change up via
	// their meta watch; this call only needs to confirm the new spec.
	writeJSON(w, http.StatusOK, a.volumeView(r.Context(), spec))
}

// ---------- quota route ----------

type quotaRequest struct {
	Quota string `json:"quota"` // bytes or 40G form; "0" = cap at volume size
}

// resizeRequest grows a volume online. Shrink is refused (400): data
// in the removed range would strand, and no guest filesystem expects
// its disk to shrink under it.
type resizeRequest struct {
	Size string `json:"size"` // bytes or 40G form; must exceed the current size
}

// handleVolumeQuota edits a volume's allocation cap live (plan M0.4):
// open NBD targets and exports pick the change up through their meta
// watch, the same channel that carries QoS edits. Shrinking never
// touches existing data.
func (a *adminAPI) handleVolumeQuota(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req quotaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	quota, err := sizeSuffix(req.Quota)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "bad quota: %v", err)
		return
	}
	spec, err := a.cl.SetVolumeQuota(r.Context(), name, quota)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		if strings.Contains(err.Error(), "exceeds volume size") {
			jsonError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "quota: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q quota set to %s", name, humanSize(quota))
	writeJSON(w, http.StatusOK, a.volumeView(r.Context(), spec))
}

// handleVolumeResize grows a volume online (grow-only). Open NBD
// targets and exports pick the new size up through their volume watch;
// a fresh NBD handshake serves the new capacity (see resize-export).
func (a *adminAPI) handleVolumeResize(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req resizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	size, err := sizeSuffix(req.Size)
	if err != nil {
		jsonError(w, http.StatusBadRequest, "bad size: %v", err)
		return
	}
	spec, err := a.cl.ResizeVolume(r.Context(), name, size)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		if errors.Is(err, meta.ErrShrinkUnsupported) {
			jsonError(w, http.StatusBadRequest, "shrinking is not supported (volume %q stays at its size)", name)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "resize: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q resized to %s (%d extent(s))", name, humanSize(size), len(spec.Extents))
	writeJSON(w, http.StatusOK, a.volumeView(r.Context(), spec))
}

// ---------- snapshot routes (M3) ----------

// openVolumeVol opens a volume handle for one control-plane call. The
// data plane is not touched: Snapshot/Rollback only flush and swap meta
// (plus hand discarded ranges to the vacuum).
func (a *adminAPI) openVolumeVol(w http.ResponseWriter, r *http.Request, name string) (*client.Volume, bool) {
	vol, err := a.openAdmin(r.Context(), name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return nil, false
		}
		jsonError(w, http.StatusInternalServerError, "open: %v", err)
		return nil, false
	}
	return vol, true
}

// handleSnapshotCreate freezes the volume's current data as a new
// read-only generation (no object data moves). The guest should be
// quiesced by the caller; Snapshot flushes the OSDs first.
func (a *adminAPI) handleSnapshotCreate(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	vol, ok := a.openVolumeVol(w, r, name)
	if !ok {
		return
	}
	defer vol.Close()
	info, err := vol.Snapshot(r.Context())
	if err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		if errors.Is(err, meta.ErrTooManySnapshots) {
			jsonError(w, http.StatusConflict, "volume %q: %v", name, err)
			return
		}
		jsonError(w, http.StatusInternalServerError, "snapshot: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q snapshot gen %d (%d objects)", name, info.Gen, info.Objects)
	writeJSON(w, http.StatusCreated, info)
}

// handleSnapshotList answers GET /v1/volumes/{name}/snapshots.
func (a *adminAPI) handleSnapshotList(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	vol, ok := a.openVolumeVol(w, r, name)
	if !ok {
		return
	}
	defer vol.Close()
	list, err := vol.Snapshots(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "snapshots: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"volume": name, "snapshots": list})
}

// handleSnapshotDelete drops one frozen generation; its private OID
// ranges go to the vacuum. The live data is untouched.
func (a *adminAPI) handleSnapshotDelete(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	gen, err := strconv.ParseUint(r.PathValue("gen"), 10, 32)
	if name == "" || err != nil {
		jsonError(w, http.StatusBadRequest, "bad volume name or gen")
		return
	}
	vol, ok := a.openVolumeVol(w, r, name)
	if !ok {
		return
	}
	defer vol.Close()
	if err := vol.DeleteSnapshot(r.Context(), uint32(gen)); err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		if errors.Is(err, meta.ErrSnapshotNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q has no snapshot gen %d", name, gen)
			return
		}
		jsonError(w, http.StatusInternalServerError, "snapshot rm: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q snapshot gen %d dropped", name, gen)
	writeJSON(w, http.StatusOK, map[string]any{"volume": name, "gen": gen, "dropped": true})
}

// handleRollback makes snapshot gen the live generation: newer
// snapshots are discarded (rolling back discards the future) and every
// dropped OID range goes to the vacuum.
func (a *adminAPI) handleRollback(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req struct {
		Gen uint32 `json:"gen"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	vol, ok := a.openVolumeVol(w, r, name)
	if !ok {
		return
	}
	defer vol.Close()
	spec, err := vol.Rollback(r.Context(), req.Gen)
	if err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		if errors.Is(err, meta.ErrSnapshotNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q has no snapshot gen %d", name, req.Gen)
			return
		}
		jsonError(w, http.StatusInternalServerError, "rollback: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q rolled back to gen %d", name, req.Gen)
	writeJSON(w, http.StatusOK, a.volumeView(r.Context(), spec))
}

// handleClone creates a copy-on-write clone of the volume (plan M3):
// POST /v1/volumes/{name}/clone {"name":"dst","quota":N}. The source
// is flushed and frozen; the clone gets a fresh live range and reads
// the shared chain. 201 with the destination's view on success, 404
// when the source is missing, 409 when the destination name is taken
// or the source chain is at the snapshot limit.
func (a *adminAPI) handleClone(w http.ResponseWriter, r *http.Request) {
	src := volumeName(r)
	if src == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	var req struct {
		Name  string `json:"name"`
		Quota uint64 `json:"quota"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	if req.Name == "" {
		jsonError(w, http.StatusBadRequest, "clone destination name required")
		return
	}
	spec, err := a.cl.CloneVolume(r.Context(), src, req.Name, req.Quota)
	if err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", src)
			return
		}
		if errors.Is(err, meta.ErrVolumeExists) {
			jsonError(w, http.StatusConflict, "clone destination %q: volume already exists", req.Name)
			return
		}
		if errors.Is(err, meta.ErrTooManySnapshots) {
			jsonError(w, http.StatusConflict, "clone %q of %q: source chain at the snapshot limit", req.Name, src)
			return
		}
		jsonError(w, http.StatusInternalServerError, "clone: %v", err)
		return
	}
	a.logger.Printf("admin: volume %q cloned from %q (%d shared gen(s))", req.Name, src, len(spec.Snapshots))
	writeJSON(w, http.StatusCreated, a.volumeView(r.Context(), spec))
}

// ---------- QoS class routes ----------

func (a *adminAPI) handleListClasses(w http.ResponseWriter, r *http.Request) {
	recs, err := a.cl.ListQoSClasses(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "classes: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, recs)
}

func (a *adminAPI) handlePutClass(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad class name")
		return
	}
	var rec meta.QoSClassRecord
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	rec.Name = name
	if err := a.cl.PutQoSClass(r.Context(), rec); err != nil {
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "put: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (a *adminAPI) handleDeleteClass(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad class name")
		return
	}
	if err := a.cl.DeleteQoSClass(r.Context(), name); err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "class %q not found", name)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "delete: %v", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------- byte-stream routes ----------

// streamImport lives in jobs.go (shared with the URL ingest): it
// tracks per-stream bytes and refuses a stream that ends short.

func (a *adminAPI) handleImportBody(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	offset := int64(0)
	if q := r.URL.Query().Get("offset"); q != "" {
		v, err := strconv.ParseInt(q, 10, 64)
		if err != nil || v < 0 {
			jsonError(w, http.StatusBadRequest, "bad offset %q", q)
			return
		}
		offset = v
	}
	vol, err := a.openAdmin(r.Context(), name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "open: %v", err)
		return
	}
	defer vol.Close()
	a.logger.Printf("admin: import %q from body at offset %d", name, offset)
	// A body import has no declared size: the request body itself is
	// the boundary (EOF = done). The per-volume ingest lock guards it
	// too: a body push and a URL ingest interleaving would corrupt.
	lockJob := "body:" + a.owner()
	acquired, cur, lerr := meta.AcquireImportLock(r.Context(), a.ms, name,
		meta.ImportLock{JobID: lockJob, Owner: a.owner(), Heartbeat: time.Now().Unix()})
	if lerr != nil && !meta.IsReadOnly(lerr) {
		jsonError(w, http.StatusInternalServerError, "import lock: %v", lerr)
		return
	}
	if lerr == nil && !acquired && time.Now().Unix()-cur.Heartbeat < meta.ImportStaleSeconds {
		jsonError(w, http.StatusConflict,
			"volume %q is already being imported (job %s)", name, cur.JobID)
		return
	}
	var n int64
	err = streamImport(r.Context(), vol, r.Body, offset, 0, nil, nil, &n, a.importMaxSize)
	if lerr == nil && acquired {
		defer func() { _ = meta.ReleaseImportLock(context.Background(), a.ms, name, lockJob) }()
	}
	if err != nil {
		a.logger.Printf("admin: import %q: %v", name, err)
		jsonError(w, http.StatusInternalServerError, "import: %v", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"volume": name, "written": n, "written_human": humanSize(uint64(n))})
}

// handleExport streams the whole volume as application/octet-stream.
// Over HTTP holes are filled with zeroes (a pipe sink cannot seek);
// Content-Length carries the exact volume size so a downloader can
// sparse-restore the image itself.
func (a *adminAPI) handleExport(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	vol, err := a.openAdmin(r.Context(), name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "open: %v", err)
		return
	}
	defer vol.Close()
	size := vol.Size()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatUint(size, 10))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", name+".img"))
	w.WriteHeader(http.StatusOK)
	a.logger.Printf("admin: export %q (%s) started", name, humanSize(size))
	buf := make([]byte, importChunk)
	var off uint64
	var flushed bool
	for off < size {
		want := uint64(len(buf))
		if size-off < want {
			want = size - off
		}
		n, err := vol.ReadAt(r.Context(), buf[:want], off)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				a.logger.Printf("admin: export %q: client gone at %s: %v", name, humanSize(off), werr)
				return
			}
			off += uint64(n)
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			a.logger.Printf("admin: export %q: read at %d: %v", name, off, err)
			// Headers are gone: end the stream; Content-Length mismatch
			// tells the downloader the image is truncated.
			return
		}
		if !flushed {
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			flushed = true
		}
	}
	a.logger.Printf("admin: export %q done (%s)", name, humanSize(off))
}

// ---------- vacuum (M1.4) ----------

// vacuumView is one pending tombstone for the API.
type vacuumView struct {
	Name       string `json:"name"`
	OIDBase    uint64 `json:"oid_base"`
	NumObjects uint64 `json:"num_objects"`
	DeletedAt  string `json:"deleted_at"`
	DeletedAgo string `json:"deleted_ago"`
}

// handleVacuumList answers GET /v1/vacuum: tombstones that DeleteVolume
// left behind and that no vacuum has vacuumed yet (the objects are still
// occupying space on the OSDs).
func (a *adminAPI) handleVacuumList(w http.ResponseWriter, r *http.Request) {
	recs, err := a.cl.PendingVacuum(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "vacuum: %v", err)
		return
	}
	views := make([]vacuumView, 0, len(recs))
	for _, rec := range recs {
		at := time.Unix(rec.DeletedAt, 0)
		views = append(views, vacuumView{
			Name:       rec.Name,
			OIDBase:    rec.OIDBase,
			NumObjects: rec.NumObjs,
			DeletedAt:  at.UTC().Format(time.RFC3339),
			DeletedAgo: time.Since(at).Truncate(time.Second).String(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"pending": views})
}

// vacuumOSDView tallies one OSD's share of a vacuum pass.
type vacuumOSDView struct {
	OSD     int32  `json:"osd"`
	Removed int    `json:"removed"`
	Err     string `json:"err,omitempty"`
}

// handleVacuumRun runs one vacuum pass now (POST /v1/vacuum). The
// daemon also vacuums on a schedule; this endpoint is the operator's
// "do it now" button. The pass is bounded (one object at a time per
// OSD), so it is safe to hit during production IO.
func (a *adminAPI) handleVacuumRun(w http.ResponseWriter, r *http.Request) {
	res, err := a.cl.VacuumDeletedVolumes(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "vacuum: %v", err)
		return
	}
	views := make([]vacuumOSDView, 0, len(res.OSDs))
	for _, o := range res.OSDs {
		v := vacuumOSDView{OSD: o.OSD, Removed: o.Removed}
		if o.Err != nil {
			v.Err = o.Err.Error()
		}
		views = append(views, v)
	}
	errs := make([]string, 0, len(res.Errs))
	for _, e := range res.Errs {
		errs = append(errs, e.Error())
	}
	a.logger.Printf("admin: vacuum pass: %d tombstones, %d vacuumed, %d objects removed",
		res.Vacuums, res.Deleted, removedTotalOf(res))
	writeJSON(w, http.StatusOK, map[string]any{
		"vacuums": res.Vacuums,
		"deleted": res.Deleted,
		"osds":    views,
		"errors":  errs,
	})
}

func removedTotalOf(res *client.VacuumResult) int {
	n := 0
	for _, o := range res.OSDs {
		n += o.Removed
	}
	return n
}

// handleVacuumVolume is POST /v1/vacuum/{name}: the per-volume "do it
// now" button. A registered volume is vacuumed live — every allocated
// object found to contain only zeroes is punched out on all replicas
// and its allocation released (the outside-in equivalent of the
// guest's fstrim). A name that is not registered is treated as a
// deleted volume: only its tombstone is vacuumed. 404 when neither.
func (a *adminAPI) handleVacuumVolume(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	if _, err := meta.GetVolume(r.Context(), a.cl.Meta(), name); err == nil {
		vol, err := a.openAdmin(r.Context(), name)
		if err != nil {
			jsonError(w, http.StatusInternalServerError, "vacuum: %v", err)
			return
		}
		defer vol.Close()
		res, err := vol.Vacuum(r.Context())
		if err != nil {
			a.logger.Printf("admin: vacuum %q aborted: %v", name, err)
			writeJSON(w, http.StatusOK, map[string]any{
				"volume": name, "mode": "live", "checked": res.Checked,
				"zeroed": res.Zeroed, "freed": res.Freed, "error": err.Error(),
			})
			return
		}
		a.logger.Printf("admin: vacuum %q: checked %d, punched %d, freed %s",
			name, res.Checked, res.Zeroed, humanSize(res.Freed))
		writeJSON(w, http.StatusOK, map[string]any{
			"volume": name, "mode": "live", "checked": res.Checked,
			"zeroed": res.Zeroed, "freed": res.Freed,
			"freed_human": humanSize(res.Freed),
		})
		return
	}
	res, err := a.cl.VacuumVolume(r.Context(), name)
	if err != nil {
		if errors.Is(err, client.ErrNoVacuum) {
			jsonError(w, http.StatusNotFound, "vacuum: %v", err)
			return
		}
		if a.readOnlyGate(w, err) {
			return
		}
		jsonError(w, http.StatusInternalServerError, "vacuum: %v", err)
		return
	}
	views := make([]vacuumOSDView, 0, len(res.OSDs))
	for _, o := range res.OSDs {
		v := vacuumOSDView{OSD: o.OSD, Removed: o.Removed}
		if o.Err != nil {
			v.Err = o.Err.Error()
		}
		views = append(views, v)
	}
	errs := make([]string, 0, len(res.Errs))
	for _, e := range res.Errs {
		errs = append(errs, e.Error())
	}
	a.logger.Printf("admin: vacuum %q: %d tombstone(s), %d vacuumed, %d objects removed",
		name, res.Vacuums, res.Deleted, removedTotalOf(res))
	writeJSON(w, http.StatusOK, map[string]any{
		"volume":  name,
		"mode":    "tombstone",
		"vacuums": res.Vacuums,
		"deleted": res.Deleted,
		"osds":    views,
		"errors":  errs,
	})
}

// ---------- recovery (M6.3) ----------

// recoveryItemView is one copy to make.
type recoveryItemView struct {
	Index  uint64 `json:"index"`
	OID    uint64 `json:"oid"`
	OSD    int32  `json:"osd"`
	Source int32  `json:"source"`
}

// recoveryReportView is one volume's diverged-replica report. Items are
// capped in the listing (a huge repair set is summarised); a single
// volume GET and the repair reply carry them in full.
type recoveryReportView struct {
	Volume    string             `json:"volume"`
	Objects   int                `json:"objects"`
	Healthy   bool               `json:"healthy"`
	Ok        int                `json:"ok"`
	Thin      int                `json:"thin"`
	ToRepair  int                `json:"to_repair"`
	Down      []int32            `json:"down,omitempty"`
	Items     []recoveryItemView `json:"items,omitempty"`
	CheckedAt string             `json:"checked_at"`
}

func recoveryViewOf(rep *client.RecoveryReport, cap int) recoveryReportView {
	v := recoveryReportView{
		Volume:    rep.Volume,
		Objects:   rep.Objects,
		Healthy:   rep.Healthy(),
		Ok:        rep.Ok,
		Thin:      rep.Thin,
		ToRepair:  len(rep.Items),
		Down:      rep.Down,
		CheckedAt: rep.CheckedAt.UTC().Format(time.RFC3339),
	}
	for i, it := range rep.Items {
		if cap > 0 && i >= cap {
			break
		}
		v.Items = append(v.Items, recoveryItemView{it.Index, it.OID, it.OSD, it.Source})
	}
	return v
}

// handleRecoveryScan answers GET /v1/recovery: check every volume for
// diverged/missing replicas. Items are capped per volume in the scan
// (the polygon has small volumes; a full item list comes from the
// per-volume GET). The scan is read-only and safe under load.
func (a *adminAPI) handleRecoveryScan(w http.ResponseWriter, r *http.Request) {
	reps, err := a.cl.RecoveryScan(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "recovery: %v", err)
		return
	}
	const itemCap = 256
	views := make([]recoveryReportView, 0, len(reps))
	healthy := true
	for i := range reps {
		v := recoveryViewOf(&reps[i], itemCap)
		if !v.Healthy {
			healthy = false
		}
		views = append(views, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"healthy": healthy, "volumes": views})
}

// handleRecoveryCheck answers GET /v1/recovery/{name}: full per-object
// repair list for one volume.
func (a *adminAPI) handleRecoveryCheck(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	vol, err := a.openAdmin(r.Context(), name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "recovery: %v", err)
		return
	}
	defer vol.Close()
	rep, err := vol.RecoveryCheck(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "recovery check %q: %v", name, err)
		return
	}
	writeJSON(w, http.StatusOK, recoveryViewOf(rep, 0))
}

// recoveryStatsView is the result of a repair pass.
type recoveryStatsView struct {
	Volume     string `json:"volume"`
	Copied     int    `json:"copied"`
	Bytes      uint64 `json:"bytes"`
	BytesHuman string `json:"bytes_human,omitempty"`
	Failed     int    `json:"failed"`
	Skipped    int    `json:"skipped"`
	Elapsed    string `json:"elapsed"`
}

// handleRecoveryRepair answers POST /v1/recovery/{name}: check the volume
// and copy every diverged/missing object from a live replica. Optional
// query params pace the copier so it never starves guest IO:
//
//	iops  (default 200, 0 = unthrottled admin speed)
//	bps   (default 200M, 0 = unthrottled)
//
// The pass is resumable: copies are durable one at a time, so a repeat
// call continues where it stopped. Returns 200 with the pass stats.
func (a *adminAPI) handleRecoveryRepair(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	q := r.URL.Query()
	iops, bps := uint64(200), uint64(200<<20)
	if s := q.Get("iops"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "iops: %v", err)
			return
		}
		iops = n
	}
	if s := q.Get("bps"); s != "" {
		n, err := sizeSuffix(s)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "bps: %v", err)
			return
		}
		bps = n
	}
	vol, err := a.openAdmin(r.Context(), name)
	if err != nil {
		if errors.Is(err, meta.ErrNotFound) {
			jsonError(w, http.StatusNotFound, "volume %q not found", name)
			return
		}
		jsonError(w, http.StatusInternalServerError, "recovery: %v", err)
		return
	}
	defer vol.Close()
	rep, err := vol.RecoveryCheck(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, "recovery check %q: %v", name, err)
		return
	}
	if len(rep.Items) == 0 {
		writeJSON(w, http.StatusOK, recoveryStatsView{Volume: name, Elapsed: "0s"})
		return
	}
	opts := client.RecoveryOpts{Logf: func(f string, args ...any) {
		a.logger.Printf("recovery "+f, args...)
	}}
	opts.Record = a.recoveryRecord
	opts.Limit = qos.NewPair(0, 0, iops, bps)
	defer opts.Limit.Close()
	st, err := vol.Recover(r.Context(), rep, opts)
	if err != nil {
		a.logger.Printf("admin: recovery %q aborted: %v", name, err)
	}
	a.logger.Printf("admin: recovery %q: copied %d (%s), failed %d, skipped %d",
		name, st.Copied, humanSize(st.Bytes), st.Failed, st.Skipped)
	writeJSON(w, http.StatusOK, recoveryStatsView{
		Volume:     name,
		Copied:     st.Copied,
		Bytes:      st.Bytes,
		BytesHuman: humanSize(st.Bytes),
		Failed:     st.Failed,
		Skipped:    st.Skipped,
		Elapsed:    st.Finished.Sub(st.Started).Truncate(time.Millisecond).String(),
	})
}
