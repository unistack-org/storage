// Runtime export management: the admin API adds and removes NBD/VDUSE
// exports on a running daemon (a control plane — e.g. the unistack
// cell — attaches a volume to a VM without restarting unistord), and
// lists what is live. The hub owns everything a runtime export needs:
// the NBD server registration, the volume handle (kept open for the
// export's lifetime, closed on removal), and the VDUSE device manager.
package main

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
)

// exportHub is the runtime export registry shared by the admin API and
// the data planes. nbd is late-bound: the admin server boots before the
// NBD server (the export setup may block on OSD registration), so
// setNBD wires the live server when serve() creates it.
type exportHub struct {
	cl     *client.Cluster // nil in standalone mode (no cluster volumes)
	logger *log.Logger

	mu     sync.Mutex
	nbdSrv *nbd.Server               // nil until serve() starts the NBD server
	vols   map[string]*client.Volume // runtime nbd exports: open volume handles
	vduse  *vduseManager
	vub    *vubManager

	// metrics feeds the per-export QoS sustained gauges for runtime
	// NBD exports; nil when metrics are disabled (nil-safe methods).
	metrics *clusterMetrics
}

func newExportHub(cl *client.Cluster, logger *log.Logger, vdm *vduseManager, vm *vubManager) *exportHub {
	return &exportHub{cl: cl, logger: logger, vols: map[string]*client.Volume{}, vduse: vdm, vub: vm}
}

// setNBD binds the live NBD server (called from serve).
func (h *exportHub) setNBD(s *nbd.Server) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nbdSrv = s
}

// setMetrics wires the metrics registry so runtime NBD exports keep the
// per-export QoS sustained gauges alive in cluster mode, where exports
// are created only at runtime through the admin API.
func (h *exportHub) setMetrics(m *clusterMetrics) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.metrics = m
}

// setCluster binds the cluster client (the admin's own handle) so
// runtime exports can open volumes. The hub is built before the admin
// (which owns the client), hence the late binding.
func (h *exportHub) setCluster(cl *client.Cluster) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cl = cl
}

func (h *exportHub) clusterClient() *client.Cluster {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cl
}

// liveNBD reports the bound server or a clear "not ready" error (an
// admin request can race the boot when the export setup blocks).
func (h *exportHub) liveNBD() (*nbd.Server, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.nbdSrv == nil {
		return nil, fmt.Errorf("nbd server is not listening yet")
	}
	return h.nbdSrv, nil
}

// addNBD exports a cluster volume over NBD at runtime. It is idempotent:
// re-adding an existing export answers ok with the current registration
// (a control plane retrying after a lost response must not fail).
func (h *exportHub) addNBD(name string, readOnly bool) (nbd.ExportInfo, bool, error) {
	srv, err := h.liveNBD()
	if err != nil {
		return nbd.ExportInfo{}, false, err
	}
	for _, e := range srv.ListExports() {
		if e.Name == name {
			return e, false, nil
		}
	}
	if h.clusterClient() == nil {
		return nbd.ExportInfo{}, false, fmt.Errorf("runtime exports need cluster mode (-meta-dir)")
	}
	vol, err := h.clusterClient().OpenVolume(context.Background(), name)
	if err != nil {
		return nbd.ExportInfo{}, false, err
	}
	h.mu.Lock()
	if _, dup := h.vols[name]; dup {
		h.mu.Unlock()
		vol.Close()
		for _, e := range srv.ListExports() {
			if e.Name == name {
				return e, false, nil
			}
		}
		return nbd.ExportInfo{}, false, fmt.Errorf("export %q is being added", name)
	}
	h.vols[name] = vol
	h.mu.Unlock()
	backend := h.metrics.trackExport(name, vol, nbd.NewVolumeBackend(vol))
	srv.AddExport(&nbd.Export{Name: name, Backend: backend, ReadOnly: readOnly})
	return nbd.ExportInfo{Name: name, ReadOnly: readOnly, Size: vol.Size()}, true, nil
}

// removeNBD unregisters an nbd export and releases its volume handle.
// It refuses (409 semantics) while a client is transmitting.
func (h *exportHub) removeNBD(name string) error {
	srv, err := h.liveNBD()
	if err != nil {
		return err
	}
	if err := srv.RemoveExport(name); err != nil {
		return err
	}
	h.mu.Lock()
	if v, ok := h.vols[name]; ok {
		delete(h.vols, name)
		h.mu.Unlock()
		h.metrics.untrackExport(name)
		_ = v.Close()
		return nil
	}
	h.mu.Unlock()
	return nil
}

// list answers GET /v1/exports: every live nbd registration plus the
// VDUSE devices (empty on non-Linux) and the vhost-user-blk sockets.
func (h *exportHub) list() map[string]any {
	out := map[string]any{"nbd": []nbd.ExportInfo{}}
	if s, err := h.liveNBD(); err == nil {
		out["nbd"] = s.ListExports()
	}
	if h.vduse != nil {
		out["vduse"] = h.vduse.list()
	}
	if h.vub != nil {
		out["vhost-user-blk"] = h.vub.list()
	}
	return out
}

// ---------- admin handlers ----------

// exportRequest is the POST /v1/exports body.
type exportRequest struct {
	Volume   string `json:"volume"`
	Type     string `json:"type"`      // "nbd" (default) | "vduse" | "vhost-user-blk" (legacy alias "vub")
	DevName  string `json:"devname"`   // vduse: kernel device name; vhost-user-blk: export name (optional)
	ReadOnly bool   `json:"read_only"` // serve the device/export read-only
}

// normalizeExportType maps the accepted spellings of a transport onto the
// canonical name used on the wire, in meta records and in replies.
// "vhost-user-blk" is that canonical name (it matches the QEMU device
// name); "vub" is the legacy short form, accepted inbound for callers
// (and meta records) written before the rename. Empty means the default
// transport. An unknown type returns ok=false.
func normalizeExportType(typ string) (string, bool) {
	switch typ {
	case "", "nbd", "vduse", "vhost-user-blk":
		return typ, true
	case "vub", "vhost_user_blk", "vhostuserblk", "vhost-user":
		return "vhost-user-blk", true
	}
	return "", false
}

// handleAddExport creates a runtime export. The read_only flag is
// persisted on the volume record (meta is the source of truth), so a
// daemon restart that replays exports from meta records reproduces the
// same access mode.
func (a *adminAPI) handleAddExport(w http.ResponseWriter, r *http.Request) {
	var req exportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, "bad json: %v", err)
		return
	}
	if req.Volume == "" || !validExportVolumeName(req.Volume) {
		jsonError(w, http.StatusBadRequest, "bad volume name")
		return
	}
	typ, okType := normalizeExportType(req.Type)
	if !okType {
		jsonError(w, http.StatusBadRequest, "type must be nbd, vduse or vhost-user-blk")
		return
	}
	if typ == "" {
		typ = "nbd"
	}
	if a.exports == nil {
		jsonError(w, http.StatusServiceUnavailable, "runtime exports are not enabled (cluster mode required)")
		return
	}
	switch typ {
	case "nbd":
		if req.ReadOnly {
			if _, err := a.cl.SetVolumeFlags(r.Context(), req.Volume, true, false, ""); err != nil {
				if a.readOnlyGate(w, err) {
					return
				}
				jsonError(w, http.StatusInternalServerError, "persist read-only: %v", err)
				return
			}
		}
		info, created, err := a.exports.addNBD(req.Volume, req.ReadOnly)
		if err != nil {
			a.exportError(w, req.Volume, err)
			return
		}
		a.persistExport("nbd", req.Volume, req.Volume, req.ReadOnly)
		if created {
			a.logger.Printf("admin: exported volume %q over nbd (%s, ro=%v)", req.Volume, humanSize(info.Size), info.ReadOnly)
			writeJSON(w, http.StatusCreated, info)
			return
		}
		writeJSON(w, http.StatusOK, info)
	case "vduse":
		if req.DevName == "" {
			jsonError(w, http.StatusBadRequest, "vduse export needs a devname")
			return
		}
		if err := validateDevName(req.DevName); err != nil {
			jsonError(w, http.StatusBadRequest, "%v", err)
			return
		}
		if req.ReadOnly {
			if _, err := a.cl.SetVolumeFlags(r.Context(), req.Volume, true, false, ""); err != nil {
				if a.readOnlyGate(w, err) {
					return
				}
				jsonError(w, http.StatusInternalServerError, "persist read-only: %v", err)
				return
			}
		}
		spec, err := meta.GetVolume(r.Context(), a.ms, req.Volume)
		if err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				jsonError(w, http.StatusNotFound, "volume %q not found", req.Volume)
				return
			}
			jsonError(w, http.StatusInternalServerError, "volume: %v", err)
			return
		}
		created, err := a.exports.vduse.add(req.Volume, req.DevName, a.cl, spec, req.ReadOnly)
		if err != nil {
			a.exportError(w, req.DevName, err)
			return
		}
		a.persistExport("vduse", req.DevName, req.Volume, req.ReadOnly)
		code := http.StatusOK
		if created {
			code = http.StatusCreated
			a.logger.Printf("admin: vduse device %q -> volume %q (ro=%v)", req.DevName, req.Volume, req.ReadOnly)
		}
		writeJSON(w, code, map[string]any{"type": "vduse", "name": req.DevName, "volume": req.Volume,
			"read_only": req.ReadOnly, "size": spec.Size})
	case "vhost-user-blk":
		if a.exports.vub == nil {
			jsonError(w, http.StatusServiceUnavailable, "vhost-user-blk exports are not enabled")
			return
		}
		if req.ReadOnly {
			if _, err := a.cl.SetVolumeFlags(r.Context(), req.Volume, true, false, ""); err != nil {
				if a.readOnlyGate(w, err) {
					return
				}
				jsonError(w, http.StatusInternalServerError, "persist read-only: %v", err)
				return
			}
		}
		spec, err := meta.GetVolume(r.Context(), a.ms, req.Volume)
		if err != nil {
			if errors.Is(err, meta.ErrNotFound) {
				jsonError(w, http.StatusNotFound, "volume %q not found", req.Volume)
				return
			}
			jsonError(w, http.StatusInternalServerError, "volume: %v", err)
			return
		}
		name := req.DevName
		if name == "" {
			name = req.Volume
		}
		info, created, err := a.exports.vub.add(req.Volume, name, a.cl, spec, req.ReadOnly)
		if err != nil {
			a.exportError(w, name, err)
			return
		}
		a.persistExport("vhost-user-blk", info.Name, req.Volume, req.ReadOnly)
		code := http.StatusOK
		if created {
			code = http.StatusCreated
			a.logger.Printf("admin: vhost-user-blk socket %s -> volume %q (%s, ro=%v)", info.Socket, req.Volume, humanSize(info.Size), req.ReadOnly)
		}
		writeJSON(w, code, map[string]any{"type": "vhost-user-blk", "name": info.Name, "socket": info.Socket,
			"volume": req.Volume, "read_only": req.ReadOnly, "size": info.Size})
	default:
		jsonError(w, http.StatusBadRequest, "type must be nbd, vduse or vhost-user-blk")
	}
}

// handleDeleteExport removes a runtime export: ?type=nbd (default,
// {name} is the export name) or ?type=vduse ({name} is the devname).
func (a *adminAPI) handleDeleteExport(w http.ResponseWriter, r *http.Request) {
	name := volumeName(r)
	if name == "" {
		jsonError(w, http.StatusBadRequest, "bad export name")
		return
	}
	if a.exports == nil {
		jsonError(w, http.StatusServiceUnavailable, "runtime exports are not enabled (cluster mode required)")
		return
	}
	typ, okType := normalizeExportType(r.URL.Query().Get("type"))
	if !okType {
		jsonError(w, http.StatusBadRequest, "type must be nbd, vduse or vhost-user-blk")
		return
	}
	switch typ {
	case "", "nbd":
		if err := a.exports.removeNBD(name); err != nil {
			a.exportError(w, name, err)
			return
		}
		a.forgetExport("nbd", name)
		a.logger.Printf("admin: removed nbd export %q", name)
		writeJSON(w, http.StatusOK, map[string]any{"type": "nbd", "name": name, "removed": true})
	case "vduse":
		if err := a.exports.vduse.remove(name); err != nil {
			a.exportError(w, name, err)
			return
		}
		a.forgetExport("vduse", name)
		a.logger.Printf("admin: removed vduse device %q", name)
		writeJSON(w, http.StatusOK, map[string]any{"type": "vduse", "name": name, "removed": true})
	case "vhost-user-blk":
		if a.exports.vub == nil {
			jsonError(w, http.StatusServiceUnavailable, "vhost-user-blk exports are not enabled")
			return
		}
		if err := a.exports.vub.remove(name); err != nil {
			a.exportError(w, name, err)
			return
		}
		// Records written before the rename carry the legacy short name;
		// forget both spellings so the old key never survives.
		a.forgetExport("vhost-user-blk", name)
		a.forgetExport("vub", name)
		a.logger.Printf("admin: removed vhost-user-blk export %q", name)
		writeJSON(w, http.StatusOK, map[string]any{"type": "vhost-user-blk", "name": name, "removed": true})
	default:
		jsonError(w, http.StatusBadRequest, "type must be nbd, vduse or vhost-user-blk")
	}
}

// ---------- durable export records (gap: restart survival) ----------

// persistExport records a live export in meta so a daemon restart can
// replay it (attach is not lost when unistord restarts under a running
// VM). The record is a reconstruction hint, not the truth: failures are
// logged, never fatal to the live export the caller just created.
func (a *adminAPI) persistExport(typ, name, volume string, readOnly bool) {
	_, err := meta.PutExportRecord(context.Background(), a.ms, meta.ExportRecord{
		Type: typ, Name: name, Volume: volume, ReadOnly: readOnly,
		CreatedAt: time.Now().Unix(),
	})
	if err != nil && !meta.IsReadOnly(err) {
		a.logger.Printf("admin: persist export %s:%s: %v", typ, name, err)
	}
}

// forgetExport drops the record; a missing record is fine.
func (a *adminAPI) forgetExport(typ, name string) {
	if err := meta.DeleteExportRecord(context.Background(), a.ms, typ, name); err != nil && !meta.IsReadOnly(err) && !errors.Is(err, meta.ErrNotFound) {
		a.logger.Printf("admin: forget export %s:%s: %v", typ, name, err)
	}
}

// replayExports re-creates the recorded exports after a restart. It is
// best-effort and idempotent (add* paths return created=false when the
// export is already live, e.g. a boot -vduse/-vhost-user-blk device
// created before replay started): NBD waits for the server to listen,
// VDUSE and VUB run right away. A volume that is gone (deleted while
// the daemon was down) drops its record.
func (a *adminAPI) replayExports(ctx context.Context) {
	if a.exports == nil || a.ms == nil {
		return
	}
	recs, err := meta.ListExportRecords(ctx, a.ms)
	if err != nil {
		a.logger.Printf("admin: export replay: list: %v", err)
		return
	}
	if len(recs) == 0 {
		return
	}
	// NBD exports need the listening server; serve() binds it after
	// the admin starts, so wait (bounded) for it.
	nbdReady := make(chan struct{})
	go func() {
		for i := 0; i < 600; i++ {
			if _, err := a.exports.liveNBD(); err == nil {
				close(nbdReady)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	}()
	for _, rec := range recs {
		if ctx.Err() != nil {
			return
		}
		spec, err := meta.GetVolume(ctx, a.ms, rec.Volume)
		if errors.Is(err, meta.ErrNotFound) {
			a.logger.Printf("admin: export replay: volume %q gone, dropping %s:%s", rec.Volume, rec.Type, rec.Name)
			a.forgetExport(rec.Type, rec.Name)
			continue
		}
		if err != nil {
			a.logger.Printf("admin: export replay: volume %q: %v", rec.Volume, err)
			continue
		}
		switch rec.Type {
		case "nbd":
			select {
			case <-nbdReady:
			case <-ctx.Done():
				return
			}
			_, created, err := a.exports.addNBD(rec.Name, rec.ReadOnly)
			if err != nil {
				a.logger.Printf("admin: export replay: nbd %q: %v", rec.Name, err)
			} else if created {
				a.logger.Printf("admin: export replay: nbd %q restored (ro=%v)", rec.Name, rec.ReadOnly)
			}
		case "vduse":
			if a.exports.vduse == nil {
				continue
			}
			created, err := a.exports.vduse.add(rec.Volume, rec.Name, a.cl, spec, rec.ReadOnly)
			if err != nil {
				a.logger.Printf("admin: export replay: vduse %q: %v", rec.Name, err)
			} else if created {
				a.logger.Printf("admin: export replay: vduse %q -> %q restored (ro=%v)", rec.Name, rec.Volume, rec.ReadOnly)
			}
		case "vhost-user-blk", "vub": // "vub" = records written before the rename
			if a.exports.vub == nil {
				continue
			}
			_, created, err := a.exports.vub.add(rec.Volume, rec.Name, a.cl, spec, rec.ReadOnly)
			if err != nil {
				a.logger.Printf("admin: export replay: vhost-user-blk %q: %v", rec.Name, err)
			} else if created {
				a.logger.Printf("admin: export replay: vhost-user-blk %q -> %q restored (ro=%v)", rec.Name, rec.Volume, rec.ReadOnly)
			}
			if rec.Type == "vub" {
				// Migrate the legacy record onto the canonical type name.
				a.persistExport("vhost-user-blk", rec.Name, rec.Volume, rec.ReadOnly)
				a.forgetExport("vub", rec.Name)
			}
		}
	}
}

func (a *adminAPI) handleListExports(w http.ResponseWriter, r *http.Request) {
	if a.exports == nil {
		writeJSON(w, http.StatusOK, map[string]any{"nbd": []nbd.ExportInfo{}, "vduse": []vduseDeviceInfo{}, "vhost-user-blk": []vubDeviceInfo{}})
		return
	}
	writeJSON(w, http.StatusOK, a.exports.list())
}

// exportError maps hub errors onto HTTP codes: busy → 409 (the caller
// must detach the VM first), not-found → 404, quorum → 503, platform →
// 501, everything else 500.
func (a *adminAPI) exportError(w http.ResponseWriter, name string, err error) {
	switch {
	case a.readOnlyGate(w, err):
	case errors.Is(err, nbd.ErrExportBusy):
		jsonError(w, http.StatusConflict, "export %q is in use", name)
	case errors.Is(err, meta.ErrNotFound):
		jsonError(w, http.StatusNotFound, "%v", err)
	case errors.Is(err, errVDUSEUnsupported), errors.Is(err, errVDUSEKernelAbsent):
		jsonError(w, http.StatusNotImplemented, "%v", err)
	case strings.HasPrefix(err.Error(), "nbd: export") && strings.HasSuffix(err.Error(), "not found"):
		jsonError(w, http.StatusNotFound, "%v", err)
	default:
		jsonError(w, http.StatusInternalServerError, "%v", err)
	}
}

// ---------- bearer auth ----------

// bearerGate wraps the admin mux: when a token is configured every
// endpoint except /healthz demands "Authorization: Bearer <token>".
// The comparison is constant-time.
func (a *adminAPI) bearerGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		const prefix = "Bearer "
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, prefix) ||
			!hmac.Equal([]byte(strings.TrimPrefix(h, prefix)), []byte(a.token)) {
			jsonError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// validExportVolumeName validates a volume name coming in a JSON body
// (the path-segment validator volumeName reads r.PathValue).
func validExportVolumeName(name string) bool {
	if name == "" || len(name) > 128 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.'
		if !ok {
			return false
		}
	}
	return name != "." && name != ".." && !strings.HasSuffix(name, ".json")
}
