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

	"github.com/unistack-org/sds-nbd/nbd"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
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
}

func newExportHub(cl *client.Cluster, logger *log.Logger, vdm *vduseManager) *exportHub {
	return &exportHub{cl: cl, logger: logger, vols: map[string]*client.Volume{}, vduse: vdm}
}

// setNBD binds the live NBD server (called from serve).
func (h *exportHub) setNBD(s *nbd.Server) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nbdSrv = s
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
		return nbd.ExportInfo{}, false, fmt.Errorf("runtime exports need cluster mode (-cluster)")
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
	srv.AddExport(&nbd.Export{Name: name, Backend: nbd.NewVolumeBackend(vol), ReadOnly: readOnly})
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
		_ = v.Close()
		return nil
	}
	h.mu.Unlock()
	return nil
}

// list answers GET /v1/exports: every live nbd registration plus the
// VDUSE devices (empty on non-Linux).
func (h *exportHub) list() map[string]any {
	out := map[string]any{"nbd": []nbd.ExportInfo{}}
	if s, err := h.liveNBD(); err == nil {
		out["nbd"] = s.ListExports()
	}
	if h.vduse != nil {
		out["vduse"] = h.vduse.list()
	}
	return out
}

// ---------- admin handlers ----------

// exportRequest is the POST /v1/exports body.
type exportRequest struct {
	Volume   string `json:"volume"`
	Type     string `json:"type"`      // "nbd" (default) | "vduse"
	DevName  string `json:"devname"`   // vduse: kernel device name (required)
	ReadOnly bool   `json:"read_only"` // serve the device/export read-only
}

// handleAddExport creates a runtime export. The read_only flag is
// persisted on the volume record (meta is the source of truth), so a
// daemon restart that re-creates exports from -export specs reproduces
// the same access mode.
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
	typ := req.Type
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
		code := http.StatusOK
		if created {
			code = http.StatusCreated
			a.logger.Printf("admin: vduse device %q -> volume %q (ro=%v)", req.DevName, req.Volume, req.ReadOnly)
		}
		writeJSON(w, code, map[string]any{"type": "vduse", "name": req.DevName, "volume": req.Volume,
			"read_only": req.ReadOnly, "size": spec.Size})
	default:
		jsonError(w, http.StatusBadRequest, "type must be nbd or vduse")
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
	switch r.URL.Query().Get("type") {
	case "", "nbd":
		if err := a.exports.removeNBD(name); err != nil {
			a.exportError(w, name, err)
			return
		}
		a.logger.Printf("admin: removed nbd export %q", name)
		writeJSON(w, http.StatusOK, map[string]any{"type": "nbd", "name": name, "removed": true})
	case "vduse":
		if err := a.exports.vduse.remove(name); err != nil {
			a.exportError(w, name, err)
			return
		}
		a.logger.Printf("admin: removed vduse device %q", name)
		writeJSON(w, http.StatusOK, map[string]any{"type": "vduse", "name": name, "removed": true})
	default:
		jsonError(w, http.StatusBadRequest, "type must be nbd or vduse")
	}
}

func (a *adminAPI) handleListExports(w http.ResponseWriter, r *http.Request) {
	if a.exports == nil {
		writeJSON(w, http.StatusOK, map[string]any{"nbd": []nbd.ExportInfo{}, "vduse": []vduseDeviceInfo{}})
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
	case errors.Is(err, errVDUSEUnsupported):
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
