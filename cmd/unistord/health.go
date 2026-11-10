// Cluster health check (todo 6.3, Vitastor 3.1.0 `/healthcheck`): an
// explicit orchestration verdict on top of the informational
// /v1/status. The status page is for humans and pollers; the health
// check answers one machine question — "is the cluster healthy right
// now?" — with a fixed set of named checks, a per-check state, and an
// overall status an orchestrator can branch on (HTTP 200 = ok,
// non-200 = not; unistorctl mirrors it as exit code 0/1).
//
// The endpoint is deliberately unauthenticated (like /healthz): probes
// run inside trusted infrastructure (kubelet-style), and the reply
// carries no secrets — only check names, states and human messages.
//
// Every metric in the reply is a point-in-time read of the cluster
// (meta listing + OSD ping fan-out). That is exactly what Vitastor
// itself notes about statsum: values are "not suitable for monitoring"
// per-sample (3.0: «статистика OSD теперь усреднённая, а не суммарная,
// она не подходит для мониторинга») — for monitoring scrape
// /metrics, which exports the per-OSD counters, histograms and
// averaged-latency pairs. /healthcheck's job is the orchestration
// verdict, not the time series.
package main

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/unistack-org/storage/pkg/meta"
)

// healthcheckProbeTimeout bounds the OSD liveness fan-out. The polygon
// is a LAN: anything slower is as good as down for a verdict.
const healthcheckProbeTimeout = 2 * time.Second

// Check states, ordered by severity: the overall status is the worst
// state among the checks. "ok" and "fail" are the orchestration
// contract; "warn" is advisory (a healthy-enough cluster that an
// operator should look at).
const (
	checkOK   = "ok"
	checkWarn = "warn"
	checkFail = "fail"
)

type healthCheck struct {
	Name    string `json:"name"`
	State   string `json:"state"`
	Message string `json:"message"`
}

// worstState returns the most severe of the given states ("" is
// ignored); empty input is "ok".
func worstState(states ...string) string {
	worst := checkOK
	for _, s := range states {
		switch s {
		case checkFail:
			worst = checkFail
		case checkWarn:
			if worst != checkFail {
				worst = checkWarn
			}
		}
	}
	return worst
}

// runHealthchecks evaluates the fixed check set. Checks are
// independent: a failing meta read degrades only the checks that need
// it, so the reply always names every problem instead of aborting at
// the first one. ctx bounds the fan-out; the caller's request context
// is fine.
func (a *adminAPI) runHealthchecks(ctx context.Context) []healthCheck {
	checks := make([]healthCheck, 0, 8)
	add := func(name, state, format string, args ...any) {
		checks = append(checks, healthCheck{Name: name, State: state, Message: fmt.Sprintf(format, args...)})
	}

	// meta: the control plane must answer at all.
	if _, err := a.cl.ListVolumes(ctx); err != nil {
		add("meta", checkFail, "meta store unreadable: %v", err)
	} else {
		add("meta", checkOK, "meta store reachable")
	}

	// raft: control-plane replication (dir-backed mode has no raft and
	// the check passes trivially).
	if a.raft != nil {
		switch st := a.raft.RaftState(); st {
		case "Leader", "Follower":
			add("raft", checkOK, "replication state %s (leader %s)", st, a.raft.Leader())
		default:
			add("raft", checkFail, "no quorum (state %q): control plane is read-only", st)
		}
	} else {
		add("raft", checkOK, "dir-backed meta, replication not in use")
	}

	// map: a placement map must exist and compile (a corrupt map makes
	// every placement meaningless).
	epoch := uint32(0)
	rec, mapErr := meta.LatestMap(ctx, a.ms)
	if mapErr != nil {
		add("map", checkFail, "no placement map: %v", mapErr)
	} else if _, err := a.cl.Refresh(ctx); err != nil {
		add("map", checkFail, "map epoch %d failed to compile: %v", rec.Epoch, err)
	} else {
		epoch = rec.Epoch
		add("map", checkOK, "map epoch %d compiles", epoch)
	}

	// osds: every registered non-evicted OSD must answer pings.
	infos, probes, metaErr := a.cl.ProbeOSDs(ctx, healthcheckProbeTimeout)
	live, registered, evicted := 0, len(infos), 0
	var down []int32
	for _, info := range infos {
		if info.Weight <= 0 {
			evicted++
			continue
		}
		if probes[info.ID] == nil {
			live++
		} else {
			down = append(down, info.ID)
		}
	}
	switch {
	case metaErr != nil:
		add("osds", checkFail, "OSD registry unreadable: %v", metaErr)
	case registered == 0:
		add("osds", checkWarn, "no OSDs registered")
	case len(down) > 0:
		add("osds", checkFail, "%d of %d OSDs are not answering pings: %v", len(down), registered-evicted, down)
	case evicted > 0:
		add("osds", checkWarn, "all %d active OSDs answer, %d evicted (recovery pending)", live, evicted)
	default:
		add("osds", checkOK, "%d/%d OSDs answer pings", live, registered)
	}

	// volumes: placement must resolve to at least min_size live targets
	// per volume. Evicted OSDs count as not-live (their copies need
	// recovery), mirroring /v1/status.
	vols, err := a.cl.ListVolumes(ctx)
	if err != nil {
		add("volumes", checkFail, "volume listing failed: %v", err)
	} else {
		var degraded []string
		for _, spec := range vols {
			h, err := a.cl.VolumeHealth(ctx, spec)
			if err != nil {
				degraded = append(degraded, fmt.Sprintf("%s: %v", spec.Name, err))
				continue
			}
			if !h.Writable {
				degraded = append(degraded, fmt.Sprintf("%s (%d live < min_size %d)", spec.Name, h.LiveTargets, h.MinSize))
			}
		}
		if len(degraded) > 0 {
			add("volumes", checkFail, "%d of %d volumes degraded: %v", len(degraded), len(vols), degraded)
		} else {
			add("volumes", checkOK, "all %d volumes have full placement", len(vols))
		}
	}

	// disks: local OSDs' free space (the warning threshold matches
	// /v1/status: below 10% of the filesystem is warn).
	if a.disks != nil {
		if ds := a.disks.stats(); len(ds) > 0 {
			var low []string
			for i := range ds {
				d := &ds[i]
				if d.OK {
					d.UsageHuman, d.FreeHuman, d.TotalHuman = humanSize(uint64(d.Usage)), humanSize(uint64(d.Free)), humanSize(uint64(d.Total))
				}
				if !d.OK {
					low = append(low, fmt.Sprintf("osd %d: stats unavailable", d.OSD))
				} else if d.Total > 0 && d.Free*10 < d.Total {
					low = append(low, fmt.Sprintf("osd %d: %s free of %s", d.OSD, d.FreeHuman, d.TotalHuman))
				}
			}
			if len(low) > 0 {
				add("disks", checkWarn, "local OSD disks need attention: %v", low)
			} else {
				add("disks", checkOK, "local OSD disks have space")
			}
		}
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	return checks
}

// handleHealthcheck answers the orchestration verdict. The body always
// lists every check; the HTTP status is the contract: 200 when the
// overall state is ok, 503 when it is fail (warn counts as ok — a
// degraded disk does not stop the cluster).
func (a *adminAPI) handleHealthcheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	checks := a.runHealthchecks(ctx)
	states := make([]string, 0, len(checks))
	for _, c := range checks {
		states = append(states, c.State)
	}
	overall := worstState(states...)
	code := http.StatusOK
	if overall == checkFail {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status": overall,
		"time":   time.Now().UTC().Format(time.RFC3339),
		"checks": checks,
	})
}
