// Unified reload: one path that re-applies everything the daemon can
// adopt at runtime, not only the certificate material. SIGHUP,
// POST /v1/reload and the certificate watcher all funnel here, so an
// operator has a single answer to "how do I make the daemon pick up my
// config change".
//
// The reloadable surface is the daemon's declarative start: the flags
// that describe cluster state (-qos-class, -crushmap, -create) plus the
// certificate files and the GC tunable. Every section is independent and
// keep-last-good: a broken crushmap file never stops the certificates
// from being re-read, and a failed section never takes the running
// cluster down. What a reload cannot do is re-read the flags that shape
// the process itself (listeners, storage backends, OSD lifecycle, the CA
// file, the admin token): those still need a restart, and the endpoint
// reports them.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/tlsconf"
)

// declarativeSpec is the part of the command line a running daemon can
// re-apply: the cluster state the operator declared at start. The
// placement fields keep the flag sentinels (0/"") and are filled from
// the cluster snapshot when a create needs them, exactly like at boot.
type declarativeSpec struct {
	crushmap   string   // -crushmap path ("" = nothing to publish)
	epoch      uint32   // -epoch (0 = auto)
	pgNum      int      // -pg-num
	replicas   int      // -replicas
	minSize    int      // -min-size
	rule       string   // -rule
	creates    []string // -create specs
	qosClasses []string // -qos-class specs
	gogc       int      // -gogc (0 = leave the runtime default alone)
}

// specFromParams builds the declarative view of a boot parameter set, so
// boot and reload run the same sections over the same data.
func specFromParams(p clusterParams) declarativeSpec {
	return declarativeSpec{
		crushmap:   p.crushmap,
		epoch:      p.epoch,
		pgNum:      p.pgNum,
		replicas:   p.replicas,
		minSize:    p.minSize,
		rule:       p.rule,
		creates:    append([]string(nil), p.creates...),
		qosClasses: append([]string(nil), p.qosClasses...),
	}
}

// Reloadable section names, reported in this order.
const (
	sectionTLS     = "tls"
	sectionQoS     = "qos"
	sectionCrush   = "crushmap"
	sectionVolumes = "volumes"
	sectionGC      = "gogc"
)

// sectionResult is the per-section outcome of one reload.
type sectionResult struct {
	Name    string `json:"name"`
	Applied bool   `json:"applied"`
	Detail  string `json:"detail,omitempty"`
	Error   string `json:"error,omitempty"`
}

// reloadReport is what POST /v1/reload answers and what the SIGHUP
// handler logs: one entry per section plus the overall verdict.
type reloadReport struct {
	At     time.Time `json:"at"`
	Reason string    `json:"reason,omitempty"`
	OK     bool      `json:"ok"`
	// Sections lists every section the daemon knows, applied or not: an
	// operator reading the reply sees what a reload covered.
	Sections []sectionResult `json:"sections"`
	// RestartRequired lists what a reload cannot adopt, so an operator
	// who changed one of those flags is told to restart instead of
	// silently wondering why nothing moved.
	RestartRequired []string `json:"restart_required,omitempty"`
}

func (r reloadReport) failed() []string {
	var out []string
	for _, s := range r.Sections {
		if s.Error != "" {
			out = append(out, s.Name+": "+s.Error)
		}
	}
	return out
}

// String renders the report for the log (SIGHUP has no HTTP reply).
func (r reloadReport) String() string {
	parts := make([]string, 0, len(r.Sections))
	for _, s := range r.Sections {
		switch {
		case s.Error != "":
			parts = append(parts, s.Name+"=FAILED ("+s.Error+")")
		case s.Applied:
			parts = append(parts, s.Name+"=ok ("+s.Detail+")")
		default:
			parts = append(parts, s.Name+"=skipped ("+s.Detail+")")
		}
	}
	return strings.Join(parts, ", ")
}

// reloadState is the daemon's reload coordinator: the certificate state
// plus the declarative spec and the cluster handles needed to re-apply
// it. A state without a cluster (nil cl/ms: standalone mode) reloads
// only what it has, the certificates and the GC knob.
//
// Reloads are serialised: two SIGHUPs, or an HTTP reload racing the
// watcher, must not interleave two crushmap publishes.
type reloadState struct {
	tls    *tlsSetup
	spec   *declarativeSpec // nil = no declarative sections at all
	cl     *client.Cluster
	ms     meta.Store
	logger *log.Logger

	mu      sync.Mutex // serialises reloadAll and the last-report view
	count   int64
	lastAt  time.Time
	lastOK  bool
	lastErr string
}

// newReloadState builds the coordinator over the certificate state.
// gogc is the -gogc flag value (0 = the daemon does not manage GC).
func newReloadState(tlsState *tlsSetup, gogc int, logger *log.Logger) *reloadState {
	return &reloadState{tls: tlsState, spec: &declarativeSpec{gogc: gogc}, logger: logger}
}

// enableCluster arms the declarative sections: from now on a reload
// re-applies the QoS classes, the crush map and the declared volumes.
// cl is a dedicated cluster client, so a reload never disturbs the
// handle the exports and the admin API are using.
func (r *reloadState) enableCluster(spec declarativeSpec, cl *client.Cluster, ms meta.Store) {
	spec.gogc = r.spec.gogc
	r.spec = &spec
	r.cl = cl
	r.ms = ms
}

// reloadView is the snapshot /v1/status and the metrics family report.
type reloadView struct {
	Enabled bool `json:"enabled"`
	// Declarative is true when the cluster sections (qos, crushmap,
	// volumes) are part of a reload; false in standalone mode.
	Declarative bool   `json:"declarative"`
	Reloads     int64  `json:"reloads"`
	LastOK      bool   `json:"last_ok"`
	LastAt      string `json:"last_at,omitempty"`
	LastError   string `json:"last_error,omitempty"`
}

func (r *reloadState) view() reloadView {
	if r == nil {
		return reloadView{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	v := reloadView{
		Enabled:     r.tls != nil || r.spec != nil,
		Declarative: r.cl != nil && r.ms != nil,
		Reloads:     r.count,
		LastOK:      r.lastOK,
		LastError:   r.lastErr,
	}
	if !r.lastAt.IsZero() {
		v.LastAt = r.lastAt.UTC().Format(time.RFC3339)
	}
	return v
}

// metaWriter runs a meta write that has to land, retrying while the
// store reports read-only (a raft node without a quorum, a permanent
// follower, an election in progress). Boot and reload share it, which is
// why a reload fired a second after a leader election converges instead
// of answering "no quorum".
type metaWriter func(what string, fn func() error) error

func (r *reloadState) retryWriter(ctx context.Context) metaWriter {
	return func(what string, fn func() error) error {
		for attempt := 0; ; attempt++ {
			err := fn()
			if err == nil {
				return nil
			}
			if !meta.IsReadOnly(err) {
				return err
			}
			if attempt == 0 {
				r.logger.Printf("reload: %s: meta is read-only for now (waiting for a raft leader)", what)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
		}
	}
}

// reloadAll runs every section the daemon knows. It always returns a
// report: a section that failed is reported, not fatal, and the
// remaining sections still run. reason names the trigger (signal, admin
// URL, watcher) for the log.
func (r *reloadState) reloadAll(ctx context.Context, reason string) reloadReport {
	rep := reloadReport{At: time.Now().UTC(), Reason: reason}
	if r == nil {
		rep.OK = true
		return rep
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	add := func(name string, applied bool, detail string, err error) {
		s := sectionResult{Name: name, Applied: applied, Detail: detail}
		if err != nil {
			s.Error = err.Error()
		}
		rep.Sections = append(rep.Sections, s)
	}

	// Certificates first: raft, the admin API and the data plane all run
	// on top of them, and this is the section an operator is most often
	// after.
	if r.tls == nil {
		add(sectionTLS, false, "the daemon runs without -tls-cert", nil)
	} else if err := r.tls.Reload(); err != nil {
		add(sectionTLS, false, "previous material stays in effect", err)
	} else {
		add(sectionTLS, true, tlsSummary(r.tls.Status()), nil)
	}

	if r.cl == nil || r.ms == nil || r.spec == nil {
		// A daemon without -meta-dir (standalone exports) has no cluster
		// state to re-apply: the sections are reported, not silently
		// dropped, so the reply always shows what a reload covers.
		const why = "not applied: the daemon runs without -meta-dir"
		add(sectionQoS, false, why, nil)
		add(sectionCrush, false, why, nil)
		add(sectionVolumes, false, why, nil)
		rep.RestartRequired = []string{"no cluster (-meta-dir): -qos-class, -crushmap and -create are not applied here"}
	} else {
		writeMeta := r.retryWriter(ctx)
		logf := func(f string, a ...any) { r.logger.Printf(f, a...) }

		if len(r.spec.qosClasses) == 0 {
			add(sectionQoS, false, "the daemon runs without -qos-class", nil)
		} else {
			detail, err := applyQoSClasses(ctx, r.cl, r.spec, writeMeta, logf)
			add(sectionQoS, err == nil && detail != "", detail, err)
		}

		if r.spec.crushmap == "" {
			add(sectionCrush, false, "the daemon runs without -crushmap (publish a map with \"unistorctl map publish\")", nil)
		} else {
			validate := newMapValidator(r.cl, r.spec)
			detail, applied, err := applyCrushmap(ctx, r.cl, r.ms, r.spec, writeMeta, logf, publishReload, validate)
			add(sectionCrush, applied, detail, err)
		}

		if len(r.spec.creates) == 0 {
			add(sectionVolumes, false, "the daemon runs without -create", nil)
		} else {
			detail, err := applyCreates(ctx, r.cl, r.ms, r.spec, writeMeta, logf)
			add(sectionVolumes, err == nil && detail != "", detail, err)
		}

		rep.RestartRequired = restartRequired(r.spec)
	}

	switch {
	case r.spec == nil || r.spec.gogc <= 0:
		add(sectionGC, false, "the daemon does not manage GOGC (-gogc 0)", nil)
	default:
		debug.SetGCPercent(r.spec.gogc)
		add(sectionGC, true, fmt.Sprintf("GOGC=%d", r.spec.gogc), nil)
	}

	rep.OK = len(rep.failed()) == 0
	r.count++
	r.lastAt = rep.At
	r.lastOK = rep.OK
	if fails := rep.failed(); len(fails) > 0 {
		r.lastErr = strings.Join(fails, "; ")
	} else {
		r.lastErr = ""
	}
	return rep
}

// tlsSummary names the live certificate material for the reload report.
func tlsSummary(st *tlsconf.Status) string {
	if st == nil {
		return "TLS is off"
	}
	if st.NotAfter.IsZero() {
		return "certificate loaded"
	}
	return fmt.Sprintf("%s valid to %s", st.Subject, st.NotAfter.UTC().Format(time.RFC3339))
}

// restartRequired names the flags a reload cannot adopt. It is advice
// for the operator, not a verdict: the daemon keeps no copy of its old
// command line, so a changed value cannot be detected and the list is
// what those flags are in general.
func restartRequired(spec *declarativeSpec) []string {
	out := []string{
		"listeners and addresses (-http, -metrics, -listen, -raft, -pprof)",
		"storage backends and OSD lifecycle (-store, -direct, -prealloc, -commit, -fsync-batch, -osd, -data-dir, -atomic-*)",
		"certificate authority (-tls-ca, -tls-client-ca-file), -tls-planes and -tls-skip-verify",
		"-admin-token",
	}
	if spec != nil && spec.crushmap == "" {
		out = append(out, "no -crushmap file: publish a map with \"unistorctl map publish\"")
	}
	return out
}

// applyQoSClasses registers (or refreshes) every -qos-class so volumes
// can reference it by name. Re-registering is the live-change path: the
// limiters pick the class up from the meta watch, so a rate edited here
// takes effect on already-open volumes.
func applyQoSClasses(ctx context.Context, cl *client.Cluster, spec *declarativeSpec, writeMeta metaWriter, logf func(string, ...any)) (string, error) {
	if len(spec.qosClasses) == 0 {
		return "", nil
	}
	names := make([]string, 0, len(spec.qosClasses))
	for _, cls := range spec.qosClasses {
		name, rates, err := parseQoSNamed(cls)
		if err != nil {
			return "", fmt.Errorf("-qos-class %q: %w", cls, err)
		}
		riops, rbps, wiops, wbps, err := parseQoSRates(rates)
		if err != nil {
			return "", fmt.Errorf("-qos-class %q: %w", cls, err)
		}
		if err := writeMeta("qos class "+name, func() error {
			return cl.PutQoSClass(ctx, meta.QoSClassRecord{
				Name: name, ReadIOPS: riops, ReadBPS: rbps, WriteIOPS: wiops, WriteBPS: wbps,
			})
		}); err != nil {
			return "", fmt.Errorf("qos class %q: %w", name, err)
		}
		logf("qos class %q: r=%d/%s w=%d/%s", name, riops, humanSize(rbps), wiops, humanSize(wbps))
		names = append(names, name)
	}
	return fmt.Sprintf("%d class(es): %s", len(names), strings.Join(names, ", ")), nil
}

// mapValidator checks a crush map before it reaches meta. Boot has its own
// safety net: a map that cannot be compiled fails the startup placement check
// and the daemon refuses to start. Reload has no such net, so it validates
// here -- a published epoch is consumed by every writer in the cluster, and
// an unusable one is exactly the kind of damage a reload must not do.
type mapValidator func(ctx context.Context, src []byte) error

// newMapValidator builds the gate: the crush AST check (the same one
// "unistorctl map check" runs) plus a real compile against the live OSD
// registry. The compile is skipped when the daemon was started without
// placement options (-pgnum/-replicas), because there it is not what the
// data path uses.
func newMapValidator(cl *client.Cluster, spec *declarativeSpec) mapValidator {
	return func(ctx context.Context, src []byte) error {
		// The messages carry no "crushmap:" prefix: the reload report
		// already labels them with the section name.
		ast, err := placement.DecodeMap(src)
		if err != nil {
			return err
		}
		verdict := ast.Check()
		if !verdict.OK() {
			return fmt.Errorf("%d structural error(s): %s", len(verdict.Errors), strings.Join(verdict.Errors, "; "))
		}
		if cl == nil || spec.pgNum == 0 || spec.replicas <= 0 {
			return nil
		}
		reg, err := cl.Registry(ctx)
		if err != nil {
			return fmt.Errorf("osd registry: %w", err)
		}
		if len(reg) == 0 {
			// No OSD has registered yet: the compile would fail for that
			// reason alone, and a map is exactly what an operator needs to
			// publish during an outage. Structural check only.
			return nil
		}
		if _, err := placement.Compile(0, src, reg, placement.Options{PGNum: uint32(spec.pgNum), Replicas: spec.replicas, Rule: spec.rule}); err != nil {
			return err
		}
		return nil
	}
}

// publishMode is how the crushmap section chooses the epoch.
type publishMode int

const (
	// publishBoot is the start-time rule: -epoch 0 means 1 on a fresh
	// cluster and "a map already exists, keep it" on a running one, so an
	// idempotent restart never overwrites the live placement.
	publishBoot publishMode = iota
	// publishReload is the reload rule: -epoch 0 means the newest
	// generation of the operator's file, latest+1 when the content
	// changed and a no-op when it did not. This is what makes "edit the
	// crushmap file, hit /v1/reload" work; an explicit -epoch publishes
	// that exact epoch when it is newer than the latest.
	publishReload
)

// applyCrushmap publishes the -crushmap file over the meta write path.
// It reports whether the map was published: "the file did not change" and
// "a map is already live" are outcomes, not applications. A non-nil validate
// runs on the bytes about to be published.
func applyCrushmap(ctx context.Context, cl *client.Cluster, ms meta.Store, spec *declarativeSpec, writeMeta metaWriter, logf func(string, ...any), mode publishMode, validate mapValidator) (string, bool, error) {
	if spec.crushmap == "" {
		return "", false, nil
	}
	src, err := os.ReadFile(spec.crushmap)
	if err != nil {
		return "", false, fmt.Errorf("crushmap: %w", err)
	}
	rec, err := meta.LatestMap(ctx, ms)
	have := err == nil
	if !have && !errors.Is(err, meta.ErrNotFound) {
		return "", false, fmt.Errorf("crushmap: read latest epoch: %w", err)
	}

	var pubEpoch uint32
	switch {
	case spec.epoch == 0 && mode == publishBoot && have:
		logf("crushmap already published (latest epoch %d): keeping it", rec.Epoch)
		return fmt.Sprintf("keeping the live map (epoch %d)", rec.Epoch), false, nil
	case spec.epoch == 0 && mode == publishBoot:
		pubEpoch = 1
	case spec.epoch == 0 && mode == publishReload && have:
		if string(rec.Map) == string(src) {
			return fmt.Sprintf("unchanged (latest epoch %d)", rec.Epoch), false, nil
		}
		pubEpoch = rec.Epoch + 1
	case spec.epoch == 0:
		pubEpoch = 1
	default:
		pubEpoch = spec.epoch
		if have && rec.Epoch >= pubEpoch {
			logf("crushmap epoch %d already present (latest %d)", pubEpoch, rec.Epoch)
			return fmt.Sprintf("epoch %d already present (latest %d)", pubEpoch, rec.Epoch), false, nil
		}
	}
	if validate != nil {
		if err := validate(ctx, src); err != nil {
			logf("crushmap epoch %d NOT published: %v", pubEpoch, err)
			return "", false, err
		}
	}
	if err := writeMeta(fmt.Sprintf("crushmap epoch %d", pubEpoch), func() error {
		return cl.MapEpoch(ctx, pubEpoch, src)
	}); err != nil && !errors.Is(err, meta.ErrConflict) {
		return "", false, fmt.Errorf("publish map epoch %d: %w", pubEpoch, err)
	}
	logf("published crushmap epoch %d (%d bytes)", pubEpoch, len(src))
	return fmt.Sprintf("published epoch %d (%d bytes)", pubEpoch, len(src)), true, nil
}

// applyCreates runs the -create declarations. A volume that already
// exists is skipped, so the section is idempotent: a reload never
// resizes, never re-places and never deletes.
func applyCreates(ctx context.Context, cl *client.Cluster, ms meta.Store, spec *declarativeSpec, writeMeta metaWriter, logf func(string, ...any)) (string, error) {
	if len(spec.creates) == 0 {
		return "", nil
	}
	// The flag values are 0/"" sentinels when unset: without a pool the
	// compiled cluster snapshot fills them (built-in defaults when the
	// daemon booted flagless); with a pool the pool record does, and the
	// flags win over the pool.
	ensureDefaults := func() {
		if spec.replicas != 0 && spec.pgNum != 0 {
			return
		}
		if snap, serr := cl.Refresh(ctx); serr == nil {
			if spec.replicas == 0 {
				spec.replicas = snap.Replicas
			}
			if spec.pgNum == 0 {
				spec.pgNum = int(snap.PGNum)
			}
			if spec.rule == "" {
				spec.rule = snap.Rule
			}
		}
	}
	var created, existing []string
	for _, decl := range spec.creates {
		pool, name, sizeStr, perr := parseCreateSpec(decl)
		if perr != nil {
			return "", fmt.Errorf("-create %q: %w", decl, perr)
		}
		size, err := sizeSuffix(sizeStr)
		if err != nil {
			return "", fmt.Errorf("-create %q: %w", decl, err)
		}
		if _, err := meta.GetVolume(ctx, ms, name); err == nil {
			existing = append(existing, name)
			continue
		}
		var vspec meta.VolumeSpec
		if err := writeMeta("create volume "+name, func() error {
			var cerr error
			if pool == "" {
				ensureDefaults()
			}
			if pool != "" {
				vspec, cerr = cl.CreateVolumeInPool(ctx, pool, name, size, 0, spec.replicas, spec.minSize, spec.pgNum, spec.rule)
			} else {
				vspec, cerr = cl.CreateVolume(ctx, name, size, 0, spec.replicas, spec.minSize, spec.pgNum, spec.rule)
			}
			return cerr
		}); err != nil {
			return "", fmt.Errorf("create volume %q: %w", name, err)
		}
		poolTag := ""
		if vspec.Pool != "" {
			poolTag = " pool=" + vspec.Pool
		}
		logf("created volume %q (%s, r=%d, min_size=%d, pg=%d)%s", name, humanSize(size), vspec.Replicas, vspec.MinSize, vspec.PGNum, poolTag)
		created = append(created, name)
	}
	detail := fmt.Sprintf("%d declared, %d new", len(spec.creates), len(created))
	if len(existing) > 0 {
		detail += ", kept " + strings.Join(existing, ", ")
	}
	return detail, nil
}

// signalReload reloads everything on SIGHUP. systemd sends no SIGHUP on
// its own, so the operator's command is `systemctl kill -s HUP unistord`;
// POST /v1/reload on the admin API is the other trigger.
func (r *reloadState) signalReload(ctx context.Context, logger *log.Logger) {
	if r == nil {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	go func() {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				rep := r.reloadAll(ctx, "signal SIGHUP")
				if !rep.OK {
					logger.Printf("reload (SIGHUP) incomplete, previous state stays in effect: %s", rep)
					continue
				}
				logger.Printf("reload (SIGHUP): %s", rep)
			}
		}
	}()
}
