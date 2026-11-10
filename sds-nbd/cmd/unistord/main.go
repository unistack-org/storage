// Command unistord is the SDStorage daemon: it exposes one or more block
// exports over the NBD protocol. It is the MVP data-plane entry point
// (phase 4 of the roadmap): QEMU attaches to it as an NBD client and can
// present the export to a guest as an NVMe/virtio disk.
//
// Usage:
//
//	unistord -listen :10809 -export vol0=/var/lib/unistord/vol0.img:10G [-readonly vol0] [-trim] [-write-zeroes]
//	unistord ... -export vol0 -vduse vol0=sds-vol0   # VDUSE virtio-blk device (Linux)
//	unistord ... -cluster ... -metrics 0.0.0.0:8080  # Prometheus /metrics endpoint
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/unistack-org/sds-nbd/nbd"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/osd"
	"github.com/unistack-org/storage/pkg/qos"
	"github.com/unistack-org/storage/pkg/sdsnet"
	"github.com/unistack-org/storage/pkg/store"
	"github.com/unistack-org/storage/pkg/tlsconf"
)

// sizeSuffix parses sizes like 512M, 10G, 1T, 4096.
func sizeSuffix(s string) (uint64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	mult := uint64(1)
	switch {
	case strings.HasSuffix(s, "K"):
		mult, s = 1<<10, s[:len(s)-1]
	case strings.HasSuffix(s, "M"):
		mult, s = 1<<20, s[:len(s)-1]
	case strings.HasSuffix(s, "G"):
		mult, s = 1<<30, s[:len(s)-1]
	case strings.HasSuffix(s, "T"):
		mult, s = 1<<40, s[:len(s)-1]
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return v * mult, nil
}

type exportFlag []string

func (e *exportFlag) String() string { return strings.Join(*e, ",") }
func (e *exportFlag) Set(v string) error {
	*e = append(*e, v)
	return nil
}

// ballast is the retained steady-state allocation from -ballast-mb.
// Package-level so the GC always counts it as live.
var ballast []byte

// osdFlag collects repeatable -osd "id@addr" specs (cluster mode).
type osdFlag []string

func (e *osdFlag) String() string { return strings.Join(*e, ",") }
func (e *osdFlag) Set(v string) error {
	*e = append(*e, v)
	return nil
}

// qosFlag collects repeatable QoS specs (strings, parsed by the caller).
type qosFlag []string

func (e *qosFlag) String() string { return strings.Join(*e, ",") }
func (e *qosFlag) Set(v string) error {
	*e = append(*e, v)
	return nil
}

// qosRates is a parsed QoS limit: the four rates and a builder for a
// qos.Pair (nil when every axis is unlimited).
type qosRates struct {
	readIOPS, readBPS, writeIOPS, writeBPS uint64
}

func (r qosRates) pair() *qos.Pair {
	return qos.NewPair(r.readIOPS, r.readBPS, r.writeIOPS, r.writeBPS)
}

// parseQoSRatesStruct parses "riops:rbps:wiops:wbps" into a qosRates.
func parseQoSRatesStruct(s string) (qosRates, error) {
	riops, rbps, wiops, wbps, err := parseQoSRates(s)
	return qosRates{riops, rbps, wiops, wbps}, err
}

// parseQoSRates parses "riops:rbps:wiops:wbps": four rates separated by
// ':', each empty or 0 meaning unlimited on that axis. Bandwidth rates
// accept the K/M/G/T size suffixes (-qos 5000:200M:5000:200M = 5k IOPS
// and 200 MB/s each way).
func parseQoSRates(s string) (riops, rbps, wiops, wbps uint64, err error) {
	parts := strings.Split(s, ":")
	if len(parts) != 4 {
		return 0, 0, 0, 0, fmt.Errorf(`expected "riops:rbps:wiops:wbps", got %q`, s)
	}
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var v uint64
		if i == 0 || i == 2 {
			v, err = strconv.ParseUint(p, 10, 64)
		} else {
			v, err = sizeSuffix(p)
		}
		if err != nil {
			return 0, 0, 0, 0, fmt.Errorf("qos rate %d in %q: %v", i+1, s, err)
		}
		switch i {
		case 0:
			riops = v
		case 1:
			rbps = v
		case 2:
			wiops = v
		case 3:
			wbps = v
		}
	}
	return riops, rbps, wiops, wbps, nil
}

// parseQoSNamed parses "name=rates" (-qos export=...).
func parseQoSNamed(spec string) (name, rates string, err error) {
	name, rates, ok := strings.Cut(spec, "=")
	if !ok || name == "" {
		return "", "", fmt.Errorf(`expected "name=riops:rbps:wiops:wbps", got %q`, spec)
	}
	return name, rates, nil
}

func main() {
	var exports exportFlag
	listen := flag.String("listen", ":10809", "TCP address to listen on")
	dataDir := flag.String("data-dir", "", "directory for relative export paths")
	trim := flag.Bool("trim", true, "advertise and handle TRIM (punches holes: Linux fallocate, darwin APFS F_PUNCHHOLE)")
	writeZeroes := flag.Bool("write-zeroes", true, "advertise and handle WRITE_ZEROES")
	storeMode := flag.Bool("store", false, "treat export paths as object-store directories (phase-1 data path) instead of sparse files")
	direct := flag.Bool("direct", false, "with -store: request O_DIRECT on object files")
	prealloc := flag.Bool("preallocate", false, "with -store: fallocate each new object to full size")
	asyncRing := flag.Int("async-ring", 0, "with -store: route object reads/writes through the async I/O ring (io_uring on Linux, worker-pool fallback elsewhere); value = ring depth, e.g. 128; 0 = plain pread/pwrite")
	maxConns := flag.Int("max-conns", 0, "max concurrent client connections (0 = unlimited)")
	clusterDir := flag.String("cluster", "", "meta directory of a cluster (enables clustered exports backed by OSDs)")
	var osds osdFlag
	flag.Var(&osds, "osd", "with -cluster: start an in-process OSD as id@data-dir[@host:port], repeatable")
	crushPath := flag.String("crushmap", "", "with -cluster: crushmap file (text/json/binary) to publish")
	epoch := flag.Uint("epoch", 1, "with -cluster: epoch to publish -crushmap under (must be unique)")
	pgNum := flag.Int("pg-num", 64, "with -cluster: cluster pg_num for the placement table")
	replicas := flag.Int("replicas", 3, "with -cluster: replicas per object")
	minSize := flag.Int("min-size", 0, "with -cluster: write quorum width (0 = replicas/2+1); below it writes fail fast (degraded volume)")
	rule := flag.String("rule", "", "with -cluster: crush rule name (default: first rule in the map)")
	var creates exportFlag
	flag.Var(&creates, "create", "with -cluster: create volume name:size if absent, repeatable")
	flag.Var(&exports, "export", "export spec name=path[:size] (file/-store) or name (cluster), repeatable")
	var vduseSpecs exportFlag
	flag.Var(&vduseSpecs, "vduse", "export a -export as a VDUSE virtio-blk device (Linux): spec export or export=devname, repeatable")
	gogc := flag.Int("gogc", 0, "GOGC heap-growth percent (0 = runtime default 100; higher = fewer GC cycles, more memory)")
	ballastMB := flag.Int("ballast-mb", 0, "steady-state ballast in MiB: a retained allocation that raises the live-heap floor so the GC runs proportionally less often")
	pprofAddr := flag.String("pprof", "", "serve net/http/pprof on this address (e.g. :6061; empty = disabled)")
	readWorkers := flag.Int("read-workers", 0, "max concurrent in-flight requests per NBD connection in structured-reply mode (0 = default 32)")
	var qosSpecs qosFlag
	flag.Var(&qosSpecs, "qos", "per-export limits name=riops:rbps:wiops:wbps (bandwidth accepts K/M/G/T, 0/empty = unlimited), repeatable; applies to -export exports and cluster exports")
	var qosClasses qosFlag
	flag.Var(&qosClasses, "qos-class", "with -cluster: register a QoS class name=riops:rbps:wiops:wbps in meta (volumes reference it via QoSTemplate), repeatable")
	osdQoS := flag.String("osd-qos", "", "with -cluster: aggregate limits per in-process OSD riops:rbps:wiops:wbps (the per-disk ceiling shared by all volumes on it; 0/empty = unlimited)")
	httpAddr := flag.String("http", "", "with -cluster: admin HTTP API on this address (e.g. 127.0.0.1:8443; empty = disabled): volumes, QoS, import/export over octet-stream, runtime exports")
	adminToken := flag.String("admin-token", "", "with -http: require Authorization: Bearer <token> on every admin endpoint except /healthz (the export endpoints attach volumes to VMs, so a control plane on a shared network must authenticate; empty = no auth)")
	metricsAddr := flag.String("metrics", "", "with -cluster: Prometheus metrics endpoint (VictoriaMetrics scrape target) on this address, served at http://<addr>/metrics (e.g. 0.0.0.0:8080; empty = disabled). Independent of -http: metrics are served only here, never on the admin API")
	commit := flag.String("commit", "none", "durability of OSD/-store writes: none (page cache; a guest FLUSH/FUA syncs the disk), flush (fsync every write/punch), sync (O_SYNC at open; on macOS degrades to flush semantics), journal (append to a write-ahead log + fsync per write, background flattening into objects; fastest safe mode for small random writes)")
	fsyncBatch := flag.Duration("fsync-batch", 0, "with -commit none: coalesce Sync() calls that land within this window (e.g. 2ms) into one fsync pass (0 = fsync disk on every sync request)")
	vacuumInterval := flag.Int("vacuum-interval", 0, "with -cluster: seconds between background vacuums that unlink deleted volumes' objects from the OSDs (0 = off; the admin API and unistorctl vacuum run are the manual triggers)")
	raftAddr := flag.String("raft", "", "with -cluster: replicate meta over raft on this bind address (host:port). Reads always work; writes need a quorum, without one the meta is read-only (503 on the admin API). A node with -raft but no -osd/-export is a storage-less witness voter")
	raftPeers := flag.String("raft-peers", "", "with -raft: FULL voter set as comma-separated host:port (including this node), e.g. 10.0.0.1:9401,10.0.0.2:9401,10.0.0.3:9401")
	raftDir := flag.String("raft-dir", "", "with -raft: raft data directory (default: <cluster>/raft)")
	raftProxy := flag.String("raft-proxy", "", "with -raft: serve write forwarding on this host:port, so writes that reach a follower are relayed to the leader (recommended; use the storage net)")
	raftProxyPeers := flag.String("raft-proxy-peers", "", "with -raft-proxy: proxy addresses parallel to -raft-peers (same order, including this node), e.g. 10.0.0.1:9501,10.0.0.2:9501,10.0.0.3:9501")
	tlsCert := flag.String("tls-cert", "", "PEM certificate (with -tls-key): enables mutual TLS on the OSD data plane, raft transport and the admin API; the certificate must list the node's dial addresses (IPs included) as SANs")
	tlsKey := flag.String("tls-key", "", "PEM private key for -tls-cert")
	tlsCA := flag.String("tls-ca", "", "PEM CA certificate(s) that signed the peers'/clients' certificates; required for the OSD/raft planes; it is also used to verify admin API clients unless -tls-client-ca-file is set")
	tlsClientCA := flag.String("tls-client-ca-file", "", "with -tls-ca: require admin API client certificates signed by this CA (by default the admin API presents a certificate but does not demand client certificates, so plain 'curl --cacert' works)")
	tlsMinVer := flag.String("tls-min-version", "1.2", "minimum TLS version: 1.2 or 1.3")
	flag.Parse()

	if *gogc > 0 {
		debug.SetGCPercent(*gogc)
	}
	if *ballastMB > 0 {
		// The ballast is written once and kept in a package-level
		// variable for the process lifetime; it raises the live-heap
		// base so the GC target is reached later, trading a fixed
		// chunk of RSS for fewer GC cycles.
		ballast = make([]byte, *ballastMB<<20)
		for i := range ballast {
			ballast[i] = 1 // touch every page so RSS reflects it
		}
	}

	logger := log.New(os.Stderr, "unistord: ", log.LstdFlags|log.Lmsgprefix)

	commitMode, err := store.ParseCommitMode(*commit)
	if err != nil {
		logger.Fatalf("commit: %v", err)
	}

	// TLS material: when any -tls-* flag is given, every network plane
	// (OSD data, raft transport and write proxy, admin HTTP) switches to
	// mutual TLS at once - a mixed plain/TLS fleet is not supported.
	tlsMaterial := tlsconf.Config{
		CertFile:   *tlsCert,
		KeyFile:    *tlsKey,
		CAFile:     *tlsCA,
		MinVersion: *tlsMinVer,
	}
	var tlsServer, tlsClient, tlsAdmin *tls.Config
	if tlsMaterial.Any() {
		if !tlsMaterial.Complete() {
			logger.Fatalf("-tls-cert, -tls-key and -tls-ca must all be set together")
		}
		scfg, err := tlsMaterial.ServerConfig()
		if err != nil {
			logger.Fatalf("tls: %v", err)
		}
		ccfg, err := tlsMaterial.ClientConfig()
		if err != nil {
			logger.Fatalf("tls: %v", err)
		}
		// The admin API presents the certificate and, unlike the
		// storage planes, does not demand client certificates by
		// default: operators use plain "curl --cacert ca.pem".
		// -tls-client-ca-file (a CA for operator/client certs) turns
		// mutual TLS on for the admin API too.
		adminCA := *tlsClientCA
		if adminCA == "" {
			adminCA = *tlsCA
		}
		acf, err := tlsconf.Config{CertFile: *tlsCert, KeyFile: *tlsKey, CAFile: adminCA, MinVersion: *tlsMinVer}.HTTPServerConfig()
		if err != nil {
			logger.Fatalf("tls: %v", err)
		}
		tlsServer, tlsClient, tlsAdmin = scfg, ccfg, acf
		logger.Printf("tls: mutual TLS enabled (min %s, cert %s, ca %s)", *tlsMinVer, *tlsCert, *tlsCA)
	}

	// QoS: per-export limits (client side) and an optional per-OSD
	// aggregate (the disk-side ceiling). An export with no -qos entry
	// is unlimited at the client layer (the admin class).
	qosMap := map[string]string{}
	for _, spec := range qosSpecs {
		name, rates, err := parseQoSNamed(spec)
		if err != nil {
			logger.Fatalf("bad -qos %q: %v", spec, err)
		}
		qosMap[name] = rates
	}

	if *pprofAddr != "" {
		pprofSrv := &http.Server{Addr: *pprofAddr, Handler: nil}
		go func() {
			logger.Printf("pprof on %s", *pprofAddr)
			if err := pprofSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Printf("pprof: %v", err)
			}
		}()
		defer func() {
			sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pprofSrv.Shutdown(sctx)
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Clustered mode: exports are volumes placed by CRUSH across OSDs
	// (this process hosts -osd daemons; peers may run in other unistord).
	if *clusterDir != "" {
		if *raftAddr != "" && *raftPeers == "" {
			logger.Fatalf("-raft requires -raft-peers (the full voter set)")
		}
		if *raftProxy != "" && *raftProxyPeers == "" {
			logger.Fatalf("-raft-proxy requires -raft-proxy-peers (proxy addresses parallel to -raft-peers)")
		}
		// The meta store is opened once per process and shared by the
		// exports, the OSDs they host and the admin API: with raft it
		// must be a single instance (it owns the transport and the
		// replicated state machine).
		var ms meta.Store
		var raftSt *meta.RaftStore
		if *raftAddr != "" {
			dir := *raftDir
			if dir == "" {
				dir = filepath.Join(*clusterDir, "raft")
			}
			var peers []string
			for _, p := range strings.Split(*raftPeers, ",") {
				if p = strings.TrimSpace(p); p != "" {
					peers = append(peers, p)
				}
			}
			var proxyPeers []string
			if *raftProxy != "" {
				for _, p := range strings.Split(*raftProxyPeers, ",") {
					if p = strings.TrimSpace(p); p != "" {
						proxyPeers = append(proxyPeers, p)
					}
				}
				if len(proxyPeers) != len(peers) {
					logger.Fatalf("-raft-proxy-peers must list one proxy address per -raft-peers entry (%d, got %d)", len(peers), len(proxyPeers))
				}
			}
			rst, err := meta.OpenRaft(meta.RaftConfig{
				Dir:        dir,
				BindAddr:   *raftAddr,
				Peers:      peers,
				ProxyAddr:  *raftProxy,
				ProxyPeers: proxyPeers,
				Logger:     log.New(os.Stderr, "raft: ", log.LstdFlags|log.Lmsgprefix),
				TLS:        tlsServer,
				ProxyTLS:   tlsClient,
			})
			if err != nil {
				logger.Fatalf("raft meta: %v", err)
			}
			ms, raftSt = rst, rst
			logger.Printf("meta raft on %s (peers: %s, dir %s)", *raftAddr, *raftPeers, dir)
			if *raftProxy != "" {
				logger.Printf("raft write forwarding on %s (leaders writes are relayed to the leader)", *raftProxy)
			}
		} else {
			ds, err := meta.OpenDir(*clusterDir)
			if err != nil {
				logger.Fatalf("meta: %v", err)
			}
			ms = ds
		}
		defer ms.Close()

		// Background vacuum (M1.4): periodically unlink the objects of
		// deleted volumes from every OSD. Any cluster node with network
		// access to the OSD pool can run it; 0 disables the loop (the
		// admin API and unistorctl remain the manual triggers).
		if *vacuumInterval > 0 {
			cl := client.NewCluster(ms)
			if tlsClient != nil {
				cl.SetTLS(tlsClient)
			}
			go cl.VacuumLoop(ctx, time.Duration(*vacuumInterval)*time.Second,
				func(f string, a ...any) { logger.Printf(f, a...) })
			logger.Printf("vacuum: autovacuum every %vs", *vacuumInterval)
		}

		// Witness: raft voter without storage. No OSDs, no exports:
		// the process keeps the quorum (and can serve the read-only
		// admin/status surface) and hosts no data.
		witness := len(osds) == 0 && len(exports) == 0 && raftSt != nil
		var es []*nbd.Export
		disks := newDiskRegistry()
		// One metrics registry per process. The standalone endpoint
		// (-metrics) is the only place metrics are exposed: the admin
		// API never serves them. The same registry feeds the OSD/export
		// hot-path hooks below.
		var mreg *clusterMetrics
		if *metricsAddr != "" {
			mreg = newClusterMetrics()
		}
		var hub *exportHub
		var vdm *vduseManager
		if *metricsAddr != "" {
			mcl := client.NewCluster(ms)
			if tlsClient != nil {
				mcl.SetTLS(tlsClient)
			}
			defer mcl.Close()
			msrc := metricsSource{cl: mcl, ms: ms, raft: raftSt, disks: disks, vacuumInterval: *vacuumInterval}
			go func() {
				if err := mreg.serveMetrics(ctx, *metricsAddr, msrc, logger); err != nil {
					logger.Printf("metrics http: %v", err)
				}
			}()
		}
		if *httpAddr != "" || len(vduseSpecs) > 0 {
			vdm = newVDUSEManager(logger, vduseFlags{Trim: *trim, WriteZeroes: *writeZeroes})
		}
		if *httpAddr != "" {
			// Start the admin API before the exports: on a raft cluster
			// the export setup may block waiting for OSD registration
			// (which needs a quorum), and the operator needs the status
			// and write endpoints available throughout.
			hub = newExportHub(nil, logger, vdm)
			admin, err := newAdminServer(ms, *httpAddr, logger, raftSt, disks, tlsAdmin, tlsClient, *adminToken, hub)
			if err != nil {
				logger.Fatalf("admin http: %v", err)
			}
			hub.setCluster(admin.cluster())
			// Recovery runs started over the admin API feed the metrics
			// registry (nil-safe); the admin itself never serves them.
			admin.recoveryRecord = mreg.recoveryRecorder()
			go admin.serve(ctx)
		}
		if !witness {
			var err error
			es, err = clusterExports(ctx, clusterParams{
				store:      ms,
				metaDir:    *clusterDir,
				osds:       osds,
				crushmap:   *crushPath,
				epoch:      uint32(*epoch),
				pgNum:      *pgNum,
				replicas:   *replicas,
				minSize:    *minSize,
				rule:       *rule,
				creates:    creates,
				exports:    exports,
				qosMap:     qosMap,
				qosClasses: qosClasses,
				osdQoS:     *osdQoS,
				commit:     commitMode,
				fsyncBatch: *fsyncBatch,
				tlsServer:  tlsServer,
				tlsClient:  tlsClient,
				disks:      disks,
				metrics:    mreg,
				logger:     logger,
			})
			if err != nil {
				logger.Fatalf("cluster: %v", err)
			}
		} else {
			logger.Printf("witness mode: raft voter, no OSDs and no exports")
		}
		if witness {
			if len(vduseSpecs) > 0 {
				logger.Fatalf("witness mode has no exports to expose via -vduse")
			}
			<-ctx.Done()
			logger.Printf("bye")
			return
		}
		if len(vduseSpecs) > 0 {
			if err := vdm.start(ctx, es, vduseSpecs); err != nil {
				logger.Fatalf("vduse: %v", err)
			}
			go func() {
				<-ctx.Done()
				vdm.closeAll()
			}()
		}
		serve(ctx, logger, *listen, hub, nbd.ServerOptions{
			MaxConnections:    *maxConns,
			MaxReadWorkers:    *readWorkers,
			Logger:            logger,
			EnableTrim:        *trim,
			EnableWriteZeroes: *writeZeroes,
		}, es)
		return
	}

	if len(exports) == 0 {
		fmt.Fprintln(os.Stderr, "unistord: at least one -export name=path[:size] is required")
		flag.Usage()
		os.Exit(2)
	}

	var es []*nbd.Export
	for _, spec := range exports {
		name, path, sizeStr, err := parseExportSpec(spec)
		if err != nil {
			logger.Fatalf("bad -export %q: %v", spec, err)
		}
		if *dataDir != "" && !filepath.IsAbs(path) {
			path = filepath.Join(*dataDir, path)
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			logger.Fatalf("mkdir: %v", err)
		}
		size := uint64(0)
		if sizeStr != "" {
			size, err = sizeSuffix(sizeStr)
			if err != nil {
				logger.Fatalf("bad size in -export %q: %v", spec, err)
			}
		}
		b, err := newBackend(*storeMode, path, size, *direct, *prealloc, commitMode, *fsyncBatch, *asyncRing)
		if err != nil {
			logger.Fatalf("export %s: %v", name, err)
		}
		// -qos wraps the backend: every guest op (reads on the read
		// axis, writes/trim/zeroes on the write axis) pays the token
		// bucket before touching the disk.
		if rates, ok := qosMap[name]; ok {
			r, err := parseQoSRatesStruct(rates)
			if err != nil {
				logger.Fatalf("bad -qos %q: %v", name, err)
			}
			b = nbd.WithQoS(b, r.pair())
			logger.Printf("export %q: qos r=%d/%s w=%d/%s", name, r.readIOPS, humanSize(r.readBPS), r.writeIOPS, humanSize(r.writeBPS))
		}
		logger.Printf("export %q -> %s (%s, store=%v)", name, path, humanSize(b.Size()), *storeMode)
		es = append(es, &nbd.Export{Name: name, Backend: b})
	}

	if len(vduseSpecs) > 0 {
		if _, err := startVDUSE(ctx, logger, es, vduseSpecs, *trim, *writeZeroes); err != nil {
			logger.Fatalf("vduse: %v", err)
		}
	}

	serve(ctx, logger, *listen, nil, nbd.ServerOptions{
		MaxConnections:    *maxConns,
		MaxReadWorkers:    *readWorkers,
		Logger:            logger,
		EnableTrim:        *trim,
		EnableWriteZeroes: *writeZeroes,
	}, es)
}

// serve runs the NBD server until ctx is cancelled. hub (nullable) is
// bound to the server so the admin API can manage exports at runtime.
func serve(ctx context.Context, logger *log.Logger, listen string, hub *exportHub, opts nbd.ServerOptions, es []*nbd.Export) {
	srv := nbd.NewServer(es, opts)
	if hub != nil {
		hub.setNBD(srv)
	}

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		logger.Fatalf("listen %s: %v", listen, err)
	}
	logger.Printf("listening on %s", ln.Addr())

	go func() {
		<-ctx.Done()
		logger.Printf("shutting down")
		// Graceful drain: in-flight NBD requests finish, guests get
		// ESHUTDOWN on new ones; after the deadline the server
		// force-closes whatever is left so SIGTERM is bounded.
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		if err := srv.Shutdown(sctx); err != nil {
			logger.Printf("nbd shutdown: %v", err)
		}
	}()

	if err := srv.Serve(ln); err != nil && ctx.Err() == nil {
		logger.Fatalf("serve: %v", err)
	}
	// Drain the exports after the server stops: the store backend
	// fsyncs every object file it holds, the file backend syncs its
	// file, so a SIGTERM-interrupted run leaves the same bytes on
	// disk as a clean one.
	for _, e := range es {
		if e.Backend == nil {
			continue
		}
		if err := e.Backend.Close(); err != nil {
			logger.Printf("export %s close: %v", e.Name, err)
		}
	}
	logger.Printf("bye")
}

type clusterParams struct {
	store      meta.Store // shared meta store (dir-backed or raft), opened by the caller
	metaDir    string     // for logs only
	osds       []string
	crushmap   string
	epoch      uint32
	pgNum      int
	replicas   int
	minSize    int
	rule       string
	creates    []string
	exports    []string
	qosMap     map[string]string // export name -> rates spec
	qosClasses []string          // -qos-class name=rates registrations
	osdQoS     string            // per-OSD aggregate rates spec ("" = none)
	commit     store.CommitMode  // OSD commit mode
	fsyncBatch time.Duration     // OSD fsync coalescing window
	tlsServer  *tls.Config       // mutual TLS for the OSD data plane (nil = plain)
	tlsClient  *tls.Config       // client side for cluster pools (nil = plain)
	disks      *diskRegistry     // hosted OSDs publish disk stats here (admin status)
	metrics    *clusterMetrics   // prometheus registry (nil = metrics disabled)
	logger     *log.Logger
}

// clusterExports brings up a single-process (or multi-process, shared
// meta directory) cluster: host the requested in-process OSDs, publish
// the crush map, create the requested volumes and open every -export
// name as a clustered volume backend.
func clusterExports(ctx context.Context, p clusterParams) ([]*nbd.Export, error) {
	if len(p.exports) == 0 {
		return nil, fmt.Errorf("at least one -export name is required")
	}
	ms := p.store
	var err error
	cl := client.NewCluster(ms)
	// The VDUSE data path runs up to 64 concurrent requests per queue,
	// each touching every replica: 64 x 3 replicas = 192 simultaneous
	// connections, so the default 64-per-OSD pool cap would queue ops
	// inside the pool. Raise it; each connection is 256KB of bufio.
	cl.SetPoolLimit(256)
	if p.tlsClient != nil {
		cl.SetTLS(p.tlsClient)
	}

	// writeMeta runs a meta write that must land before the cluster is
	// usable (class registration, map publish, volume create). On a raft
	// cluster a start may race the election, or this node may be a
	// permanent follower: retry while the store reports read-only, so a
	// three-node boot converges as soon as a leader exists.
	writeMeta := func(what string, fn func() error) error {
		for attempt := 0; ; attempt++ {
			err := fn()
			if err == nil {
				return nil
			}
			if !meta.IsReadOnly(err) {
				return err
			}
			if attempt == 0 {
				p.logger.Printf("%s: meta is read-only for now (waiting for a raft leader)", what)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
		}
	}

	// Register QoS classes so volumes can reference them by name.
	for _, spec := range p.qosClasses {
		name, rates, err := parseQoSNamed(spec)
		if err != nil {
			return nil, fmt.Errorf("-qos-class %q: %w", spec, err)
		}
		riops, rbps, wiops, wbps, err := parseQoSRates(rates)
		if err != nil {
			return nil, fmt.Errorf("-qos-class %q: %w", spec, err)
		}
		if err := writeMeta("qos class "+name, func() error {
			return cl.PutQoSClass(ctx, meta.QoSClassRecord{
				Name: name, ReadIOPS: riops, ReadBPS: rbps, WriteIOPS: wiops, WriteBPS: wbps,
			})
		}); err != nil {
			return nil, err
		}
		p.logger.Printf("qos class %q: r=%d/%s w=%d/%s", name, riops, humanSize(rbps), wiops, humanSize(wbps))
	}

	// Per-disk aggregate ceiling shared by all volumes on each hosted
	// OSD (the anti-noisy-neighbour cap on the NVMe itself).
	var aggSpec qosRates
	if p.osdQoS != "" {
		aggSpec, err = parseQoSRatesStruct(p.osdQoS)
		if err != nil {
			return nil, fmt.Errorf("-osd-qos %q: %w", p.osdQoS, err)
		}
	}

	// Host in-process OSDs: id@data-dir[@host:port].
	type hosted struct {
		id   int32
		d    *osd.Daemon
		done chan error
	}
	var running []*hosted
	for _, spec := range p.osds {
		id, dataDir, addr, err := parseOSDSpec(spec)
		if err != nil {
			return nil, fmt.Errorf("-osd %q: %w", spec, err)
		}
		d, err := osd.New(osd.Options{
			ID:         id,
			Addr:       addr,
			DataDir:    dataDir,
			Meta:       ms,
			Aggregate:  aggSpec.pair(),
			CommitMode: p.commit,
			FsyncBatch: p.fsyncBatch,
			TLS:        p.tlsServer,
			Metrics:    p.metrics.osdServerMetrics(id),
			Logf:       func(f string, a ...any) { p.logger.Printf("osd %d: %s", id, fmt.Sprintf(f, a...)) },
		})
		if err != nil {
			return nil, fmt.Errorf("osd %d: %w", id, err)
		}
		h := &hosted{id: id, d: d, done: make(chan error, 1)}
		go func() { h.done <- d.Run(ctx) }()
		running = append(running, h)
		if p.disks != nil {
			p.disks.add(id, d)
		}
	}
	// OSD self-registration is a meta write that retries in the
	// background; on a raft cluster it lands only after the first
	// leader exists, so allow a generous window here.
	waitRegistered := func() error {
		for _, h := range running {
			deadline := time.Now().Add(5 * time.Minute)
			for {
				info, err := meta.GetOSD(ctx, ms, h.id)
				if err == nil && info.Addr == h.d.Addr() {
					break
				}
				select {
				case e := <-h.done:
					if e != nil {
						return fmt.Errorf("osd %d: %w", h.id, e)
					}
				default:
				}
				if time.Now().After(deadline) {
					return fmt.Errorf("osd %d not registered: %w", h.id, err)
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		return nil
	}
	if err := waitRegistered(); err != nil {
		return nil, err
	}

	// Local-direct transport: this process hosts OSDs and acts as their
	// client (the VDUSE/NBD export path), so every op used to pay a
	// loopback TCP round trip (two syscall pairs, TCP state machine)
	// per replica. Hand the cluster a dialer that returns in-process
	// socketpairs served by each hosted OSD's own server: the wire
	// protocol, fencing, QoS and metrics are unchanged, the TCP stack
	// is gone.
	{
		byID := make(map[int32]*osd.Daemon, len(running))
		for _, h := range running {
			byID[h.id] = h.d
		}
		cl.SetLocalDial(func(osdID int32) (net.Conn, error) {
			d := byID[osdID]
			if d == nil {
				return nil, fmt.Errorf("unistord: osd %d is not hosted here", osdID)
			}
			srv := d.Server()
			if srv == nil {
				return nil, fmt.Errorf("unistord: osd %d is not serving yet", osdID)
			}
			mine, peer, err := sdsnet.Socketpair()
			if err != nil {
				return nil, fmt.Errorf("unistord: socketpair: %w", err)
			}
			go srv.ServeConn(peer)
			return mine, nil
		})
		p.logger.Printf("local-direct transport enabled for %d hosted OSD(s)", len(byID))
	}

	// Publish the crush map if asked and not already present at this
	// epoch (idempotent restarts keep the existing map).
	if p.crushmap != "" {
		src, err := os.ReadFile(p.crushmap)
		if err != nil {
			return nil, fmt.Errorf("crushmap: %w", err)
		}
		if rec, err := meta.LatestMap(ctx, ms); err != nil || rec.Epoch < p.epoch {
			if err := writeMeta(fmt.Sprintf("crushmap epoch %d", p.epoch), func() error {
				return cl.MapEpoch(ctx, p.epoch, src)
			}); err != nil && !errors.Is(err, meta.ErrConflict) {
				return nil, fmt.Errorf("publish map epoch %d: %w", p.epoch, err)
			}
			p.logger.Printf("published crushmap epoch %d (%d bytes)", p.epoch, len(src))
		} else {
			p.logger.Printf("crushmap epoch %d already present (latest %d)", p.epoch, rec.Epoch)
		}
	}

	// Placement options from flags, clamped by the map's own buckets.
	if _, err := cl.Refresh(ctx); err != nil {
		return nil, fmt.Errorf("placement: %w", err)
	}

	for _, spec := range p.creates {
		name, sizeStr, ok := strings.Cut(spec, ":")
		if !ok {
			return nil, fmt.Errorf("-create %q: expected name:size", spec)
		}
		size, err := sizeSuffix(sizeStr)
		if err != nil {
			return nil, fmt.Errorf("-create %q: %w", spec, err)
		}
		if _, err := meta.GetVolume(ctx, ms, name); err == nil {
			continue // already exists
		}
		var spec meta.VolumeSpec
		if err := writeMeta("create volume "+name, func() error {
			var cerr error
			spec, cerr = cl.CreateVolume(ctx, name, size, 0, p.replicas, p.minSize, p.pgNum, p.rule)
			return cerr
		}); err != nil {
			return nil, fmt.Errorf("create volume %q: %w", name, err)
		}
		p.logger.Printf("created volume %q (%s, r=%d, min_size=%d, pg=%d)", name, humanSize(size), p.replicas, spec.MinSize, p.pgNum)
	}

	var es []*nbd.Export
	for _, name := range p.exports {
		volName, _, _ := strings.Cut(name, "=") // tolerate name=... form
		vol, err := cl.OpenVolume(ctx, volName)
		if err != nil {
			return nil, err
		}
		var backend nbd.Backend = nbd.NewVolumeBackend(vol)
		// -qos overrides the volume's registered limits for this
		// process (the operator's local decision; meta stays the
		// source of truth for other clients).
		if rates, ok := p.qosMap[volName]; ok {
			r, err := parseQoSRatesStruct(rates)
			if err != nil {
				return nil, fmt.Errorf("-qos %q: %w", volName, err)
			}
			vol.SetQoS(meta.VolumeSpec{
				ReadIOPS: r.readIOPS, ReadBPS: r.readBPS,
				WriteIOPS: r.writeIOPS, WriteBPS: r.writeBPS,
			})
			p.logger.Printf("export %q: qos r=%d/%s w=%d/%s", volName, r.readIOPS, humanSize(r.readBPS), r.writeIOPS, humanSize(r.writeBPS))
		}
		// QoS sustained/limit gauges + per-op counters need the export.
		backend = p.metrics.trackExport(volName, vol, backend)
		// The read-only flag lives on the volume record (the admin API
		// persists it), so a restart reproduces the same access mode.
		ro := vol.Spec().ReadOnly
		p.logger.Printf("export %q -> cluster volume %q (%s, ro=%v)", volName, volName, humanSize(vol.Size()), ro)
		es = append(es, &nbd.Export{Name: volName, Backend: backend, ReadOnly: ro})
	}
	return es, nil
}

// parseOSDSpec parses "id@data-dir[@host:port]" (default 127.0.0.1:0).
func parseOSDSpec(spec string) (id int32, dataDir, addr string, err error) {
	parts := strings.Split(spec, "@")
	if len(parts) < 2 {
		return 0, "", "", fmt.Errorf(`expected "id@data-dir[@host:port]"`)
	}
	n, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return 0, "", "", fmt.Errorf("bad id %q", parts[0])
	}
	addr = "127.0.0.1:0"
	if len(parts) >= 3 {
		addr = parts[2]
	}
	return int32(n), parts[1], addr, nil
}

// newBackend builds the export backend: a plain sparse file (MVP
// default) or, with -store, the phase-1 object-store data path.
func newBackend(storeMode bool, path string, size uint64, direct, prealloc bool, commitMode store.CommitMode, fsyncBatch time.Duration, asyncRing int) (nbd.Backend, error) {
	if !storeMode {
		return nbd.NewFileBackend(path, size)
	}
	if size == 0 {
		return nil, fmt.Errorf("-store exports require an explicit :size")
	}
	return nbd.NewStoreBackend(path, size, store.Options{
		Direct:      direct,
		Preallocate: prealloc,
		CommitMode:  commitMode,
		FsyncBatch:  fsyncBatch,
		AsyncRing:   asyncRing,
	})
}

// parseExportSpec parses "name=path[:size]". The size may be omitted for a
// pre-existing file (its current size is used).
func parseExportSpec(spec string) (name, path, size string, err error) {
	i := strings.IndexByte(spec, '=')
	if i <= 0 {
		return "", "", "", fmt.Errorf(`expected "name=path[:size]"`)
	}
	name, rest := spec[:i], spec[i+1:]
	if rest == "" {
		return "", "", "", fmt.Errorf("empty path")
	}
	// Size is the part after the LAST ':' that parses as a size suffix;
	// paths may contain ':' on exotic filesystems, so be conservative.
	if j := strings.LastIndexByte(rest, ':'); j > 0 {
		if _, err := sizeSuffix(rest[j+1:]); err == nil {
			return name, rest[:j], rest[j+1:], nil
		}
	}
	return name, rest, "", nil
}

func humanSize(n uint64) string {
	switch {
	case n >= 1<<40 && n%(1<<40) == 0:
		return fmt.Sprintf("%dT", n/(1<<40))
	case n >= 1<<30 && n%(1<<30) == 0:
		return fmt.Sprintf("%dG", n/(1<<30))
	case n >= 1<<20 && n%(1<<20) == 0:
		return fmt.Sprintf("%dM", n/(1<<20))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
