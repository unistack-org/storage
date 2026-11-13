// Command unistord is the SDStorage daemon: it exposes one or more block
// exports over the NBD protocol. It is the MVP data-plane entry point
// (phase 4 of the roadmap): QEMU attaches to it as an NBD client and can
// present the export to a guest as an NVMe/virtio disk.
//
// Usage:
//
//	unistord -listen :10809 -export vol0=/var/lib/unistord/vol0.img:10G [-readonly vol0] [-trim] [-write-zeroes]
//	unistord ... -export vol0 -vduse vol0=sds-vol0   # VDUSE virtio-blk device (Linux)
//	unistord ... -export vol0 -vhost-user-blk vol0=/run/vol0.sock   # vhost-user-blk socket (QEMU/Dragonball)
//	unistord ... -meta-dir /var/lib/unistord/meta -data-dir /var/lib/unistord/data1 -data-dir /var/lib/unistord/data2 ...  # cluster mode
//	unistord ... -meta-dir ... -vhost-user-blk vm0=/run/vm0.sock   # cluster: attach a volume at boot (no -export)
//	in cluster mode exports are runtime objects: "unistorctl export create" (records live in meta and are replayed on restart)
//	unistord ... -meta-dir ... -osd 1@/var/lib/unistord/data1@10.0.0.5:10810@10.0.1.5:10811  # full OSD spec: id@dir[@data-addr[@ctrl-addr]]
//	unistord ... -meta-dir ... -metrics 0.0.0.0:8080  # Prometheus /metrics endpoint
//
// Cluster placement policy lives in pools (unistorctl pool create), not
// in daemon flags: start without -replicas/-pg-num/-rule and create
// volumes with "vol create -pool <pool>" (or -create pool/name:size).
// The placement flags remain as per-boot defaults for -create only.
// An OSD data directory is pinned to its id in <dir>/id (written on
// first start; the id is kept across restarts).
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/unistack-org/storage/internal/buildinfo"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/cpu"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/nbd"
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
	exportDir := flag.String("export-dir", "", "base directory for relative -export image paths")
	trim := flag.Bool("trim", true, "advertise and handle TRIM (punches holes: Linux fallocate, darwin APFS F_PUNCHHOLE)")
	writeZeroes := flag.Bool("write-zeroes", true, "advertise and handle WRITE_ZEROES")
	vduseReadAhead := flag.Bool("vduse-readahead", true, "with -vduse: prefetch a window on sequential guest read runs (ideas §3.8); disable to force one backend read per request")
	vduseReadAheadWindow := flag.Int("vduse-readahead-window", 0, "with -vduse-readahead: prefetch window bytes per queue (default 262144 = 256 KiB)")
	vduseQueues := flag.Int("vduse-queues", 1, "with -vduse: virtqueues per device (VIRTIO_BLK_F_MQ multi-queue); 1 = single queue (default)")
	storeMode := flag.Bool("store", false, "treat export paths as object-store directories (phase-1 data path) instead of sparse files")
	direct := flag.Bool("direct", false, "with -store: request O_DIRECT on object files")
	prealloc := flag.Bool("preallocate", false, "with -store: fallocate each new object to full size")
	asyncRing := flag.Int("async-ring", 0, "with -store: route object reads/writes through the async I/O ring (io_uring on Linux, worker-pool fallback elsewhere); value = ring depth, e.g. 128; 0 = plain pread/pwrite")
	ringPinCPU := flag.String("ring-pin-cpu", "", "with -async-ring: pin the ring's worker threads to this CPU set (Linux cpulist syntax, e.g. \"2\" or \"2-3\"); empty = scheduler default")
	ringArenaSlots := flag.Int("ring-arena-slots", 0, "with -async-ring: registered buffer arena size in slots (io_uring fixed buffers: the kernel pins the arena once, ring ops skip per-I/O page pinning); 0 = plain buffers, e.g. 128")
	ringArenaSlot := flag.Int("ring-arena-slot", 64<<10, "with -ring-arena-slots: slot size in bytes; I/O larger than a slot takes the plain path")
	ringHugePages := flag.Bool("ring-hugepages", false, "with -ring-arena-slots: back the arena with huge pages (Linux MADV_HUGEPAGE; advisory) to shrink TLB pressure")
	maxConns := flag.Int("max-conns", 0, "max concurrent client connections (0 = unlimited)")
	metaDir := flag.String("meta-dir", "", "meta directory of a cluster (enables clustered exports backed by OSDs): holds the volume/OSD registry, crush epochs, export records and jobs - not the data, which lives in the OSD directories")
	clusterDir := flag.String("cluster", "", "deprecated alias for -meta-dir")
	var osds osdFlag
	flag.Var(&osds, "osd", "with -meta-dir: start an in-process OSD as id@data-dir[@host:port[@ctrl-host:ctrl-port]], repeatable (full form; see -data-dir for the simple one)")
	var dataDirs osdFlag
	flag.Var(&dataDirs, "data-dir", "with -meta-dir: start an in-process OSD on a data directory, repeatable: plain <dir> gets the lowest free id (pinned in <dir>/id, device identity pinned in <dir>/device and re-checked at every boot - a disk swap is a fatal error, never silent data drift); uuid=<identity>@<dir> pins the id to a device identity from \"unistorctl disk probe\"")
	crushPath := flag.String("crushmap", "", "with -meta-dir: crushmap file (text/json/binary) to publish")
	epoch := flag.Uint("epoch", 0, "with -meta-dir: epoch to publish -crushmap under (0 = auto: latest+1, i.e. 1 on a fresh cluster)")
	pgNum := flag.Int("pg-num", 0, "with -meta-dir: placement default for -create only (0 = cluster snapshot; pools carry their own pg_num)")
	replicas := flag.Int("replicas", 0, "with -meta-dir: placement default for -create only (0 = cluster snapshot; pools carry their own replicas)")
	minSize := flag.Int("min-size", 0, "with -meta-dir: write quorum width for -create (0 = replicas/2+1); below it writes fail fast (degraded volume)")
	rule := flag.String("rule", "", "with -meta-dir: crush rule default for -create (empty = first rule in the map)")
	var creates exportFlag
	flag.Var(&creates, "create", "with -meta-dir: create volume name:size (or pool/name:size for pool policy) if absent, repeatable")
	flag.Var(&exports, "export", "standalone only: export spec name=path[:size] (file/-store), repeatable; in cluster mode (-meta-dir) exports are runtime objects: unistorctl export create")
	var vduseSpecs exportFlag
	flag.Var(&vduseSpecs, "vduse", "expose an export (standalone) or a cluster volume (-meta-dir) as a VDUSE virtio-blk device (Linux): spec name or name=devname, repeatable")
	var vubSpecs exportFlag
	flag.Var(&vubSpecs, "vhost-user-blk", "expose an export (standalone) or a cluster volume (-meta-dir) as a vhost-user-blk unix socket (QEMU/Dragonball): spec name or name=socket[:ro], repeatable")
	flag.Var(&vubSpecs, "vub", "deprecated alias of -vhost-user-blk, repeatable")
	vubQueues := flag.Int("vub-queues", 1, "vhost-user-blk request queues per export (num-queues)")
	vubWorkers := flag.Int("vub-workers", 4, "vhost-user-blk concurrent in-flight requests per queue")
	gogc := flag.Int("gogc", 0, "GOGC heap-growth percent (0 = runtime default 100; higher = fewer GC cycles, more memory)")
	pinCPUs := flag.String("pin-cpus", "", "pin the daemon's main thread to this CPU set (Linux cpulist, e.g. \"0-7\"); threads created afterwards inherit the mask, so the whole process (rings, acceptors, GC) stays on the set — pair it with -gomaxprocs for NUMA locality; empty = scheduler default")
	gomaxprocs := flag.Int("gomaxprocs", 0, "GOMAXPROCS (0 = runtime default = CPUs visible to the process); set it to the size of -pin-cpus so the Go scheduler never runs a worker on a CPU outside the set (cross-NUMA memory accesses)")
	reusePort := flag.Int("reuseport", 0, "bind N SO_REUSEPORT listeners for the NBD and every OSD data plane (Linux): the kernel fans connections over N accept queues (N per OSD), removing the single accept lock; pair with -ring-pin-cpu/-pin-cpus for per-node accepting; 0/1 = single listener")
	writebackMB := flag.Int("writeback-mb", 0, "client-side writeback cache (vitastor-ideas §3.5) for volumes this daemon serves (NBD exports, -vduse/-vub, admin import/export jobs): MiB of dirty 4 KiB pages per volume, writes complete on memcpy and reach the OSDs in the background; 0 = off (every write goes to the replicas). A crash of THIS daemon loses unflushed pages, so enable it for guests whose OS journals and fsyncs (fsync = NBD FLUSH = -writeback flush); the cap is the writeback backpressure: a guest outrunning the OSDs is throttled when it fills")
	writebackMS := flag.Int("writeback-ms", 500, "with -writeback-mb: background flush interval in milliseconds (how long a write may sit dirty before the flusher pushes it; Flush and Close always flush)")
	ballastMB := flag.Int("ballast-mb", 0, "steady-state ballast in MiB: a retained allocation that raises the live-heap floor so the GC runs proportionally less often")
	pprofAddr := flag.String("pprof", "", "serve net/http/pprof on this address (e.g. :6061; empty = disabled)")
	readWorkers := flag.Int("read-workers", 0, "max concurrent in-flight requests per NBD connection in structured-reply mode (0 = default 32)")
	var qosSpecs qosFlag
	flag.Var(&qosSpecs, "qos", "standalone only: per-export limits name=riops:rbps:wiops:wbps (bandwidth accepts K/M/G/T, 0/empty = unlimited), repeatable; in cluster mode QoS rides on the volume (qos-class), in standalone it wraps -export exports")
	var qosClasses qosFlag
	flag.Var(&qosClasses, "qos-class", "with -meta-dir: register a QoS class name=riops:rbps:wiops:wbps in meta (volumes reference it via QoSTemplate), repeatable")
	osdQoS := flag.String("osd-qos", "", "with -meta-dir: aggregate limits per in-process OSD riops:rbps:wiops:wbps (the per-disk ceiling shared by all volumes on it; 0/empty = unlimited)")
	httpAddr := flag.String("http", "", "with -meta-dir: admin HTTP API on this address (e.g. 127.0.0.1:8443; empty = disabled): volumes, QoS, import/export over octet-stream, runtime exports")
	adminToken := flag.String("admin-token", "", "with -http: require Authorization: Bearer <token> on every admin endpoint except /healthz (the export endpoints attach volumes to VMs, so a control plane on a shared network must authenticate; empty = no auth)")
	metricsAddr := flag.String("metrics", "", "with -meta-dir: Prometheus metrics endpoint (VictoriaMetrics scrape target) on this address, served at http://<addr>/metrics (e.g. 0.0.0.0:8080; empty = disabled). Independent of -http: metrics are served only here, never on the admin API")
	commit := flag.String("commit", "none", "durability of OSD/-store writes: none (page cache; a guest FLUSH/FUA syncs the disk), flush (fsync every write/punch), sync (O_SYNC at open; on macOS degrades to flush semantics), journal (append to a write-ahead log + fsync per write, background flattening into objects; fastest safe mode for small random writes)")
	fsyncBatch := flag.Duration("fsync-batch", 0, "with -commit none: coalesce Sync() calls that land within this window (e.g. 2ms) into one fsync pass (0 = fsync disk on every sync request)")
	intentLog := flag.Bool("intent", false, "with -commit none|flush|sync (OSD and -store exports): keep a write intent log (vitastor-ideas §2.1) — every write is announced before its data and verified at reopen, so a power loss produces a loud damage report (status warnings, unistor_osd_intent mismatches) instead of silent corruption. Ignored in -commit journal mode (the WAL already rebuilds objects)")
	atomicWrites := flag.Bool("atomic-writes", false, "OSD and -store exports: write with RWF_ATOMIC (power-loss atomicity, vitastor-ideas §2.2) on disks that publish an atomic write unit (Linux 6.11+; NVMe Micron 7450/7500, Kioxia CD7/CD8, ...). Auto-disabled per disk when the device has no atomic unit or the kernel refuses it. Not combined with the write journal (-commit journal)")
	atomicSize := flag.Int64("atomic-size", 4096, "with -atomic-writes: largest write (bytes) issued as an atomic write; larger writes take the normal path (the device's atomic_write_unit_max is the hard ceiling)")
	vacuumInterval := flag.Int("vacuum-interval", 0, "with -meta-dir: seconds between background vacuums that unlink deleted volumes' objects from the OSDs (0 = off; the admin API and unistorctl vacuum run are the manual triggers)")
	raftAddr := flag.String("raft", "", "with -meta-dir: replicate meta over raft on this bind address (host:port). Reads always work; writes need a quorum, without one the meta is read-only (503 on the admin API). A node with -raft but no -osd is a storage-less witness voter")
	raftPeers := flag.String("raft-peers", "", "with -raft: FULL voter set as comma-separated host:port (including this node), e.g. 10.0.0.1:9401,10.0.0.2:9401,10.0.0.3:9401")
	raftDir := flag.String("raft-dir", "", "with -raft: raft data directory (default: <meta-dir>/raft)")
	raftProxy := flag.String("raft-proxy", "", "with -raft: serve write forwarding on this host:port, so writes that reach a follower are relayed to the leader (recommended; use the storage net)")
	raftProxyPeers := flag.String("raft-proxy-peers", "", "with -raft-proxy: proxy addresses parallel to -raft-peers (same order, including this node), e.g. 10.0.0.1:9501,10.0.0.2:9501,10.0.0.3:9501")
	tlsCert := flag.String("tls-cert", "", "PEM certificate (with -tls-key): enables mutual TLS on the OSD data plane, raft transport and the admin API; the certificate must list the node's dial addresses (IPs included) as SANs")
	tlsKey := flag.String("tls-key", "", "PEM private key for -tls-cert")
	tlsCA := flag.String("tls-ca", "", "PEM CA certificate(s) that signed the peers'/clients' certificates; required for the OSD/raft planes; it is also used to verify admin API clients unless -tls-client-ca-file is set")
	tlsClientCA := flag.String("tls-client-ca-file", "", "with -tls-ca: require admin API client certificates signed by this CA (by default the admin API presents a certificate but does not demand client certificates, so plain 'curl --cacert' works)")
	tlsMinVer := flag.String("tls-min-version", "1.2", "minimum TLS version: 1.2 or 1.3")
	tlsPlanes := flag.String("tls-planes", "data,raft,nbd,admin", "with -tls-cert: comma-separated planes TLS is enabled for: data (OSD-to-OSD replication/recovery), raft (meta transport + write forwarding), nbd (export START_TLS upgrade), admin (admin HTTP API). Drop a plane to keep it in the clear - \"raft,admin\" runs the bulk data path unencrypted (vitastor-style) with the control plane encrypted; every peer must be configured the same way per plane")
	tlsSkipVerify := flag.Bool("tls-skip-verify", false, "encrypt the channels but authenticate nothing: a client does not verify the peer's certificate chain or address, a server accepts any client certificate. Meant for standing a cluster up (and for keeping it running through broken material) - a deployment in this state can be impersonated by anyone on the network, so the daemon warns at startup. Server side of the flag: peers presenting no certificate are accepted")
	tlsReloadSecs := flag.Int("tls-reload-interval", 0, "with -tls-cert: seconds between polls of the certificate files that reload them when their mtime changes (0 = off; SIGHUP and POST /v1/tls/reload are the primary reload triggers; the CA file is never re-loaded - rotating a CA needs a restart)")
	importAllowPrivate := flag.Bool("import-allow-private", false, "let URL imports (-import / job url) fetch from private, loopback and link-local networks. Off by default: the ingest fetch is an HTTP client driven by the control plane, so without this guard a supplied URL can reach cloud metadata, the placement plane on localhost, and other internal services. Overrides stay blocked (unspecified, multicast, CGNAT/overlay) and bare hostnames are always refused; set this only on an isolated storage net that legitimately imports from a private mirror")
	importMaxSize := flag.String("import-max-size", "0", "cap on how many bytes one URL/stream import may land in a volume (suffixes K/M/G/T, e.g. 40G; 0 = unlimited). Guards against an unbounded source filling the cluster; a stream that would cross the cap fails the job")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.Line("unistord"))
		return
	}

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
	if *pinCPUs != "" {
		// Pinning the first thread shades the whole process: Linux
		// sched_setaffinity masks are inherited by every thread the
		// runtime creates afterwards (see pkg/cpu). A NUMA daemon
		// wants this plus -gomaxprocs and the arena on the same node.
		if _, perr := cpu.ParseCPUs(*pinCPUs); perr != nil {
			logger.Printf("cpu: -pin-cpus %q invalid (%v): affinity left alone", *pinCPUs, perr)
		} else if perr := cpu.PinCurrent(*pinCPUs); perr != nil {
			logger.Printf("cpu: -pin-cpus %q: %v", *pinCPUs, perr)
		}
	}
	if *gomaxprocs > 0 {
		if n := runtime.GOMAXPROCS(*gomaxprocs); n != *gomaxprocs {
			logger.Printf("sched: GOMAXPROCS %d -> %d", n, *gomaxprocs)
		}
	}

	// -cluster was the original name of -meta-dir. It stays accepted (units
	// and scripts in the field use it), but the canonical spelling is
	// -meta-dir, which says what the directory is and matches unistorctl.
	clusterPath := *metaDir
	if *clusterDir != "" {
		if clusterPath != "" && clusterPath != *clusterDir {
			logger.Fatalf("-meta-dir and -cluster point to different directories (%q and %q); use only -meta-dir", clusterPath, *clusterDir)
		}
		clusterPath = *clusterDir
		logger.Printf("flag -cluster is deprecated, it is the same as -meta-dir (%s)", *clusterDir)
	}

	// -vub was the original short name of -vhost-user-blk. The export TYPE on
	// the wire is "vhost-user-blk" too; the old spelling keeps working here.
	tlsPlanesSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "vub" {
			logger.Printf("flag -vub is deprecated, it is the same as -vhost-user-blk")
		}
		if f.Name == "tls-planes" {
			tlsPlanesSet = true
		}
	})

	commitMode, err := store.ParseCommitMode(*commit)
	if err != nil {
		logger.Fatalf("commit: %v", err)
	}

	// TLS material: when any -tls-* flag is given, the planes named by
	// -tls-planes (all of them by default) switch to mutual TLS. A plane
	// outside the set stays in the clear - that is how a cluster runs the
	// bulk data path plain, the way vitastor deployments do, while raft
	// and the admin API stay encrypted. Peers must agree per plane: a
	// plain listener and a TLS peer on the same plane cannot talk.
	planes, err := tlsconf.ParsePlanes(*tlsPlanes)
	if err != nil {
		logger.Fatalf("tls: %v", err)
	}
	tlsMaterial := tlsconf.Config{
		CertFile:   *tlsCert,
		KeyFile:    *tlsKey,
		CAFile:     *tlsCA,
		MinVersion: *tlsMinVer,
		SkipVerify: *tlsSkipVerify,
	}
	planeSet := tlsconf.NewPlaneSet(planes...)
	if !tlsMaterial.Any() && !tlsPlanesSet {
		// The default of -tls-planes names every plane; without material
		// that is not a contradiction to complain about, it is just the
		// default. Only a planes list the operator typed is worth an
		// error ("you asked to encrypt planes that have no certificate").
		planeSet = nil
	}
	tlsState, err := setupTLS(tlsMaterial, *tlsClientCA, planeSet, logger)
	if err != nil {
		logger.Fatalf("tls: %v", err)
	}
	// Per-plane configurations: nil means the plane runs in the clear.
	var (
		tlsDataServer, tlsDataClient *tls.Config
		tlsRaftServer, tlsRaftClient *tls.Config
		tlsNBDServer, tlsAdmin       *tls.Config
	)
	if tlsState != nil {
		tlsDataServer, tlsDataClient = tlsState.dataServer, tlsState.dataClient
		tlsRaftServer, tlsRaftClient = tlsState.raftServer, tlsState.raftClient
		tlsNBDServer, tlsAdmin = tlsState.nbdServer, tlsState.adminSrv
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

	// Reload lifecycle: SIGHUP re-applies everything the daemon can
	// adopt at runtime - the certificate material and the declarative
	// cluster sections (-qos-class, -crushmap, -create) - so the
	// operator's command is "systemctl kill -s HUP unistord" (or
	// "kill -HUP <pid>") for any config change, not only for a new
	// certificate. POST /v1/reload is the same path, and the optional
	// poller below notices certificate files that changed on disk. Every
	// section keeps its last good state when the new one is broken.
	reloadSt := newReloadState(tlsState, *gogc, logger)
	reloadSt.signalReload(ctx, logger)
	tlsState.watchReload(ctx, *tlsReloadSecs, logger)

	// Clustered mode: exports are volumes placed by CRUSH across OSDs
	// (this process hosts -osd daemons; peers may run in other unistord).
	if clusterPath != "" {
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
				dir = filepath.Join(clusterPath, "raft")
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
				TLS:        tlsRaftServer,
				ProxyTLS:   tlsRaftClient,
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
			ds, err := meta.OpenDir(clusterPath)
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
			if tlsDataClient != nil {
				cl.SetTLS(tlsDataClient)
			}
			go cl.VacuumLoop(ctx, time.Duration(*vacuumInterval)*time.Second,
				func(f string, a ...any) { logger.Printf(f, a...) })
			logger.Printf("vacuum: autovacuum every %vs", *vacuumInterval)
		}

		// -data-dir entries: assign ids and verify the disks. Every
		// entry is a data directory, optionally bound to a device
		// identity ("uuid=<identity>@<dir>"). The id is pinned in
		// <dir>/id and the device identity in <dir>/device (plus
		// <meta-dir>/devices.json), and every restart re-checks the
		// pins and the meta registration against the live device, so
		// an operator who mounts by /dev/sdX and boots after the
		// enumeration swapped disks gets a fatal error instead of
		// silently serving foreign data under the wrong ids.
		var osdSpecs []string
		osdSpecs = append(osdSpecs, osds...)
		osdDevice := map[int32]string{}
		var dataDirSpecs []dataDirSpec
		for _, spec := range dataDirs {
			sp, perr := parseDataDirSpec(spec)
			if perr != nil {
				logger.Fatalf("-data-dir %q: %v", spec, perr)
			}
			dataDirSpecs = append(dataDirSpecs, sp)
		}
		// -osd specs claim their ids outright; a data dir pinned to a
		// taken id is a collision the operator must resolve.
		used := map[int32]bool{}
		for _, spec := range osds {
			id, dir, _, _, perr := parseOSDSpec(spec)
			if perr != nil {
				logger.Fatalf("-osd %q: %v", spec, perr)
			}
			used[id] = true
			// An explicit -osd dir may carry a device pin from an
			// earlier -data-dir boot (<dir>/device). Honour it: the
			// operator chose this dir for this id, so a pin that does
			// not match the live device means the disks moved.
			if want := readDevicePin(dir); want != "" {
				if live, derr := store.DeviceID(dir); derr == nil {
					if verr := store.ValidateDeviceID(live, want); verr != nil {
						logger.Fatalf("-osd %d@%s: %v", id, dir, verr)
					}
					osdDevice[id] = live.Identity()
				}
			}
		}
		// Meta registration is the cluster-wide view: an id registered
		// here is taken (it may live on another node), and its recorded
		// device is the strongest evidence of where an OSD belongs. On
		// a raft cluster meta may not be readable yet (no leader); the
		// pins stay primary and the meta checks are skipped.
		registered := map[int32]meta.OSDInfo{}
		if infos, rerr := meta.ListOSDs(ctx, ms); rerr == nil {
			for _, in := range infos {
				registered[in.ID] = in
			}
		} else if !meta.IsReadOnly(rerr) {
			logger.Printf("meta: OSD registry unreadable (%v): id assignment relies on the dir pins alone", rerr)
		}
		if len(dataDirSpecs) > 0 {
			pins, err := readOSDPins(dataDirSpecs)
			if err != nil {
				logger.Fatalf("-data-dir: %v", err)
			}
			for dir, p := range pins {
				if p.id != 0 && used[p.id] {
					logger.Fatalf("-data-dir %s: id pin %d is already used by an -osd spec", dir, p.id)
				}
				if p.id != 0 {
					used[p.id] = true
				}
			}
			// A registered id is taken cluster-wide (it may be served by
			// another node), so a brand new dir must never draw it.
			for id := range registered {
				used[id] = true
			}
			nodeMap, err := readDeviceMap(clusterPath)
			if err != nil {
				logger.Printf("meta: %v: device map unavailable, relying on the dir pins", err)
				nodeMap = nil
			}
			plans, err := detectDataDirs(dataDirSpecs, pins, nodeMap)
			if err != nil {
				logger.Fatalf("-data-dir: %v", err)
			}
			for _, p := range plans {
				logger.Printf("-data-dir %s: on %s%s", p.spec.dir, p.live.IdentityLabel(),
					func() string {
						if v := p.live.MountedByKernelName(); v != "" {
							return fmt.Sprintf(" (fstab mounts it by kernel name %s: a reboot can swap disks between mount points - mount by UUID=... instead)", v)
						}
						return ""
					}())
			}
			if err := checkDataDirSwaps(plans, registered, func(f string, a ...any) { logger.Printf(f, a...) }); err != nil {
				logger.Fatalf("%v", err)
			}
			assigned, err := assignDataDirIDs(plans, used, func(f string, a ...any) { logger.Printf(f, a...) })
			if err != nil {
				logger.Fatalf("-data-dir: %v", err)
			}
			for _, a := range assigned {
				if err := writeOSDPin(a.dir, a.id, liveIdentity(plans, a.dir)); err != nil {
					logger.Fatalf("-data-dir %s: %v", a.dir, err)
				}
				osdSpecs = append(osdSpecs, fmt.Sprintf("%d@%s", a.id, a.dir))
				if id := liveIdentity(plans, a.dir); id != "" {
					osdDevice[a.id] = id
				}
				logger.Printf("osd %d on %s (-data-dir)", a.id, a.dir)
			}
			if clusterPath != "" {
				merged := map[string]string{}
				for k, v := range nodeMap {
					merged[k] = v
				}
				for _, p := range plans {
					if p.live.Identity() != "" {
						merged[mapKey(p.spec.dir)] = p.live.Identity()
					}
				}
				if err := writeDeviceMap(clusterPath, merged); err != nil {
					logger.Printf("meta: write %s: %v", deviceMapFile, err)
				}
			}
		}

		if len(exports) > 0 {
			logger.Fatalf("-export is a standalone flag: in cluster mode exports are runtime objects, create them with \"unistorctl export create\"")
		}

		// Witness: raft voter without storage. The process keeps the
		// quorum (and can serve the read-only admin/status surface)
		// and hosts no data.
		witness := len(osdSpecs) == 0 && raftSt != nil
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
		// The boot parameters are also the reload spec: the declarative
		// sections (-qos-class, -crushmap, -create) are re-applied from
		// the same data over a dedicated cluster client, so a reload
		// never disturbs the handle the admin API and the exports use.
		cp := clusterParams{
			store:         ms,
			metaDir:       clusterPath,
			osds:          osdSpecs,
			osdDevice:     osdDevice,
			crushmap:      *crushPath,
			epoch:         uint32(*epoch),
			pgNum:         *pgNum,
			replicas:      *replicas,
			minSize:       *minSize,
			rule:          *rule,
			creates:       creates,
			qosClasses:    qosClasses,
			osdQoS:        *osdQoS,
			commit:        commitMode,
			fsyncBatch:    *fsyncBatch,
			intentLog:     *intentLog,
			atomicWrites:  *atomicWrites,
			atomicSize:    *atomicSize,
			asyncRing:     *asyncRing,
			ringPinCPUs:   *ringPinCPU,
			ringArena:     *ringArenaSlots,
			ringArenaSz:   *ringArenaSlot,
			ringHugepages: *ringHugePages,
			reusePort:     *reusePort,
			tlsDataSrv:    tlsDataServer,
			tlsDataCli:    tlsDataClient,
			disks:         disks,
			metrics:       mreg,
			logger:        logger,
		}
		var reloadCl *client.Cluster
		if !witness {
			reloadCl = client.NewCluster(ms)
			if tlsDataClient != nil {
				reloadCl.SetTLS(tlsDataClient)
			}
			defer reloadCl.Close()
			reloadSt.enableCluster(specFromParams(cp), reloadCl, ms)
		}
		var hub *exportHub
		var admin *adminAPI
		var vdm *vduseManager
		var vubm *vubManager
		if *metricsAddr != "" {
			mcl := client.NewCluster(ms)
			if tlsDataClient != nil {
				mcl.SetTLS(tlsDataClient)
			}
			defer mcl.Close()
			msrc := metricsSource{cl: mcl, ms: ms, raft: raftSt, disks: disks, vacuumInterval: *vacuumInterval, tls: tlsState, reload: reloadSt}
			go func() {
				if err := mreg.serveMetrics(ctx, *metricsAddr, msrc, logger); err != nil {
					logger.Printf("metrics http: %v", err)
				}
			}()
		}
		if *httpAddr != "" || len(vduseSpecs) > 0 {
			vdm = newVDUSEManager(logger, vduseFlags{Trim: *trim, WriteZeroes: *writeZeroes, ReadAhead: *vduseReadAhead, ReadAheadWindow: *vduseReadAheadWindow, Queues: *vduseQueues})
		}
		if *httpAddr != "" || len(vubSpecs) > 0 {
			vubm = newVUBManager(logger, vubFlags{Trim: *trim, WriteZeroes: *writeZeroes, Queues: *vubQueues, Workers: *vubWorkers})
		}
		if *httpAddr != "" {
			// Start the admin API before the exports: on a raft cluster
			// the export setup may block waiting for OSD registration
			// (which needs a quorum), and the operator needs the status
			// and write endpoints available throughout.
			hub = newExportHub(nil, logger, vdm, vubm)
			admin, err = newAdminServer(ms, *httpAddr, logger, raftSt, disks, tlsAdmin, tlsDataClient, tlsState, reloadSt, *adminToken, hub)
			if err != nil {
				logger.Fatalf("admin http: %v", err)
			}
			// URL imports fetch a control-plane-supplied address, so the
			// import client runs behind the SSRF guard (dial-time IP
			// policy + bounded redirects) and the size cap.
			maxSize, err := sizeSuffix(*importMaxSize)
			if err != nil {
				logger.Fatalf("import max size: %v", err)
			}
			guard := newSSRFGuard(*importAllowPrivate, logger)
			admin.importHTTP = &http.Client{Transport: guard.importTransport(), CheckRedirect: guard.checkRedirect, Timeout: 0}
			admin.importMaxSize = int64(maxSize)
			// The hub's exports, import/export jobs and runtime NBD
			// exports all attach through the admin's cluster client.
			admin.cluster().SetWriteback(int64(*writebackMB)<<20, time.Duration(*writebackMS)*time.Millisecond)
			hub.setCluster(admin.cluster())
			hub.setMetrics(mreg)
			// Recovery runs started over the admin API feed the metrics
			// registry (nil-safe); the admin itself never serves them.
			admin.recoveryRecord = mreg.recoveryRecorder()
			go admin.serve(ctx)
			go admin.resumeJobs(ctx)
		}
		if !witness {
			if err := clusterBoot(ctx, cp); err != nil {
				logger.Fatalf("cluster: %v", err)
			}
		} else {
			logger.Printf("witness mode: raft voter, no OSDs")
			if len(vduseSpecs) > 0 || len(vubSpecs) > 0 {
				logger.Fatalf("witness mode hosts no storage: nothing to expose via -vduse/-vhost-user-blk")
			}
			<-ctx.Done()
			logger.Printf("bye")
			return
		}
		// Boot devices: each -vduse / -vhost-user-blk spec names a
		// cluster volume (exports are runtime objects, so there is no
		// boot export list to resolve against). Devices created here
		// are also recorded in meta, so a restart replays them exactly
		// like admin-API exports.
		bootCl := client.NewCluster(ms)
		bootCl.SetPoolLimit(256)
		bootCl.SetWriteback(int64(*writebackMB)<<20, time.Duration(*writebackMS)*time.Millisecond)
		if tlsDataClient != nil {
			bootCl.SetTLS(tlsDataClient)
		}
		defer bootCl.Close()
		if vdm != nil {
			vdm.setContext(ctx)
		}
		if vubm != nil {
			vubm.setContext(ctx)
		}
		if len(vduseSpecs) > 0 {
			if err := bootClusterDevices(ctx, ms, "vduse", vduseSpecs, qosMap, logger, func(volume, name, _ string, readOnly bool) (bool, error) {
				created, err := vdm.add(volume, name, bootCl, meta.VolumeSpec{}, readOnly)
				return created, err
			}); err != nil {
				logger.Fatalf("vduse: %v", err)
			}
			go func() {
				<-ctx.Done()
				vdm.closeAll()
			}()
		}
		if len(vubSpecs) > 0 {
			if err := bootClusterDevices(ctx, ms, "vhost-user-blk", vubSpecs, qosMap, logger, func(volume, name, socket string, readOnly bool) (bool, error) {
				v, err := vubm.openVolume(volume, bootCl)
				if err != nil {
					return false, err
				}
				size := v.Size()
				err = vubm.create(vubm.shutdownContext(), name, volume, socket, nbd.NewVolumeBackend(v), size, readOnly || v.Spec().ReadOnly)
				return err == nil, err
			}); err != nil {
				logger.Fatalf("vub: %v", err)
			}
			go func() {
				<-ctx.Done()
				vubm.closeAll()
			}()
		}
		// Restart survival: re-create the runtime exports recorded in
		// meta (each add path is idempotent) and resume ingest jobs this
		// daemon owned. Replay starts AFTER the boot devices: boot specs
		// may pin a custom socket, and add() keyed by name is a no-op on
		// the device boot already created.
		if admin != nil {
			go admin.replayExports(ctx)
		}
		serve(ctx, logger, *listen, *reusePort, hub, nbd.ServerOptions{
			MaxConnections:    *maxConns,
			MaxReadWorkers:    *readWorkers,
			Logger:            logger,
			EnableTrim:        *trim,
			EnableWriteZeroes: *writeZeroes,
			TLSServerConfig:   tlsNBDServer,
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
		if *exportDir != "" && !filepath.IsAbs(path) {
			path = filepath.Join(*exportDir, path)
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
		b, err := newBackend(*storeMode, path, size, *direct, *prealloc, commitMode, *fsyncBatch, *asyncRing, *ringPinCPU, *ringArenaSlots, *ringArenaSlot, *ringHugePages, *intentLog, *atomicWrites, *atomicSize)
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
		if _, err := startVDUSE(ctx, logger, es, vduseSpecs, *trim, *writeZeroes, *vduseReadAhead, *vduseReadAheadWindow, *vduseQueues); err != nil {
			logger.Fatalf("vduse: %v", err)
		}
	}

	if len(vubSpecs) > 0 {
		vm, err := startVUB(ctx, logger, es, vubSpecs, vubFlags{
			Trim: *trim, WriteZeroes: *writeZeroes, Queues: *vubQueues, Workers: *vubWorkers,
		})
		if err != nil {
			logger.Fatalf("vub: %v", err)
		}
		go func() {
			<-ctx.Done()
			vm.closeAll()
		}()
	}

	serve(ctx, logger, *listen, *reusePort, nil, nbd.ServerOptions{
		MaxConnections:    *maxConns,
		MaxReadWorkers:    *readWorkers,
		Logger:            logger,
		EnableTrim:        *trim,
		EnableWriteZeroes: *writeZeroes,
		TLSServerConfig:   tlsNBDServer,
	}, es)
}

// serve runs the NBD server until ctx is cancelled. hub (nullable) is
// bound to the server so the admin API can manage exports at runtime.
func serve(ctx context.Context, logger *log.Logger, listen string, reusePort int, hub *exportHub, opts nbd.ServerOptions, es []*nbd.Export) {
	srv := nbd.NewServer(es, opts)
	if hub != nil {
		hub.setNBD(srv)
	}

	lns, err := sdsnet.ListenReusePort(listen, reusePort)
	if err != nil {
		logger.Fatalf("listen %s: %v", listen, err)
	}
	logger.Printf("listening on %s (%d listener(s))", lns[0].Addr(), len(lns))

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

	if err := srv.ServeListeners(lns); err != nil && ctx.Err() == nil {
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

// parseCreateSpec splits a -create spec: "name:size" or "pool/name:size".
func parseCreateSpec(spec string) (pool, name, sizeStr string, err error) {
	left, size, cut := strings.Cut(spec, ":")
	if !cut || size == "" {
		return "", "", "", fmt.Errorf("expected [pool/]name:size")
	}
	if pName, vol, hasPool := strings.Cut(left, "/"); hasPool {
		pool, left = pName, vol
	}
	if left == "" {
		return "", "", "", fmt.Errorf("expected [pool/]name:size")
	}
	return pool, left, size, nil
}

type clusterParams struct {
	store   meta.Store // shared meta store (dir-backed or raft), opened by the caller
	metaDir string     // for logs only
	osds    []string
	// osdDevice maps an OSD id to the stable device identity of its
	// data dir, resolved (and swap-checked) at boot. Registered in
	// meta.OSDInfo.Device so the cluster sees where each OSD lives.
	osdDevice     map[int32]string
	crushmap      string
	epoch         uint32
	pgNum         int
	replicas      int
	minSize       int
	rule          string
	creates       []string
	qosClasses    []string         // -qos-class name=rates registrations
	osdQoS        string           // per-OSD aggregate rates spec ("" = none)
	commit        store.CommitMode // OSD commit mode
	fsyncBatch    time.Duration    // OSD fsync coalescing window
	intentLog     bool             // write intent log (ideas §2.1)
	atomicWrites  bool             // RWF_ATOMIC writes (ideas §2.2)
	atomicSize    int64            // cap of one atomic write (bytes)
	asyncRing     int              // io_uring depth for hosted OSDs
	ringPinCPUs   string           // OSD ring worker pinning
	ringArena     int              // OSD registered-buffer arena slots
	ringArenaSz   int              // arena slot bytes
	ringHugepages bool             // arena on huge pages
	reusePort     int              // SO_REUSEPORT listeners per data plane
	tlsDataSrv    *tls.Config      // OSD data plane server side (-tls-planes data; nil = plain)
	tlsDataCli    *tls.Config      // client side for cluster pools (nil = plain)
	disks         *diskRegistry    // hosted OSDs publish disk stats here (admin status)
	metrics       *clusterMetrics  // prometheus registry (nil = metrics disabled)
	logger        *log.Logger
}

// clusterBoot brings up a single-process (or multi-process, shared
// meta directory) cluster: host the requested in-process OSDs, publish
// the crush map and create the requested volumes. Exports are not part
// of the boot: they are runtime objects (admin API / unistorctl) that
// every node replays from meta.
func clusterBoot(ctx context.Context, p clusterParams) error {
	ms := p.store
	var err error
	cl := client.NewCluster(ms)
	// The VDUSE data path runs up to 64 concurrent requests per queue,
	// each touching every replica: 64 x 3 replicas = 192 simultaneous
	// connections, so the default 64-per-OSD pool cap would queue ops
	// inside the pool. Raise it; each connection is 256KB of bufio.
	cl.SetPoolLimit(256)
	if p.tlsDataCli != nil {
		cl.SetTLS(p.tlsDataCli)
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

	// Register QoS classes so volumes can reference them by name. The
	// same code runs on reload: boot and reload share every declarative
	// section, so a change made to the flags' source and pushed through
	// /v1/reload lands the same records as a restart would.
	spec := specFromParams(p)
	logf := func(f string, a ...any) { p.logger.Printf(f, a...) }
	if _, err := applyQoSClasses(ctx, cl, &spec, writeMeta, logf); err != nil {
		return err
	}

	// Per-disk aggregate ceiling shared by all volumes on each hosted
	// OSD (the anti-noisy-neighbour cap on the NVMe itself).
	var aggSpec qosRates
	if p.osdQoS != "" {
		aggSpec, err = parseQoSRatesStruct(p.osdQoS)
		if err != nil {
			return fmt.Errorf("-osd-qos %q: %w", p.osdQoS, err)
		}
	}

	// Host in-process OSDs: id@data-dir[@host:port[@ctrl-host:ctrl-port]].
	type hosted struct {
		id   int32
		d    *osd.Daemon
		done chan error
	}
	var running []*hosted
	for _, spec := range p.osds {
		id, dataDir, addr, ctrlAddr, err := parseOSDSpec(spec)
		if err != nil {
			return fmt.Errorf("-osd %q: %w", spec, err)
		}
		d, err := osd.New(osd.Options{
			ID:             id,
			Addr:           addr,
			CtrlAddr:       ctrlAddr,
			DataDir:        dataDir,
			DeviceID:       p.osdDevice[id],
			Meta:           ms,
			Aggregate:      aggSpec.pair(),
			CommitMode:     p.commit,
			FsyncBatch:     p.fsyncBatch,
			Intent:         p.intentLog,
			AtomicWrites:   p.atomicWrites,
			AtomicSize:     p.atomicSize,
			AsyncRing:      p.asyncRing,
			RingPinCPUs:    p.ringPinCPUs,
			RingArenaSlots: p.ringArena,
			RingArenaSlot:  p.ringArenaSz,
			RingHugePages:  p.ringHugepages,
			ReusePort:      p.reusePort,
			TLS:            p.tlsDataSrv,
			Metrics:        p.metrics.osdServerMetrics(id),
			Logf:           func(f string, a ...any) { p.logger.Printf("osd %d: %s", id, fmt.Sprintf(f, a...)) },
		})
		if err != nil {
			return fmt.Errorf("osd %d: %w", id, err)
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
		return err
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
		// Read-replica selection: prefer the in-process (local) OSDs,
		// then the least loaded, then map order (roadmap «чтение с
		// любой реплики», Vitastor local_reads).
		ids := make([]int32, 0, len(byID))
		for id := range byID {
			ids = append(ids, id)
		}
		cl.SetLocalOSDs(ids)
		p.logger.Printf("local-direct transport enabled for %d hosted OSD(s)", len(byID))
	}

	// Publish the crush map if asked and not already present at this
	// epoch (idempotent restarts keep the existing map). Epoch 0 means
	// "auto": 1 on a fresh cluster; on a running one a map already
	// exists, so publishing is skipped (use "unistorctl map publish", or
	// a reload, to push a new generation).
	if _, _, err := applyCrushmap(ctx, cl, ms, &spec, writeMeta, logf, publishBoot, nil); err != nil {
		return err
	}

	// Placement options from flags, clamped by the map's own buckets.
	if _, err := cl.Refresh(ctx); err != nil {
		return fmt.Errorf("placement: %w", err)
	}

	// Create the declared volumes (an existing volume is kept as is).
	if _, err := applyCreates(ctx, cl, ms, &spec, writeMeta, logf); err != nil {
		return err
	}

	return nil
}

// bootClusterDevices wires boot -vduse / -vhost-user-blk specs in
// cluster mode: every spec names a cluster volume, and open (the
// manager path) creates the device through the cluster client. Each
// freshly created device is recorded in meta exactly like an
// admin-API export, so "export ls" lists it and restart replay keeps
// it alive (the replay add is an idempotent no-op for a device the
// boot path re-created, so a custom socket from the spec survives).
// A -qos entry for a boot volume is a standalone mechanism: cluster
// QoS rides on the volume (qos-class), so it is only reported.
func bootClusterDevices(ctx context.Context, ms meta.Store, typ string, specs []string, qosMap map[string]string, logger *log.Logger, open func(volume, name, socket string, readOnly bool) (bool, error)) error {
	for _, spec := range specs {
		var volume, name, socket string
		var readOnly bool
		switch typ {
		case "vduse":
			volume, name = parseVDUSESpec(spec)
			if err := validateDevName(name); err != nil {
				return fmt.Errorf("-vduse %q: %w", spec, err)
			}
		case "vhost-user-blk":
			volume, socket, readOnly = parseVUBSpec(spec)
			name = volume
			if err := validateVUBSocket(socket); err != nil {
				return fmt.Errorf("-vhost-user-blk %q: %w", spec, err)
			}
		default:
			return fmt.Errorf("boot devices: unsupported export type %q", typ)
		}
		if _, ok := qosMap[volume]; ok {
			logger.Printf("%s %q: -qos %s=... has no effect in cluster mode: QoS rides on the volume (qos-class)", typ, volume, volume)
		}
		created, err := open(volume, name, socket, readOnly)
		if err != nil {
			return fmt.Errorf("%s %q: %w", typ, spec, err)
		}
		if !created {
			continue
		}
		if _, err := meta.PutExportRecord(ctx, ms, meta.ExportRecord{
			Type: typ, Name: name, Volume: volume, ReadOnly: readOnly,
			CreatedAt: time.Now().Unix(),
		}); err != nil && !meta.IsReadOnly(err) {
			logger.Printf("%s: persist boot device %q: %v", typ, name, err)
		}
	}
	return nil
}

// firstFreeOSDID returns the lowest positive id absent from used.
func firstFreeOSDID(used map[int32]bool) int32 {
	for id := int32(1); ; id++ {
		if !used[id] {
			return id
		}
	}
}

// parseOSDSpec parses an -osd entry (and, for compatibility, a bare
// -data-dir path: id = -1 marks the auto-assigned case, the boot
// sequence pins the chosen id in <dir>/id, see osd_boot.go). The full
// form is
// "id@data-dir[@host:port[@ctrl-host:ctrl-port]]" (default
// 127.0.0.1:0, no control plane). The 4th segment is the optional
// control-plane listen address: pings/StatFS probes dial it instead of
// the data address, so a saturated data network cannot make the
// cluster consider this OSD dead.
func parseOSDSpec(spec string) (id int32, dataDir, addr, ctrlAddr string, err error) {
	if !strings.Contains(spec, "@") {
		if spec == "" {
			return 0, "", "", "", fmt.Errorf("empty data dir")
		}
		return -1, spec, "127.0.0.1:0", "", nil
	}
	parts := strings.Split(spec, "@")
	if len(parts) < 2 {
		return 0, "", "", "", fmt.Errorf(`expected "id@data-dir[@host:port[@ctrl-host:ctrl-port]]"`)
	}
	n, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return 0, "", "", "", fmt.Errorf("bad id %q", parts[0])
	}
	addr = "127.0.0.1:0"
	if len(parts) >= 3 {
		addr = parts[2]
	}
	if len(parts) >= 4 {
		ctrlAddr = parts[3]
	}
	if len(parts) > 4 {
		return 0, "", "", "", fmt.Errorf("too many @ segments")
	}
	return int32(n), parts[1], addr, ctrlAddr, nil
}

// newBackend builds the export backend: a plain sparse file (MVP
// default) or, with -store, the phase-1 object-store data path.
func newBackend(storeMode bool, path string, size uint64, direct, prealloc bool, commitMode store.CommitMode, fsyncBatch time.Duration, asyncRing int, ringPinCPU string, ringArenaSlots, ringArenaSlot int, ringHugePages bool, intentLog bool, atomicWrites bool, atomicSize int64) (nbd.Backend, error) {
	if !storeMode {
		return nbd.NewFileBackend(path, size)
	}
	if size == 0 {
		return nil, fmt.Errorf("-store exports require an explicit :size")
	}
	return nbd.NewStoreBackend(path, size, store.Options{
		Direct:         direct,
		Preallocate:    prealloc,
		CommitMode:     commitMode,
		FsyncBatch:     fsyncBatch,
		AsyncRing:      asyncRing,
		RingPinCPUs:    ringPinCPU,
		RingArenaSlots: ringArenaSlots,
		RingArenaSlot:  ringArenaSlot,
		RingHugePages:  ringHugePages,
		Intent:         intentLog,
		AtomicWrites:   atomicWrites,
		AtomicSize:     atomicSize,
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
