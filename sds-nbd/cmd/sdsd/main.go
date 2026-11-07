// Command sdsd is the SDStorage daemon: it exposes one or more block
// exports over the NBD protocol. It is the MVP data-plane entry point
// (phase 4 of the roadmap): QEMU attaches to it as an NBD client and can
// present the export to a guest as an NVMe/virtio disk.
//
// Usage:
//
//	sdsd -listen :10809 -export vol0=/var/lib/sdsd/vol0.img:10G [-readonly vol0] [-trim] [-write-zeroes]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/unistack-org/sds-nbd/nbd"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/osd"
	"github.com/unistack-org/storage/pkg/store"
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

// osdFlag collects repeatable -osd "id@addr" specs (cluster mode).
type osdFlag []string

func (e *osdFlag) String() string { return strings.Join(*e, ",") }
func (e *osdFlag) Set(v string) error {
	*e = append(*e, v)
	return nil
}

func main() {
	var exports exportFlag
	listen := flag.String("listen", ":10809", "TCP address to listen on")
	dataDir := flag.String("data-dir", "", "directory for relative export paths")
	trim := flag.Bool("trim", true, "advertise and handle TRIM (requires hole punching: Linux)")
	writeZeroes := flag.Bool("write-zeroes", true, "advertise and handle WRITE_ZEROES")
	storeMode := flag.Bool("store", false, "treat export paths as object-store directories (phase-1 data path) instead of sparse files")
	direct := flag.Bool("direct", false, "with -store: request O_DIRECT on object files")
	prealloc := flag.Bool("preallocate", false, "with -store: fallocate each new object to full size")
	maxConns := flag.Int("max-conns", 0, "max concurrent client connections (0 = unlimited)")
	clusterDir := flag.String("cluster", "", "meta directory of a cluster (enables clustered exports backed by OSDs)")
	var osds osdFlag
	flag.Var(&osds, "osd", "with -cluster: start an in-process OSD as id@data-dir[@host:port], repeatable")
	crushPath := flag.String("crushmap", "", "with -cluster: crushmap file (text/json/binary) to publish")
	epoch := flag.Uint("epoch", 1, "with -cluster: epoch to publish -crushmap under (must be unique)")
	pgNum := flag.Int("pg-num", 64, "with -cluster: cluster pg_num for the placement table")
	replicas := flag.Int("replicas", 3, "with -cluster: replicas per object")
	rule := flag.String("rule", "", "with -cluster: crush rule name (default: first rule in the map)")
	var creates exportFlag
	flag.Var(&creates, "create", "with -cluster: create volume name:size if absent, repeatable")
	flag.Var(&exports, "export", "export spec name=path[:size] (file/-store) or name (cluster), repeatable")
	flag.Parse()

	logger := log.New(os.Stderr, "sdsd: ", log.LstdFlags|log.Lmsgprefix)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Clustered mode: exports are volumes placed by CRUSH across OSDs
	// (this process hosts -osd daemons; peers may run in other sdsd).
	if *clusterDir != "" {
		es, err := clusterExports(ctx, clusterParams{
			metaDir:  *clusterDir,
			osds:     osds,
			crushmap: *crushPath,
			epoch:    uint32(*epoch),
			pgNum:    *pgNum,
			replicas: *replicas,
			rule:     *rule,
			creates:  creates,
			exports:  exports,
			logger:   logger,
		})
		if err != nil {
			logger.Fatalf("cluster: %v", err)
		}
		serve(ctx, logger, *listen, *maxConns, *trim, *writeZeroes, es)
		return
	}

	if len(exports) == 0 {
		fmt.Fprintln(os.Stderr, "sdsd: at least one -export name=path[:size] is required")
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
		b, err := newBackend(*storeMode, path, size, *direct, *prealloc)
		if err != nil {
			logger.Fatalf("export %s: %v", name, err)
		}
		logger.Printf("export %q -> %s (%s, store=%v)", name, path, humanSize(b.Size()), *storeMode)
		es = append(es, &nbd.Export{Name: name, Backend: b})
	}

	serve(ctx, logger, *listen, *maxConns, *trim, *writeZeroes, es)
}

// serve runs the NBD server until ctx is cancelled.
func serve(ctx context.Context, logger *log.Logger, listen string, maxConns int, trim, writeZeroes bool, es []*nbd.Export) {
	srv := nbd.NewServer(es, nbd.ServerOptions{
		MaxConnections:    maxConns,
		Logger:            logger,
		EnableTrim:        trim,
		EnableWriteZeroes: writeZeroes,
	})

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		logger.Fatalf("listen %s: %v", listen, err)
	}
	logger.Printf("listening on %s", ln.Addr())

	go func() {
		<-ctx.Done()
		logger.Printf("shutting down")
		srv.Close()
	}()

	if err := srv.Serve(ln); err != nil && ctx.Err() == nil {
		logger.Fatalf("serve: %v", err)
	}
	logger.Printf("bye")
}

type clusterParams struct {
	metaDir  string
	osds     []string
	crushmap string
	epoch    uint32
	pgNum    int
	replicas int
	rule     string
	creates  []string
	exports  []string
	logger   *log.Logger
}

// clusterExports brings up a single-process (or multi-process, shared
// meta directory) cluster: host the requested in-process OSDs, publish
// the crush map, create the requested volumes and open every -export
// name as a clustered volume backend.
func clusterExports(ctx context.Context, p clusterParams) ([]*nbd.Export, error) {
	if len(p.exports) == 0 {
		return nil, fmt.Errorf("at least one -export name is required")
	}
	ms, err := meta.OpenDir(p.metaDir)
	if err != nil {
		return nil, fmt.Errorf("meta: %w", err)
	}
	cl := client.NewCluster(ms)

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
			ID:      id,
			Addr:    addr,
			DataDir: dataDir,
			Meta:    ms,
			Logf:    func(f string, a ...any) { p.logger.Printf("osd %d: %s", id, fmt.Sprintf(f, a...)) },
		})
		if err != nil {
			return nil, fmt.Errorf("osd %d: %w", id, err)
		}
		h := &hosted{id: id, d: d, done: make(chan error, 1)}
		go func() { h.done <- d.Run(ctx) }()
		running = append(running, h)
	}
	waitRegistered := func() error {
		for _, h := range running {
			var info meta.OSDInfo
			for i := 0; i < 100; i++ {
				info, err = meta.GetOSD(ctx, ms, h.id)
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
				time.Sleep(50 * time.Millisecond)
			}
			if err != nil {
				return fmt.Errorf("osd %d not registered: %w", h.id, err)
			}
		}
		return nil
	}
	if err := waitRegistered(); err != nil {
		return nil, err
	}

	// Publish the crush map if asked and not already present at this
	// epoch (idempotent restarts keep the existing map).
	if p.crushmap != "" {
		src, err := os.ReadFile(p.crushmap)
		if err != nil {
			return nil, fmt.Errorf("crushmap: %w", err)
		}
		if rec, err := meta.LatestMap(ctx, ms); err != nil || rec.Epoch < p.epoch {
			if err := cl.MapEpoch(ctx, p.epoch, src); err != nil && !errors.Is(err, meta.ErrConflict) {
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
		if _, err := cl.CreateVolume(ctx, name, size, p.replicas, p.pgNum, p.rule); err != nil {
			return nil, fmt.Errorf("create volume %q: %w", name, err)
		}
		p.logger.Printf("created volume %q (%s, r=%d, pg=%d)", name, humanSize(size), p.replicas, p.pgNum)
	}

	var es []*nbd.Export
	for _, name := range p.exports {
		volName, _, _ := strings.Cut(name, "=") // tolerate name=... form
		vol, err := cl.OpenVolume(ctx, volName)
		if err != nil {
			return nil, err
		}
		p.logger.Printf("export %q -> cluster volume %q (%s)", volName, volName, humanSize(vol.Size()))
		es = append(es, &nbd.Export{Name: volName, Backend: nbd.NewVolumeBackend(vol)})
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
func newBackend(storeMode bool, path string, size uint64, direct, prealloc bool) (nbd.Backend, error) {
	if !storeMode {
		return nbd.NewFileBackend(path, size)
	}
	if size == 0 {
		return nil, fmt.Errorf("-store exports require an explicit :size")
	}
	return nbd.NewStoreBackend(path, size, store.Options{
		Direct:      direct,
		Preallocate: prealloc,
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
