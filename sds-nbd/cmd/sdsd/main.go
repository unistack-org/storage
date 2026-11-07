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

	"github.com/unistack-org/sds-nbd/nbd"
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

func main() {
	var exports exportFlag
	listen := flag.String("listen", ":10809", "TCP address to listen on")
	dataDir := flag.String("data-dir", "", "directory for relative export paths")
	trim := flag.Bool("trim", true, "advertise and handle TRIM (requires hole punching: Linux)")
	writeZeroes := flag.Bool("write-zeroes", true, "advertise and handle WRITE_ZEROES")
	maxConns := flag.Int("max-conns", 0, "max concurrent client connections (0 = unlimited)")
	flag.Var(&exports, "export", "export spec name=path[:size], repeatable")
	flag.Parse()

	if len(exports) == 0 {
		fmt.Fprintln(os.Stderr, "sdsd: at least one -export name=path[:size] is required")
		flag.Usage()
		os.Exit(2)
	}

	logger := log.New(os.Stderr, "sdsd: ", log.LstdFlags|log.Lmsgprefix)

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
		b, err := nbd.NewFileBackend(path, size)
		if err != nil {
			logger.Fatalf("export %s: %v", name, err)
		}
		logger.Printf("export %q -> %s (%s)", name, path, humanSize(b.Size()))
		es = append(es, &nbd.Export{Name: name, Backend: b})
	}

	srv := nbd.NewServer(es, nbd.ServerOptions{
		MaxConnections:    *maxConns,
		Logger:            logger,
		EnableTrim:        *trim,
		EnableWriteZeroes: *writeZeroes,
	})

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Fatalf("listen %s: %v", *listen, err)
	}
	logger.Printf("listening on %s", ln.Addr())

	// Graceful shutdown on SIGINT/SIGTERM: stop accepting, close conns.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Printf("shutting down")
		srv.Close()
	}()

	if err := srv.Serve(ln); err != nil {
		logger.Fatalf("serve: %v", err)
	}
	logger.Printf("bye")
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
