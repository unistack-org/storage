// Command unistorctl is the control tool of the unistack storage
// cluster: it edits the cluster meta directory (the same one unistord
// hosts) and moves volume data.
//
//	vol create  <name> <size> [-quota S] [-replicas n] [-min-size n] [-pg-num n] [-rule r]
//	vol ls
//	vol rm      <name>
//	vol info    <name>
//	vol qos     <name> [-class c] [-r iops[:Bps]] [-w iops[:Bps]]
//	vol quota   <name> <size|0>                (allocation cap; 0 = cap at volume size)
//	vol resize  <name> <size>                   (online grow; shrink is refused)
//	vol import  <name> <file|url|-> [-offset N]  (streamed, 1 MiB chunks; "-" = stdin)
//	vol export  <name> <file|->                (holes preserved)
//	vol snap    create <name>                   (freeze a read-only generation)
//	vol snap    ls     <name>                   (list generations, newest last)
//	vol snap    rm     <name> <gen>             (drop one generation)
//	vol snap    rollback <name> <gen>           (live = gen; newer ones are discarded)
//	vol clone   <src> <dst> [-quota S]          (copy-on-write clone over the shared chain)
//	qos ls
//	qos put     <class> <riops:rbps:wiops:wbps>
//	qos rm      <class>
//	osd ls | out <id> | in <id> [-weight w]      (registry weight; 0 = out of placement)
//	osd add <id> <addr> [-data P] [-uuid U]      (register an osd record by hand)
//	map dump [-format raw|text|json|yaml|binary]  (latest crush map epoch; raw = as stored)
//	map check   [file] [-pg-num n] [-replicas n] [-rule r] [-class ssd|hdd]  (compile + pg census + class validators)
//	map publish <file> [-epoch n]                (store a new epoch; default latest+1)
//	map reweight <id> <w>                        (vitastor-style alias of osd out/in)
//	vacuum      [ls|run [vol]]                 (M1.4: deleted-volume garbage; run <vol> targets one tombstone or punches a live volume's all-zero objects)
//	recovery    ls | check <vol> | run <vol>   (M6.3: heal diverged/missing replicas)
//	export      create <vol> [-type nbd|vduse|vhost-user-blk] | ls | rm <name>   (runtime exports via the admin API; shorthand: vub)
//	status      -http URL                       (operational overview, see unistord)
//
// Bandwidth rates accept the K/M/G/T suffixes. A volume whose QoS class
// is missing (or whose rates are all zero) is unlimited: that is the
// admin class. Live open volumes pick rate changes up through the meta
// watch, no remount needed. "recovery run" is paced by -iops/-bps
// (default 200 iops / 200M/s on the write lane) so repairs never
// starve live guests.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/unistack-org/storage/crush"
	"github.com/unistack-org/storage/internal/buildinfo"
	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/placement"
	"github.com/unistack-org/storage/pkg/qos"
	"github.com/unistack-org/storage/pkg/tlsconf"
	"sigs.k8s.io/yaml"
)

const chunk = 1 << 20 // import/export streaming granularity

func main() {
	metaDir := flag.String("meta-dir", "/var/lib/unistord/meta", "cluster meta directory (shared with unistord)")
	tlsCert := flag.String("tls-cert", "", "with a TLS-enabled cluster: PEM client certificate for the OSD data plane (with -tls-key, -tls-ca)")
	tlsKey := flag.String("tls-key", "", "PEM private key for -tls-cert")
	tlsCA := flag.String("tls-ca", "", "PEM CA certificate(s): verify server certificates (OSD data plane, admin -http https URLs)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Usage = usage
	flag.Parse()
	if *showVersion {
		fmt.Println(buildinfo.Line("unistorctl"))
		return
	}
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ms, err := meta.OpenDir(*metaDir)
	if err != nil {
		fatal("meta: %v", err)
	}
	defer ms.Close()
	cl := client.NewCluster(ms)
	if *tlsCA != "" {
		cfg, err := tlsconf.CAOnlyConfig(*tlsCA, "")
		if err != nil {
			fatal("tls: %v", err)
		}
		if *tlsCert != "" || *tlsKey != "" {
			if *tlsCert == "" || *tlsKey == "" {
				fatal("-tls-cert and -tls-key must be set together")
			}
			cfg, err = tlsconf.Config{CertFile: *tlsCert, KeyFile: *tlsKey, CAFile: *tlsCA}.ClientConfig()
			if err != nil {
				fatal("tls: %v", err)
			}
		}
		cl.SetTLS(cfg)
		httpClient = &http.Client{Timeout: 30 * time.Minute, Transport: &http.Transport{TLSClientConfig: cfg}}
	}

	if err := dispatch(ctx, cl, ms, args); err != nil {
		fatal("%v", err)
	}
}

// httpClient is used by the HTTP-speaking commands (status, import from
// URL); it is replaced with a TLS-configured client when -tls-ca is set.
var httpClient = http.DefaultClient

func dispatch(ctx context.Context, cl *client.Cluster, ms meta.Store, args []string) error {
	switch args[0] {
	case "vol":
		return volCmd(ctx, cl, ms, args[1:])
	case "pool":
		return poolCmd(ctx, cl, args[1:])
	case "qos":
		return qosCmd(ctx, cl, args[1:])
	case "osd":
		return osdCmd(ctx, ms, args[1:])
	case "map":
		return mapCmd(ctx, cl, ms, args[1:])
	case "vacuum":
		return vacuumCmd(ctx, cl, args[1:])
	case "recovery":
		return recoveryCmd(ctx, cl, args[1:])
	case "export":
		return exportCmd(ctx, args[1:])
	case "status":
		return statusCmd(ctx, args[1:])
	case "healthcheck":
		return healthcheckCmd(ctx, args[1:])
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// ---- vol ----

func volCmd(ctx context.Context, cl *client.Cluster, ms meta.Store, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("vol: subcommand required (create/ls/rm/info/qos/quota/resize/import/export/snap/clone)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		fs := flag.NewFlagSet("vol create", flag.ExitOnError)
		replicas := fs.Int("replicas", 0, "replica count (0 = pool, else cluster default)")
		minSize := fs.Int("min-size", 0, "min writable replica set (0 = replicas/2+1)")
		pgNum := fs.Int("pg-num", 0, "placement groups (0 = pool, else cluster default)")
		rule := fs.String("rule", "", "crush rule name (empty = pool, else cluster default)")
		pool := fs.String("pool", "", "create the volume in this pool (pool policy fills omitted placement flags)")
		quotaStr := fs.String("quota", "", "allocation cap (thin provisioning), e.g. 20G (0/omitted = volume size)")
		mustParse(fs, rest)
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New(`vol create: <name> <size> e.g. "vol create vm0 10G"`)
		}
		size, err := sizeSuffix(fs.Arg(1))
		if err != nil {
			return fmt.Errorf("vol create: size: %w", err)
		}
		var quota uint64
		if *quotaStr != "" {
			if quota, err = sizeSuffix(*quotaStr); err != nil {
				return fmt.Errorf("vol create: quota: %w", err)
			}
		}
		var spec meta.VolumeSpec
		if *pool != "" {
			// Omitted placement flags stay 0/"" and are filled from
			// the pool record (client side); explicit flags win.
			spec, err = cl.CreateVolumeInPool(ctx, *pool, fs.Arg(0), size, quota, *replicas, *minSize, *pgNum, *rule)
		} else {
			// No pool: omitted placement flags come from the compiled
			// cluster snapshot (built-in defaults on a daemon started
			// without -replicas/-pg-num; the daemon flags only steer
			// its own -create shortcut).
			if *replicas == 0 || *pgNum == 0 || *rule == "" {
				if snap, rerr := cl.Refresh(ctx); rerr == nil {
					if *replicas == 0 {
						*replicas = snap.Replicas
					}
					if *pgNum == 0 {
						*pgNum = int(snap.PGNum)
					}
					if *rule == "" {
						*rule = snap.Rule
					}
				} else if *replicas == 0 || *pgNum == 0 || *rule == "" {
					return fmt.Errorf("vol create: cluster defaults unavailable (%v): pass -replicas/-pg-num/-rule explicitly", rerr)
				}
			}
			spec, err = cl.CreateVolume(ctx, fs.Arg(0), size, quota, *replicas, *minSize, *pgNum, *rule)
		}
		if err != nil {
			return err
		}
		fmt.Printf("volume %q: size=%s quota=%s oid_base=%d replicas=%d min_size=%d pg_num=%d epoch=%d",
			spec.Name, humanSize(spec.Size), quotaLabel(spec), spec.OIDBase, spec.Replicas, spec.MinSize, spec.PGNum, spec.Epoch)
		if spec.Pool != "" {
			fmt.Printf(" pool=%s", spec.Pool)
		}
		fmt.Println()
		return nil

	case "ls":
		specs, err := cl.ListVolumes(ctx)
		if err != nil {
			return err
		}
		if len(specs) == 0 {
			fmt.Println("(no volumes)")
			return nil
		}
		fmt.Printf("%-24s %10s %10s %10s %8s %8s %6s %-10s\n", "NAME", "SIZE", "QUOTA", "USED", "REPLICA", "MINSIZE", "PGNUM", "QOS")
		for _, s := range specs {
			used, err := allocUsage(ctx, ms, s)
			usedStr := humanSize(used)
			if err != nil {
				usedStr = "?"
			}
			fmt.Printf("%-24s %10s %10s %10s %8d %8d %6d %-10s\n",
				s.Name, humanSize(s.Size), quotaLabel(s), usedStr, s.Replicas, s.MinSize, s.PGNum, qosSummary(s))
		}
		return nil

	case "rm":
		if len(rest) != 1 {
			return errors.New("vol rm: <name>")
		}
		if err := cl.DeleteVolume(ctx, rest[0]); err != nil {
			return err
		}
		fmt.Printf("volume %q deregistered (objects stay on the OSDs until vacuum)\n", rest[0])
		return nil

	case "info":
		if len(rest) != 1 {
			return errors.New("vol info: <name>")
		}
		s, err := meta.GetVolume(ctx, ms, rest[0])
		if err != nil {
			return err
		}
		fmt.Printf("name=%s size=%s quota=%s oid_base=%d oid_shift=%d replicas=%d min_size=%d pg_num=%d epoch=%d rule=%s\n",
			s.Name, humanSize(s.Size), quotaLabel(s), s.OIDBase, s.OIDShift, s.Replicas, s.MinSize, s.PGNum, s.Epoch, s.Rule)
		fmt.Printf("qos: %s\n", qosFull(s))
		used, err := allocUsage(ctx, ms, s)
		if err != nil {
			fmt.Printf("usage: ? (%v)\n", err)
		} else {
			fmt.Printf("usage: %s\n", humanSize(used))
		}
		return nil

	case "quota":
		fs := flag.NewFlagSet("vol quota", flag.ExitOnError)
		mustParse(fs, rearg(rest))
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New(`vol quota: <name> <size|0> e.g. "vol quota vm0 20G"`)
		}
		quota, err := sizeSuffix(fs.Arg(1))
		if err != nil {
			return fmt.Errorf("vol quota: %w", err)
		}
		spec, err := cl.SetVolumeQuota(ctx, fs.Arg(0), quota)
		if err != nil {
			return err
		}
		fmt.Printf("volume %q: quota = %s\n", spec.Name, quotaLabel(spec))
		return nil

	case "resize":
		fs := flag.NewFlagSet("vol resize", flag.ExitOnError)
		mustParse(fs, rearg(rest))
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New(`vol resize: <name> <size> e.g. "vol resize vm0 40G" (grow only)`)
		}
		size, err := sizeSuffix(fs.Arg(1))
		if err != nil {
			return fmt.Errorf("vol resize: %w", err)
		}
		spec, err := cl.ResizeVolume(ctx, fs.Arg(0), size)
		if err != nil {
			if errors.Is(err, meta.ErrShrinkUnsupported) {
				cur, cerr := meta.GetVolume(ctx, ms, fs.Arg(0))
				if cerr == nil {
					return fmt.Errorf("vol resize: shrinking is not supported (volume %q is %d bytes, asked for %d)",
						fs.Arg(0), cur.Size, size)
				}
				return fmt.Errorf("vol resize: shrinking is not supported: %w", err)
			}
			return err
		}
		fmt.Printf("volume %q: size = %d bytes (%d objects, %d extent(s))\n",
			spec.Name, spec.Size, spec.ReservedObjs(), len(spec.Extents))
		return nil

	case "qos":
		return volQoS(ctx, cl, rest)

	case "import":
		fs := flag.NewFlagSet("vol import", flag.ExitOnError)
		offset := fs.Uint64("offset", 0, "byte offset inside the volume")
		size := fs.Int64("size", 0, "expected input size in bytes (0 = take the whole source)")
		sum := fs.String("sha256", "", "expected sha256 hex digest of the input")
		mustParse(fs, rest)
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New(`vol import: <name> <file|http(s)://url|-> ("-" reads stdin)`)
		}
		return volImport(ctx, cl, fs.Arg(0), fs.Arg(1), *offset, *size, *sum)

	case "export":
		if len(rest) != 2 {
			return errors.New("vol export: <name> <file|->")
		}
		return volExport(ctx, cl, rest[0], rest[1])

	case "snap":
		return volSnap(ctx, cl, rest)

	case "clone":
		fs := flag.NewFlagSet("vol clone", flag.ExitOnError)
		quotaStr := fs.String("quota", "", "clone allocation cap (0/omitted = inherit the source's)")
		mustParse(fs, rearg(rest))
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New(`vol clone: <src> <dst> e.g. "vol clone base vm7"`)
		}
		var quota uint64
		if *quotaStr != "" {
			var perr error
			if quota, perr = sizeSuffix(*quotaStr); perr != nil {
				return fmt.Errorf("vol clone: quota: %w", perr)
			}
		}
		spec, err := cl.CloneVolume(ctx, fs.Arg(0), fs.Arg(1), quota)
		if err != nil {
			return err
		}
		fmt.Printf("volume %q: cloned from %q (size=%s quota=%s, %d snapshot generation(s) in the shared chain)\n",
			spec.Name, fs.Arg(0), humanSize(spec.Size), quotaLabel(spec), len(spec.Snapshots))
		return nil

	default:
		usage()
		return fmt.Errorf("vol: unknown subcommand %q", sub)
	}
}

// rearg moves leading positional args after flags so that
// `vol qos vm1 -r 20` and `vol qos -r 20 vm1` behave the same: Go's
// flag package stops at the first non-flag word.
func rearg(args []string) []string {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") && len(a) > 1 {
			flags = append(flags, a)
			// Space-separated value: -name value (not -name=value, not bool-like).
			if !strings.Contains(a, "=") && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		pos = append(pos, a)
	}
	return append(flags, pos...)
}

func volQoS(ctx context.Context, cl *client.Cluster, rest []string) error {
	rest = rearg(rest) // tolerate flags after the volume name
	fs := flag.NewFlagSet("vol qos", flag.ExitOnError)
	class := fs.String("class", "", "QoS class name (\"\" = none; missing class = unlimited/admin)")
	rd := fs.String("r", "", "read limits: iops[:Bps]")
	wr := fs.String("w", "", "write limits: iops[:Bps]")
	mustParse(fs, rest)
	if fs.NArg() != 1 {
		fs.Usage()
		return errors.New(`vol qos: <name> [-class c] [-r iops[:Bps]] [-w iops[:Bps]]`)
	}
	name := fs.Arg(0)
	var spec meta.VolumeSpec
	spec.QoSTemplate = *class
	if *rd != "" {
		iops, bps, err := parsePair(*rd)
		if err != nil {
			return fmt.Errorf("-r: %w", err)
		}
		spec.ReadIOPS, spec.ReadBPS = iops, bps
	}
	if *wr != "" {
		iops, bps, err := parsePair(*wr)
		if err != nil {
			return fmt.Errorf("-w: %w", err)
		}
		spec.WriteIOPS, spec.WriteBPS = iops, bps
	}
	cur, err := cl.SetVolumeQoS(ctx, name, spec)
	if err != nil {
		return err
	}
	fmt.Printf("volume %q: qos = %s\n", name, qosFull(cur))
	return nil
}

func volSnap(ctx context.Context, cl *client.Cluster, args []string) error {
	if len(args) == 0 {
		return errors.New(`vol snap: subcommand required (create/ls/rm/rollback)`)
	}
	sub, rest := args[0], args[1:]
	if len(rest) < 1 {
		return errors.New(`vol snap: <name> required`)
	}
	name := rest[0]
	open := func() (*client.Volume, error) { return cl.OpenVolume(ctx, name) }
	switch sub {
	case "create":
		if len(rest) != 1 {
			return errors.New("vol snap create: <name>")
		}
		v, err := open()
		if err != nil {
			return err
		}
		defer v.Close()
		info, err := v.Snapshot(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("volume %q: snapshot gen %d created at %s (size %s, %d objects)\n",
			name, info.Gen, time.Unix(info.CreatedAt, 0).UTC().Format(time.RFC3339),
			humanSize(info.Size), info.Objects)
		return nil

	case "ls":
		if len(rest) != 1 {
			return errors.New("vol snap ls: <name>")
		}
		v, err := open()
		if err != nil {
			return err
		}
		defer v.Close()
		list, err := v.Snapshots(ctx)
		if err != nil {
			return err
		}
		if len(list) == 0 {
			fmt.Printf("volume %q: (no snapshots)\n", name)
			return nil
		}
		fmt.Printf("%-6s %-22s %10s %10s\n", "GEN", "CREATED", "SIZE", "OBJECTS")
		for _, g := range list {
			fmt.Printf("%-6d %-22s %10s %10d\n", g.Gen,
				time.Unix(g.CreatedAt, 0).UTC().Format(time.RFC3339), humanSize(g.Size), g.Objects)
		}
		return nil

	case "rm", "rollback":
		if len(rest) != 2 {
			return fmt.Errorf("vol snap %s: <name> <gen>", sub)
		}
		gen, err := strconv.ParseUint(rest[1], 10, 32)
		if err != nil {
			return fmt.Errorf("vol snap %s: gen: %w", sub, err)
		}
		v, err := open()
		if err != nil {
			return err
		}
		defer v.Close()
		if sub == "rm" {
			if err := v.DeleteSnapshot(ctx, uint32(gen)); err != nil {
				return err
			}
			fmt.Printf("volume %q: snapshot gen %d dropped (its objects go to vacuum)\n", name, gen)
			return nil
		}
		spec, err := v.Rollback(ctx, uint32(gen))
		if err != nil {
			return err
		}
		fmt.Printf("volume %q: rolled back to gen %d (live gen is %d; discarded ranges go to vacuum)\n",
			name, gen, spec.Gen)
		return nil

	default:
		return fmt.Errorf("vol snap: unknown subcommand %q (create/ls/rm/rollback)", sub)
	}
}

// volImport streams a local file, HTTP(S) URL, or stdin (src "-") into a
// volume. The
// byte accounting is strict: a declared size (-size, or the server's
// Content-Length for HTTP) that the stream ends short of is an error,
// and -sha256 is verified over exactly the bytes written, so a torn
// transfer can never pass as a finished import.
func volImport(ctx context.Context, cl *client.Cluster, name, src string, offset uint64, size int64, want string) error {
	v, err := cl.OpenVolume(ctx, name)
	if err != nil {
		return err
	}
	defer v.Close()
	if offset%512 != 0 || offset > v.Size() {
		return fmt.Errorf("import: offset %d must be 512-aligned and within the volume (%s)", offset, humanSize(v.Size()))
	}
	if size < 0 {
		return fmt.Errorf("import: size must not be negative")
	}
	if size > 0 && int64(v.Size())-int64(offset) < size {
		return fmt.Errorf("import: volume has %s at offset %d, need %s", humanSize(v.Size()-offset), offset, humanSize(uint64(size)))
	}
	if want != "" {
		if _, err := hex.DecodeString(want); err != nil || len(want) != 64 {
			return fmt.Errorf("import: -sha256 must be 64 hex digits")
		}
	}
	var r io.Reader
	var expected int64 // declared input size (0 = unbounded: EOF ends it)
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
		if err != nil {
			return err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("import: fetch %s: %w", src, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("import: fetch %s: HTTP %s", src, resp.Status)
		}
		r = resp.Body
		if size == 0 && resp.ContentLength > 0 {
			expected = resp.ContentLength
		}
	} else if src == "-" {
		// stdin: qcow2 pipes, dd, zcat, ... The size cannot be stat'ed,
		// so the stream ends at EOF unless -size declares it.
		r = os.Stdin
	} else {
		f, err := os.Open(src)
		if err != nil {
			return err
		}
		defer f.Close()
		if size == 0 {
			if fi, serr := f.Stat(); serr == nil && !fi.IsDir() {
				expected = fi.Size()
			}
		}
		r = f
	}
	if size > 0 {
		expected = size
	}
	if expected > 0 && int64(v.Size())-int64(offset) < expected {
		return fmt.Errorf("import: declared %s does not fit (volume has %s at offset %d)",
			humanSize(uint64(expected)), humanSize(v.Size()-offset), offset)
	}
	fmt.Fprintf(os.Stderr, "import: streaming %s -> volume %q\n", src, name)
	var hr io.Writer
	hasher := sha256.New()
	if want != "" {
		hr = hasher
	}
	buf := make([]byte, chunk)
	var total uint64
	for {
		// Read at most the declared remainder so the digest covers
		// exactly the declared input.
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if offset+uint64(n) > v.Size() {
				return fmt.Errorf("import: source larger than the volume (beyond %s at offset %d)", humanSize(v.Size()), offset)
			}
			if _, werr := v.WriteAt(ctx, buf[:n], offset); werr != nil {
				return fmt.Errorf("import: write at %d: %w", offset, werr)
			}
			if hr != nil {
				if _, herr := hr.Write(buf[:n]); herr != nil {
					return fmt.Errorf("import: hash: %w", herr)
				}
			}
			offset += uint64(n)
			total += uint64(n)
			if total%(64<<20) < chunk { // progress every 64 MiB
				fmt.Fprintf(os.Stderr, "import: %s\r", humanSize(total))
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// A mid-chunk break that did land data at a declared
			// size that is not met means the transfer ended short.
			if expected > 0 && int64(total) < expected {
				return fmt.Errorf("import: stream ended at %s of expected %s", humanSize(total), humanSize(uint64(expected)))
			}
			break
		}
		if err != nil {
			return fmt.Errorf("import: read: %w", err)
		}
	}
	if expected > 0 && int64(total) > expected {
		return fmt.Errorf("import: source sent %s, more than the declared %s", humanSize(total), humanSize(uint64(expected)))
	}
	if want != "" {
		got := hex.EncodeToString(hasher.Sum(nil))
		if got != strings.ToLower(want) {
			return fmt.Errorf("import: sha256 mismatch: input is %s", got)
		}
	}
	if total%chunk != 0 {
		fmt.Fprintf(os.Stderr, "import: source length not a multiple of %s: trailing partial chunk written\n", humanSize(chunk))
	}
	fmt.Fprintf(os.Stderr, "import: done, %s written\n", humanSize(total))
	return nil
}

func volExport(ctx context.Context, cl *client.Cluster, name, dst string) error {
	v, err := cl.OpenVolume(ctx, name)
	if err != nil {
		return err
	}
	defer v.Close()
	var w io.Writer = os.Stdout
	var out *os.File
	if dst != "-" {
		out, err = os.Create(dst)
		if err != nil {
			return err
		}
		defer out.Close()
		w = out
	}
	buf := make([]byte, chunk)
	var off uint64
	for off < v.Size() {
		n := uint64(len(buf))
		if v.Size()-off < n {
			n = v.Size() - off
		}
		got, err := v.ReadAt(ctx, buf[:n], off)
		if err != nil {
			return fmt.Errorf("export: read at %d: %w", off, err)
		}
		if isZero(buf[:got]) {
			// Sparse image: a whole-chunk run of zeroes becomes a hole
			// when the sink is seekable (a file). For a pipe the zeroes
			// are written out: a downloader does not need holes, and
			// there is nothing to seek in a stream.
			if seeker, ok := w.(io.Seeker); ok {
				if _, serr := seeker.Seek(int64(got), io.SeekCurrent); serr != nil {
					return fmt.Errorf("export: seek: %w", serr)
				}
				off += uint64(got)
				continue
			}
		}
		if _, err := w.Write(buf[:got]); err != nil {
			return fmt.Errorf("export: write: %w", err)
		}
		off += uint64(got)
	}
	if out != nil {
		// Truncate fixes the size when the volume's tail is a hole run.
		if err := out.Truncate(int64(v.Size())); err != nil {
			return fmt.Errorf("export: truncate: %w", err)
		}
	}
	fmt.Fprintf(os.Stderr, "export: done, %s\n", humanSize(v.Size()))
	return nil
}

// ---- pool ----

// poolCmd manages cluster pools: a pool is a namespace carrying the
// shared placement policy (replicas, min_size, pg_num, crush rule, QoS
// class) for the volumes created inside it. "pool create" registers
// one, "pool ls" lists them, "pool rm" removes one (refused while
// volumes still reference it).
func poolCmd(ctx context.Context, cl *client.Cluster, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("pool: subcommand required (create/ls/rm)")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("pool create", flag.ExitOnError)
		replicas := fs.Int("replicas", 0, "replica count (0 = cluster default)")
		minSize := fs.Int("min-size", 0, "min writable replica set (0 = replicas/2+1)")
		pgNum := fs.Int("pg-num", 0, "placement groups (0 = cluster default)")
		rule := fs.String("rule", "", "crush rule name (empty = cluster default)")
		qos := fs.String("qos", "", "qos class name applied to volumes created in this pool")
		mustParse(fs, args[1:])
		if fs.NArg() != 1 {
			fs.Usage()
			return errors.New(`pool create: <name> e.g. "pool create fast -replicas 3"`)
		}
		rec, err := cl.CreatePool(ctx, meta.PoolRecord{
			Name:        fs.Arg(0),
			Replicas:    *replicas,
			MinSize:     *minSize,
			PGNum:       *pgNum,
			Rule:        *rule,
			QoSTemplate: *qos,
		})
		if err != nil {
			return err
		}
		fmt.Printf("pool %q: replicas=%d min_size=%d pg_num=%d rule=%s", rec.Name, rec.Replicas, rec.MinSize, rec.PGNum, rec.Rule)
		if rec.QoSTemplate != "" {
			fmt.Printf(" qos=%s", rec.QoSTemplate)
		}
		fmt.Println()
		return nil
	case "ls":
		recs, err := cl.ListPools(ctx)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			fmt.Println("(no pools)")
			return nil
		}
		fmt.Printf("%-16s %8s %8s %8s %-12s %-10s\n", "POOL", "REPLICA", "MINSIZE", "PGNUM", "RULE", "QOS")
		for _, r := range recs {
			fmt.Printf("%-16s %8d %8d %8d %-12s %-10s\n", r.Name, r.Replicas, r.MinSize, r.PGNum, r.Rule, r.QoSTemplate)
		}
		return nil
	case "rm":
		if len(args) != 2 {
			return errors.New("pool rm: <name>")
		}
		if err := cl.DeletePool(ctx, args[1]); err != nil {
			return err
		}
		fmt.Printf("pool %q removed\n", args[1])
		return nil
	default:
		usage()
		return fmt.Errorf("pool: unknown subcommand %q", args[0])
	}
}

// ---- qos ----

func qosCmd(ctx context.Context, cl *client.Cluster, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("qos: subcommand required (ls/put/rm)")
	}
	switch args[0] {
	case "ls":
		recs, err := cl.ListQoSClasses(ctx)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			fmt.Println("(no qos classes)")
			return nil
		}
		fmt.Printf("%-16s %10s %10s %10s %10s\n", "CLASS", "R_IOPS", "R_BPS", "W_IOPS", "W_BPS")
		for _, r := range recs {
			ri, rb, wi, wb := r.Rates()
			fmt.Printf("%-16s %10d %10s %10d %10s\n", r.Name, ri, humanRate(rb), wi, humanRate(wb))
		}
		return nil
	case "put":
		if len(args) != 3 {
			return errors.New(`qos put: <class> <riops:rbps:wiops:wbps> e.g. "qos put std :200M::200M"`)
		}
		riops, rbps, wiops, wbps, err := parseRates(args[2])
		if err != nil {
			return err
		}
		if err := cl.PutQoSClass(ctx, meta.QoSClassRecord{
			Name: args[1], ReadIOPS: riops, ReadBPS: rbps, WriteIOPS: wiops, WriteBPS: wbps,
		}); err != nil {
			return err
		}
		fmt.Printf("class %q: r=%d/%s w=%d/%s\n", args[1], riops, humanRate(rbps), wiops, humanRate(wbps))
		return nil
	case "rm":
		if len(args) != 2 {
			return errors.New("qos rm: <class>")
		}
		if err := cl.DeleteQoSClass(ctx, args[1]); err != nil {
			return err
		}
		fmt.Printf("class %q removed (volumes bound to it are unlimited)\n", args[1])
		return nil
	default:
		usage()
		return fmt.Errorf("qos: unknown subcommand %q", args[0])
	}
}

// ---- osd ----

// osdCmd manages the OSD registry: "osd ls" lists it, "osd add <id>
// <addr> [-data P] [-uuid U]" registers an OSD record by hand (the
// normal path is the OSD self-registering at startup — this is the
// operator's rescue tool), "osd out <id>" sets weight 0 (the OSD leaves
// placement: rows are rebuilt without it, its data needs recovery
// elsewhere), "osd in <id> [-weight w]" brings it back. The registry is
// what placement compiles pg rows from, so the change takes effect on
// every client's next map refresh; publishing a fresh map epoch is not
// required (it only moves the fence).
func osdCmd(ctx context.Context, ms meta.Store, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("osd: subcommand required (ls | add <id> <addr> | out <id> | in <id> [-weight w])")
	}
	switch args[0] {
	case "add":
		fs := flag.NewFlagSet("osd add", flag.ExitOnError)
		data := fs.String("data", "", "data path the osd serves")
		uuid := fs.String("uuid", "", "osd uuid (must match what the daemon reports)")
		wFlag := fs.Float64("weight", 1, "crush weight (> 0)")
		parseMix(fs, args[1:])
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New("osd add: <id> <addr> [-data P] [-uuid U] [-weight w]")
		}
		id, err := strconv.ParseInt(fs.Arg(0), 10, 32)
		if err != nil {
			return fmt.Errorf("osd add: bad id %q", fs.Arg(0))
		}
		if _, err := meta.PutOSD(ctx, ms, meta.OSDInfo{
			ID: int32(id), Addr: fs.Arg(1), DataPath: *data, UUID: *uuid, Weight: *wFlag,
		}); err != nil {
			return err
		}
		fmt.Printf("osd %d: registered at %s (weight %g)\n", id, fs.Arg(1), *wFlag)
		return nil
	case "out", "in":
		if len(args) < 2 {
			return errors.New("osd " + args[0] + ": <id>")
		}
		id, err := strconv.ParseInt(args[1], 10, 32)
		if err != nil {
			return fmt.Errorf("osd %s: bad id %q", args[0], args[1])
		}
		w := 0.0
		if args[0] == "in" {
			fs := flag.NewFlagSet("osd in", flag.ExitOnError)
			wFlag := fs.Float64("weight", 1, "crush weight (> 0)")
			parseMix(fs, args[2:])
			if fs.NArg() != 0 {
				fs.Usage()
				return errors.New("osd in: <id> [-weight w]")
			}
			w = *wFlag
			if w <= 0 {
				return errors.New("osd in: weight must be > 0 (use `osd out` to evict)")
			}
		}
		if err := meta.SetOSDWeight(ctx, ms, int32(id), w); err != nil {
			return err
		}
		if w == 0 {
			fmt.Printf("osd %d: weight 0 — out of placement; its replicas now need recovery (`recovery check`)\n", id)
		} else {
			fmt.Printf("osd %d: weight %g — back in placement; rebuild data (`recovery run`) before trusting it alone\n", id, w)
		}
		return nil
	case "ls":
	default:
		usage()
		return fmt.Errorf("osd: unknown subcommand %q (ls | out <id> | in <id>)", args[0])
	}
	infos, err := meta.ListOSDs(ctx, ms)
	if err != nil {
		return err
	}
	fmt.Printf("%-6s %-24s %8s %-10s %s\n", "ID", "ADDR", "WEIGHT", "UUID", "DATA")
	for _, o := range infos {
		uuid := o.UUID
		if len(uuid) > 8 {
			uuid = uuid[:8]
		}
		fmt.Printf("%-6d %-24s %8.3f %-10s %s\n", o.ID, o.Addr, o.Weight, uuid, o.DataPath)
	}
	rec, mapErr := meta.LatestMap(ctx, ms)
	if mapErr == nil {
		fmt.Printf("map epoch=%d\n", rec.Epoch)
	} else {
		fmt.Printf("map: %v\n", mapErr)
	}
	return nil
}

// ---- map ----

// mapCmd inspects and edits the crush map stored in meta//maps:
// "map dump [-format raw|text|json|yaml|binary]" prints the latest
// epoch (raw = as stored: text, binary hex, or json; the others
// decode and re-encode), "map check [-f FILE]" compiles a map file (or
// the stored one) and reports PG-table uniformity, "map publish FILE
// [-epoch n]" stores a new epoch (n defaults to latest+1; OSDs raise
// their fence and clients rebuild on watch). Per-OSD weights live in
// the OSD registry ("osd out/in"), not in the map.
func mapCmd(ctx context.Context, cl *client.Cluster, ms meta.Store, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("map: subcommand required (dump | check [file] | publish file [-epoch n])")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "dump":
		fs := flag.NewFlagSet("map dump", flag.ExitOnError)
		format := fs.String("format", "raw", "output format: raw (as stored), text, json, yaml, binary (hex)")
		fs.Usage = func() {
			fmt.Fprintln(os.Stderr, "usage: unistorctl map dump [-format raw|text|json|yaml|binary]")
		}
		fs.Parse(args[1:])
		rec, err := meta.LatestMap(ctx, ms)
		if err != nil {
			return mapLookupErr(err)
		}
		switch *format {
		case "raw":
			fmt.Fprintf(os.Stderr, "epoch=%d format=%s bytes=%d\n", rec.Epoch, rec.Format, len(rec.Map))
			if rec.Format == "binary" {
				fmt.Printf("%x\n", rec.Map)
				return nil
			}
			os.Stdout.Write(rec.Map)
			return nil
		case "yaml":
			m, err := placement.DecodeMap(rec.Map)
			if err != nil {
				return err
			}
			b, err := yaml.Marshal(m)
			if err != nil {
				return err
			}
			os.Stdout.Write(b)
			return nil
		default:
			b, err := placement.FormatMap(rec.Map, *format)
			if err != nil {
				return err
			}
			os.Stdout.Write(b)
			if len(b) > 0 && b[len(b)-1] != '\n' {
				fmt.Println()
			}
			return nil
		}
	case "check":
		fs := flag.NewFlagSet("map check", flag.ExitOnError)
		pgNum := fs.Uint("pg-num", 1024, "pg table width for the compile")
		replicas := fs.Int("replicas", 3, "replicas per pg row")
		rule := fs.String("rule", "", "crush rule name (default: lowest-id rule)")
		class := fs.String("class", "", "validate rules for a volume class (e.g. ssd, hdd): class filters, fault-domain capacity, reweight preview against the live registry")
		parseMix(fs, rest)
		var src []byte
		label := "stored map"
		if fs.NArg() > 0 {
			b, err := os.ReadFile(fs.Arg(0))
			if err != nil {
				return err
			}
			src, label = b, fs.Arg(0)
		} else {
			rec, err := meta.LatestMap(ctx, ms)
			if err != nil {
				return mapLookupErr(err)
			}
			src = rec.Map
			label = fmt.Sprintf("stored map (epoch %d, %s)", rec.Epoch, rec.Format)
		}
		// AST-валидатор: структура карты ловится до компиляции (битые
		// pos/ссылки/классы из etcd приходят именно так).
		ast, astErr := placement.DecodeMap(src)
		if astErr == nil {
			verdict := ast.Check()
			fmt.Print(formatMapVerdict(verdict))
			if !verdict.OK() {
				return fmt.Errorf("map check %s: %d structural error(s)", label, len(verdict.Errors))
			}
			if *class != "" {
				cverdict := ast.CheckClass(*class)
				if live, lerr := cl.Registry(ctx); lerr == nil && len(live) > 0 {
					// reweight-preview: osd out (вес 0 в реестре) учитывается
					weights := make(map[int32]float64, len(ast.Devices))
					for _, d := range ast.Devices {
						if _, ok := live[d.ID]; ok {
							weights[d.ID] = 1
						} else {
							weights[d.ID] = 0
						}
					}
					cverdict = ast.CheckWeights(*class, weights)
				}
				fmt.Printf("map check %s class %s: classes in map: %v\n", label, *class, ast.Classes())
				fmt.Print(formatMapVerdict(cverdict))
				if !cverdict.OK() {
					return fmt.Errorf("map check %s class %s: %d error(s)", label, *class, len(cverdict.Errors))
				}
			}
		} else if *class != "" {
			return fmt.Errorf("map check %s: -class needs a decodable map: %w", label, astErr)
		}
		reg, err := cl.Registry(ctx)
		if err != nil {
			return err
		}
		if len(reg) == 0 {
			fmt.Fprintln(os.Stderr, "warning: OSD registry is empty — checking map structure only (pg census needs registered osds)")
		}
		snap, err := placement.Compile(0, src, reg, placement.Options{
			PGNum: uint32(*pgNum), Replicas: *replicas, Rule: *rule,
		})
		if err != nil {
			return fmt.Errorf("map check %s: %w", label, err)
		}
		if len(reg) == 0 {
			fmt.Printf("map check %s: ok (rule %q, pg_num %d, replicas %d)\n", label, snap.Rule, snap.PGNum, snap.Replicas)
			return nil
		}
		// Count how many live targets each pg row resolves to.
		ok, degraded, down := 0, 0, 0
		for pg := uint32(0); pg < snap.PGNum; pg++ {
			n := 0
			for _, id := range snap.Row(pg) {
				if id >= 0 {
					n++
				}
			}
			switch {
			case n == snap.Replicas:
				ok++
			case n >= snap.Replicas/2+1:
				degraded++
			default:
				down++
			}
		}
		fmt.Printf("map check %s: ok (rule %q, pg_num %d, replicas %d)\n", label, snap.Rule, snap.PGNum, snap.Replicas)
		fmt.Printf("pg rows: %d full, %d degraded, %d down — osds in table: %v\n", ok, degraded, down, snap.LiveOSDs())
		if down > 0 {
			return fmt.Errorf("map check: %d pg row(s) below quorum — not enough live osds for this layout", down)
		}
		return nil
	case "publish":
		fs := flag.NewFlagSet("map publish", flag.ExitOnError)
		epoch := fs.Int("epoch", -1, "new epoch (default: latest+1)")
		parseMix(fs, rest)
		if fs.NArg() != 1 {
			fs.Usage()
			return errors.New("map publish: <file> [-epoch n]")
		}
		src, err := os.ReadFile(fs.Arg(0))
		if err != nil {
			return err
		}
		newEpoch := uint32(*epoch)
		if *epoch < 0 {
			rec, err := meta.LatestMap(ctx, ms)
			if err != nil && !errors.Is(err, meta.ErrNotFound) {
				return mapLookupErr(err)
			}
			newEpoch = rec.Epoch + 1
		}
		if err := cl.MapEpoch(ctx, newEpoch, src); err != nil {
			return err
		}
		fmt.Printf("map epoch %d published (OSDs fence at it, clients rebuild on watch)\n", newEpoch)
		return nil
	case "reweight":
		// vitastor-style one-liner over the OSD registry: same effect
		// as `osd out` / `osd in -weight`.
		if len(rest) != 2 {
			return errors.New("map reweight: <id> <weight> (0 = out)")
		}
		id, err := strconv.ParseInt(rest[0], 10, 32)
		if err != nil {
			return fmt.Errorf("map reweight: bad id %q", rest[0])
		}
		w, err := strconv.ParseFloat(rest[1], 64)
		if err != nil {
			return fmt.Errorf("map reweight: bad weight %q", rest[1])
		}
		if w < 0 {
			return errors.New("map reweight: weight must be >= 0")
		}
		if err := meta.SetOSDWeight(ctx, ms, int32(id), w); err != nil {
			return err
		}
		fmt.Printf("osd %d: weight %g\n", id, w)
		return nil
	default:
		usage()
		return fmt.Errorf("map: unknown subcommand %q (dump | check | publish | reweight)", args[0])
	}
}

// formatMapVerdict печатает вердикт crush-валидатора.
func formatMapVerdict(r *crush.CheckResult) string {
	return crush.FormatCheckResult(r)
}

func mapLookupErr(err error) error {
	if errors.Is(err, meta.ErrNotFound) {
		return errors.New("no crush map in meta (publish one: `map publish <file>`)")
	}
	return err
}

// ---- vacuum ----

// vacuumCmd vacuums the object ranges of deleted volumes (plan M1.4).
// "vacuum ls" lists the tombstones that still await a vacuum; the
// default (and "run") performs one bounded pass over every registered
// OSD, one object at a time, and reports what it removed.
//
// "vacuum run <vol>" narrows the pass to one volume: a registered
// volume is vacuumed live (objects found all-zeroes are punched out
// and their allocation released — the outside-in equivalent of the
// guest's fstrim); a deleted volume's tombstone is vacuumed by name.
func vacuumCmd(ctx context.Context, cl *client.Cluster, args []string) error {
	sub, name := "", ""
	if len(args) > 0 {
		sub = args[0]
	}
	if sub == "run" && len(args) > 1 {
		name = args[1]
	}
	if name != "" {
		return vacuumOne(ctx, cl, name)
	}
	if sub == "ls" {
		recs, err := cl.PendingVacuum(ctx)
		if err != nil {
			return err
		}
		if len(recs) == 0 {
			fmt.Println("vacuum: nothing pending")
			return nil
		}
		fmt.Printf("%-24s %16s %10s %s\n", "NAME", "OID_BASE", "OBJECTS", "DELETED")
		for _, r := range recs {
			fmt.Printf("%-24s %16d %10d %ds ago\n", r.Name, r.OIDBase, r.NumObjs,
				time.Now().Unix()-r.DeletedAt)
		}
		return nil
	}
	if sub != "" && sub != "run" {
		return fmt.Errorf("vacuum: unknown subcommand %q (ls | run)", sub)
	}
	res, err := cl.VacuumDeletedVolumes(ctx)
	if err != nil {
		return err
	}
	for _, o := range res.OSDs {
		if o.Err != nil {
			fmt.Printf("osd %d: removed %d: %v\n", o.OSD, o.Removed, o.Err)
			continue
		}
		fmt.Printf("osd %d: removed %d\n", o.OSD, o.Removed)
	}
	fmt.Printf("vacuum: %d tombstone(s), %d vacuumed", res.Vacuums, res.Deleted)
	if len(res.Errs) > 0 {
		fmt.Printf(", %d error(s): %v", len(res.Errs), res.Errs[0])
	}
	fmt.Println()
	return nil
}

// vacuumOne vacuums a single volume by name: if it is a registered
// live volume, run the all-zeroes punch-out pass; otherwise, if a
// tombstone for that deleted volume is pending, vacuum just it.
func vacuumOne(ctx context.Context, cl *client.Cluster, name string) error {
	if _, err := meta.GetVolume(ctx, cl.Meta(), name); err == nil {
		v, err := cl.OpenVolume(ctx, name)
		if err != nil {
			return err
		}
		defer v.Close()
		res, err := v.Vacuum(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("vacuum %q: checked %d object(s), punched %d, freed %s\n",
			name, res.Checked, res.Zeroed, humanSize(res.Freed))
		return nil
	}
	res, err := cl.VacuumVolume(ctx, name)
	if err != nil {
		return err
	}
	for _, o := range res.OSDs {
		if o.Err != nil {
			fmt.Printf("osd %d: removed %d: %v\n", o.OSD, o.Removed, o.Err)
			continue
		}
		fmt.Printf("osd %d: removed %d\n", o.OSD, o.Removed)
	}
	fmt.Printf("vacuum %q: %d tombstone(s), %d vacuumed", name, res.Vacuums, res.Deleted)
	if len(res.Errs) > 0 {
		fmt.Printf(", %d error(s): %v", len(res.Errs), res.Errs[0])
	}
	fmt.Println()
	return nil
}

// ---- recovery ----

// recoveryCmd is plan M6.3: detect replicas that diverged from their
// row-mates (a crashed-and-restarted node that lost writes) and copy
// them back from a live replica. "recovery ls" scans every volume;
// "recovery check <vol>" shows one volume's repair list; "recovery run
// <vol>" copies the missing objects, paced by -iops/-bps (the write
// lane, so a repair never starves live guests).
func recoveryCmd(ctx context.Context, cl *client.Cluster, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("recovery: subcommand required (ls | check <vol> | run <vol>)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "ls":
		reps, err := cl.RecoveryScan(ctx)
		if err != nil {
			return err
		}
		if len(reps) == 0 {
			fmt.Println("recovery: no volumes")
			return nil
		}
		fmt.Printf("%-24s %8s %6s %6s %9s %-12s %s\n",
			"VOLUME", "OBJECTS", "OK", "THIN", "TO_REPAIR", "DOWN", "HEALTHY")
		healthy := true
		for _, r := range reps {
			if !r.Healthy() {
				healthy = false
			}
			fmt.Printf("%-24s %8d %6d %6d %9d %-12d %t\n",
				r.Volume, r.Objects, r.Ok, r.Thin, len(r.Items), r.Down, r.Healthy())
		}
		if healthy {
			fmt.Println("recovery: all volumes consistent")
		}
		return nil
	case "check":
		if len(rest) != 1 {
			return errors.New("recovery check: <vol>")
		}
		rep, err := recoveryCheck(ctx, cl, rest[0])
		if err != nil {
			return err
		}
		printRecoveryReport(rep)
		return nil
	case "run":
		fs := flag.NewFlagSet("recovery run", flag.ExitOnError)
		iops := fs.Uint64("iops", 200, "repair write IOPS (0 = unthrottled)")
		bpsStr := fs.String("bps", "200M", "repair write bandwidth (K/M/G/T; 0 = unthrottled)")
		parseMix(fs, rest)
		if fs.NArg() != 1 {
			fs.Usage()
			return errors.New("recovery run: <vol>")
		}
		bpsVal, err := sizeSuffix(*bpsStr)
		if err != nil {
			return fmt.Errorf("recovery run: bps: %w", err)
		}
		name := fs.Arg(0)
		v, err := cl.OpenVolume(ctx, name)
		if err != nil {
			return err
		}
		defer v.Close()
		rep, err := v.RecoveryCheck(ctx)
		if err != nil {
			return err
		}
		if len(rep.Items) == 0 {
			fmt.Printf("recovery %s: nothing to repair\n", name)
			return nil
		}
		fmt.Printf("recovery %s: %d object(s) to copy\n", name, len(rep.Items))
		opts := client.RecoveryOpts{
			Limit: qos.NewPair(0, 0, *iops, bpsVal),
			Logf:  func(f string, a ...any) { fmt.Fprintf(os.Stderr, "  "+f+"\n", a...) },
		}
		defer opts.Limit.Close()
		st, err := v.Recover(ctx, rep, opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "recovery %s aborted: %v\n", name, err)
		}
		fmt.Printf("recovery %s: copied %d (%s) in %s, failed %d, skipped %d\n",
			name, st.Copied, humanSize(st.Bytes),
			st.Finished.Sub(st.Started).Round(time.Millisecond), st.Failed, st.Skipped)
		return err
	default:
		return fmt.Errorf("recovery: unknown subcommand %q (ls | check | run)", sub)
	}
}

// recoveryCheck opens a volume, runs one consistency check, and closes
// it (the caller owns the cluster lifecycle).
func recoveryCheck(ctx context.Context, cl *client.Cluster, name string) (*client.RecoveryReport, error) {
	v, err := cl.OpenVolume(ctx, name)
	if err != nil {
		return nil, err
	}
	defer v.Close()
	return v.RecoveryCheck(ctx)
}

// printRecoveryReport renders one volume's repair list.
func printRecoveryReport(rep *client.RecoveryReport) {
	fmt.Printf("volume %s: %d objects, ok %d, thin %d, to-repair %d, down %v (%s)\n",
		rep.Volume, rep.Objects, rep.Ok, rep.Thin, len(rep.Items), rep.Down,
		rep.CheckedAt.Format(time.RFC3339))
	if len(rep.Items) == 0 {
		fmt.Println("  consistent")
		return
	}
	fmt.Printf("  %-10s %-18s %-6s %s\n", "OBJECT", "OID", "OSD", "SOURCE")
	for _, it := range rep.Items {
		fmt.Printf("  %-10d %-18d %-6d %d\n", it.Index, it.OID, it.OSD, it.Source)
	}
}

// ---- status ----

// statusCmd reads the unistord admin /v1/status JSON and prints it for
// humans. It is the only unistorctl command that talks HTTP: the meta
// directory has the control-plane state, but liveness probes, per-disk
// capacity and raft leadership live in the running daemon.
func statusCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	httpURL := fs.String("http", "http://127.0.0.1:8443", "unistord admin URL")
	fs.Parse(args)
	url := strings.TrimRight(*httpURL, "/") + "/v1/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("status: GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status: GET %s: %s: %s", url, resp.Status, strings.TrimSpace(string(body)))
	}
	var st map[string]any
	if err := json.Unmarshal(body, &st); err != nil {
		return fmt.Errorf("status: parse reply: %w", err)
	}
	fmt.Printf("cluster: %v volumes, osds %v/%v live, map epoch %v, meta %v",
		st["volumes"], st["osds_live"], st["osds"], st["map_epoch"], st["meta"])
	if rs, ok := st["raft_state"]; ok {
		fmt.Printf(", raft %v (leader %v)", rs, st["raft_leader"])
		if w, ok := st["writable"].(bool); ok && !w {
			fmt.Printf(" — CONTROL PLANE READ-ONLY")
		}
	}
	fmt.Println()
	if d, ok := st["osd_detail"].([]any); ok && len(d) > 0 {
		fmt.Printf("%-6s %-24s %8s %-8s %s\n", "ID", "ADDR", "WEIGHT", "LIVE", "NOTE")
		for _, e := range d {
			o, _ := e.(map[string]any)
			note := ""
			ev, _ := o["evicted"].(bool)
			live, _ := o["live"].(bool)
			switch {
			case ev:
				note = "EVICTED (needs recovery)"
			case !live:
				note = "NOT ANSWERING"
			}
			fmt.Printf("%-6v %-24s %8.3f %-8v %s\n", o["id"], o["addr"], o["weight"], live, note)
		}
	}
	if h, ok := st["volume_health"].([]any); ok && len(h) > 0 {
		fmt.Printf("%-20s %-14s %s\n", "VOLUME", "TARGETS/MINSZ", "WRITES")
		for _, e := range h {
			v, _ := e.(map[string]any)
			wh, _ := v["writable"].(bool)
			state := "ok"
			if !wh {
				state = "DEGRADED (fail fast)"
			}
			fmt.Printf("%-20s %-14v %s\n", v["name"], fmt.Sprint(v["live_targets"], "/", v["min_size"]), state)
		}
	}
	if dk, ok := st["disks"].([]any); ok && len(dk) > 0 {
		fmt.Printf("%-6s %10s %10s %10s\n", "DISK", "USED", "FREE", "TOTAL")
		for _, e := range dk {
			d, _ := e.(map[string]any)
			fmt.Printf("%-6v %10v %10v %10v\n", d["osd"], d["usage_human"], d["free_human"], d["total_human"])
		}
	}
	if ws, ok := st["warnings"].([]any); ok && len(ws) > 0 {
		fmt.Println("warnings:")
		for _, w := range ws {
			fmt.Printf("  ! %v\n", w)
		}
	}
	if ok, _ := st["healthy"].(bool); ok {
		fmt.Println("healthy")
	}
	return nil
}

// healthcheckCmd asks the running unistord for the orchestration verdict
// (/healthcheck) and prints one line per check. The exit code is the
// contract: 0 when the cluster is healthy (warn counts as healthy), 1
// when it is not — including "the daemon did not answer", which is
// itself an unhealthy answer. This is what an orchestrator, a cron
// probe, or a systemd ExecStartPre condition runs.
func healthcheckCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)
	httpURL := fs.String("http", "http://127.0.0.1:8443", "unistord admin URL")
	asJSON := fs.Bool("json", false, "print the raw reply instead of the table")
	fs.Parse(args)
	url := strings.TrimRight(*httpURL, "/") + "/healthcheck"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cluster is unhealthy: unistord did not answer at %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("cluster is unhealthy: reading %s: %v", url, err)
	}
	if *asJSON {
		os.Stdout.Write(body)
		if len(body) > 0 && body[len(body)-1] != '\n' {
			fmt.Println()
		}
	}
	var hc struct {
		Status string `json:"status"`
		Time   string `json:"time"`
		Checks []struct {
			Name    string `json:"name"`
			State   string `json:"state"`
			Message string `json:"message"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(body, &hc); err != nil {
		return fmt.Errorf("cluster is unhealthy: parse reply from %s: %w", url, err)
	}
	if !*asJSON {
		fmt.Printf("%-10s %s\n", "CHECK", "STATE")
		for _, c := range hc.Checks {
			fmt.Printf("%-10s %-5s %s\n", c.Name, c.State, c.Message)
		}
	}
	if resp.StatusCode == http.StatusOK && (hc.Status == "ok" || hc.Status == "warn") {
		if !*asJSON {
			fmt.Println("healthy")
		}
		return nil
	}
	if !*asJSON {
		fmt.Println("UNHEALTHY")
	}
	return fmt.Errorf("cluster is unhealthy: status %q (HTTP %d)", hc.Status, resp.StatusCode)
}

// ---- helpers ----

func usage() {
	fmt.Fprintf(os.Stderr, `unistorctl: unistack storage control tool

unistorctl [-meta-dir DIR] <command> ...

vol create <name> <size> [-pool P] [-quota S] [-replicas n] [-min-size n] [-pg-num n] [-rule r]  (in a pool, omitted placement flags come from the pool; explicit flags win)
vol ls | vol info <name> | vol rm <name>
vol qos <name> [-class c] [-r iops[:Bps]] [-w iops[:Bps]]
vol quota <name> <size|0>  (allocation cap; open volumes pick it up live)
vol import <name> <file|http(s)://url|-> [-offset N] [-size N] [-sha256 HEX]  ("-" reads stdin)
vol export <name> <file|->
vol snap create <name> | vol snap ls <name> | vol snap rm <name> <gen> | vol snap rollback <name> <gen>
pool create <name> [-replicas n] [-min-size n] [-pg-num n] [-rule r] [-qos class]  (cluster-wide placement policy; volumes created in it inherit it)
pool ls | pool rm <name>  (rm is refused while volumes reference the pool)
qos ls | qos put <class> <riops:rbps:wiops:wbps> | qos rm <class>
osd ls | osd add <id> <addr> [-data P] [-uuid U] | osd out <id> | osd in <id> [-weight w]   (weight 0 = out of placement)
map dump [-format raw|text|json|yaml|binary] | map check [file] [-pg-num n] [-replicas n] [-rule r] | map publish <file> [-epoch n] | map reweight <id> <w>
vacuum [ls|run [vol]]  (M1.4: free deleted volumes' objects; run <vol> = targeted tombstone, live volume = punch out all-zero objects)
recovery ls | recovery check <vol> | recovery run <vol> [-iops N] [-bps B]
export create <volume> [-type nbd|vduse|vhost-user-blk] [-devname N] [-ro] [-http URL] [-token T]  (attach a volume to a running unistord; -type vub is accepted as a shorthand)
export ls [-json] [-http URL] [-token T]  (live exports per type)
export rm <name> [-type nbd|vduse|vhost-user-blk] [-http URL] [-token T]  (detach; 409 when in use; -type vub accepted)
status [-http URL]  (reads unistord admin /v1/status; default http://127.0.0.1:8443)
healthcheck [-http URL] [-json]  (verdict on unistord /healthcheck; exit 1 when the cluster is unhealthy)

Sizes and bandwidth accept K/M/G/T suffixes (10G, 200M). Rates are
iops[:bytes/s]; all zero (or a missing class) = unlimited, the admin
class. Rates change live on already-open volumes. A quota caps the
bytes a volume may allocate (thin provisioning); writing past it fails
with ENOSPC, guest TRIM frees room back.
`)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "unistorctl: "+format+"\n", args...)
	os.Exit(1)
}

func mustParse(fs *flag.FlagSet, args []string) {
	if err := fs.Parse(args); err != nil {
		fatal("%v", err)
	}
}

// parseMix parses args with flags interleaved with positionals (Go's
// flag package stops at the first positional; commands like
// "osd add <id> <addr> -data P" want both orders to work).
func parseMix(fs *flag.FlagSet, args []string) {
	var flags, pos []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		name := ""
		if len(a) > 1 && a[0] == '-' {
			name = strings.TrimLeft(a, "-")
			if j := strings.Index(name, "="); j >= 0 {
				name = name[:j]
			}
		}
		def := fs.Lookup(name)
		if def == nil {
			pos = append(pos, a)
			continue
		}
		flags = append(flags, a)
		isBool := false
		if bv, ok := def.Value.(interface{ IsBoolFlag() bool }); ok {
			isBool = bv.IsBoolFlag()
		}
		if isBool || strings.Contains(a, "=") {
			continue
		}
		if i+1 < len(args) {
			flags = append(flags, args[i+1])
			i++
		}
	}
	mustParse(fs, append(flags, pos...))
}

// parseRates parses "riops:rbps:wiops:wbps" (bandwidth: K/M/G/T).
func parseRates(s string) (riops, rbps, wiops, wbps uint64, err error) {
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
			return 0, 0, 0, 0, fmt.Errorf("rate %d in %q: %v", i+1, s, err)
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

// parsePair parses "iops[:Bps]".
func parsePair(s string) (iops, bps uint64, err error) {
	parts := strings.Split(s, ":")
	if len(parts) > 2 {
		return 0, 0, fmt.Errorf(`expected "iops[:Bps]", got %q`, s)
	}
	if parts[0] != "" {
		iops, err = strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("iops: %v", err)
		}
	}
	if len(parts) == 2 && parts[1] != "" {
		bps, err = sizeSuffix(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("bytes/s: %v", err)
		}
	}
	return iops, bps, nil
}

// sizeSuffix parses a byte size with an optional K/M/G/T suffix
// (binary: K=1024).
func sizeSuffix(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	mult := uint64(1)
	switch s[len(s)-1] {
	case 'K', 'k':
		mult = 1 << 10
		s = s[:len(s)-1]
	case 'M', 'm':
		mult = 1 << 20
		s = s[:len(s)-1]
	case 'G', 'g':
		mult = 1 << 30
		s = s[:len(s)-1]
	case 'T', 't':
		mult = 1 << 40
		s = s[:len(s)-1]
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return v * mult, nil
}

// quotaLabel renders a volume's allocation cap ("-" when uncapped,
// i.e. the cap is the logical size).
func quotaLabel(s meta.VolumeSpec) string {
	if s.Quota == 0 || s.Quota >= s.Size {
		return "-"
	}
	return humanSize(s.Quota)
}

// allocUsage reads a volume's allocation bitmap popcount (quota usage).
func allocUsage(ctx context.Context, ms meta.Store, s meta.VolumeSpec) (uint64, error) {
	nbits := int((s.Size + object.Mask) >> object.SizeShift)
	rec, err := meta.GetAllocMap(ctx, ms, s.Name, nbits)
	if err != nil {
		return 0, err
	}
	return uint64(meta.AllocMapPopcount(rec.Bits)) * object.Size, nil
}

func humanSize(n uint64) string {
	switch {
	case n >= 1<<40 && n%(1<<40) == 0:
		return strconv.FormatUint(n/(1<<40), 10) + "T"
	case n >= 1<<30 && n%(1<<30) == 0:
		return strconv.FormatUint(n/(1<<30), 10) + "G"
	case n >= 1<<20 && n%(1<<20) == 0:
		return strconv.FormatUint(n/(1<<20), 10) + "M"
	case n >= 1<<10 && n%(1<<10) == 0:
		return strconv.FormatUint(n/(1<<10), 10) + "K"
	default:
		return strconv.FormatUint(n, 10)
	}
}

// humanRate renders a bytes/s rate for tables.
func humanRate(n uint64) string {
	if n == 0 {
		return "unltd"
	}
	return humanSize(n) + "/s"
}

// qosSummary renders the compact per-volume QoS column.
func qosSummary(s meta.VolumeSpec) string {
	if s.QoSTemplate != "" {
		return s.QoSTemplate
	}
	if !s.QoSed() {
		return "admin"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "r:%d", s.ReadIOPS)
	if s.ReadBPS != 0 {
		fmt.Fprintf(&b, "/%s", humanSize(s.ReadBPS))
	}
	fmt.Fprintf(&b, " w:%d", s.WriteIOPS)
	if s.WriteBPS != 0 {
		fmt.Fprintf(&b, "/%s", humanSize(s.WriteBPS))
	}
	return b.String()
}

func qosFull(s meta.VolumeSpec) string {
	if s.QoSTemplate != "" {
		return fmt.Sprintf("class %q (inline r:%d:%s w:%d:%s)", s.QoSTemplate,
			s.ReadIOPS, humanRate(s.ReadBPS), s.WriteIOPS, humanRate(s.WriteBPS))
	}
	if !s.QoSed() {
		return "unlimited (admin)"
	}
	return fmt.Sprintf("r:%d %s w:%d %s", s.ReadIOPS, humanRate(s.ReadBPS), s.WriteIOPS, humanRate(s.WriteBPS))
}

func isZero(p []byte) bool {
	for _, b := range p {
		if b != 0 {
			return false
		}
	}
	return true
}
