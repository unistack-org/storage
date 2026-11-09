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
//	vol import  <name> <file|url> [-offset N]   (streamed, 1 MiB chunks)
//	vol export  <name> <file|->                (holes preserved)
//	qos ls
//	qos put     <class> <riops:rbps:wiops:wbps>
//	qos rm      <class>
//	osd ls
//	vacuum      [ls|run [vol]]                 (M1.4: deleted-volume garbage; run <vol> targets one tombstone or punches a live volume's all-zero objects)
//	recovery    ls | check <vol> | run <vol>   (M6.3: heal diverged/missing replicas)
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

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/meta"
	"github.com/unistack-org/storage/pkg/object"
	"github.com/unistack-org/storage/pkg/qos"
)

const chunk = 1 << 20 // import/export streaming granularity

func main() {
	metaDir := flag.String("meta-dir", "/var/lib/unistord/meta", "cluster meta directory (shared with unistord)")
	flag.Usage = usage
	flag.Parse()
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

	if err := dispatch(ctx, cl, ms, args); err != nil {
		fatal("%v", err)
	}
}

func dispatch(ctx context.Context, cl *client.Cluster, ms meta.Store, args []string) error {
	switch args[0] {
	case "vol":
		return volCmd(ctx, cl, ms, args[1:])
	case "qos":
		return qosCmd(ctx, cl, args[1:])
	case "osd":
		return osdCmd(ctx, ms, args[1:])
	case "vacuum":
		return vacuumCmd(ctx, cl, args[1:])
	case "recovery":
		return recoveryCmd(ctx, cl, args[1:])
	case "status":
		return statusCmd(ctx, args[1:])
	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// ---- vol ----

func volCmd(ctx context.Context, cl *client.Cluster, ms meta.Store, args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("vol: subcommand required (create/ls/rm/info/qos/quota/import/export)")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "create":
		fs := flag.NewFlagSet("vol create", flag.ExitOnError)
		replicas := fs.Int("replicas", 3, "replica count")
		minSize := fs.Int("min-size", 0, "min writable replica set (0 = replicas/2+1)")
		pgNum := fs.Int("pg-num", 0, "placement groups (0 = cluster default)")
		rule := fs.String("rule", "replicated", "crush rule name")
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
		spec, err := cl.CreateVolume(ctx, fs.Arg(0), size, quota, *replicas, *minSize, *pgNum, *rule)
		if err != nil {
			return err
		}
		fmt.Printf("volume %q: size=%s quota=%s oid_base=%d replicas=%d min_size=%d pg_num=%d epoch=%d\n",
			spec.Name, humanSize(spec.Size), quotaLabel(spec), spec.OIDBase, spec.Replicas, spec.MinSize, spec.PGNum, spec.Epoch)
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

	case "qos":
		return volQoS(ctx, cl, rest)

	case "import":
		fs := flag.NewFlagSet("vol import", flag.ExitOnError)
		offset := fs.Uint64("offset", 0, "byte offset inside the volume")
		mustParse(fs, rest)
		if fs.NArg() != 2 {
			fs.Usage()
			return errors.New(`vol import: <name> <file|http(s)://url>`)
		}
		return volImport(ctx, cl, fs.Arg(0), fs.Arg(1), *offset)

	case "export":
		if len(rest) != 2 {
			return errors.New("vol export: <name> <file|->")
		}
		return volExport(ctx, cl, rest[0], rest[1])

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

func volImport(ctx context.Context, cl *client.Cluster, name, src string, offset uint64) error {
	v, err := cl.OpenVolume(ctx, name)
	if err != nil {
		return err
	}
	defer v.Close()
	if offset%512 != 0 || offset > v.Size() {
		return fmt.Errorf("import: offset %d must be 512-aligned and within the volume (%s)", offset, humanSize(v.Size()))
	}
	var r io.Reader
	if strings.HasPrefix(src, "http://") || strings.HasPrefix(src, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("import: fetch %s: %w", src, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("import: fetch %s: HTTP %s", src, resp.Status)
		}
		r = resp.Body
	} else {
		f, err := os.Open(src)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	fmt.Fprintf(os.Stderr, "import: streaming %s -> volume %q\n", src, name)
	buf := make([]byte, chunk)
	var total uint64
	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			if offset+uint64(n) > v.Size() {
				return fmt.Errorf("import: source larger than the volume (beyond %s at offset %d)", humanSize(v.Size()), offset)
			}
			if _, werr := v.WriteAt(ctx, buf[:n], offset); werr != nil {
				return fmt.Errorf("import: write at %d: %w", offset, werr)
			}
			offset += uint64(n)
			total += uint64(n)
			if total%(64<<20) < chunk { // progress every 64 MiB
				fmt.Fprintf(os.Stderr, "import: %s\r", humanSize(total))
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return fmt.Errorf("import: read: %w", err)
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

func osdCmd(ctx context.Context, ms meta.Store, args []string) error {
	if len(args) == 0 || args[0] != "ls" {
		usage()
		return errors.New("osd: subcommand required (ls)")
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
		mustParse(fs, rest)
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
	resp, err := http.DefaultClient.Do(req)
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

// ---- helpers ----

func usage() {
	fmt.Fprintf(os.Stderr, `unistorctl: unistack storage control tool

unistorctl [-meta-dir DIR] <command> ...

vol create <name> <size> [-quota S] [-replicas n] [-min-size n] [-pg-num n] [-rule r]
vol ls | vol info <name> | vol rm <name>
vol qos <name> [-class c] [-r iops[:Bps]] [-w iops[:Bps]]
vol quota <name> <size|0>  (allocation cap; open volumes pick it up live)
vol import <name> <file|http(s)://url> [-offset N]
vol export <name> <file|->
qos ls | qos put <class> <riops:rbps:wiops:wbps> | qos rm <class>
osd ls
vacuum [ls|run [vol]]  (M1.4: free deleted volumes' objects; run <vol> = targeted tombstone, live volume = punch out all-zero objects)
recovery ls | recovery check <vol> | recovery run <vol> [-iops N] [-bps B]
status [-http URL]  (reads unistord admin /v1/status; default http://127.0.0.1:8443)

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
