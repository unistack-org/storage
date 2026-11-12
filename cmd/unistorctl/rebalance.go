package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/unistack-org/storage/pkg/client"
	"github.com/unistack-org/storage/pkg/rebalance"
)

// rebalanceCmd runs the free-space rebalancer: one pass with -once,
// otherwise a loop. It is stateless — every pass re-derives the plan
// from the latest map epoch and live disk usage, so any host can run
// it and a crash costs nothing.
func rebalanceCmd(ctx context.Context, cl *client.Cluster, args []string) error {
	fs := flag.NewFlagSet("rebalance", flag.ExitOnError)
	interval := fs.Duration("interval", 30*time.Second, "pass interval in the daemon loop")
	threshold := fs.Float64("threshold", 0.05, "minimum relative weight change worth a new epoch (hysteresis)")
	floor := fs.Float64("floor", 0.05, "minimum weight as a fraction of the largest current weight (nearly-full osds slow down, never drain out)")
	timeout := fs.Duration("probe-timeout", 5*time.Second, "disk probe round timeout")
	once := fs.Bool("once", false, "single pass, print the plan, exit")
	dryRun := fs.Bool("dry-run", false, "report the plan without publishing anything")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: unistorctl rebalance [-once] [-dry-run] [-interval 30s] [-threshold 0.05] [-floor 0.05] [-probe-timeout 5s]")
		fs.PrintDefaults()
	}
	fs.Parse(args)

	opts := rebalance.Options{
		Threshold:      *threshold,
		MinWeightFloor: *floor,
		ProbeTimeout:   *timeout,
		DryRun:         *dryRun,
	}
	d := rebalance.New(cl, opts)
	if !*once {
		d.Run(ctx, *interval, func(format string, args ...any) { fmt.Printf(format+"\n", args...) })
		return nil
	}
	rep, err := d.Once(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("rebalance: epoch %d, %d eligible osd(s), %d probe failure(s)\n", rep.Epoch, rep.Eligible, rep.ProbeFail)
	if len(rep.Changes) == 0 {
		if rep.Eligible < 2 {
			fmt.Println("plan: nothing to do (need at least 2 in-service osds answering the disk probe)")
		} else {
			fmt.Println("plan: balanced (no material weight change)")
		}
		return nil
	}
	for _, c := range rep.Changes {
		fmt.Printf("  osd %d: weight %.4f -> %.4f (free %s)\n", c.OSD, c.Current, c.Target, humanSize(c.Free))
	}
	if rep.NewEpoch > 0 {
		fmt.Printf("published epoch %d\n", rep.NewEpoch)
	} else {
		fmt.Println("dry run: nothing published")
	}
	return nil
}
