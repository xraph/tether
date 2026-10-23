// Command jetstream is the Phase 0, Task 4 spike: JetStream behaviour on the
// pinned nats-server for Tether's QoS 1/2 log, session keys and replay.
//
// It starts a 3-server R3 JetStream cluster of child processes per run and
// measures publish-ack latency (sync and async), session-key CAS latency, and
// ordered-consumer catch-up speed. The correctness checks (CAS, TTL, dedup) are
// the tests next to it. Run from this directory with GOWORK=off; see README.md.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type config struct {
	runs      int
	window    time.Duration
	rounds    int
	modes     string
	out       string
	sizes     string
	rates     string
	casRates  string
	casKeys   int
	workers   int
	perTenant int
	tenants   int
}

func main() {
	MaybeRunChild()

	var cfg config

	flag.IntVar(&cfg.runs, "runs", 3, "runs; each starts a fresh 3-server cluster")
	flag.DurationVar(&cfg.window, "window", 3*time.Second, "length of one cell window")
	flag.IntVar(&cfg.rounds, "rounds", 3, "windows per cell per run, interleaved with every other cell")
	flag.StringVar(&cfg.modes, "modes", "publish,cas,catchup", "comma separated: publish, cas, catchup")
	flag.StringVar(&cfg.out, "out", "", "write per-run results as JSON into this directory")
	flag.StringVar(&cfg.sizes, "sizes", "256,65536", "publish payload sizes in bytes")
	flag.StringVar(&cfg.rates, "rates", "1000,10000,50000", "publish rates, msgs/s")
	flag.StringVar(&cfg.casRates, "cas-rates", "1700,5000,10000", "session CAS rates, ops/s")
	flag.IntVar(&cfg.casKeys, "cas-keys", 60_000, "session keys in the CAS bucket")
	flag.IntVar(&cfg.workers, "workers", 64, "sync publishers per connection (3 connections)")
	flag.IntVar(&cfg.perTenant, "per-tenant", 1_000_000, "catch-up: messages per tenant in the log")
	flag.IntVar(&cfg.tenants, "tenants", 10, "catch-up: tenants in the log")
	flag.Parse()

	if err := harness(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func has(modes, m string) bool {
	for _, x := range strings.Split(modes, ",") {
		if strings.TrimSpace(x) == m {
			return true
		}
	}

	return false
}

func harness(cfg config) error {
	fmt.Printf("machine: %s\n", machine())
	fmt.Printf("config: %+v\n", cfg)
	fmt.Printf("load at start: %s\n", loadavg())

	var pubRuns, casRuns, catchRuns, scaleRuns [][]row

	for r := 1; r <= cfg.runs; r++ {
		fmt.Printf("\n== run %d: starting cluster (load %s) ==\n", r, loadavg())

		cl, err := StartCluster(3)
		if err != nil {
			return fmt.Errorf("run %d: %w", r, err)
		}

		err = func() error {
			defer cl.Stop()

			if has(cfg.modes, "publish") {
				rows, err := runPublish(cfg, cl, r)
				if err != nil {
					return fmt.Errorf("publish: %w", err)
				}

				printRows(fmt.Sprintf("run %d: publish-ack latency", r), rows)
				save(cfg.out, fmt.Sprintf("publish-%d", r), rows)
				pubRuns = append(pubRuns, rows)
			}

			if has(cfg.modes, "cas") {
				rows, err := runCAS(cfg, cl, r)
				if err != nil {
					return fmt.Errorf("cas: %w", err)
				}

				printRows(fmt.Sprintf("run %d: session CAS", r), rows)
				save(cfg.out, fmt.Sprintf("cas-%d", r), rows)
				casRuns = append(casRuns, rows)
			}

			if has(cfg.modes, "catchup") {
				rows, err := runCatchup(cfg, cl, r)
				if err != nil {
					return fmt.Errorf("catchup: %w", err)
				}

				printCatch(fmt.Sprintf("run %d: catch-up", r), rows)
				save(cfg.out, fmt.Sprintf("catchup-%d", r), rows)
				catchRuns = append(catchRuns, rows)

				nc, err := nats.Connect(cl.URLs()[0])
				if err != nil {
					return err
				}
				defer nc.Close()

				js, _ := jetstream.New(nc)

				srows, err := runFilterScaling(js)
				if err != nil {
					return fmt.Errorf("filter scaling: %w", err)
				}

				printCatch(fmt.Sprintf("run %d: multi-filter scaling", r), srows)
				save(cfg.out, fmt.Sprintf("scaling-%d", r), srows)
				scaleRuns = append(scaleRuns, srows)
			}

			return nil
		}()
		if err != nil {
			return fmt.Errorf("run %d: %w", r, err)
		}
	}

	if len(pubRuns) > 0 {
		printRows(fmt.Sprintf("publish-ack latency, median of %d runs", len(pubRuns)), medianRows(pubRuns))
	}

	if len(casRuns) > 0 {
		printRows(fmt.Sprintf("session CAS, median of %d runs", len(casRuns)), medianRows(casRuns))
	}

	if len(catchRuns) > 0 {
		printCatch(fmt.Sprintf("catch-up, median of %d runs", len(catchRuns)), medianRows(catchRuns))
	}

	if len(scaleRuns) > 0 {
		printCatch(fmt.Sprintf("multi-filter scaling, median of %d runs", len(scaleRuns)), medianRows(scaleRuns))
	}

	fmt.Printf("\nload at end: %s\n", loadavg())

	return nil
}
