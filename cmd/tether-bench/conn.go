package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// connResult is the report of the conn command.
type connResult struct {
	Machine  machine     `json:"machine"`
	Targets  []string    `json:"targets"`
	Wanted   int         `json:"clients_wanted"`
	Rate     float64     `json:"rate_per_s"`
	Connect  connStats   `json:"connect"`
	HeldS    float64     `json:"held_s"`
	Lost     int         `json:"lost_while_held"`
	RSSMiB   *memReport  `json:"memory,omitempty"`
	NodeSide []nodeStats `json:"node_side,omitempty"`
}

// memReport is the brokers' resident memory before the connections and after
// them, one entry per process.
type memReport struct {
	BeforeMiB   []float64 `json:"before_mib"`
	AfterMiB    []float64 `json:"after_mib"`
	PerConnKiB  float64   `json:"per_connection_kib"`
	TotalDeltaM float64   `json:"total_delta_mib"`
}

// memoryReport turns two RSS readings into the report, over conns connections.
func memoryReport(before, after []int64, conns int) *memReport {
	if len(before) == 0 {
		return nil
	}

	r := &memReport{BeforeMiB: make([]float64, len(before)), AfterMiB: make([]float64, len(after))}

	var delta int64

	for i := range before {
		r.BeforeMiB[i], r.AfterMiB[i] = mib(before[i]), mib(after[i])
		delta += after[i] - before[i]
	}

	r.TotalDeltaM = mib(delta)

	if conns > 0 {
		r.PerConnKiB = float64(delta) / float64(conns) / 1024
	}

	return r
}

func runConn(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("conn", stderr)
	tf := addTargetFlags(fs)
	clients := fs.Int("clients", 1000, "connections to open")
	rate := fs.Float64("rate", 500, "connections opened per second (0 means as fast as --parallel allows)")
	parallel := fs.Int("parallel", 256, "most dials in flight at once")
	hold := fs.Duration("hold", 10*time.Second, "how long to hold the connections before measuring memory and closing")
	asJSON := fs.Bool("json", false, "print the result as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	t, err := tf.build()
	if err != nil {
		return err
	}

	if *clients < 1 || *parallel < 1 {
		return errors.New("conn: --clients and --parallel must be at least 1")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res := connResult{Machine: describeMachine(ctx), Targets: t.addrs, Wanted: *clients, Rate: *rate}
	before := rssBytes(ctx, t.pids)

	cs, st := t.connectMany(ctx, "c", *clients, *rate, *parallel, nil, nil)
	res.Connect = st

	// A reset, not a FIN, so the closed ports are free for the next run at once.
	defer closeAll(cs, true)

	sleepUntil(ctx, time.Now().Add(*hold))

	res.HeldS = hold.Seconds()

	for _, c := range cs {
		select {
		case <-c.lost:
			res.Lost++
		default:
		}
	}

	res.RSSMiB = memoryReport(before, rssBytes(ctx, t.pids), len(cs)-res.Lost)

	if res.NodeSide, err = t.adminStats(ctx); err != nil {
		fmt.Fprintln(stderr, "tether-bench: admin stats:", err)
	}

	if *asJSON {
		return writeJSON(stdout, res)
	}

	printConn(stdout, &res)

	return nil
}

func printConn(w io.Writer, r *connResult) {
	fmt.Fprintf(w, "machine: %s, %d cores, load %s\n", r.Machine.CPU, r.Machine.Cores, r.Machine.LoadAvg)
	fmt.Fprintf(w, "targets: %v\n", r.Targets)
	fmt.Fprintf(w, "asked for %d connections at %.0f/s; %d connected, %d failed, in %.1fs (%.0f attempts/s)\n",
		r.Wanted, r.Rate, r.Connect.OK, r.Connect.Failed, r.Connect.Elapsed, r.Connect.Achieved)
	fmt.Fprintf(w, "connected per node (client side): %v\n", r.Connect.PerNode)
	fmt.Fprintf(w, "connect latency: %s\n", r.Connect.Latency)

	for k, v := range r.Connect.Errors {
		fmt.Fprintf(w, "  error x%d: %s\n", v, k)
	}

	fmt.Fprintf(w, "held %.0fs, %d lost\n", r.HeldS, r.Lost)

	if len(r.NodeSide) > 0 {
		fmt.Fprintf(w, "connected per node (node side): %v\n", r.NodeSide)
	}

	if m := r.RSSMiB; m != nil {
		fmt.Fprintf(w, "broker memory (RSS MiB) before %v, after %v; %.1f MiB more, %.1f KiB per connection\n",
			roundAll(m.BeforeMiB), roundAll(m.AfterMiB), m.TotalDeltaM, m.PerConnKiB)
	}
}

func roundAll(v []float64) []float64 {
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = float64(int(x*10+0.5)) / 10
	}

	return out
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(v)
}
