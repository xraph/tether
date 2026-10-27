package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// parseWhen reads a moment as RFC 3339 or as Unix seconds (fractions allowed).
func parseWhen(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither RFC 3339 nor Unix seconds", s)
	}

	return time.Unix(0, int64(f*float64(time.Second))), nil
}

// stormReport is the report of the storm command.
type stormReport struct {
	Machine machine     `json:"machine"`
	Targets []string    `json:"targets"`
	Setup   connStats   `json:"setup"`
	Storm   stormResult `json:"storm"`
	At      time.Time   `json:"dropped_at"`
	// CPUCores is how much CPU this process used from the drop to the last
	// reconnect, in cores, so a run can say how much of the machine the storm took.
	CPUCores float64 `json:"storm_process_cpu_cores"`
}

// processCPU is the CPU time this process has used, user and system.
func processCPU() time.Duration {
	var ru syscall.Rusage

	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// runStormCommand is a reconnect storm in a process of its own: it connects the group,
// waits for the moment, then drops every client and has each one reconnect at a
// random time inside the window. Running it apart from pubsub keeps the cost of
// the storm out of the process that times the messages.
func runStormCommand(args []string, stdout, stderr io.Writer) error {
	fs := newFlags("storm", stderr)
	tf := addTargetFlags(fs)
	clients := fs.Int("clients", 8000, "clients in the storm")
	rate := fs.Float64("rate", 1000, "connections opened per second while setting up")
	parallel := fs.Int("parallel", 256, "most dials in flight at once")
	window := fs.Duration("window", 5*time.Second, "the reconnects are spread over this long")
	at := fs.String("at", "", "the moment to drop the clients (RFC 3339 or Unix seconds); default: right after setup")
	until := fs.String("until", "", "keep the reconnected clients until this moment (RFC 3339 or Unix seconds); default: exit as soon as the storm ends")
	asJSON := fs.Bool("json", false, "print the result as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	t, err := tf.build()
	if err != nil {
		return err
	}

	if *clients < 1 || *window <= 0 || *parallel < 1 {
		return errors.New("storm: --clients and --parallel must be at least 1 and --window above 0")
	}

	var when, hold time.Time

	if *at != "" {
		if when, err = parseWhen(*at); err != nil {
			return err
		}
	}

	if *until != "" {
		if hold, err = parseWhen(*until); err != nil {
			return err
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rep := stormReport{Machine: describeMachine(ctx), Targets: t.addrs}

	group, st := t.connectMany(ctx, "s", *clients, *rate, *parallel, nil, nil)
	rep.Setup = st

	defer closeAll(group, true)

	if !when.IsZero() {
		if time.Now().After(when) {
			return fmt.Errorf("storm: --at %s has passed; setup took too long", when.Format(time.RFC3339))
		}

		sleepUntil(ctx, when)
	}

	rep.At = time.Now()
	cpu0 := processCPU()

	sr, again := runStorm(ctx, t, group, *window, *parallel)
	rep.Storm = sr

	if d := time.Since(rep.At); d > 0 {
		rep.CPUCores = float64(processCPU()-cpu0) / float64(d)
	}

	defer closeAll(again, true)

	// Closing the clients is a reset of thousands of connections at once. Hold
	// them past the end of the measured window, or that reset lands in it.
	sleepUntil(ctx, hold)

	if *asJSON {
		return writeJSON(stdout, rep)
	}

	s := rep.Storm
	fmt.Fprintf(stdout, "storm: %d clients over %.0fs (%.0f/s): %d reconnected, %d failed, reconnect %s\n",
		s.Clients, s.WindowS, s.PerSecond, s.Reconnect.OK, s.Reconnect.Failed, s.Reconnect.Latency)

	return nil
}
