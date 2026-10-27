// Command tether-bench is Tether's load harness. It opens MQTT 5 connections
// against a node or a cell and measures what they cost and how fast messages
// cross: connection counts and memory, publish-to-deliver latency at QoS 0 and
// QoS 1, and what a reconnect storm does to the clients that stay connected.
//
// The subcommands:
//
//	conn       open N connections at a rate and hold them
//	pubsub     publish at a rate and measure publish-to-deliver latency
//	storm      drop and reconnect a group of clients, for pubsub to measure
//	provision  make a tenant, a role and devices through the admin API
//	cell       run a local cell (NATS servers and broker processes) to test against
//
// README.md has the commands, and RESULTS.md has the numbers from the first run.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const usage = `usage: tether-bench <command> [flags]

commands:
  conn       open N connections at a rate and hold them
  pubsub     publish at a rate and measure publish-to-deliver latency
  storm      drop and reconnect a group of clients (run beside pubsub)
  provision  make a tenant, a role and devices through the admin API
  cell       run a local cell (NATS servers and broker processes)

Run "tether-bench <command> -h" for the flags of one command.
`

func main() {
	if code := run(os.Args[1:], os.Stdout, os.Stderr); code != 0 {
		os.Exit(code)
	}
}

// run is the whole program. It returns the exit code.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)

		return 2
	}

	var err error

	switch args[0] {
	case "conn":
		err = runConn(args[1:], stdout, stderr)
	case "pubsub":
		err = runPubsub(args[1:], stdout, stderr)
	case "storm":
		err = runStormCommand(args[1:], stdout, stderr)
	case "provision":
		err = runProvision(args[1:], stdout, stderr)
	case "cell":
		err = runCell(args[1:], stdout, stderr)
	case natsCommand:
		err = runNATS(args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		fmt.Fprint(stdout, usage)

		return 0
	default:
		fmt.Fprintf(stderr, "tether-bench: unknown command %q\n\n%s", args[0], usage)

		return 2
	}

	switch {
	case err == nil:
		return 0
	case errors.Is(err, flag.ErrHelp):
		return 0
	default:
		fmt.Fprintln(stderr, "tether-bench:", err)

		return 1
	}
}

// discard drops an error that cannot be acted on: a close or a kill on the way
// out. It exists so the choice shows in the code and the linter does not trip.
func discard(error) {}

// newFlags makes a flag set that reports its own errors through the returned
// error and writes its help to w.
func newFlags(name string, w io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("tether-bench "+name, flag.ContinueOnError)
	fs.SetOutput(w)

	return fs
}

// splitList splits a comma separated flag value, dropping blanks.
func splitList(s string) []string {
	var out []string

	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}

	return out
}
