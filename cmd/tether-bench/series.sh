#!/usr/bin/env bash
# Run one scenario several times, each time against a fresh cell.
#
#   TETHER_BIN=bin/tether BENCH_BIN=bin/tether-bench \
#     ./series.sh OUTDIR NAME RUNS COMMAND [flags...]
#
# COMMAND is conn or pubsub. A run starts a cell of three NATS servers and three
# nodes, waits for it, runs the command against it with --json, writes
# OUTDIR/NAME-<n>.json, and stops the cell. A fresh cell per run keeps one run's
# memory and logs out of the next one's numbers. However the script ends (a
# failed command, Ctrl-C), it stops the cell, the storm process and removes the
# cell's directory, so a failed run does not leave processes and ports behind.
#
# For a reconnect storm, set STORM_CLIENTS, STORM_WINDOW, STORM_AT and
# STORM_DURATION (seconds) with COMMAND pubsub. STORM_DURATION is the --duration
# given to pubsub. The script then starts a second process, tether-bench storm,
# aimed at the measured window, so the storm's own cost stays out of the process
# that times the messages. That process keeps its clients connected until the
# measured window is over, so closing them does not land inside it. Set
# STORM_GOMAXPROCS to limit how many cores the storm process may use.
set -euo pipefail

out=$1 name=$2 runs=$3 cmd=$4
shift 4

: "${TETHER_BIN:?set TETHER_BIN to the tether binary}"
: "${BENCH_BIN:?set BENCH_BIN to the tether-bench binary}"

nodes=${NODES:-3}
nats=${NATS:-3}
warmup=${WARMUP:-5}
setup=${SETUP:-60} # seconds between the start and the moment publishing begins
settle=${SETTLE:-15}

cell="" storm="" dir=""

cleanup() {
  if [ -n "$storm" ]; then
    kill "$storm" 2>/dev/null || true
    wait "$storm" 2>/dev/null || true
    storm=""
  fi
  if [ -n "$cell" ]; then
    kill -TERM "$cell" 2>/dev/null || true
    wait "$cell" 2>/dev/null || true
    cell=""
  fi
  if [ -n "$dir" ]; then
    rm -rf "$dir"
    dir=""
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

mkdir -p "$out"

for n in $(seq 1 "$runs"); do
  dir=$(mktemp -d "${TMPDIR:-/tmp}/tether-bench-cell.XXXXXX")
  "$BENCH_BIN" cell --bin "$TETHER_BIN" --dir "$dir" --nodes "$nodes" --nats "$nats" >"$dir/cell.out" 2>&1 &
  cell=$!

  for _ in $(seq 1 120); do
    [ -s "$dir/cell.json" ] && break
    sleep 1
  done
  [ -s "$dir/cell.json" ] || { echo "cell did not start: $(cat "$dir/cell.out")" >&2; exit 1; }

  sleep "$settle"
  echo "run $n of $runs: $name (load: $(uptime | sed 's/.*load averages*: //'))" >&2

  if [ -n "${STORM_CLIENTS:-}" ]; then
    : "${STORM_WINDOW:?}" "${STORM_AT:?}" "${STORM_DURATION:?}"
    t0=$(( $(date +%s) + setup ))
    env ${STORM_GOMAXPROCS:+GOMAXPROCS=$STORM_GOMAXPROCS} "$BENCH_BIN" storm --cell "$dir/cell.json" \
      --clients "$STORM_CLIENTS" --window "${STORM_WINDOW}s" \
      --at $(( t0 + warmup + STORM_AT )) --until $(( t0 + warmup + STORM_DURATION + 5 )) --json \
      >"$out/$name-$n.storm.json" &
    storm=$!
    "$BENCH_BIN" "$cmd" --cell "$dir/cell.json" --json --warmup "${warmup}s" --storm-external \
      --storm-window "${STORM_WINDOW}s" --storm-at "${STORM_AT}s" --start-at "$t0" "$@" >"$out/$name-$n.json"
    wait "$storm"
    storm=""
  else
    "$BENCH_BIN" "$cmd" --cell "$dir/cell.json" --json "$@" >"$out/$name-$n.json"
  fi

  # Fence counts from the brokers' logs: a run that fenced a node is not a run
  # to take numbers from.
  fences=$(cat "$dir"/n*.log | grep -c 'fenced from the cell' || true)
  echo "{\"fence_events\": $fences}" >"$out/$name-$n.cell.json"

  cleanup
  sleep 10
done
