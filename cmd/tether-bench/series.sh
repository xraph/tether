#!/usr/bin/env bash
# Run one scenario several times, each time against a fresh cell.
#
#   TETHER_BIN=bin/tether BENCH_BIN=bin/tether-bench \
#     ./series.sh OUTDIR NAME RUNS COMMAND [flags...]
#
# COMMAND is conn or pubsub. A run starts a cell of three NATS servers and three
# nodes, waits for it, runs the command against it with --json, writes
# OUTDIR/NAME-<n>.json, and stops the cell. A fresh cell per run keeps one run's
# memory and logs out of the next one's numbers.
#
# For a reconnect storm, set STORM_CLIENTS, STORM_WINDOW and STORM_AT (seconds)
# with COMMAND pubsub. The script then starts a second process, tether-bench
# storm, aimed at the measured window, so the storm's own cost stays out of the
# process that times the messages. STORM_WINDOW and STORM_AT are in seconds, and
# STORM_DURATION is the --duration given to pubsub, in seconds: the storm process
# keeps its clients connected until the measured window is over, so closing them
# does not land inside it.
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

mkdir -p "$out"

for n in $(seq 1 "$runs"); do
  dir=$(mktemp -d "${TMPDIR:-/tmp}/tether-bench-cell.XXXXXX")
  "$BENCH_BIN" cell --bin "$TETHER_BIN" --dir "$dir" --nodes "$nodes" --nats "$nats" >"$dir/cell.out" 2>&1 &
  cell=$!

  for _ in $(seq 1 120); do
    [ -s "$dir/cell.json" ] && break
    sleep 1
  done
  [ -s "$dir/cell.json" ] || { echo "cell did not start: $(cat "$dir/cell.out")" >&2; kill "$cell" 2>/dev/null; exit 1; }

  sleep "$settle"
  echo "run $n of $runs: $name (load: $(uptime | sed 's/.*load averages*: //'))" >&2

  if [ -n "${STORM_CLIENTS:-}" ]; then
    : "${STORM_WINDOW:?}" "${STORM_AT:?}" "${STORM_DURATION:?}"
    t0=$(( $(date +%s) + setup ))
    "$BENCH_BIN" storm --cell "$dir/cell.json" --clients "$STORM_CLIENTS" --window "${STORM_WINDOW}s" \
      --at $(( t0 + warmup + STORM_AT )) --until $(( t0 + warmup + STORM_DURATION + 5 )) --json >"$out/$name-$n.storm.json" &
    storm=$!
    "$BENCH_BIN" "$cmd" --cell "$dir/cell.json" --json --warmup "${warmup}s" --storm-external \
      --storm-window "${STORM_WINDOW}s" --storm-at "${STORM_AT}s" --start-at "$t0" "$@" >"$out/$name-$n.json"
    wait "$storm"
  else
    "$BENCH_BIN" "$cmd" --cell "$dir/cell.json" --json "$@" >"$out/$name-$n.json"
  fi

  # Heartbeat and fence counts from the brokers' logs: a run that fenced a node
  # is not a run to take numbers from.
  fences=$(cat "$dir"/n*.log | grep -c 'fenced from the cell' || true)
  echo "{\"fence_events\": $fences}" >"$out/$name-$n.cell.json"

  kill -TERM "$cell"
  wait "$cell" 2>/dev/null || true
  rm -rf "$dir"
  sleep 10
done
