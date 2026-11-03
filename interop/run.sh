#!/bin/bash
# Run the Paho interoperability suite against Tether, four ways.
#
#   interop/run.sh <suite-checkout> <out-dir> [spread]
#
# Builds the harness, then runs client_test.py (3.1.1) and client_test5.py (5.0)
# against a single node and against a three-node cell. With "spread" the cell
# runs go through a round-robin proxy, so the suite's clients land on different
# nodes. Output for each run goes to <out-dir>/<run>.{out,broker.log}.
set -u

suite=${1:?usage: run.sh <suite-checkout> <out-dir> [spread]}
out=${2:?usage: run.sh <suite-checkout> <out-dir> [spread]}
spread=${3:-}
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$out"
out=$(cd "$out" && pwd)

(cd "$here/.." && GOWORK=off go build -o "$out/interop" ./interop) || exit 1

run() {
  local name=$1 mode=$2 module=$3 target=${4:-18831}
  "$out/interop" -mode "$mode" -port 18831 >"$out/$name.broker.log" 2>&1 &
  local pid=$! ppid="" port=18831
  for _ in $(seq 1 120); do grep -q '^ready' "$out/$name.broker.log" && break; sleep 1; done
  if [ "$target" != 18831 ]; then
    python3 -I "$here/spread_proxy.py" 18900 "$target" "$out/$name.pkts" &
    ppid=$!
    port=18900
    sleep 1
  fi
  python3 -I "$here/run_suite.py" --suite "$suite" --module "$module" --port "$port" >"$out/$name.out" 2>&1
  [ -n "$ppid" ] && kill "$ppid"
  kill "$pid"
  wait "$pid" 2>/dev/null
  echo "== $name"
  grep '^RESULT' "$out/$name.out"
}

cell_target=18831
suffix=cell
if [ "$spread" = spread ]; then
  cell_target=18831,18832,18833
  suffix=spread
fi

run single.311 single client_test
run single.5 single client_test5
run $suffix.311 cell client_test "$cell_target"
run $suffix.5 cell client_test5 "$cell_target"
