# tether-bench

A load harness for Tether. It opens MQTT 5 connections with paho.golang and measures three things: how many connections a node or a cell holds and what they cost in memory, how long a message takes from publish to delivery at QoS 0 and QoS 1, and what a reconnect storm does to the clients that stay connected.

[`RESULTS.md`](RESULTS.md) has the numbers from the first run on one Mac. Read its first section before you quote any of them.

## Build it

```sh
go build -o bin/tether ./cmd/tether
go build -o bin/tether-bench ./cmd/tether-bench
```

You need Go and nothing else. There's no Docker and no `nats-server` to install, because the harness runs the NATS servers as itself (`tether-bench nats`, which the `cell` command starts for you).

## Start a cell to test against

```sh
bin/tether-bench cell --bin bin/tether --dir /tmp/cell --nodes 3 --nats 3
```

That starts three NATS servers (one JetStream cluster) and three Tether nodes on loopback ports it picks, waits until every node is healthy, makes a tenant and 16 devices through the admin API, and writes `cell.json` and `creds.csv` into the directory. Then it sits there until you press Ctrl-C. Every process logs to a file in the directory (`n1.log` to `n3.log`, `s1.log` to `s3.log`).

`cell.json` lists the MQTT address, admin URL and process ID of every node, the admin token and the tenant. Every other command takes `--cell /tmp/cell/cell.json` and reads what it needs from there.

Ctrl-C and SIGTERM stop everything. If `cell` is killed with `kill -9`, the NATS servers go with it (they exit when their stdin closes), but the three `tether` nodes keep running. Stop them with `kill` on the `broker_pids` in `cell.json`.

To test a cell you started some other way, skip `cell`. Make the tenant and the devices with `provision`, then pass `--target host:port[,host:port...]` and `--creds creds.csv` to the load commands:

```sh
bin/tether-bench provision --admin http://127.0.0.1:8080/tether/v1 --token "$TETHER_ADMIN_TOKEN" --devices 16 --out creds.csv
```

The credentials CSV is `username,password` per row, with an optional header. The devices have no pinned client ID, so any number of clients can share one pair, and the commands hand them out round robin. Keysmith caches a key for a minute, so this keeps authentication warm. A real fleet has one key per device, and its first connect each minute pays for a lookup that this harness doesn't.

## Connections: `conn`

```sh
bin/tether-bench conn --cell /tmp/cell/cell.json --clients 15000 --rate 500 --hold 20s
```

It opens 15,000 clients, 500 a second, spread over the nodes. Then it holds them for 20 seconds and prints the connect latency (p50 and p99), how many each node holds by the client's count and by the node's own, and the brokers' resident memory before and after. That's where the per-connection number comes from. Pass `--only-node 0` to put every client on the first node and leave the others idle.

Memory comes from `ps` and the process IDs in `cell.json` (or `--pids`). Start from a fresh cell when you want a clean baseline. A broker keeps memory it has used, so a second run on the same cell starts from a higher number.

## Latency: `pubsub`

```sh
bin/tether-bench pubsub --cell /tmp/cell/cell.json --qos 1 --clients 200 --rate 2000 \
  --duration 30s --background 12000
```

It opens 12,000 idle connections as background load, then 200 measured clients: 100 subscribers, each with one publisher on a different node (`--placement cross`, the default) or on the same node (`--placement same`). The publishers send 2,000 messages a second between them for 5 seconds of warmup and then 30 seconds that count. Each payload carries the time its slot was scheduled for, and the subscriber reads the clock when the message arrives. The scheduled time, not the moment the publisher got round to sending, so a publisher that wakes late (a garbage collection, a busy machine) adds that delay to the latency of the messages it held up instead of hiding it. The report also gives the publishers' lateness (p50, p99, max), so you can see how often it happened. Both ends live in this process, so there's no clock skew to correct.

You get exact percentiles over every delivered message (a sorted list, not buckets), the publish-to-PUBACK time at QoS 1, and the counts of messages sent, acknowledged, received, duplicated and lost. It also prints the machine and the load average it saw.

## Reconnect storms: `storm`

A storm drops a group of connected clients all at once, with a reset and no DISCONNECT, the way a node crash or a load balancer flap does. Each client then reconnects at a random moment inside a window, so 8,000 clients over 5 seconds is 1,600 reconnects a second.

You can run one inside `pubsub` with `--storm 8000 --storm-window 5s --storm-at 15s`. It's the easy way, and the storm's reconnects then share a process with the measuring, which slows both. For a number you'll quote, run the storm as its own process, aimed at the measured window:

```sh
t0=$(( $(date +%s) + 60 ))
bin/tether-bench storm --cell /tmp/cell/cell.json --clients 8000 --window 5s --at $(( t0 + 5 + 15 )) --until $(( t0 + 5 + 40 + 5 )) --json > storm.json &
bin/tether-bench pubsub --cell /tmp/cell/cell.json --qos 1 --clients 200 --rate 2000 --duration 40s \
  --background 5000 --storm-external --storm-window 5s --storm-at 15s --start-at $t0 --json > pubsub.json
```

`--start-at` is when the publishers begin and `--at` is when the storm fires, both as Unix seconds. `--until` keeps the reconnected clients connected past the end of the measured window. Without it the storm process exits right after the storm, and its 8,000 connections reset inside the window you're measuring. Leave enough time before `t0` for both processes to open their connections. The `pubsub` report splits the window into baseline, storm and after, and prints the storm phase's p99 over the baseline's.

## Repeat a run: `series.sh`

```sh
TETHER_BIN=bin/tether BENCH_BIN=bin/tether-bench \
  ./cmd/tether-bench/series.sh out qos1-cross 3 pubsub --qos 1 --rate 2000 \
  --clients 200 --duration 30s --background 12000
```

That starts a fresh cell, runs the command, saves `out/qos1-cross-1.json` and stops the cell, three times over. A fresh cell per run keeps one run's memory and logs out of the next. Each run also records how many times a node fenced itself (`out/qos1-cross-1.cell.json`), and a run where that isn't zero isn't a run to take numbers from. Set `STORM_CLIENTS`, `STORM_WINDOW`, `STORM_AT` and `STORM_DURATION` (the last three in seconds, the last one matching `--duration`) and it runs the storm process alongside `pubsub` for you.

## What to know before you trust a number

One loopback address has about 16,000 ports. Every client connects from `127.0.0.1`, and macOS hands out ephemeral ports from 49152 to 65535 for all of them. Past roughly 15,000 clients, new connects fail with "can't assign requested address", and so do the NATS servers' own connections to each other, which can take the cell down along with the bench. Don't go past 15,000 on one machine. It's why the first run stopped there.

The bench and the cell share the machine. The brokers, the NATS servers and the load generator compete for the same cores, and the latencies include the load generator's scheduling. Use a quiet machine, and keep `uptime` next to every number (the harness prints the load average it saw).

At the end of a run the clients reset their connections, so no port lingers in TIME_WAIT for the next run. The brokers see a hard disconnect, not a DISCONNECT.

Every client connects with clean start and no session expiry. Persistent sessions cost more per connection, and this harness doesn't measure that.

This tests a cell on one machine. The spec's goal of a million connections on ten nodes needs several machines, and nothing here can show it.
