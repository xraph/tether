# First cell numbers

This is [`tether-bench`](README.md) against a Tether cell on one Mac, at reduced scale. The short version: latency passes its targets with room to spare, a node costs about 107 KiB per connection, and a reconnect storm at the spec's rate fails the 2x rule. At 1,667 reconnects a second the p99 of clients that stayed connected went up 4.9 times for QoS 1 and 4.3 times for QoS 0. The million connection target wasn't tested, because it needs several machines.

Every number below is the median of three runs, each on a fresh cell, and the three raw values sit beside it where they differ enough to matter. The JSON for every run is in [`results/2026-10-27-634de53/`](results/2026-10-27-634de53/).

## Where these came from

| | |
|---|---|
| Broker code | Tether `634de53` (the cluster and broker code of `036c81d`, the latest heartbeat fix). The harness is `ba4b409` |
| Machine | Apple M3 Max, 16 cores, 128 GiB RAM, macOS 27.0.1, arm64, Go 1.26.9 |
| Cell | 3 `tether` nodes and 3 `nats-server` processes (one JetStream cluster, 3 replicas, nats-server v2.15.1), all on loopback, default heartbeat and lease (2 s, 24 s) |
| Load generator | `tether-bench` on the same machine, paho.golang v0.23.0, MQTT 5, clean start, 16 shared device credentials |
| When | 2026-10-27, 07:35 to 09:07 local time |
| Load average | 3.4 to 9.9 on the 1-minute figure at the start of each run. `series.sh` held a run back until it was under 10 (`MAX_LOAD`). Each run's value is in its JSON under `machine.load_average` |

The machine was never quiet. Other work ran on it throughout, so treat the latencies as what a busy laptop gives you, not what the hardware can do alone.

None of the 48 runs had a node fence itself, lose a connection it was holding, fail a publish, or lose, duplicate or skip a message. Every message that got a PUBACK (QoS 1) or a clean write (QoS 0) reached its subscriber. All 10,000 clients came back in every storm.

## How a latency is measured

A publisher sends one message every interval, and stamps it with the time that slot was scheduled for, not the moment its loop got round to it. A publisher that wakes late (a garbage collection, a busy machine, a blocked write) then adds that delay to the latency of the messages it held up, so the tail isn't flattered. The first pass of these runs stamped the send time, which hid exactly that.

The runs also record how late the publishers woke. Across the 42 pubsub runs the wake-up lateness was 0.03 to 0.10 ms at p50, 0.14 to 1.09 ms at p99, and 2.7 to 37 ms at the worst single wake. So a stamp at the send time would have hidden under a millisecond at p99, and more at the extremes.

Percentiles are exact, from every delivered message, not buckets.

## Connections and memory

15,000 clients opened at 500 a second and held for 20 seconds. First spread over the three nodes, then all on one.

| | 3 nodes, 5,000 each | 1 node, 15,000 |
|---|---|---|
| Connected | 15,000 of 15,000 | 15,000 of 15,000 |
| Connect latency p50 / p99 | 0.85 ms / 3.46 ms | 0.70 ms / 3.67 ms |
| Memory per node before (RSS) | 59 MiB | 59 MiB |
| Memory of the biggest node after (RSS) | 635 MiB (557, 635, 672) | 1,644 MiB (1,556, 1,644, 1,874) |
| Memory per connection | 106.6 KiB (101.2 to 112.0) | 109.0 KiB (103.1 to 124.8) |

That's about 107 KiB of resident memory for each connection, session state included, however the clients are spread. Stretch it in a straight line to the spec's 100,000 connections on each of ten nodes and you get about 10 GiB per node. Nobody has measured that, and a straight line through 15,000 is a guess for 100,000. It does tell you what size of machine to try first.

A node never got near a limit. The ceiling was the client side. macOS gives one loopback address about 16,000 ephemeral ports, shared by every connection the bench opens, and that's why the run stops at 15,000. Past it the load generator starves the NATS servers of ports too. An earlier, exploratory run on an older broker (`6dfe2dd`) asked for 45,000 clients and ran out at 16,163. The NATS servers then couldn't open routes to each other, JetStream lost quorum, and all three brokers fenced after 12 seconds without an acknowledged heartbeat and dropped every client. They took connections again 41 seconds later. That's the fence working as designed, and the harness caused it. It was a one-off, and the log lines are in [`results/incident-port-exhaustion/`](results/incident-port-exhaustion/) for what they're worth.

## Publish-to-deliver latency

100 publisher and subscriber pairs (200 measured clients), 2,000 messages a second between them, 64 byte payloads, 30 seconds after a 5 second warmup, with 12,000 idle connections held on top (12,200 in all, about 4,070 per node). "Cross" means a pair's publisher and subscriber sit on different nodes, so the message goes through NATS. "Same" means one node.

| | p50 | p99 | p99.9 | p99 in the three runs |
|---|---|---|---|---|
| QoS 0, cross | 0.26 ms | 2.72 ms | 4.42 ms | 2.72, 2.88, 2.63 |
| QoS 0, same node | 0.20 ms | 3.44 ms | 19.17 ms | 3.30, 4.54, 3.44 |
| QoS 1, cross | 0.42 ms | 3.00 ms | 5.72 ms | 3.23, 2.71, 3.00 |
| QoS 1, same node | 0.42 ms | 3.09 ms | 7.74 ms | 3.09, 3.08, 3.27 |

The p99.9 of QoS 0 on one node is the noisy one: 5.9, 19.2 and 26.0 ms in the three runs.

At QoS 1 the time from sending a PUBLISH to its PUBACK, which includes the JetStream commit, was 0.40 ms at p50 and 2.74 ms at p99 (cross). The PUBACK comes back about as fast as the message is delivered.

The spec asks for p99 under 10 ms at QoS 0 and under 25 ms at QoS 1, at "nominal load", and doesn't say what nominal is. At 2,000 messages a second and 12,200 connections the cell is well inside both, using about a quarter and an eighth of the budget. At 10,000 messages a second (and 10,000 deliveries a second, nothing lost) QoS 0 barely moves and QoS 1 slows down:

| 10,000 msg/s, cross | p50 | p99 | p99.9 | p99 in the three runs |
|---|---|---|---|---|
| QoS 0 | 0.18 ms | 2.45 ms | 5.35 ms | 2.69, 2.44, 2.45 |
| QoS 1 | 1.68 ms | 8.73 ms | 23.37 ms | 11.60, 8.73, 7.69 |

At QoS 1 the three brokers used 2.4 cores and the NATS servers 3.1 between them. The p99 is still a third of the 25 ms budget, but the p99.9 is close to it.

The p99 also depends on how many connections the cell holds. The same QoS 1 run with 3,200 connections instead of 12,200 gave a p99 of 1.18 ms (1.14, 1.25, 1.18) against 3.00. We haven't looked at why.

## Reconnect storm

The spec wants a storm of 100,000 clients in 60 seconds to leave already-connected clients' p99 at no more than twice its baseline. That's 1,667 reconnects a second, and this machine can't hold 100,000 clients. The storm here is 10,000 clients, dropped all at once with a TCP reset (no DISCONNECT, like a node crash), each coming back at a random moment inside a window. Three window lengths give three rates. The fastest, 6 seconds, is the spec's rate exactly, on a tenth of the clients for a tenth of the time.

The cell held 13,000 connections: 2,800 idle, the 10,000 in the storm, and 200 measured clients at 2,000 messages a second (cross). The storm ran in its own process, so its cost stays out of the process that times the messages. A phase's p99 is of the messages sent in it. Baseline is the 15 seconds before the drop, storm runs from the drop to two seconds after the last reconnect, and after is the rest.

| | Reconnects/s | Baseline p99 | Storm p99 | After p99 | Storm over baseline | In the three runs | Reconnect p99 | Verdict |
|---|---|---|---|---|---|---|---|---|
| QoS 1, 25 s window | 400 | 3.23 ms | 4.56 ms | 2.44 ms | 1.42 | 1.42, 0.96, 1.99 | 5.2 ms | met (one run at 1.99) |
| QoS 1, 12 s window | 833 | 3.20 ms | 9.04 ms | 2.99 ms | 2.66 | 3.01, 1.83, 2.66 | 7.0 ms | not met |
| QoS 1, 6 s window | 1,667 | 3.20 ms | 15.38 ms | 3.31 ms | 4.85 | 4.40, 4.85, 4.95 | 19.1 ms | not met |
| QoS 0, 6 s window | 1,667 | 3.50 ms | 14.32 ms | 2.87 ms | 4.25 | 2.13, 5.53, 4.25 | 33.1 ms | not met |
| QoS 1, 6 s, storm limited to 2 cores | 1,667 | 3.23 ms | 8.93 ms | 3.27 ms | 2.72 | 2.72, 2.43, 3.01 | 15.1 ms | not met |
| QoS 1, no storm | none | 3.23 ms | 3.08 ms | 3.21 ms | 0.98 | 0.29, 0.98, 1.07 | none | |
| QoS 0, no storm | none | 2.37 ms | 2.82 ms | 3.02 ms | 1.11 | 1.11, 1.44, 1.07 | none | |

The "storm over baseline" column is the median of the three per-run ratios, not the ratio of the medians. The no-storm rows are the same runs with the phases cut at the same times and nothing dropped. Their first QoS 1 run had a bad baseline (10.7 ms), which is why one ratio is 0.29.

The cell passes the 2x rule at 400 reconnects a second, only on the median at that. At 833 it fails. At the spec's rate of 1,667 it fails by more than a factor of two for QoS 1, and QoS 0 fails too and swings between 2.1 and 5.5 from run to run. The absolute numbers are modest: 15 ms for QoS 1 is inside its 25 ms budget, and 14 ms for QoS 0 is over its 10 ms one. The rule is about the ratio though, and the ratio fails.

Is the miss the cell or the load generator sharing the machine? Part of each, going by two measurements. During the QoS 1 storm at 1,667 a second the three brokers used 1.8 cores (0.8 outside the storm), the NATS servers 2.8 (1.6 outside it) and the storm process 1.1, so the machine wasn't out of cores. When the storm process was limited to two cores (`GOMAXPROCS=2`, where it used 0.8 of a core), the storm p99 fell from 15.4 ms to 8.9 ms and the ratio from 4.85 to 2.72. How hard the load generator hits matters, and the cell still misses 2x with the gentler storm. A second machine for the load generator would show how much of the remaining 2.7x is the cell.

One run on its own isn't safe to quote. An earlier full pass on the commit before the heartbeat fix (`f25e789`, kept in [`results/2026-10-27-f25e789/`](results/2026-10-27-f25e789/)) gave the same picture at 1,667 a second, 5.9x and 6.5x for QoS 1 in two series, and 3.0x for QoS 0. Its 833 a second series had one run at 15x (a 48 ms storm p99, at a load average of 10.9) and two near 1.7x. Storm numbers move a lot between runs.

## Against the spec's success criteria

| Criterion | Result here |
|---|---|
| 1M concurrent connections on at most 10 nodes, external 3 or 5 server NATS | Not tested, it needs several machines. This run held 15,000 on three nodes (and on one), at about 107 KiB per connection |
| QoS 0 p99 under 10 ms, same cell | Met: 2.7 ms at 2,000 msg/s with 12,200 connections, 2.5 ms at 10,000 msg/s |
| QoS 1 p99 under 25 ms, same cell | Met: 3.0 ms at 2,000 msg/s, 8.7 ms at 10,000 msg/s |
| Reconnect storm of 100k in 60 s keeps connected clients under 2x baseline p99 | Not met at the spec's rate of 1,667 reconnects a second: 4.85x for QoS 1 and 4.25x for QoS 0 (10,000 clients over 6 s, a tenth of the spec's storm). Met at 400 a second (1.42x), not met at 833 (2.66x) |
| `kill -9` of a node or a NATS server, partitions | Not part of this harness. The [chaos suite](../../chaos/README.md) covers them |
| Paho interoperability suite | Not run |

## What the numbers leave out

Clients and cell share one machine, so the latencies include the load generator's own scheduling, and loopback has none of a real network's delay. The p99s will be higher with the clients elsewhere.

Sixteen device keys serve all the clients, and Keysmith caches a key for a minute, so authentication stayed warm. A storm of 10,000 distinct devices would make 10,000 first-time key lookups against the database. The storm numbers don't include that cost, and a real fleet pays it.

Every session was clean with no expiry. Persistent sessions write more to JetStream on connect and on every acknowledged message, and the spec assumes half of all sessions are persistent. That's untested here.

The storm ran 6 to 25 seconds. A storm of 60 seconds at the same rate would keep the brokers handling reconnects for longer, and we don't know whether the p99 holds or drifts.

The other directories in [`results/`](results/README.md) are earlier passes, kept for comparison. `f25e789` is a full second pass on the previous broker commit. The `6dfe2dd` ones are the first pass, which stamped each message when it was sent and ran its storms with 8,000 clients. Don't quote those.
