# First cell numbers

This is the first run of [`tether-bench`](README.md) against a Tether cell, on one Mac, at reduced scale. It covers three of the spec's success criteria with real numbers: connections and memory per node, same-cell publish-to-deliver latency, and a reconnect storm's effect on clients that stay connected. It can't say anything about the fourth, a million connections on ten nodes. That needs several machines, and a single Mac ran out of ports at 15,000 clients long before it ran out of anything else.

Every number below is the median of three runs, each against a fresh cell. The three raw values sit next to it when they differ enough to matter. The JSON for every run is in [`results/2026-10-27/`](results/2026-10-27/).

## Where these came from

| | |
|---|---|
| Tether commit | `6dfe2dd` (the harness only adds `cmd/tether-bench`, so the broker is that commit's) |
| Machine | Apple M3 Max, 16 cores, 128 GiB RAM, macOS 27.0.1, arm64 |
| Go | 1.26.9 |
| Cell | 3 `tether` nodes and 3 `nats-server` processes (one JetStream cluster, 3 replicas, nats-server v2.15.1), all on loopback, default heartbeat and lease (2 s, 24 s) |
| Date | 2026-10-27, between 03:35 and 05:10 local time |
| Load generator | `tether-bench` on the same machine, paho.golang v0.23.0, MQTT 5, clean start, 16 shared device credentials |
| Load average | 5 to 13 on the 1-minute figure, depending on the run, from other work on the machine. Each run's value is in its JSON, under `machine.load_average` |

The machine was never quiet. Other work ran on it the whole time and the 1-minute load average never dropped below 5, so treat the latencies as what you get on a busy laptop, not what the hardware can do on its own.

Not one of the 39 runs had a node fence itself, lose a connection while it was held, or lose a message. Every QoS 1 message that got a PUBACK was delivered, and nothing was delivered twice.

## Connections and memory

15,000 clients opened at 500 a second, held for 20 seconds. First spread over the three nodes, then all on one.

| | 3 nodes, 5,000 each | 1 node, 15,000 |
|---|---|---|
| Connected | 15,000 of 15,000 | 15,000 of 15,000 |
| Connect latency p50 / p99 | 0.87 ms / 3.39 ms | 0.83 ms / 3.93 ms |
| Memory per node, idle (RSS) | 53 to 60 MiB | 53 to 60 MiB |
| Memory per node, loaded (RSS) | 624 MiB (591, 624, 664 for the biggest node) | 1,694 MiB (1,526, 1,694, 1,784) |
| Memory per connection | 108.6 KiB (103.5 to 112.9) | 112.6 KiB (101.1 to 118.7) |

That's about 110 KiB of resident memory for each connection, session state included, whichever way the clients are spread. If you stretch it in a straight line to the spec's target of 100,000 connections on each of ten nodes, you get roughly 10.5 GiB per node. Nobody has measured that, and a straight line through 15,000 is a guess for 100,000. It does tell you what size of machine to try first.

A node never got near its limit here. The ceiling was the client side: macOS gives one loopback address about 16,000 ephemeral ports, shared by every connection the bench opens. An earlier exploratory run asked for 45,000 clients. At 16,163 it got "can't assign requested address" for everything after, and then the NATS servers couldn't open routes to each other either. The JetStream cluster lost quorum, and all three brokers fenced themselves after 12 seconds without an acknowledged heartbeat and dropped every client. They took connections again 41 seconds after that. That's the fence working as designed, and the harness caused it, so it isn't in the table. It's the reason the README says to stay under 15,000.

## Publish-to-deliver latency

100 publisher and subscriber pairs (200 measured clients), 2,000 messages a second between them, 64 byte payloads, 30 seconds after a 5 second warmup, with 12,000 idle connections held on top (12,200 in all, about 4,070 per node). "Cross" means the publisher and the subscriber of a pair sit on different nodes, so the message goes through NATS. "Same" means one node.

| | p50 | p99 | p99.9 | p99 in the three runs |
|---|---|---|---|---|
| QoS 0, cross | 0.18 ms | 2.54 ms | 4.35 ms | 2.54, 2.44, 2.58 |
| QoS 0, same node | 0.12 ms | 2.91 ms | 5.47 ms | 2.91, 3.04, 2.76 |
| QoS 1, cross | 0.37 ms | 2.69 ms | 5.69 ms | 2.81, 2.53, 2.69 |
| QoS 1, same node | 0.37 ms | 2.79 ms | 5.31 ms | 2.79, 2.76, 2.81 |

At QoS 1 the time from sending a PUBLISH to getting its PUBACK, which includes the JetStream commit, was 0.34 ms at p50 and 2.54 ms at p99 (cross). The PUBACK comes back about as fast as the message is delivered.

The spec asks for p99 under 10 ms at QoS 0 and under 25 ms at QoS 1, "at nominal load", and doesn't say what nominal is. At 2,000 messages a second and 12,200 connections the cell is a long way inside both, at about a quarter and a tenth of the budget. Pushing the offered rate to 10,000 a second (and 10,000 deliveries a second, with no message lost) moved QoS 0 hardly at all and QoS 1 a good deal:

| 10,000 msg/s, cross | p50 | p99 | p99.9 | p99 in the three runs |
|---|---|---|---|---|
| QoS 0 | 0.15 ms | 2.29 ms | 3.47 ms | 2.29, 2.34, 2.23 |
| QoS 1 | 1.70 ms | 6.68 ms | 14.33 ms | 8.41, 6.68, 6.40 |

QoS 1 at 10,000 a second is still well inside 25 ms at p99 and p99.9, and its slowest message took 21 to 26 ms. Those runs also had some of the highest load averages of the day (6 to 13), so it may be partly the machine.

One thing to know when you read these. The p99 depends on how many connections the cell holds. With about 3,200 connections in a trial run it was under 1 ms for QoS 0, and at 12,200 it's around 2.5 ms. We haven't looked at why (keep-alive timers and garbage collection are the first suspects), and we haven't measured it with the load generator on a separate machine.

## Reconnect storm

The spec wants a storm of 100,000 clients in 60 seconds to leave already-connected clients' p99 at no more than twice its baseline. That's 1,667 reconnects a second, and this machine can't hold 100,000 clients. So the storm here is 8,000 clients, dropped all at once with a TCP reset (no DISCONNECT, like a node crash) and each coming back at a random moment inside a window. We ran three window lengths, which gives three rates, and the middle one is the spec's rate on a group 8% of its size for 5 seconds instead of 60.

The cell held 13,200 connections: 5,000 idle, the 8,000 in the storm, and 200 measured clients at 2,000 messages a second (cross, as above). The storm ran in its own process, so its cost stays out of the process that times the messages. The p99 is of messages sent in each phase: baseline is the 15 seconds before the drop, storm runs from the drop to two seconds after the last reconnect, and after is the rest.

| | Reconnects/s | Baseline p99 | Storm p99 | After p99 | Storm over baseline | In the three runs |
|---|---|---|---|---|---|---|
| QoS 1, 20 s window | 400 | 3.11 ms | 3.56 ms | 2.58 ms | 1.13 | 1.13, 1.00, 1.57 |
| QoS 1, 10 s window | 800 | 3.06 ms | 5.33 ms | 3.18 ms | 1.75 | 2.00, 1.52, 1.75 |
| QoS 1, 5 s window | 1,600 | 3.05 ms | 13.02 ms | 3.52 ms | 4.35 | 4.58, 4.35, 3.17 |
| QoS 0, 5 s window | 1,600 | 3.20 ms | 6.07 ms | 2.99 ms | 1.90 | 4.50, 1.90, 1.18 |

All 8,000 clients came back every time, in every run, with none failing. The same cell with no storm at all (three more runs, phases cut at the same times) held a ratio of 1.1, so the baseline isn't drifting.

Read it this way. At up to about 800 reconnects a second the cell keeps inside the 2x rule, though one of the three runs at 800 touched it. At 1,600 a second, which is the spec's rate, QoS 1 misses it by a factor of two, and QoS 0 passes on the median and swings from 1.2 to 4.5 between runs. The absolute numbers stay modest: 13 ms at QoS 1 is inside the 25 ms budget, and 6 ms at QoS 0 is inside the 10 ms one. The 2x rule is about the ratio though, and the ratio fails at that rate.

The p99.9 in the storm phase sat between 19 and 64 ms in every run at every rate, including the gentlest, and mostly above 40. That looks like the moment of the drop itself, with 8,000 sessions ending at once, and not the reconnects. We haven't confirmed it.

We don't know why QoS 1 degrades more than QoS 0, or how much of the 1,600 a second result is the cell and how much is the storm process taking CPU from the brokers on a machine with a load average of 6 to 13. That's the next thing to find out, and a second machine for the load generator would answer half of it.

## Against the spec's success criteria

| Criterion | Result here |
|---|---|
| 1M concurrent connections on at most 10 nodes, external 3 or 5 server NATS | Not tested. It needs several machines. This run held 15,000 on three nodes (and on one) and costs about 110 KiB per connection |
| QoS 0 p99 under 10 ms, same cell | 2.5 ms at 2,000 msg/s with 12,200 connections. 2.3 ms at 10,000 msg/s |
| QoS 1 p99 under 25 ms, same cell | 2.7 ms at 2,000 msg/s. 6.7 ms at 10,000 msg/s |
| Reconnect storm of 100k in 60 s keeps connected clients under 2x baseline p99 | Scaled down to 8,000 clients. 1.13x at 400 reconnects a second, 1.75x at 800, 4.35x at 1,600 for QoS 1 (1.90x for QoS 0). The spec's rate is 1,667 a second |
| `kill -9` of a node or a NATS server, partitions | Not part of this harness. The [chaos suite](../../chaos/README.md) covers them |
| Paho interoperability suite | Not run |

## What the numbers leave out

Clients and cell share one machine, so the latencies include the load generator's own scheduling, and loopback has none of the delay of a real network. The p99s will be higher with the clients elsewhere.

Sixteen device keys serve all the clients, and Keysmith caches a key for a minute, so authentication stayed warm. A storm of 8,000 distinct devices would make 8,000 first-time key lookups against the database. The storm numbers above don't include that cost, and a real fleet pays it.

Every session was clean with no expiry. Persistent sessions write more to JetStream on connect and on every acknowledged message, and the spec assumes half of all sessions are persistent. That's untested here.

The storm ran 5 to 20 seconds. A storm of 60 seconds at the same rate would have the brokers handling reconnects for longer, and we don't know whether the p99 holds or drifts.

An earlier pass at the storm runs ended the storm process right after the storm, which reset its 8,000 connections inside the storm phase, and showed ratios of about 7 at 1,600 a second and 5 at 800. We threw those runs out and added `--until` so the clients are held until the measured window is over. The `--until` runs are the ones in this file.
