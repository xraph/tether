# Phase 0, Task 3: NATS interest at scale

Machine: Apple M3 Max, 16 cores, 128 GiB RAM, macOS 27 (Darwin 27.0.0), Go 1.26.3 (darwin/arm64). nats-server v2.15.1, nats.go v1.54.0, mochi v2.7.9. Route pooling left at its default (pool size 3).

Load: this is the shared Mac, and it was busy. The 1-minute load average ran from 20 to 158 across the runs (16 cores). Every run's load at its start and end is in the tables below and in `runs/*/*.json`. Where a number moved with the load, the text says so.

Setup, for every run: 3 nats-servers in a full mesh on localhost, each in its own child process, so the RSS and CPU figures are per server and don't include the harness. We didn't need Docker or in-process estimates. 10 broker connections, spread 4/3/3 over the servers, each subscribing 100k distinct subjects `f.ttnt_x.devices.<n>.cmd` built with spike 2's encoder. That's the full 1M filters; nothing here is extrapolated from a smaller run. Traffic is 5k msgs/s round robin over 1,000 subjects the storm never touches, always published on a server other than the subscriber's, so every message crosses a route. Latency is publish to receive in one process (monotonic clock), into HDR histograms. Every received message is decoded and looked up in a mochi `TopicsIndex` holding that connection's 100k filters, in both modes, so the CPU comparison doesn't flatter the interest side.

Raw output: `runs/full-1m.log` (the plan's measurements as written), `runs/interleaved-1m.log` (the storm, measured so load can't bias it), `runs/fast-storm-1m.log` (the same storm 5x faster), `runs/cpu-1m.log` (paired CPU). The `full-1m` runs used the harness before `-window` and `-mode cpu` existed; the continuous storm path didn't change, only moved into `continuousStorm`.

## Summary

| Measurement | Result (median of 3) | Threshold | Verdict |
|---|---|---|---|
| Interest propagation, 1M filters | 3.54 s (full-1m), 3.62 s (interleaved-1m) | under 5 s | pass |
| Server memory at 1M filters | 1,120 to 1,140 bytes of heap per filter on every server; 1.24 to 1.49 GiB RSS per server | none | pass, but see concerns |
| Storm p99 vs baseline, plan's before/during comparison | 2.45x (7.07, 2.45, 0.49) | under 2x | inconclusive, load-distorted |
| Storm p99, interleaved on vs off windows | 0.96x (0.96, 0.71, 1.70) | under 2x | pass |
| Storm p99, 5x faster storm, interleaved | 0.71x (0.71, 0.61, 0.74) | under 2x | pass |
| Overlap `a/+` and `a/#`, 10k publishes to `a/b` | 20,000 deliveries, 10,000 after dedup | exactly one | pass |
| Fallback CPU per message, paired windows | 1.93x interest (1.94, 1.93, 1.59) | none | fallback costs more |

## 1. Interest propagation and memory

Propagation here is the time from the first SUB to when a publish from each other server first reaches each connection's last subscription. Probes go out every 5 ms from both remote servers, starting at the first SUB. After that, 1,000 random subjects per connection get checked (10,000 publishes) to confirm that the last subscription arriving means everything arrived.

| Run | Load start / end | Last sub reachable | Local flush done | Lag after local flush | Sample check |
|---|---|---|---|---|---|
| full-1m 1 | 20.4 / 130.3 | 3.33 s | 3.33 s | 0.026 s | 10,000 / 10,000 |
| full-1m 2 | 147.6 / 152.8 | 6.81 s | 6.74 s | 0.269 s | 10,000 / 10,000 |
| full-1m 3 | 138.3 / 68.4 | 3.54 s | 3.54 s | 0.047 s | 10,000 / 10,000 |
| interleaved-1m 1 | 23.8 / 34.2 | 3.62 s | 3.61 s | 0.141 s | 10,000 / 10,000 |
| interleaved-1m 2 | 34.2 / 46.3 | 3.65 s | 3.64 s | 0.108 s | 10,000 / 10,000 |
| interleaved-1m 3 | 46.3 / 31.8 | 3.51 s | 3.51 s | 0.094 s | 10,000 / 10,000 |
| fast-storm-1m 1 | 58.6 / 68.5 | 4.00 s | | | 10,000 / 10,000 |
| fast-storm-1m 2 | 68.9 / 78.0 | 5.36 s | | | 10,000 / 10,000 |
| fast-storm-1m 3 | 78.0 / 83.5 | 4.26 s | | | 10,000 / 10,000 |

Median over all nine runs: 3.65 s. The two runs over 5 s started at a load of 69 and 148. Nearly all of the time is the subscribing itself: 10 connections pushing 100k SUBs each and the local server filing them, about 280k subscriptions per second across the cell. Route propagation adds 26 to 269 ms after a connection's own server has acknowledged its last SUB. So a single new subscription in steady state becomes reachable cluster-wide in tens of milliseconds, and 3.5 s is the cold-start cost of a whole cell (every device subscribing at once).

Memory, median of the three full-1m runs, after `runtime.GC` and `FreeOSMemory` in each server:

| Server | Idle RSS | RSS at 1M | Heap in use at 1M | Heap bytes per filter |
|---|---|---|---|---|
| n0 (4 connections) | 19.6 MiB | 1,471 MiB | 1,087 MiB | 1,137 |
| n1 (3 connections) | 19.4 MiB | 1,336 MiB | 1,079 MiB | 1,129 |
| n2 (3 connections) | 19.5 MiB | 1,240 MiB | 1,074 MiB | 1,123 |

Every server holds all 1M subscriptions (`num_subs` is 1,000,167 on each, the 167 being the server's own). A subscription a server learned over a route costs about the same as one from a local client, because the per-filter heap is within 1.5% across servers that hold 40% and 30% of the client connections. The spread in RSS is runtime overhead (fragmentation, unscavenged spans), not subscriptions. Adding servers to a cell doesn't divide this; every server pays for every filter.

## 2. Storm against unaffected traffic

The storm unsubscribes 100k devices (10k per connection, evenly over 60 s) and resubscribes each one 1 s later: 200,000 interest changes, about 3.3k per second across the cell. All 200,000 ran in every run, no slow-consumer events, no lost traffic (`received` equals `sent`), and after each storm 10,000 sampled churned subjects were all reachable again.

### As the plan states it: 30 s baseline, then 60 s storm

| Run | Load (baseline start, storm end) | Baseline p50 / p99 | Storm p50 / p99 | p99 ratio | Server cores, baseline / storm |
|---|---|---|---|---|---|
| 1 | 22.5, 97.1 | 77 / 2,701 µs | 177 / 19,087 µs | 7.07 | 0.56 / 0.51 |
| 2 | 150.3, 157.5 | 846 / 16,767 µs | 783 / 41,119 µs | 2.45 | 0.29 / 0.38 |
| 3 | 138.2, 68.4 | 173 / 17,567 µs | 154 / 8,679 µs | 0.49 | 0.45 / 0.56 |

This can't decide anything. In run 1 the machine's load went from 19 to 97 during the storm while our three servers used no more CPU than in the baseline (0.51 cores against 0.56). In run 3 the load fell from 106 to 68 and the "storm made things better" by 2x. The ratio tracks the machine's load, so we read all three as load-distorted.

### Interleaved: the same storm, in alternating 10 s windows

`-window 10s` runs the traffic for 120 s in alternating off and on windows (off first), with the storm active only in on windows: the same 100k devices over 60 s of on-time, the same 1 s gap. The first second of each window isn't reported, so backlog from one window doesn't land in the next. Outside load now hits both arms about equally.

| Run | Load start / end | Off p50 / p99 | On p50 / p99 | On/off p99 | On/off p50 |
|---|---|---|---|---|---|
| 1 | 23.4 / 33.4 | 93 / 3,949 µs | 98 / 3,789 µs | 0.96 | 1.05 |
| 2 | 38.7 / 43.8 | 159 / 13,119 µs | 160 / 9,335 µs | 0.71 | 1.01 |
| 3 | 44.1 / 32.8 | 104 / 5,503 µs | 113 / 9,375 µs | 1.70 | 1.09 |

Medians: p99 ratio 0.96, p50 ratio 1.05. The storm doesn't move unaffected traffic beyond the noise of this machine.

### The same storm 5x faster

`-storm 12s -window 4s`: the 100k devices churn over 12 s of on-time, about 16.7k interest changes per second.

| Run | Load start / end | Off p50 / p99 | On p50 / p99 | On/off p99 |
|---|---|---|---|---|
| 1 | 60.2 / 67.4 | 899 / 12,719 µs | 860 / 9,079 µs | 0.71 |
| 2 | 76.4 / 81.2 | 277 / 12,599 µs | 257 / 7,655 µs | 0.61 |
| 3 | 95.7 / 86.1 | 601 / 14,063 µs | 522 / 10,431 µs | 0.74 |

Median 0.71. On windows come out slightly *better* in both storm shapes. The likeliest reason is the scheduler: with the storm goroutines busy, the cores stay awake and wakeups are faster. That's a property of this Mac, not of NATS, so don't read anything into a ratio under 1 beyond "no harm found".

The absolute p99s (4 to 14 ms) are the loaded machine plus the harness. The harness holds 1M client subscriptions and 1M mochi filters (about 2.8 GiB), its GC runs in both arms, and the traffic generator paces in 200 µs sleeps, so it sends small bursts. None of this says anything about the spec's 10 ms QoS 0 target; Phase 2's load tests on proper hardware answer that.

## 3. Overlapping filters on one node

One connection on n0 subscribes the subjects for MQTT `a/+` and `a/#`: `f.ttnt_x.a.*`, `f.ttnt_x.a` and `f.ttnt_x.a.>`. A connection on n1 publishes 10,000 messages to each of three topics, each with a `Tm-Id` header holding a node-local ID. The receiver runs the `streaming/dedup.go` shape (16 shards, 2 s TTL, 65,536 entries).

| Topic | Published | Raw deliveries | After dedup |
|---|---|---|---|
| `a/b` (matches `a.*` and `a.>`) | 10,000 | 20,000 | 10,000 |
| `a` (matches only `a`, the parent from `a/#`) | 10,000 | 10,000 | 10,000 |
| `a/b/c` (matches only `a.>`) | 10,000 | 10,000 | 10,000 |

Identical in all six runs that used 10,000 messages. NATS delivers once per matching subscription on a connection, exactly as the spec expects, and the dedup window removes every duplicate and nothing else. The dedup check cost a median of about 275 ns per delivery (149 to 1,236 ns across runs), timed with `time.Now` around each call, so that's an upper bound.

## 4. Fallback cost

Fallback: each connection holds `f.ttnt_x.>` and `f.==ttnt_x.>` (both `TenantAll` subjects, because a tenant's `$` topics live under `==<tenant>`) and matches every message locally. Every message reaches all 10 connections; 1 matches, 9 don't. Server memory stays at idle (21 MiB per server), and propagation is instant.

### Paired windows (the numbers to use)

`-mode cpu` keeps an interest cluster (1M subscriptions) and a fallback cluster up at the same time and alternates 10 s traffic windows between them (I F, F I, I F, F I), so both see the same load. CPU is user plus system time of the three server processes and the harness, per published message, minus each cluster's idle rate (about 0.001 cores, which is negligible).

| Run | Load start / end | Interest µs/msg (server + harness) | Fallback µs/msg (server + harness) | Fallback / interest |
|---|---|---|---|---|
| 1 | 31.8 / 72.6 | 59.1 + 38.1 = 97.0 | 104.5 + 84.1 = 188.4 | 1.94 |
| 2 | 73.8 / 61.4 | 109.7 + 74.9 = 183.6 | 183.2 + 170.5 = 353.6 | 1.93 |
| 3 | 61.4 / 60.3 | 94.9 + 63.7 = 158.4 | 133.8 + 117.8 = 251.4 | 1.59 |

Median: interest 158 µs, fallback 251 µs per message, so the fallback costs 1.93x. The absolute numbers are high because at 5k msgs/s nothing batches (a write syscall per message per hop) and because they swell with machine load; use the ratio and treat the microseconds as rough. The difference works out at about 10 µs per extra delivery (93 µs over 9 wasted deliveries). The fallback's cost grows with the number of nodes serving a tenant, which is every node for a big tenant. At 10 nodes it's 10 deliveries per message; at 30 it would be 30.

The unpaired fallback runs in `full-1m` (median 251 µs against an interest baseline median of 152 µs, ratio about 1.6) agree, but their loads differ by up to 130 between the arms, so treat them as load-distorted.

## Concerns for Phase 2

- Memory is what limits interest routing here. About 1.1 KiB of heap per distinct filter, paid by every server in the cell, so 1M per-device filters is about 1.1 GiB of heap and 1.3 to 1.5 GiB RSS on each NATS server. 10M would be 11+ GiB per server. Adding servers doesn't help. Size NATS memory for the cell's distinct QoS 0 filter count, and keep collapsing filters per node (one NATS subscription per distinct tenant and filter, reference-counted, as the spec says).
- A cold start of a full cell takes seconds. Every device subscribing at once took 3.3 to 6.8 s depending on load. QoS 0 publishes in that window reach only the nodes whose interest has landed. That's allowed for QoS 0, but say it in the docs.
- We measured storms of 3.3k and 16.7k interest changes per second. Neither moved unaffected traffic. We didn't find the point where it does. The per-tenant CONNECT throttle in the spec still matters, mainly for the session bucket.
- Routes ran over loopback here. A real network adds about one RTT to the lag after a local flush, which is small next to the subscribing time.
- Keep the dedup window. Duplicates are exact and predictable (one per extra overlapping filter on the node), so the dedup window works. A node could also skip subscribing a filter already covered by another of its filters, but that needs the covering logic to be right for `$` topics and the `#` parent subject, so dedup is the simpler path.
- To tighten these numbers, run the same harness on a quiet machine (or three Linux hosts) and at a higher message rate, so per-message CPU amortises and the latency percentiles reflect NATS instead of this Mac's scheduler. The decision below doesn't depend on that.

Decision: Interest routing. Propagation of 1M filters finished in a median 3.6 s (under 5 s), and the storm left unaffected traffic's p99 at 0.96x when measured with interleaved windows (0.71x at 5x the storm rate), under the 2x threshold. The plan's literal before-and-during comparison came out at a median 2.45x, but that is load-distorted on this machine (the load moved from 19 to 97 during one storm with no extra server CPU) and isn't evidence against interest routing. Keep the per-tenant fallback documented as the escape hatch: it costs about 1.9x CPU per message at 10 nodes and grows linearly with node count. Plan NATS server memory at about 1.1 KiB per distinct QoS 0 filter on every server in the cell.
