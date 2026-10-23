# Phase 0, Task 4: JetStream behaviour on the pinned version

Short version: CAS, per-message TTL with limit markers, and dedup all behave the way the spec assumes, with two error-handling details you have to build in. Latency is fine for 256 B messages up to 50k msgs/s. One assumption fails badly, and it comes from spike 2's `$` encoding: a node consumer with the two `TenantAll` filters reads about 1.5k msgs/s where a single-filter consumer reads 160k to 200k. The log subject layout has to change so a tenant's whole log sits under one filter.

To reproduce, from this directory:

```sh
GOWORK=off go test -count=3 -v ./...            # the checks; runs/tests.log
GOWORK=off go build -o /tmp/jsspike .
/tmp/jsspike -runs 3 -out runs/full > runs/full.log    # the measurements, about 30 min
```

Machine for every number below: Apple M3 Max, 16 cores, 128 GiB RAM (137438953472 bytes), macOS 27 (Darwin 27.0.0), Go 1.26.3 darwin/arm64. nats-server v2.15.1, nats.go v1.54.0 (the `jetstream` package). Each run starts a fresh 3-server cluster, every server in its own child process with full routes and its own temp store dir, so all three replicas share one SSD and one CPU. File storage, R3 everywhere.

Load: the shared Mac was busy, as usual. The 1-minute load average was 31 to 94 during the measurement runs and 81 to 233 while the tests ran (each window's load is in `runs/full.log` and the per-run JSON). The publish and CAS cells ran in interleaved 3 s windows, three per cell per run, each round starting at a different cell, so the comparisons inside a run share the same load. Tables are medians of 3 runs. Where a number moved with the load, the text says so.

## Summary

| # | Check | Result | Verdict |
|---|---|---|---|
| 1 | Publish-ack latency, 256 B | p50 0.3 to 0.6 ms, p99 8 to 38 ms at 1k to 50k msgs/s, sync and async alike | holds |
| 1 | Publish-ack latency, 64 KiB | fine at 1k/s; saturates near 6.5k/s sync and 9k/s async on this one-disk cluster, 50k/s is out of reach | holds for the design, see below |
| 2 | Session CAS at 1.7k/s and 5k/s | p99 29 and 36 ms sync, 14 and 14 ms async; zero conflicts, every op landed | holds |
| 3 | `cas_test.go`, two writers, same revision | exactly one winner in 26,000 rounds; loser gets 10071 or 10164 | holds, two error codes |
| 4 | `ttl_test.go`, `Nats-TTL` + expected sequence on the KV stream | accepted; watcher sees the marker 2.0 s after a 2 s TTL; stale sequence refused | holds, marker is a Purge op |
| 5 | `dedup_test.go`, `Nats-Msg-Id` | duplicate inside a 2 s window, stored again at +2.01 s; survives a leader stepdown | holds |
| 6 | Ordered consumer catch-up, 1M behind | 164k msgs/s with one filter `d.T.>`; 1.5k msgs/s with the two `TenantAll` filters | **fails as specified**, layout must change |

## 1. Publish-ack latency

An R3 file stream on `d.>`, 1 GiB cap, discard old. Traffic goes out on three connections, one per server, so a third of it hits the stream leader directly. Sync means 64 goroutines per connection, each waiting for its own ack (a blocking `OnPublish`). Async means `PublishMsgAsync` with at most 2,048 pending per connection, latency taken in the ack handler (the async-ack path spike 1 recommends). Latency runs from the publish call to the ack.

Median of 3 runs:

| Cell | Achieved/s | p50 ms | p90 ms | p99 ms |
|---|---|---|---|---|
| async 256 B 1k/s | 999 | 0.51 | 2.0 | 7.7 |
| sync 256 B 1k/s | 999 | 0.49 | 3.1 | 10.1 |
| async 256 B 10k/s | 9,998 | 0.34 | 1.9 | 10.9 |
| sync 256 B 10k/s | 9,998 | 0.38 | 2.7 | 12.1 |
| async 256 B 50k/s | 49,995 | 0.64 | 10.1 | 38.2 |
| sync 256 B 50k/s | 44,706 | 0.62 | 3.1 | 17.2 |
| async 64 KiB 1k/s | 999 | 0.84 | 5.1 | 47.5 |
| sync 64 KiB 1k/s | 999 | 0.71 | 2.0 | 16.0 |
| async 64 KiB 10k/s | 9,150 | 772 | 1,018 | 1,182 |
| sync 64 KiB 10k/s | 6,577 | 26.2 | 39.0 | 73.2 |
| async 64 KiB 50k/s | 21,059 | 760 | 987 | 1,330 |
| sync 64 KiB 50k/s | 6,433 | 26.8 | 43.1 | 73.8 |

Per run, the 256 B p99s tracked the machine's load: async 10k/s was 33.6, 10.9 and 3.9 ms at load 54, 45 and 37, and async 50k/s was 73.0, 38.2 and 19.9 ms. Read the p99 column as load-distorted upward; the p50 barely moves. The run 1 async 64 KiB 1k/s p99 (379 ms) is the same story.

What the numbers say:

- 256 B is comfortable. At the 10k/s a busy cell might see, half a millisecond median and around 11 ms p99 for the commit, on a cluster sharing one machine with everything else. The spec's QoS 1 target (p99 publish-to-deliver under 25 ms) has room for the commit, but not a lot on a machine this loaded.
- Sync and async look alike until saturation. Where they differ, the sync rows look better only because a fixed pool of 192 waiting goroutines can't offer the full load ("no free worker" counted 28k to 60k missed sends at 256 B 50k/s, and the sync 64 KiB rows never got past 7k/s). That's coordinated omission, so don't read the sync rows at saturation as faster.
- 64 KiB at 10k/s is 640 MB/s of payload, written three times to the same SSD. Both modes saturate: sync settles at about 6.5k/s (about 430 MB/s), async queues up in the client and its latency becomes the queue (p50 near 800 ms). At 50k/s the server starts refusing with `10167` (JSStreamTooManyRequests, a 429) and the client stalls. This is a property of three replicas on one disk; separate machines would do better, but nothing here measures that.

For the design: QoS 1/2 payloads are usually small. A 64 KiB device stream at thousands per second per cell is not a v1 load. The large-payload threshold (claim-check through trove at 512 KiB) can stay as it is.

## 2. Session-key CAS

One R3 bucket `SESS` with limit markers, 60,000 keys, 512 B values. Every write is the cluster layer's real session write: a raw publish to `$KV.SESS.<key>` with `Nats-TTL: 168h` and `Nats-Expected-Last-Subject-Sequence`. Keys are split by connection, like sessions owned by nodes, so any failure would be a bug. Sync waits for each ack (a CONNECT takeover). Async pipelines writes per connection and takes each key's next revision from its ack, never with two writes on one key in flight (the batched cursor writes).

Median of 3 runs:

| Cell | Achieved/s | p50 ms | p90 ms | p99 ms |
|---|---|---|---|---|
| sync 1.7k/s | 1,699 | 0.78 | 6.8 | 29.3 |
| async 1.7k/s | 1,699 | 0.64 | 5.1 | 14.0 |
| sync 5k/s | 4,980 | 1.10 | 8.7 | 36.4 |
| async 5k/s | 4,999 | 0.57 | 4.2 | 13.5 |
| sync 10k/s | 9,952 | 0.48 | 5.1 | 16.9 |
| async 10k/s | 9,998 | 0.37 | 1.8 | 11.1 |

Zero failed CAS writes in any run. Creating the 60,000 keys (expected sequence 0, pipelined) took 0.58 to 0.79 s.

Run 2 was slower in every cell (p50 2.5 to 7.9 ms, p99 up to 135 ms at async 10k/s) at load 56 to 68; runs 1 and 3 agree with each other. So run 2 is load-distorted, and the medians above are the slower of runs 1 and 3. Sync and async at the same rate are within noise of each other at 1.7k/s; at 5k/s sync's p99 is about 2.7x async's, partly because 192 sync workers occasionally all wait at once (6 to 285 missed sends per run).

The reconnect storm's 1.7k CAS/s is about a sixth of what one bucket took here without strain. Going higher than 10k/s wasn't measured.

## 3. CAS contention (`cas_test.go`)

Two goroutines on connections to different servers (n0 and n1) CAS the same key from the same revision, 1,000 rounds per run, through `kv.Update` and through the raw publish with the expected-sequence header. Across 13 test runs that's 26,000 rounds, and every round had exactly one winner. The stored revision always matched the last winner, and a write from the previous revision always failed afterwards.

The loser's error code isn't always the same, and that matters for the code that handles it:

- `10071` "wrong last sequence: N" when the winner was already applied. It tells you the current sequence N.
- `10164` "wrong last sequence" (`JSStreamWrongLastSequenceConstantErr`, no N) when the winner's proposal was still in flight on the stream leader. In the tests 73% to 99% of losers got this one. nats.go v1.54 maps both to `ErrKeyExists` in `kv.Create`, but has no exported constant for 10164.

Both mean "not stored". A loser that gets 10164 has to re-read the key to learn the winner's revision.

`TestCASPipelinedOnOneKey` shows the other side of 10164: two CAS writes on one key sent back to back, the second naming the sequence the first will get. In 2,400 pairs (12 test runs) the first always landed and the second was always refused with 10164. The stream leader refuses any expected-sequence write to a subject that has a proposal in flight (`expectedPerSubjectInProcess` and `inflight` in `jetstream_batching.go`), even when the expected value would have been right. So a node may have at most one outstanding CAS per session key. Batching across keys is fine, and that's what measurement 2's async cells did.

## 4. TTL and limit markers (`ttl_test.go`)

The bucket has `LimitMarkerTTL: 3s`, which turns on `AllowMsgTTL` and `SubjectDeleteMarkerTTL` on `KV_SESS`. The writer is on n0 and the watcher on n2.

- A raw publish to `$KV.SESS.<key>` with `Nats-TTL: 2s` and expected sequence 0 is accepted. The watcher sees the marker 2.00 to 2.06 s after the stream stored the put, every run. It arrives as `KeyValuePurgeOp`, not `KeyValueDeleteOp`: the marker carries `Nats-Marker-Reason: MaxAge`, `Nats-Rollup: sub` and its own `Nats-TTL: 3s`, and nats.go maps `MaxAge` to Purge. `kv.Get` then returns key not found.
- A publish with a stale expected sequence is refused with 10071, and so is a create (expected 0) over a live key.
- A CAS that carries a new `Nats-TTL` renews the expiry. A key written at t0 with 2 s and rewritten at +1.2 s with 2 s was still readable at +2.5 s, and its one marker came 2.0 to 2.2 s after the rewrite. The replaced message leaves no marker.
- While the marker is in the stream, it is the subject's last message. A create with expected 0 is refused ("wrong last sequence: 9", the marker's sequence), and a CAS naming the marker's sequence succeeds. After `LimitMarkerTTL` the marker expires silently (the watcher sees nothing) and expected 0 works again, 3.03 to 3.10 s after the marker appeared.
- `Nats-TTL` on a bucket without limit markers is refused with 10166 ("per-message TTL is disabled").
- The server parses `Nats-TTL` in whole seconds and refuses anything under one second (`parseMessageTTL`). Session expiry and MQTT 5 message expiry are already in seconds, so this costs nothing.

One loose end. `kv.Create` over an expired key reads the key first (a direct get, which any replica may answer) and writes over the marker. Once, early on and at load 233, it failed with "wrong last sequence: 13: key exists" right after the watcher had seen the marker. `TestCreateAfterExpiry` then tried it on 20 freshly expired keys per run, about 260 keys in all, and never saw it again. The likely cause is a replica that hadn't applied the marker yet answering the direct get, but the spike didn't confirm it. The cluster layer shouldn't need `kv.Create` here anyway: on 10071 the error already names the current sequence, and the watcher hands you the marker's.

## 5. Dedup (`dedup_test.go`)

Window used: 2 s, so the test can wait it out. A resend with the same `Nats-Msg-Id` was reported as a duplicate with the original sequence, a different ID inside the window was stored, and the same ID was last a duplicate at +1.90 to +1.94 s and stored again at +2.00 to +2.06 s, in every one of 13 runs. A duplicate doesn't extend the window. Production keeps the default 2 minutes (or more), which has to exceed the longest takeover as the spec says.

`TestDedupSurvivesLeaderChange` publishes an ID on a stream with a 2 minute window, asks the stream leader to step down, waits for a new leader and resends. It was a duplicate every time, over 13 stepdowns. So a QoS 2 resend that lands after a NATS server failover still deduplicates.

## 6. Catch-up from 1M messages behind

One R3 file stream holding 10 tenants x 1M messages of 128 B, interleaved round robin, 1,000 device topics per tenant, and 1 in 100 of each tenant's messages under the `==<tenant>` token the way spike 2 encodes `$` topics. Filling it took 55 to 83 s (120k to 181k msgs/s async from three connections); it's 1,791 MiB per replica. Each variant reads from the start of the stream through a new consumer, and the variants rotate order per run.

| Variant | Messages | Run 1 | Run 2 | Run 3 | Median msgs/s |
|---|---|---|---|---|---|
| one tenant, 1 filter `d.T.>`, ordered | 990,000 | 163,866 | 199,617 | 39,799 | 163,866 |
| same, `PullMaxMessages(10000)` | 990,000 | 194,547 | 192,293 | 43,239 | 192,293 |
| unfiltered, first 1M | 1,000,000 | 438,428 | 558,358 | 53,061 | 438,428 |
| `TenantAll` 2 filters, ordered, first 20k | 20,000 | 1,658 | 1,522 | 1,499 | 1,522 |
| `TenantAll` 2 filters, plain pull, first 20k | 20,000 | 1,707 | 1,353 | 1,502 | 1,502 |
| 10 tenants in parallel, 1 filter each (sum) | 9,900,000 | 432,286 | 463,625 | 280,184 | 432,286 |

Run 3's first three rows ran right after the 10-tenant read, at load 62 to 94, and are load-distorted; runs 1 and 2 agree. The two-filter rows were capped at 20k messages because a full 1M would take about 11 minutes each.

So with one filter, a node replays one tenant's backlog at about 160k to 200k msgs/s per consumer, and ten tenants at once at about 430k msgs/s in total. That's the replay-batching estimate the spec wanted: a batched replay of 1M messages takes 5 to 6 s, and a node that lost a peer can replay ten tenants' worth of 1M each in about 23 s. A tenant at 100 msgs/s with 7 days of retention is 60M messages, about 5 minutes for a consumer starting at the oldest cursor.

### Why two filters are slow

Any consumer with more than one filter subject takes `LoadNextMsgMulti` in the file store, and its per-block search (`firstMatchingMulti`) walks every subject in the block that matches the filters, for every message it returns. Its cost grows with the number of distinct subjects, not with the number of matching messages. Ordered or plain makes no difference (rows 4 and 5). Two dense filters (two tenants) are just as slow as `T.>` plus a sparse `==T.>`.

The scaling check pins it: a stream of 10 tenants x 20k messages, the first 5,000 messages read with one filter (one tenant) or two (two tenants), medians of 3 runs.

| Distinct subjects per tenant | 1 filter, msgs/s | 2 filters, msgs/s |
|---|---|---|
| 10 | 227,220 | 67,322 |
| 100 | 147,326 | 9,871 |
| 1,000 | 118,060 | 1,259 |

With two filters, ten times the subjects gives about a tenth of the speed, while one filter barely notices. A Tether tenant has a topic per device, so tens of thousands to millions of subjects, and a two-filter node consumer would crawl.

### The design change

The `$` rule from spike 2 (a `$` topic gets the tenant token `==<tenant>`) exists so that NATS matching of MQTT wildcards stays correct. The QoS 1/2 log doesn't need it, because a node reads the whole tenant log and matches it locally with mochi's topic index. So:

- Log subjects keep every message of a tenant under `d.<tenant>.`. A `$` topic's first level is encoded like any other non-passthrough level (`$SYS` becomes `=JFNZUw`), with no `==` tenant token. The node consumer has one filter, `d.<tenant>.>`.
- The `==<tenant>` space stays where NATS does the MQTT matching: QoS 0 subjects (`f.`) and retained KV keys read by filter.
- Spike 2's `TenantAll` is not for consumers. Never give a JetStream consumer more than one filter subject on a stream with many subjects. That also rules out the spec's phase 3 idea of narrowing node consumers to non-overlapping filter sets, unless the server's multi-filter path improves.

Both encodings come out of the same `EncodeLevel`, so this is a different function for the `d.` prefix (no `$` swap), not a new scheme.

## Other things the code has to do

- Handle both 10071 and 10164 as "CAS lost". Take the current sequence from a 10071's text when you can; re-read on 10164.
- Keep at most one CAS in flight per session key per node. The cursor writer can batch across keys but must wait for a key's ack before its next write. A non-CAS write in flight on the subject blocks a CAS too.
- Treat a watcher's `KeyValuePurgeOp` on a session key as expiry (marker reason `MaxAge`). The KV watcher API doesn't expose headers, so if you ever need to tell an expiry apart from an explicit purge, read the KV stream with a raw consumer.
- A write after expiry names the marker's sequence while the marker lives (`LimitMarkerTTL`), and 0 after that. Pick `LimitMarkerTTL` long enough for a watcher that was briefly disconnected to still see the marker on reconnect.
- Expect `10167` (too many requests) under overload and back off.

## Concerns

- Every number here comes from three servers on one machine, one disk and one busy CPU. Separate servers would add network round trips and remove the shared-disk ceiling. The 256 B commit latency is likely close to what you'd see on a LAN; the 64 KiB saturation point is not.
- The p99 columns move with the machine's load by 2x to 8x between runs. They're good for comparing cells inside a run, not as absolute targets. Phase 2's load tests need real hardware.
- The single `kv.Create` failure after expiry wasn't explained.
- Measurement 2 stopped at 10k CAS/s. The ceiling of one bucket is unknown.

Decision: Keep JetStream on nats-server 2.15.1 for the log, sessions and replay. Session CAS (one winner, 1.7k and 5k/s with p99 under 40 ms on this machine), raw `Nats-TTL` plus expected-sequence writes with limit markers (the marker reaches watchers on time), QoS 2 dedup (including across a leader change) and 256 B publish latency all hold as the spec assumes. Two handling rules are new: treat both 10071 and 10164 as a lost CAS, and allow one outstanding CAS per key. One assumption changes: the QoS 1/2 node consumer must use a single filter `d.<tenant>.>`, so log subjects drop spike 2's `==<tenant>` token and keep `$` topics under the ordinary tenant token. A two-filter consumer reads about 1.5k msgs/s against 160k to 200k with one filter, and with one filter a batched replay runs at about 160k msgs/s per consumer.
