# Chaos suite

These tests run a real Tether cell on one machine and break it on purpose. You get three `nats-server` processes, three `tether` broker processes and a fault proxy in front of every NATS connection, and the tests kill, cut and slow those pieces while MQTT 5 clients publish and subscribe. They check the guarantees in the spec's "Success criteria", "Ownership, takeover and liveness" and "Partitions" sections.

## Run it

```sh
make chaos
```

That's `GOWORK=off go test -tags chaos -count=1 -v -timeout 30m ./chaos/`. One scenario on its own:

```sh
GOWORK=off go test -tags chaos -count=1 -v -run TestKill9BrokerDuringQoS1 ./chaos/
```

A full run takes four to seven minutes, mostly waiting for leases to run out and for the slow subscriber to drain. You need nothing but Go: the test builds `cmd/tether` itself, and the NATS servers are the test binary re-run as a child, so there's no Docker and nothing to download. Everything listens on `127.0.0.1` with ports picked at random.

Set `TETHER_CHAOS_KEEP=1` and each cell keeps its directory after the run (the path is in the test output). Inside you'll find every process's log (`s1.log` to `s3.log`, `n1.log` to `n3.log`), the broker configs, the shared SQLite file, and, after a failure, each live NATS server's `/jsz` and `/connz` as JSON. A failed test also prints the warnings and errors from every broker and the tail of every server log.

Without the `chaos` tag the package doesn't build at all, so `go test ./...` never runs it. CI runs it every night (`.github/workflows/chaos.yml`, also startable by hand) and uploads the cell logs when it fails.

## How the cell is built

The harness lives in `harness.go`.

- NATS: three servers named `s1`, `s2` and `s3`, JetStream on, one cluster. Every route goes through its own proxy, one per direction per pair, so `s1` reaching `s2` and `s2` reaching `s1` are separate links you can cut. Each server advertises a cluster address nothing listens on, which stops the servers from gossiping their way around the proxies. Pings run every second with three allowed outstanding, so a dead route shows up in seconds.
- Brokers: three nodes, `n1` to `n3`, in external mode. Each node gets three proxies, one to each server, and lists them with its own server first (`n1` sits on `s1`, and so on). `no_discovery: true` keeps a node on the URLs it was given (rulings R16 and R17), so a black hole really cuts it off. Cutting a node means putting its proxies in `Blackhole`, never `Reset`.
- Start order: the brokers share one SQLite file, so they start one at a time. Each one has to answer `/_/health` and accept a TCP connection on its MQTT port before the next one starts. The cell is ready once the observer has seen a heartbeat from all three.
- Devices: a tenant, a role that may publish and subscribe under `chaos/#`, and one pinned device per client, all made through the admin REST API with a bearer token the harness makes up.
- Cleanup: a broker runs under a small supervisor (the test binary again), and NATS servers watch their stdin. When the test binary goes away for any reason, its stdin pipes close and every child goes with it.

The observer is a plain NATS client that goes straight to the servers, past every proxy. It follows the sessions and members buckets with KV watches, and records every update with JetStream's timestamp and the moment it saw it. It also reads the log stream back and asks who leads it. The scenarios judge the cell from this view and from what the clients saw, not from anything a broker reports about itself.

### Timings

| Setting | Value | What follows from it |
|---|---|---|
| Heartbeat | 500 ms | |
| Lease | 3 s | A node fences after 1.5 s without an acknowledged heartbeat, and the cell calls a peer dead after 5 s of silence (lease + 2 s) |
| NATS ping interval | 1 s, 3 outstanding | |
| Slow node delay | 200 ms each way | A heartbeat round trip stays under the 750 ms write timeout (lease / 4), so the slow node never fences |
| Resume bound | 10 s | From the spec |
| Election slack | 1 s | Publishing has to resume within this long of the observer seeing the log's new leader |

## What each scenario checks

Each subscriber is a persistent MQTT 5 session (expiry 600 s) at QoS 1. Each publisher sends QoS 1 messages at a fixed rate with payloads `<publisher>:<n>`. A publish only counts as acknowledged when a PUBACK with a reason code under 0x80 came back for it. Two checks run at the end of every scenario.

- No loss: every acknowledged message reaches every subscriber at least once. The test waits up to a minute or two for stragglers.
- No cursor skips: the test reads the tenant's log back from the stream and lines it up with every session record the observer saw. A record says a session is done with a message when its sequence is at or under `Cursor`, or at or under `DoneThrough` and not in `Pending`. For each such message, the client has to have received it before the observer saw that record. A record that runs ahead of its client is a skip, because a node resuming from it would never send what was skipped.

`TestKill9BrokerDuringQoS1` is the spec's main success criterion. Five subscribers sit on `n2` while a publisher on `n1` sends 500 msg/s. About five seconds in (jittered by up to a second, so the kill doesn't line up with the once a second cursor flush), `n2` gets a `kill -9`. Each subscriber reconnects to `n3` and has to find its session present. Within 10 s of its reconnect it has to have every message acknowledged before that reconnect, and its record has to name `n3`.

`TestKill9NATSServer` kills the NATS server that leads the log stream, with a publisher on `n1` at 500 msg/s and three subscribers each on `n2` and `n3`. All clients reconnect by themselves when they're dropped. Publishing counts as resumed when PUBACKs for messages sent after the kill come at half the offered rate or better over a whole second. That has to happen within a second of the observer seeing the new log leader.

`TestPartitionBrokerFromNATS` cuts `n1` off from all three servers while four subscribers stay connected to it and a publisher on `n3` runs at 200 msg/s. It checks that:

- `n1` drops every client within lease/2 + 1 s (2.5 s) of the cut.
- The subscribers stay away until the survivors have taken the sessions, and the first record that leaves `n1` is stamped at least lease + 2 s (5 s) after `n1`'s last heartbeat.
- Every drop comes before that first takeover.
- No client ever sees a DISCONNECT with 0x8E (session taken over), and epochs never go backwards.

The subscribers then move to `n2`, `n1` heals, and the records must still name `n2` once `n1` has reclaimed its own keys.

`TestNATSMinorityNoAcks` isolates `s1` together with `n1`. It cuts the routes between `s1` and the others, `n1`'s proxies to `s2` and `s3`, and the other brokers' proxies to `s1`. It first checks that every broker really is on its own server. A publisher pinned to `n1` and one on the majority side both run at 200 msg/s for 15 s of partition. No publish sent through `n1` during the partition may get a PUBACK. `n1` must drop its publisher within 2.5 s and accept no CONNECT until the heal. The majority side has to be back at half its rate or better within the partition, counted from a lease after the cut so any fence it went through is over.

`TestSlowConsumerNoSkips` delays every NATS connection of `n2` by 200 ms each way. Two subscribers sit on it while a publisher on `n1` sends 500 msg/s; one of them sleeps 3 ms per message, so it handles about 330 msg/s and falls further behind every second. After 8 s both drop their TCP connections without a DISCONNECT and resume on `n3`. Five seconds later they do it again, back onto the slow `n2`. The slow one is a few thousand messages behind at each move, and that backlog is what the cursor check is there for.

## Results

Recorded on 27 October 2026 on an Apple M3 Max (16 cores, 128 GiB), macOS 27.0.1, Go 1.26.9, nats-server 2.15.1 and nats.go 1.54.0, at commit `6226d78` plus this suite. The machine was shared with other work, so the load average is next to each run (the 1-minute figure when the run started).

| Scenario | Run | Load | Result | Numbers |
|---|---|---|---|---|
| TestKill9BrokerDuringQoS1 | 1 | 8.1 | pass | 5,405 acknowledged, none lost; reconnected 31 ms after the kill; slowest catch-up 38 ms |
| | 2 | 7.6 | pass | 5,015 acknowledged, none lost; reconnected 20 ms after; slowest catch-up 5 ms |
| | 3 | 26.3 | pass | 5,060 acknowledged, none lost; reconnected 23 ms after; slowest catch-up 5 ms |
| TestKill9NATSServer | 1 | 8.1 | **fail** | killed `s2`; new leader after 8.8 s; PUBACKs never got back to half rate in the 90 s after the kill (28,356 of 53,945 sent were acknowledged); none lost |
| | 2 | 7.6 | **fail** | killed `s2`; new leader after 5.5 s; resumed after 11.3 s; all three brokers fenced once; none lost |
| | 3 | 26.3 | pass | killed `s1`; new leader after 5.5 s; resumed after 5.7 s; all three fenced once; none lost |
| TestPartitionBrokerFromNATS | 1 | 8.1 | pass | drops within 1.48 s; first takeover 5.08 s after the last heartbeat, 3.58 s after the last drop; four 0x88 DISCONNECTs, no 0x8E |
| | 2 | 7.6 | pass | drops within 1.47 s; first takeover 5.12 s after; no 0x8E |
| | 3 | 26.3 | pass | drops within 1.47 s; first takeover 5.01 s after; no 0x8E |
| TestNATSMinorityNoAcks | 1 | 5.9 | pass | 272 minority publishes during the partition, 0 acknowledged; publisher dropped after 1.36 s; majority at rate from cut + 3.0 s; every broker fenced once |
| | 2 | 6.1 | pass | 242 minority publishes, 0 acknowledged; dropped after 1.22 s; majority at rate from cut + 3.0 s |
| | 3 | 8.9 | pass | 268 minority publishes, 0 acknowledged; dropped after 1.34 s; majority at rate from cut + 3.0 s |
| TestSlowConsumerNoSkips | 1 | 8.1 | pass | 12,195 acknowledged, none lost; slow subscriber 1,751 then 3,068 behind at the moves; 102 records checked, no skips |
| | 2 | 7.6 | pass | 12,325 acknowledged, none lost; 1,704 then 3,028 behind; 120 records, no skips |
| | 3 | 26.3 | pass | 12,260 acknowledged, none lost; 1,690 then 3,032 behind; 130 records, no skips |

The minority runs come from a second batch, after the majority check was tightened to count from a lease after the cut. The first batch passed too, but it could be satisfied by acks that landed before the fences.

### Why TestKill9NATSServer fails

Two product bugs, both reported for their own fix. Neither is a timing problem from machine load: the first shows up at a load of 6 just as it does at 30, and the second is a consumer that never comes back.

#### PUBACKs crawl after a leader election

A client's publishes go through one pipe that acks in order (`broker/clusterlog.go`, `runWriter` and `commit`). When an append fails, `commit` retries that one publish with backoff from 100 ms up to 5 s, and nothing behind it moves until it's in. Two things follow. The publish at the head can sleep up to 5 s past the moment JetStream has a leader again. And every publish that was in flight during the outage has already failed its first attempt, so the writer reaches each one in turn, waits 100 ms, and sends it again. In one run JetStream had a new leader about 6 s after the kill, and from then on `n1` logged exactly ten "append failed, retrying" lines a second for the next 90 s. A client at 500 msg/s gets its acks back at about 10 a second until hundreds of stale publishes have been retried one by one, so publishing doesn't really resume for minutes. Nothing acknowledged is lost.

#### A tenant's reader can go silent for good

This hit 4 of about 20 runs made while building the suite, every time on the node whose own NATS server was the log leader that died. That node reconnects to another server within a millisecond, and nats.go's ordered consumer behind `cluster.Reader` resets straight away. It asks for a new R1 consumer, and the cluster places it on the server that just died, because the meta layer hasn't noticed the death yet. `CONSUMER.INFO` on it answers 10119, "consumer is offline". nats.go then retries `CreateOrUpdateConsumer` under that same name forever, inside `Next`, so `Next` never returns an error. `Reader.Done` never closes, `watchReader` in `broker/clusterlog.go` never restarts the reader, and nothing is logged. The node's connected subscribers get nothing more. In one run they missed 33,023 acknowledged messages over two minutes while the other node's subscribers got all of them. Their records stopped at the last cursor, so this is loss to connected sessions, not a cursor skip. A reconnect elsewhere would replay the gap, but nothing ever forces one. The fix belongs in `cluster/reader.go`: bound the ordered consumer's reset attempts (`MaxResetAttempts`) so the error reaches `watchReader`, which starts a fresh consumer under a new name, or watch the reader's position against the stream and restart one that has stalled. Replays build their consumers the same way and probably share the problem.

### Worth knowing

A NATS leader election usually fences every broker at these timings. If the members bucket's leader was on the server that died, nobody can renew for the 4 to 9 s an election takes, which is well past lease/2. The spec's fence rule says it should: clients get dropped and reconnect, and the persistent ones replay what they missed. With the default 10 s lease a node fences after 5 s, so a long election can do the same thing in production.

When the survivors take over, they do it right at the floor. The first takeover landed 5.01 to 5.12 s after the cut node's last heartbeat, against a floor of 5 s. The spec puts the margin in the fence instead, which drops clients after about 1.5 s.
