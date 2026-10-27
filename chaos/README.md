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

A full run takes a little over three minutes when everything passes, mostly waiting for leases to run out and for the slow subscriber to drain. A failure can add a minute or two, because the checks wait out their timeouts before they give up. You need nothing but Go: the test builds `cmd/tether` itself, and the NATS servers are the test binary re-run as a child, so there's no Docker and nothing to download. Everything listens on `127.0.0.1` with ports picked at random.

Set `TETHER_CHAOS_KEEP=1` and each cell keeps its directory after the run (the path is in the test output). Inside you'll find every process's log (`s1.log` to `s3.log`, `n1.log` to `n3.log`), the broker configs, the shared SQLite file, and, after a failure, each live NATS server's `/jsz` and `/connz` as JSON. A failed test also prints the warnings and errors from every broker and the tail of every server log.

Without the `chaos` tag the package doesn't build at all, so `go test ./...` never runs it. CI runs it every night (`.github/workflows/chaos.yml`, also startable by hand) and uploads the cell logs when it fails.

## How the cell is built

The harness lives in `harness.go`.

- NATS: three servers named `s1`, `s2` and `s3`, JetStream on, one cluster. Every route goes through its own proxy, one per direction per pair, so `s1` reaching `s2` and `s2` reaching `s1` are separate links you can cut. Each server advertises a cluster address nothing listens on, which stops the servers from gossiping their way around the proxies. Pings run every second with three allowed outstanding, so a dead route shows up in seconds.
- Brokers: three nodes, `n1` to `n3`, in external mode. Each node gets three proxies, one to each server, and lists them with its own server first (`n1` sits on `s1`, and so on). `no_discovery: true` keeps a node on the URLs it was given. Without it, nats.go learns the other servers' addresses from the cluster and reconnects around the proxy, so a black hole wouldn't cut anything off. Cutting a node means putting its proxies in `Blackhole`, never `Reset`.
- Start order: the brokers share one SQLite file, so they start one at a time. Each one has to answer `/_/health` and accept a TCP connection on its MQTT port before the next one starts. The cell is ready once the observer has seen a heartbeat from all three.
- Devices: a tenant, a role that may publish and subscribe under `chaos/#`, and one pinned device per client, all made through the admin REST API with a bearer token the harness makes up.
- Cleanup: a broker runs under a small supervisor (the test binary again), and NATS servers watch their stdin. When the test binary goes away for any reason, its stdin pipes close and every child goes with it.

The observer is a plain NATS client that goes straight to the servers, past every proxy. It follows the sessions and members buckets with KV watches, and records every update with JetStream's timestamp and the moment it saw it. It also reads the log stream back and asks who leads it. The scenarios judge the cell from this view and from what the clients saw, not from anything a broker reports about itself.

### Timings

Most scenarios run the brokers on short timings so a run takes minutes. `TestKill9NATSServer` is the exception: it runs the production defaults (`cluster.DefaultHeartbeat` and `cluster.DefaultLease`, 2 s and 24 s today), because what it checks is that losing one server of three doesn't fence anyone in a real deployment.

| Setting | Value | What follows from it |
|---|---|---|
| Heartbeat | 500 ms | |
| Lease | 3 s | A node fences after 1.5 s without an acknowledged heartbeat, and the cell calls a peer dead after 5 s of silence (lease + 2 s) |
| Heartbeat and lease in `TestKill9NATSServer` | the defaults, 2 s and 24 s | A node fences after 12 s without a renew, longer than the 4 to 9 s a JetStream election takes |
| NATS ping interval | 1 s, 3 outstanding | |
| Slow node delay | 200 ms each way | A heartbeat round trip stays under the 750 ms write timeout (lease / 4), so the slow node never fences |
| Resume bound | 10 s | From the spec |
| Election slack | 1 s | Publishing has to resume within this long of the observer seeing the log's new leader |

## What each scenario checks

Each subscriber is a persistent MQTT 5 session (expiry 600 s) at QoS 1. Each publisher sends QoS 1 messages at a fixed rate with payloads `<publisher>:<n>`. A publish only counts as acknowledged when a PUBACK with a reason code under 0x80 came back for it. Two checks run at the end of every scenario.

- No loss: every acknowledged message reaches every subscriber at least once. The test waits up to a minute or two for stragglers.
- No cursor skips: the test reads the tenant's log back from the stream and lines it up with every session record the observer saw. A record says a session is done with a message when its sequence is at or under `Cursor`, or at or under `DoneThrough` and not in `Pending`. For each such message, the client has to have received it before the observer saw that record. A record that runs ahead of its client is a skip, because a node resuming from it would never send what was skipped.

`TestKill9BrokerDuringQoS1` is the spec's main success criterion. Five subscribers sit on `n2` while a publisher on `n1` sends 500 msg/s. About five seconds in (jittered by up to a second, so the kill doesn't line up with the once a second cursor flush), `n2` gets a `kill -9`. Each subscriber reconnects to `n3` and has to find its session present. Within 10 s of its reconnect it has to have every message acknowledged before that reconnect, and its record has to name `n3`.

`TestKill9NATSServer` kills the NATS server that leads the log stream, with a publisher on `n1` at 500 msg/s and three subscribers each on `n2` and `n3`. All clients reconnect by themselves if they're dropped, though they shouldn't be. Two servers still make a quorum, so no broker may fence: the test fails if any broker logs a fence or any client is let go with DISCONNECT 0x88 (server unavailable), which is how a fence drops a 5.0 client. Publishing counts as resumed when PUBACKs for messages sent after the kill come at half the offered rate or better over a whole second, and that has to happen within a second of the observer seeing the new log leader.

`TestPartitionBrokerFromNATS` cuts `n1` off from all three servers while four subscribers stay connected to it and a publisher on `n3` runs at 200 msg/s. It checks that:

- `n1` drops every client within lease/2 + 1 s (2.5 s) of the cut.
- The subscribers stay away until the survivors have taken the sessions, and the first record that leaves `n1` is stamped at least lease + 2 s (5 s) after `n1`'s last heartbeat.
- Every drop comes before that first takeover.
- No client ever sees a DISCONNECT with 0x8E (session taken over), and epochs never go backwards.

The subscribers then move to `n2`, `n1` heals, and the records must still name `n2` once `n1` has reclaimed its own keys.

`TestNATSMinorityNoAcks` isolates `s1` together with `n1`. It cuts the routes between `s1` and the others, `n1`'s proxies to `s2` and `s3`, and the other brokers' proxies to `s1`. It first checks that every broker really is on its own server. A publisher pinned to `n1` and one on the majority side both run at 200 msg/s for 15 s of partition. No publish sent through `n1` during the partition may get a PUBACK. `n1` must drop its publisher within 2.5 s and accept no CONNECT until the heal. The majority side has to be back at half its rate or better within the partition, counted from a lease after the cut so any fence it went through is over.

`TestSlowConsumerNoSkips` delays every NATS connection of `n2` by 200 ms each way. Two subscribers sit on it while a publisher on `n1` sends 500 msg/s; one of them sleeps 3 ms per message, so it handles about 330 msg/s and falls further behind every second. After 8 s both drop their TCP connections without a DISCONNECT and resume on `n3`. Five seconds later they do it again, back onto the slow `n2`. The slow one is a few thousand messages behind at each move, and that backlog is what the cursor check is there for.

## Results

Recorded on 27 October 2026 on an Apple M3 Max (16 cores, 128 GiB), macOS 27.0.1, Go 1.26.9, nats-server 2.15.1 and nats.go 1.54.0, at commit `bc3ea5e` (the CHAOS-1 and CHAOS-2 fixes, the 24 s default lease and prompt heartbeat retries). The machine was shared with other work, so each run has its load average next to it (the 1-minute figure when it started). The first block is one full suite run. The three runs after it are `TestKill9NATSServer` on its own.

| Scenario | Load | Result | Numbers |
|---|---|---|---|
| TestKill9BrokerDuringQoS1 | 6.4 | pass | 5,175 acknowledged, none lost; reconnected 31 ms after the kill; slowest catch-up 16 ms |
| TestKill9NATSServer | 6.4 | pass | killed `s2`; new log leader after 5.5 s; resumed after 5.3 s; no fence, no 0x88; 7,750 acknowledged, none lost |
| TestPartitionBrokerFromNATS | 6.4 | pass | drops within 1.58 s; first takeover 5.10 s after the last heartbeat, 3.48 s after the last drop; four 0x88 DISCONNECTs, no 0x8E |
| TestNATSMinorityNoAcks | 6.4 | pass | 277 minority publishes during the partition, 0 acknowledged; publisher dropped after 1.39 s; majority at rate from cut + 3.0 s |
| TestSlowConsumerNoSkips | 6.4 | pass | 12,250 acknowledged, none lost; slow subscriber 1,701 then 3,028 behind at the moves; 111 records checked, no skips |
| TestKill9NATSServer | 5.7 | pass | killed `s1`; new leader after 5.5 s; resumed after 4.8 s; no fence; 7,755 acknowledged, none lost |
| TestKill9NATSServer | 6.3 | pass | killed `s1`; new leader after 6.6 s; resumed after 6.5 s; no fence; 8,300 acknowledged, none lost |
| TestKill9NATSServer | 6.6 | pass | killed `s3`; new leader after 8.8 s; resumed after 8.1 s; no fence; 9,385 acknowledged, none lost |

Before the 24 s lease, the NATS server kill failed about one run in three. A JetStream election of the members bucket that ran past half the old 10 s lease fenced every broker, the brokers dropped their clients with 0x88, and publishing resumed only after the unfence. If you see it fail now, check the brokers' logs first: a "heartbeat write failed" line followed by "fenced" means the members bucket had no leader for 12 s, which is longer than any election should take.

### Worth knowing

On the 3 s chaos lease, a JetStream election of the members bucket fences every broker: nobody can renew for the 4 to 9 s an election takes, and that's well past 1.5 s. You can see it in the minority scenario, where all three brokers fence once even though only `n1` is cut off. Clients get dropped and reconnect there, and the persistent ones replay what they missed. That's why the NATS server kill runs on the production timings, where it has to get through an election without a single fence.

When the survivors take over, they do it right at the floor. The first takeover landed 5.01 to 5.12 s after the cut node's last heartbeat, against a floor of 5 s. The spec puts the margin in the fence instead, which drops clients after about 1.5 s.
