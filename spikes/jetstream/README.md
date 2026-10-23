# JetStream behaviour spike

Phase 0, task 4. Throwaway code that checks the JetStream features Tether's QoS 1/2 log, session keys and replay lean on, on the pinned nats-server (v2.15.1) with nats.go v1.54.0. The numbers and the decision are in [RESULTS.md](RESULTS.md).

This is its own module. Run it from this directory with `GOWORK=off`.

```sh
GOWORK=off go test -count=1 -v ./...                       # the correctness checks, about 40 s
GOWORK=off go build -o /tmp/jsspike .
/tmp/jsspike -runs 3 -out runs/full > runs/full.log        # the measurements, about 30 min
/tmp/jsspike -runs 1 -rounds 1 -window 1s -per-tenant 20000 -cas-keys 3000   # smoke, about 3 min
```

Every NATS server runs in its own child process, the same shape as spike 3: the binary (or the test binary) re-executes itself with `TJS_CHILD` set and runs one server. Settings travel in environment variables, so the same entry point works under `go test`. A child exits when its stdin closes, so the servers go away with the parent however it ends. Each cluster gets a fresh temp dir for JetStream storage, removed when the cluster stops. Ports are picked before any child starts so every server has full routes to the other two.

## Tests

The tests share one 3-server cluster started in `TestMain`.

- `cas_test.go`: two goroutines on connections to different servers CAS the same session key from the same revision, 1,000 rounds, through `kv.Update` and through a raw publish with `Nats-Expected-Last-Subject-Sequence`. Exactly one must win each round. `TestCASPipelinedOnOneKey` sends two CAS writes on one key without waiting for the first ack.
- `ttl_test.go`: a bucket with `LimitMarkerTTL`, a raw publish with `Nats-TTL: 2s` and an expected sequence, and a watcher on another server that must see the marker. Also: a stale expected sequence is refused, a CAS with a new TTL renews the expiry, how to write a key again after it expired, that `Nats-TTL` needs the markers bucket, and `kv.Create` over 20 freshly expired keys.
- `dedup_test.go`: a `Nats-Msg-Id` resend inside a 2 s window is a duplicate, and the same ID is stored again after it. `TestDedupSurvivesLeaderChange` resends after a stream leader stepdown.

## Measurements

`main.go` starts a fresh cluster per run, then runs each mode in it:

- `publish` (`publish.go`): publish-ack latency on an R3 file stream for 256 B and 64 KiB at 1k, 10k and 50k msgs/s, sync (64 goroutines per connection each waiting for its ack, like a blocking `OnPublish`) and async (`PublishMsgAsync`, latency taken in the ack handler, the path spike 1 recommends). Traffic is split over one connection per server.
- `cas` (`cas.go`): session-key CAS on one R3 bucket with limit markers, 60k keys, 512 B values, every write a raw publish with `Nats-TTL: 168h` and the expected sequence. Sync and async, at 1.7k, 5k and 10k ops/s. Keys are partitioned per connection so no write should fail.
- `catchup` (`catchup.go`): 10 tenants x 1M messages (128 B, interleaved round robin, 1 in 100 under the tenant's `==` token as spike 2 encodes `$` topics) in one R3 file stream, then ordered consumers reading one tenant from the start. Then a small scaling check of multi-filter consumers against distinct subjects per tenant.

The publish and CAS cells run in interleaved windows (`-window`, default 3 s; `-rounds` windows per cell per run, each round starting at a different cell), so drift in the machine's load lands on every cell instead of on the ones that ran late. The catch-up variants rotate their order per run. Each run prints the 1-minute load average per window; the summary tables are medians over runs. Raw output is in `runs/`.

Files: `cluster.go` (child servers), `publish.go` (cell runner, pacing, publish modes), `cas.go`, `catchup.go`, `stats.go` (HDR histograms, tables, JSON), `util.go`.
