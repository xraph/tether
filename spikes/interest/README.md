# NATS interest at scale spike

Phase 0, task 3. Throwaway code that measures whether core NATS subject interest can carry Tether's QoS 0 routing at 1M distinct filters, or whether we need the per-tenant fallback. The numbers and the decision are in [RESULTS.md](RESULTS.md).

This is its own module. Build and run it from this directory with `GOWORK=off`:

```sh
GOWORK=off go build -o /tmp/interest .
/tmp/interest -runs 3 -out runs/full-1m          # the full measurement, about 12 min
/tmp/interest -filters 5000 -churn 10000 -baseline 5s -storm 5s -fallback 5s -runs 1   # smoke, 25 s
```

Run the built binary, not `go run`: the harness starts each NATS server as a child process of its own executable (`-serve`), so per-server RSS and CPU time are real process numbers and don't include the harness's heap. A child exits when its stdin closes, so the servers go away with the harness however it ends.

What a run does:

- Interest mode (`runInterest`): 3 servers in a full mesh on localhost, 10 broker connections spread over them, each subscribing 100k subjects `f.ttnt_x.devices.<n>.cmd` built with spike 2's encoder (`encode.go` is a copy). Probes published from the other servers find when each connection's last subscription became reachable. Then 30 s of 5k msgs/s on unaffected subjects (baseline), then 60 s of the same traffic while 100k devices unsubscribe and resubscribe one second later (storm), then the overlap check (`a/+` and `a/#` on one connection, dedup by a `Tm-Id` header).
- Fallback mode (`runFallback`): the same cluster and traffic, but each broker connection holds only `f.ttnt_x.>` and `f.==ttnt_x.>` and matches every message against a mochi topic index of its 100k filters.

The interest and fallback modes both decode the subject and look it up in a mochi `TopicsIndex` for every received message, so the CPU difference comes from the fallback's fan-out and not from work the interest side skips.

Two more modes came out of the load on the shared Mac, where a before-and-after comparison drifted with the machine:

```sh
/tmp/interest -mode interest -window 10s -runs 3 -out runs/interleaved-1m   # storm in alternating off/on 10 s windows
/tmp/interest -mode cpu -window 10s -pairs 4 -runs 3 -out runs/cpu-1m       # both clusters up, alternating CPU windows
```

With `-window`, the storm runs only in on windows over twice `-storm`, and the off windows are the control. `-mode cpu` keeps an interest cluster and a fallback cluster up together and alternates 10 s traffic windows between them, so both see the same load.

Files: `cluster.go` (child servers, stats endpoint, mesh check), `load.go` (broker connections, probes, traffic, both storm shapes), `main.go` (flags, the run modes, the overlap check, tables), `cpu.go` (the paired CPU mode), `dedup.go` (the `streaming/dedup.go` shape), `encode.go` (copied from `spikes/encoding`). Raw per-run metrics and logs are under `runs/`.
