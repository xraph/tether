# mochi cluster hooks spike

Phase 0, task 1. Throwaway code that answers four questions about running Tether's cluster layer on mochi v2.7.9 through hooks alone. The answers and numbers are in [RESULTS.md](RESULTS.md).

This is its own module, so run everything from this directory with `GOWORK=off`:

```sh
GOWORK=off go test -race -short ./...   # the yes/no questions, about 3 s
GOWORK=off go test -run TestBlockingOnPublish ./...   # short version of the measurement
GOWORK=off go run . -runs 3             # the measurement, medians of 3 runs, about 90 s
```

Set `SPIKE_LOG=1` if you want mochi's debug log on stderr.

What's where:

- `main.go`: the hooks under test (`sessionHook` loads and evicts sessions, `clusterHook` and `newInjector` cover injection, `commitHook` simulates the JetStream commit) and the load harness behind `go run .`.
- `lazy_test.go`: question 1, loading a session at CONNECT.
- `evict_test.go`: question 2, evicting it after DISCONNECT, plus the two races the eviction has to guard against.
- `inject_test.go`: question 3, injecting cluster messages, plus a user property quirk found on the way.
- `blocking_test.go`: question 4, a commit inside OnPublish, and the hook-written ack for QoS 1 and 2.

Nothing here imports the main module, and the root's lint and format checks skip `spikes/`. Keep the files gofmt-clean anyway: the shared CI lint job runs `gofmt -l .` over the whole checkout.
