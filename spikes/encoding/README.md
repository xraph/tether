# spikes/encoding

Phase 0, Task 2: how an MQTT topic or client ID becomes a NATS subject and a JetStream KV key. Throwaway code with its own module. If the scheme holds, `encode.go` is promoted into the main module in Phase 2.

The answer is in [RESULTS.md](RESULTS.md). The scheme in one line: each `/` level is one `.` token, a level of `[-_a-zA-Z0-9=]` that doesn't start with `=` passes through, anything else is `=` plus unpadded base64url, and a topic whose first level starts with `$` lives under a tenant token written `==<tenant>` so MQTT's `$` rule survives.

Run it:

```sh
GOWORK=off go test -count=1 ./...                      # unit, exhaustive equivalence, embedded nats-server
GOWORK=off go test -run '^$' -fuzz '^FuzzRoundTrip$' -fuzztime 2m .
```

Fuzz targets: `FuzzRoundTrip`, `FuzzKVKeySafe`, `FuzzSubjectValid`, `FuzzWildcardEquivalence`, `FuzzDecodeCanonical`.

`go.mod` replaces `github.com/xraph/tether` with `../..` so the equivalence fuzz uses tether's own `topic.Match` and `topic.Valid` as the MQTT reference.
