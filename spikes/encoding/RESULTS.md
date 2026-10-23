# Phase 0, Task 2: MQTT to NATS encoding

Machine: Apple M3 Max, 16 cores, 128 GiB RAM, macOS 27.0.1 (Darwin 27.0.0), Go 1.26.9. nats-server v2.15.1 (embedded, JetStream on, `max_control_line` 4096), nats.go v1.54.0.

Load: the load average was 16.9 when I started. It was 54 to 273 during the final fuzz runs (a shared Mac, plus the 16 fuzz workers themselves). Exec rates below are therefore low and uneven. Read them as coverage counts, not speed.

## Scheme

Each MQTT level (split on `/`) is one NATS token, joined with `.`. So MQTT depth equals token depth, and `+` is `*` one for one.

- A level that is non-empty, uses only `[-_=a-zA-Z0-9]` and does not start with `=` passes through.
- Every other level becomes `=` plus unpadded base64url of its bytes. That covers empty levels (the token `=`), UTF-8, `$`, `.`, `*`, `>`, whitespace, NUL and any level that starts with `=`.
- `+` becomes `*`. A trailing `#` becomes `>`, plus the parent subject (`a/#` gives `a` and `a.>`). A lone `#` gives only `T.>`, since it has no parent topic.
- Client IDs use `EncodeLevel` as one token.

Why a passthrough level can never be mistaken for an encoded one: the marker is `=`, and a passthrough level never starts with it (a level that does is always encoded). So the first byte of a token says which kind it is. The base64url alphabet has no `=`, so an encoded token holds `=` only at position 0. The decoder is strict: it checks the alphabet itself (Go's base64 silently skips `\r` and `\n`), uses `Strict()` canonical base64, rejects an encoded token whose payload would have passed through, and rejects a payload holding `/`. Every valid token has one decoding and every level one token. `FuzzDecodeCanonical` checks `encode(decode(s)) == s`.

Encoded tokens use only `[=A-Za-z0-9_-]`, so they are valid NATS tokens and valid KV key parts. Tokens can never be `*` or `>`.

### The `$` rule needs one extra thing

MQTT: a filter whose first level is `+` or `#` never matches a topic whose first level starts with `$`. NATS `*` and `>` match any token, so token content cannot carry this rule. A first draft with only the rule above fails the equivalence test at once (`+` against `$SYS` matches in NATS, not in MQTT; I confirmed this by switching the fix off).

Fix: a topic whose first level starts with `$` gets the tenant token `==` + `EncodeLevel(tenant)` instead of `EncodeLevel(tenant)`. No ordinary token starts with `==`, so the two spaces never collide. A wildcard-first filter is built in the ordinary space and cannot reach the `$` space; a filter starting with a literal `$` level is built in the `$` space. Depth is unchanged.

Consequence for Phase 2: MQTT `#` excludes `$` topics, so a node that wants a tenant's whole log (`d.<tenant>.>`) needs two filters. `TenantAll(tenant)` returns `T.>` and `==T.>`. The spec's "every node serving the tenant receives the message from its ordered consumer on `d.<tenant>.>`" must become a consumer with those two `FilterSubjects` (JetStream allows several). Retained KV watches by MQTT filter are unaffected.

## API (as built)

`EncodeLevel`, `DecodeLevel`, `EncodeTopic(tenant, topic)`, `EncodeFilter(tenant, filter) []string`, `DecodeTopic(subject) (tenant, topic, err)` as in the plan, plus `TenantAll(tenant) []string`. `d.` and `f.` prefixes are left to callers. `EncodeTopic` and `EncodeLevel` accept any string, invalid UTF-8 and NUL included.

## Maximum encoded-length ratio

Per level of n bytes: n if it passes through, 1 for the empty level, `1 + ceil(4n/3)` otherwise. That is at most `2n + 1` per level, so a whole topic of N bytes encodes to at most `2N + 1` bytes. The bound is tight: `N` slashes (all empty levels) or `" /" x N/2` (all one-byte levels) both hit it. A 1024 byte worst-case topic is 2049 bytes (ratio 2.001); a 1023 byte one of 2 byte UTF-8 levels is 1706 (1.67). `FuzzRoundTrip` asserts the `2N + 1` bound on every input. Ordinary ASCII topics are unchanged in length.

## 1 KiB topic verdict

Fits, with about 2x headroom. The worst-case 1024 byte topic gives the subject `d.<31 byte tenant>.<2049>` = 2083 bytes, against `max_control_line` 4096. On the real server it passes core publish and wildcard subscribe, a JetStream publish with a header (HPUB) read back through an ordered consumer filtered on `d.<tenant>.>`, and a KV put and get with key `r.<tenant>.<topic>`.

I also probed upward with worst-case topics: 1950 bytes still pass every operation, 2000 bytes fail (KV get, whose direct-get API subject adds the stream and bucket names, closes the connection with a control line error). The ceiling for hostile topics is therefore about 1950 bytes with a short bucket name, and less with longer stream, bucket or consumer names. MQTT allows 65535 bytes, so `max_topic_bytes` must be enforced and must stay at 1024 (or at most about 1500). A larger limit would need a larger `max_control_line`.

## Verification

- `TestEquivalenceExhaustive`: 21,350,190 filter and topic pairs (every combination up to 3 levels over a hostile alphabet: empty, `$SYS`, `.`, space, `*`, `>`, `é`, `=a`, `=`, `a.b`, emoji), 60,455 matching, 203,826 of them `$` topics behind a wildcard-first filter. MQTT `topic.Match` and NATS matching agree on all of them.
- `TestAgainstRealServer`: 170 filters and 132 topics through a real nats-server, 22,270 pairs, all agree with `topic.Match`. This validates the hand-written matcher.
- NATS wildcard matching in the tests is my own implementation (token by token: `*` one token, `>` one or more trailing tokens, the last filter token only). `FuzzWildcardEquivalence` also cross-checks it against nats-server's exported `server.SubjectMatchesFilter`, and `FuzzSubjectValid` uses `server.IsValidSubject` and `IsValidPublishSubject`. nats.go has no public subject validator, and its KV key check (`keyValid`) is unexported, so the KV rules (`^[-/_=\.a-zA-Z0-9]+$`, no leading or trailing `.`, no `..`) are copied into the tests; the real KV put in `TestLongTopicOnRealServer` confirms them.
- The MQTT reference is tether's own `topic.Match` and `topic.Valid` (the spike's `go.mod` replaces `github.com/xraph/tether` with `../..`).
- `FuzzWildcardEquivalence` builds topics from the filter (`derive`) so matches are common (735 of 757 derived seed pairs match, guarded by `TestDeriveMatchRate`), and also asserts that tenant A's filter never matches tenant B's topic.

## Fuzz runs

Each target ran 2 minutes (`-fuzztime 2m`, 16 workers) against the final code, all clean:

| Target | Execs | Checks |
|---|---|---|
| `FuzzRoundTrip` | 3,511,773 | level and topic round trip for any string, kinds never overlap, `2N+1` length bound |
| `FuzzKVKeySafe` | 3,396,565 | session, retained and membership keys and watch patterns match the KV regexes |
| `FuzzSubjectValid` | 2,382,183 | publish and filter subjects pass nats-server validation |
| `FuzzWildcardEquivalence` | 1,348,804 | MQTT equals NATS matching, own matcher equals nats-server's, tenant isolation, `TenantAll` covers all |
| `FuzzDecodeCanonical` | 1,945,697 | any decodable token or subject re-encodes to itself |

`FuzzDecodeCanonical` is a fifth target I added to the plan's four. It found two real bugs, both in decoding (the encoder was right). Each failing input is kept under `testdata/fuzz/FuzzDecodeCanonical/`:

1. `=.=000v.0000000`: the token `=000v` decodes to bytes that contain `/`, so a decoded topic split into a different level count and did not re-encode to the same subject. Fix: `DecodeTopic` rejects a level holding `/`.
2. `=\r`: Go's base64 decoder skips `\r` and `\n`, so a token with a stray newline decoded to a valid level. Fix: check the alphabet before decoding.

Before those two fixes, runs of the other four targets were also clean; I reran all five against the final code so the counts above match the committed code. Fuzzing also writes its coverage corpus to the Go build cache, not to the tree; only the two failing inputs are in `testdata/`.

## Concerns for Phase 2

- The `$` space doubles the filters for any tenant-wide consumer (`TenantAll`). Anything that subscribes by tenant prefix instead of by MQTT filter must remember both subjects.
- The 1024 byte topic limit is load bearing, not just a convenience. Enforce it at SUBSCRIBE and PUBLISH before encoding.
- A client ID is one token and can reach about 1.33x its length plus one; a 65535 byte client ID would not fit. Cap client ID length too (the MQTT spec lets a server do so).

Decision: Adopt the `=` marker scheme as built, including the `==<tenant>` token for `$` topics, and change the spec's tenant-wide consumer subject from `d.<tenant>.>` to the two `TenantAll` filters. Cap topics at 1024 bytes (worst case encodes to 2049, well under `max_control_line` 4096). Promote `encode.go` into the main module in Phase 2.
