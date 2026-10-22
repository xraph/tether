# Tether: a multi-tenant, clustered MQTT broker on mochi

Status: approved in brainstorming on 2026-10-22, revised after builder review the same day; Rex settled key and role ownership and the companion dashboard. Awaiting Rex's read.
Home: this file lives in forge's gitignored specs folder until the new repo exists. Move it to `docs/superpowers/specs/` in that repo on day one.

"Tether" is a placeholder name (my call). Rename freely; nothing below depends on it.

Calls marked **(my call)** were delegated to Claude in brainstorming and are open to change.

## Why

Tether is a general-purpose forgery component, built like every other forgery repo: a library plus a forge extension with its own admin API, dashboard contract and companion forge-dashboard plugin, usable by any forge app. TwinOS is the first consumer (devices publish telemetry that feeds digital twins, and twins send commands back), not the design target. Later the same broker can run as a hosted product, one cluster serving many paying tenants, billed through ledger. Off-the-shelf brokers either lack real multi-tenancy in their open editions or don't plug into the forgery identity, authorization and billing stack. The value of building this is that tenancy, auth, ACLs, quotas, metering and audit come from authsome, keysmith, warden, ledger and chronicle, and the broker ships as one Go binary.

The extension at `forge/extensions/mqtt` stays what it is: a Paho client. The broker is its own repo in the forgery tier.

## Goals

- MQTT 3.1.1 and 5.0 over TCP, TLS and WebSocket, built on `github.com/mochi-mqtt/server/v2`.
- Hard tenant isolation: a client can never see, publish to or subscribe to another tenant's topics, client IDs, retained messages or `$SYS`.
- Horizontal scale, with route churn and network partitions handled by design rather than by tuning.
- No loss of an acknowledged QoS 1 or 2 message across a node restart, a `kill -9`, the permanent loss of one broker node, or a slow consumer, for persistent sessions.
- One binary that runs as a single node with no external infrastructure (dev, edge, small installs) and as a cluster node.
- Forgery integration: authsome and keysmith for identity, warden for permissions, ledger for metering and entitlements, chronicle for audit, the forge dashboard contract for admin UI.

## Non-goals (v1)

- AMQP, Kafka protocol, STOMP. Bridges to those come later; a full second protocol never does.
- MQTT-SN, CoAP and LwM2M gateways (phase 4 at the earliest).
- A rule engine with SQL-like transforms. v1 consumers subscribe like any MQTT client.
- Issuing device certificates. mTLS client auth is phase 4 and assumes certs come from elsewhere.
- Cross-cell routing inside one tenant. A tenant lives in exactly one cell.
- Moving a phase 1 single-node install's sessions into a cluster. Clients reconnect and resubscribe; retained messages move with an export/import CLI command **(my call)**.
- MQTT 5 re-authentication (AUTH packet) in v1. Phase 4.

## Anti-goals

These would make the project a failure even if it passed every conformance test:

- An ACK that lies. If the broker sent PUBACK or PUBREC, the message is on durable, replicated storage.
- Two nodes serving the same session at the same time, under any partition.
- Any per-PUBLISH call to a database, warden, keysmith or ledger. The hot path touches memory and NATS only.
- A tenant being able to degrade other tenants by flooding, subscribing to `#`, or reconnect storms beyond its quota.

## Assumptions behind the targets (my call)

- Up to half of all sessions are persistent.
- Plans cap session expiry; default cap 7 days, and an MQTT 3.1.1 `cleanSession=0` session gets the tenant's plan cap as its expiry.
- Topics are capped at 1 KiB per plan default (MQTT allows 64 KiB; NATS control lines default to 4 KiB). Longer topics are rejected with reason 0x90.
- Most traffic is device telemetry upstream (QoS 0 or 1); downstream commands are lower volume.

## Success criteria

Targets to validate in phase 2 load tests (my call; revise once we have numbers):

- 1M concurrent connections on at most 10 broker nodes in one cell, against a 3- or 5-server external NATS cluster.
- `kill -9` of any broker node during sustained QoS 1 traffic: zero acknowledged messages lost to persistent sessions; affected clients resume on another node within 10 s of reconnecting.
- `kill -9` of one NATS server in a 3-server cell: no acknowledged loss, publishing resumes within the JetStream leader election time.
- Partition tests (Jepsen-style, netem or toxiproxy): no session is ever served by two nodes; minority-side QoS 1/2 publishes are not acknowledged; an isolated broker node drops its clients before survivors take over its sessions.
- A reconnect storm of 100k clients in 60 s does not raise p99 latency for already-connected clients above 2x baseline.
- Same-cell p99 publish-to-deliver: QoS 0 under 10 ms, QoS 1 under 25 ms at nominal load.
- Passes the Paho interoperability suite for 3.1.1 and 5.0.

## Architecture in one picture

```
            clients (MQTT TCP/TLS/WS)            HTTP admin + publish API
                      │                                   │
     ┌────────────────▼───────────────────────────────────▼──────────────┐
     │ broker node (forge app, stateless in a cell)                     │
     │  mochi server ── tenancy hooks ── auth ── ACL ── limits           │
     │      │ local subscribers (mochi topic trie, retained cache)       │
     │  cluster layer:                                                   │
     │    QoS 0   → core NATS interest routing                           │
     │    QoS 1/2 → JetStream tenant log, read by nodes serving tenant   │
     │    sessions, ownership, retained → JetStream KV                   │
     └───────────────────────────────┬───────────────────────────────────┘
                                     │
                    NATS cluster with JetStream (3 or 5 servers per cell)
       control plane: Postgres via grove (tenants, cells, plans, ACL sets)
       forgery: authsome, keysmith, warden, ledger, chronicle (relay, dispatch, vault, trove later)
```

A **cell** is a group of stateless broker nodes plus one NATS cluster with JetStream. Every tenant is assigned to one cell. Topics never cross tenants, so cells never talk to each other on the message path. v1 ships with one cell. The data model is cell-aware from the start so phase 3 can add more.

## Decisions

| Question | Decision |
|---|---|
| Broker core | mochi-mqtt/server v2, used through its hooks. Patch or fork only for what the phase 0 spike proves hooks can't do. |
| Deployment modes | **Single node**: no NATS, sessions and retained messages in mochi's own Badger storage hook (`hooks/storage/badger`), control-plane tables in SQLite or Postgres through grove. **Small cluster** (up to 3 nodes): `nats-server` embedded in each broker node. **Production cell**: external NATS cluster (3 or 5 JetStream servers) and stateless, autoscalable broker nodes **(my call)**. Embedded mode isn't used at the 1M target because broker churn would become JetStream peer churn, which needs manual `peer-remove`. |
| NATS version | nats-server 2.11 or later (per-message TTL and limit markers are required). |
| QoS 0 routing | **NATS subject interest.** Each node holds one core NATS subscription per distinct (tenant, filter) with at least one local subscriber, reference-counted. A publish goes out once on the subject for its topic, and NATS delivers to exactly the nodes with matching interest. No route table of our own. QoS 0 delivery is best effort, as MQTT defines it. |
| QoS 1/2 routing and durability | Committed once to a replicated JetStream stream (R3) before PUBACK/PUBREC. Every node serving a tenant reads that tenant's log through an ordered consumer filtered to `d.<tenant>.>` and matches locally. Delivery to sessions comes only from the log, never from core NATS, so a slow consumer can't silently skip a message. |
| Session state and ownership | One JetStream KV key per session holds both the owner (node ID, epoch) and the state (subscriptions, cursor, will, QoS 2 state). Every write, including takeover, is a compare-and-set on that key's revision. A stale owner's next write fails, and it drops the connection. |
| Node liveness | A membership heartbeat per node. A node that can't reach JetStream for half the lease drops all its clients (self-fencing). Survivors act on a node only after the full lease plus a margin. |
| Tenant-to-cell placement | Stored in the control plane. v1: everyone is in cell 1. Phase 3: SNI routing by tenant hostname at the load balancer, with MQTT 5 server-redirect as a fallback. |
| Tenant identity | Tether keeps its own tenant registry: a record per tenant with a Tether ID (`ttnt_` TypeID), the external (`app_id`, `tenant_id`) pair that keysmith, warden and authsome use, plan limits and an enabled flag. The Tether ID is the topic and client-ID prefix. Tenants are created through the admin API; a credential whose pair has no tenant record is refused **(my call)**. |
| Topic aliases | Off in v1, both directions. mochi runs the publish ACL before resolving an inbound alias, so an alias-only PUBLISH would be checked against an empty topic. |
| `$` topics | Namespaced by inserting the tenant after the first level (`$SYS/x` becomes `$SYS/<tenant>/x`), so a tenant's `#` still never matches `$` topics. mochi's own global `$SYS/broker/...` topics are never visible to tenants. |
| Admin API auth | The host app supplies the middleware that authenticates operators. Tether refuses to mount admin routes without it unless an explicit insecure option is set, the same rule warden follows **(my call)**. |
| mochi hook ownership | Tether's hook is the only one providing `OnConnectAuthenticate` and `OnACLCheck`, because mochi ORs those results across hooks and stops at the first `true`. |
| Who manages keys and roles | Tether owns it (Rex's decision). The broker app registers the keysmith and warden forge extensions in-process, so their admin APIs and dashboard plugins come with it, and Tether's own admin API adds device-shaped operations on top (provision a device, issue or rotate its key, assign roles). Tether registers keysmith and warden plugin hooks that publish revoke, rotate, suspend and role-change events on conduit. Any other service that changes the same keysmith or warden stores must publish the same events; their schema is part of Tether's public API. |
| Device credentials | keysmith keys: username = key ID, password = secret. Results cached in-broker and invalidated by those conduit events. |
| User and app credentials | authsome JWT in the password field, verified offline against cached JWKS (RS256, EdDSA). The connection is closed at the token's `exp`, with reason 0xA0 (maximum connect time) on MQTT 5 **(my call)**. |
| Topic ACLs | warden assigns roles; each role carries publish and subscribe filter lists with `%c` (client ID) and `%u` (username) placeholders. Loaded at CONNECT, matched in-broker. Role changes trigger refresh or disconnect. |
| Rate limits | Enforced per node with no cross-node traffic. A tenant's cell-wide budget is split across nodes by each node's share of the tenant's connections, rebalanced every 10 s. Brief overshoot while rebalancing is accepted. |
| Connection limits | Each node reports per-tenant connection counts in its heartbeat; every node keeps the cell totals from watching heartbeats and checks them at CONNECT. Approximate by up to one heartbeat interval. |
| Metering and entitlements | Per-tenant counters (messages in/out, bytes, connection-minutes, retained count) summed in memory and sent to ledger `Meter` every 60 s. `Entitled` checked at CONNECT and every 60 s per tenant. |
| Audit | chronicle, batched: connect, disconnect, auth failure, ACL deny, takeover, admin actions. Never per message. |
| Control-plane storage | Postgres through grove, with grove's tenant hooks. |
| Large payloads | Phase 3: claim-check through trove for payloads above a threshold (default 512 KiB). Until then, max payload equals NATS `max_payload` (default 1 MiB). |
| License | MIT. |

## How the pieces work

### Tenancy inside mochi

Everything tenant-specific is prefixed server-side. A client of tenant `acme` publishing `sensors/1/temp` is stored, routed and matched as `acme/sensors/1/temp`, and the prefix is stripped on every outbound packet. Client IDs are namespaced the same way, so two tenants can both have a device called `pump-1`.

Hooks used (exact mochi names to be confirmed in the phase 0 spike):

- `OnConnectAuthenticate`: resolve credential → tenant, principal, roles; attach to the client.
- `OnConnect` and packet read hooks: namespace the client ID.
- `OnSubscribe`, `OnPublish`, `OnUnsubscribe`: add the prefix; run ACL and rate checks.
- `OnPacketEncode`: strip the prefix outbound, including `$SYS` and retained deliveries.
- `OnACLCheck`: match against the client's cached filter list.
- Session and QoS hooks: feed the cluster layer below.

`$SYS` is per tenant and shows only that tenant's stats. A tenant's `#` matches only that tenant's topics; `$`-prefixed topics follow MQTT rules.

mochi keeps its own in-memory state, and the cluster layer has to live with that:

- **Retained messages.** mochi stores retained messages in its trie and serves them on SUBSCRIBE itself, and `OnRetainMessage` can't veto storage. So mochi's retained store becomes each node's local cache of the cluster's retained KV: loaded per tenant when the node gets that tenant's first connection (SUBACK waits for the load), kept fresh by a KV watch, and written through to KV when a local client publishes a retained message.
- **Sessions.** The `Stored*` hooks are read only at boot. In a cell, a node never loads sessions at boot; it loads one session from KV at CONNECT and evicts it from mochi's memory after flushing on disconnect, because the next CONNECT may land elsewhere.

### Encoding topics and client IDs

One reversible encoding serves both NATS subjects and JetStream KV keys, so it must satisfy the stricter of the two: KV keys allow only `[-/_=.a-zA-Z0-9]`. Each MQTT level becomes one token; `.` separates tokens; a level made only of `[-_=a-zA-Z0-9]` passes through; any other level (UTF-8, `$`, `.`, whitespace, empty levels as in `a//b`) is written as a marker plus base64url without padding. Wildcards map `+` → `*` and `#` → `>`. Client IDs use the same encoding as a single token. The exact marker is fixed in phase 0 with a round-trip fuzz test.

`a/#` in MQTT also matches `a`; NATS `>` needs at least one more token, so the node subscribes to both `a` and `a.>`.

Subject layout per cell:

- QoS 0: `f.<tenant>.<encoded topic>`
- QoS 1/2 log: `d.<tenant>.<encoded topic>`
- KV keys: `s.<tenant>.<client>` (sessions), `r.<tenant>.<encoded topic>` (retained), `n.<node>` (membership)

### QoS 0 path

The publishing node publishes once to `f.<tenant>.<topic>`. Receiving nodes may get a message once per overlapping local filter (`a/+` and `a/#`), so each message carries a node-local ID and receivers deduplicate within a short window (borrow `forge/extensions/streaming/dedup.go`). Drops under load are allowed by QoS 0.

### QoS 1 and 2 path

1. The client publishes. Hooks namespace, check ACL, check the rate budget.
2. QoS 1: publish to `d.<tenant>.<topic>`, wait for the R3 ack, then let mochi send PUBACK. A client resend after a lost PUBACK may be stored twice. QoS 1 is at-least-once, so that is allowed, and no `Nats-Msg-Id` is used. (Packet IDs are reused after PUBACK, so a packet-ID-based message ID would make JetStream silently drop new messages as duplicates.)
3. QoS 2: first write a pending entry into the session key (packet ID → a fresh random message ID), then publish with that random ID as `Nats-Msg-Id`, then send PUBREC. A DUP resend finds the pending entry and reuses its ID, so JetStream deduplicates it; the entry is removed on PUBREL. The JetStream duplicate window must exceed the longest takeover time.
4. Every node serving the tenant receives the message from its ordered consumer on `d.<tenant>.>`, matches it against local subscriptions and delivers at min(publish QoS, granted QoS).

If the JetStream wait happens inside a blocking hook, one client's QoS 1 publishes are serialized at about one per commit round trip (hundreds per second per client). That's fine for devices. Phase 0 confirms it, and the fallback is a mochi patch that acks asynchronously.

Cost of this design: every node serving a tenant reads all of that tenant's QoS 1/2 traffic, so a big tenant spread over 10 nodes costs 10x reads. Phase 3 can pin large tenants to a subset of nodes at the load balancer, or narrow node consumers to non-overlapping filter sets. v1 accepts the fan-out because it gives gap-free delivery without a route table.

### Persistent sessions and cursors

A session with expiry > 0 is persistent. Its **cursor** is the highest log sequence such that every message for that tenant up to it has either been delivered and acknowledged or doesn't match the session. Because the node reads the tenant's log in order, non-matching messages advance the cursor too, so gaps in the stream's sequence numbers don't matter.

- Online: the cursor advances as PUBACKs arrive (MQTT requires in-order acks for QoS 1). It is written to the session key only when it changed, at most once per second per session, and on disconnect, using batched async publishes per node.
- Reconnect, takeover or node loss: the new owner loads the session and replays the tenant's log from `cursor + 1`, matching locally (a per-session filtered consumer can't be used, because JetStream rejects overlapping filter subjects and sessions often have overlapping filters). Replays for the same tenant on one node are batched into one consumer starting at the lowest cursor, so 100k sessions arriving after a node loss cost a handful of consumers, not 100k. When a session's replay reaches the node's live position, it switches to the live consumer, deduplicating by sequence across the handover.
- Outbound QoS 2 state is written to the session key before each QoS 2 send, so QoS 2 never duplicates across takeover. QoS 1 may.
- Clean sessions (expiry 0) still get a session key, for ownership. They have no cursor and are deleted on disconnect.

Retention of the log must cover the plan's session-expiry cap. Messages older than retention are gone for offline sessions; that limit is part of each plan and shown to tenants.

One log stream per cell, plus a dedicated stream for any tenant over a size threshold **(my call)**.

### Ownership, takeover and liveness

- Every CONNECT, clean or persistent, takes the session key with a compare-and-set that writes a new epoch and this node's ID. That is how a second CONNECT with the same client ID anywhere in the cell displaces the first, as MQTT requires. In a 100k reconnect storm this is about 1.7k CAS per second on one R3 bucket, which phase 0 measures.
- The previous owner watches the keys it owns. When the epoch changes, it disconnects the old connection with reason 0x8E (session taken over). Its next write would fail its CAS anyway. The new owner proceeds from the last persisted cursor.
- Session expiry and MQTT 5 message expiry use per-message TTL (`Nats-TTL` on a raw publish to the KV stream, with an expected-last-subject-sequence header so it stays a CAS). nats.go's `KeyTTL` only applies on create, so the cluster layer publishes to the KV stream directly. Buckets enable limit markers so expiries show up to watchers.
- Liveness doesn't depend on key expiry. Each node heartbeats its membership key every few seconds; every node watches the heartbeats and tracks the last time it saw each peer. A node that can't renew for half the lease drops all its clients. A peer unseen for the full lease plus a margin is declared dead. The lowest-ID survivor then publishes that node's pending wills (honoring MQTT 5 will delay) and clears its ownership so clients can take over elsewhere.

### Partitions

- **Isolated broker node** (can't reach JetStream): it drops all clients after half the lease, before survivors act on it after the full lease. It never serves a session that someone else might take over.
- **NATS minority side**: JetStream can't commit, so QoS 1/2 publishes get no ack and clients retry. Session CAS fails, so no takeovers happen there. Brokers on that side self-fence once their heartbeats fail.
- **QoS 0** keeps flowing among reachable nodes until a node self-fences.
- **After healing**: NATS re-syncs interest by itself, and cursor replay closes any gaps for persistent sessions.

### Route churn

- Only QoS 0 uses interest routing. A node subscribes to a filter on NATS for the first local subscriber and unsubscribes after the last, so 10,000 devices on one node subscribed to `cmd/+` cost one subscription.
- Per-device filters (`devices/<id>/cmd`) cost one subscription per device. Phase 0 load-tests NATS interest propagation at 1M distinct filters with a reconnect storm, and that test gates the design. If it fails, the fallback is one `f.<tenant>.>` subscription per node per served tenant, with local matching, the same shape as the QoS 1/2 path.
- QoS 1/2 routing has no churn at all: node consumers are per tenant, not per filter.
- Reconnect storms are throttled per tenant at CONNECT (token bucket), so one tenant's storm can't flood interest propagation or the session bucket.

### Shared subscriptions

`$share/<group>/<filter>` has two halves, because delivery QoS is min(publish, granted) and a group receives both kinds of publish:

- QoS 1/2 publishes: a JetStream durable pull consumer per (tenant, group, filter) on the log. Nodes with group members pull from it, and each message goes to one local member. Survives member and node loss.
- QoS 0 publishes: a NATS queue group on `f.<tenant>.<filter>`.

Groups are few compared with sessions, so a durable consumer each is affordable. Shared-group messages are excluded from the node's normal per-session delivery.

### Identity at CONNECT

Resolution order: JWT if the password parses as one; otherwise keysmith key. (mTLS by SAN in phase 4.)

- keysmith `ValidateKey` costs several store reads, so results are cached by key hash for 60 s. Revoke, rotate and suspend events reach every node over conduit and evict the entry.
- authsome JWTs are verified locally with golang-jwt against authsome's claims struct and JWKS (authsome's own `NewJWT` refuses a nil signing key, so it can't be used verify-only). Tenant comes from (`app_id`, `org_id`) mapped to a Tether tenant record, and the verifier enforces `iss` and `aud` itself (authsome's validator checks only `exp` and `nbf`). Prerequisite found in planning: authsome declares `org_id` and `env_id` claims but never sets them on issued tokens, so JWT auth waits on an upstream authsome change that stamps them (and re-mints on org switch). Until then keysmith keys are the only CONNECT credential.
- Device principals are keysmith keys scoped to the tenant. Backend services connect as authsome service accounts.

### Permissions

warden holds roles per tenant, with role metadata carrying `publish` and `subscribe` filter lists. At CONNECT the broker calls warden once, expands placeholders and compiles the filters into a small per-client trie. warden's glob matcher can't express MQTT `+` and `#`, which is why matching happens in the broker. Role-change events over conduit make nodes reload the affected clients' filters; a client that loses a subscribe permission has that subscription removed, and one that loses connect rights is disconnected.

### HTTP API

On the same forge app:

- `POST /v1/publish` (tenant from authsome token or keysmith key, idempotency via `forge.WithIdempotency`).
- Admin: tenants, devices (provision, issue or rotate key, assign roles, delete), clients (list, kick), sessions (inspect, delete), subscriptions, retained (list, delete, export, import), quotas, cell and node status. Key and role storage stays in keysmith and warden; device operations call them.
- Middleware: forge `auth` registry plus `Requirement`, `middleware.RateLimitWithKey`, `security` CORS and headers, audit to chronicle.

### Admin UI

Two halves, both built as part of this project, following how every other forgery repo ships its UI:

- **Contract (Go, in Tether):** the extension implements `RegisterContractContributor` with a manifest and intents for the admin surface: queries (overview, devices, clients, sessions, subscriptions, retained, tenants, quotas, cell nodes), commands (provision device, rotate key, kick, delete session, delete retained, set quota). Live views poll a stats query: forge-dashboard's plugin SDK has no subscription hook today. Templates: `forge/extensions/streaming/contract` and the `extension/contract/manifest.yaml` files in keysmith, warden and relay. In a cell, one node aggregates by querying peers over conduit RPC.
- **Companion plugin (TypeScript, in forge-dashboard):** a new `packages/plugin-tether`, modelled on `packages/plugin-streaming` and `packages/plugin-keysmith`, consuming those intents. Key and role screens are not rebuilt: `plugin-keysmith` and `plugin-warden` already render them, and Tether's plugin links to them for a device's key and roles.

### Observability

- Health: `Extension.Health` covers NATS connectivity, JetStream availability, the self-fencing state, the control-plane DB, and keysmith/authsome reachability.
- Metrics: forge `CounterVec` / `HistogramVec`. Tenant labels only on a short list of counters and only for the top N tenants by volume, so cardinality stays bounded; full per-tenant numbers go to ledger and the admin API.
- Tracing: forge `messaging` package, system `mqtt`. MQTT 5 carries `traceparent` in user properties; 3.1.1 publishes start root spans.

## Reuse map

| Need | Use | How |
|---|---|---|
| App shell, DI, config, lifecycle | forge | The broker is a forge app; the hooks and cluster layer are a forge extension so other forge apps can embed a single-node broker. |
| Health, metrics, tracing | forge root (`health.go`, `metrics_vec.go`, `tracing.go`, `messaging/`) | Directly. `internal/` packages aren't importable. |
| HTTP API auth, rate limit, idempotency | forge `extensions/auth`, `middleware`, `extensions/security`, `forge.WithIdempotency` | Directly. |
| Password hashing for any stored device secrets | forge `extensions/security` `PasswordHasher` | Directly. |
| Seeds for external NATS and node bootstrap | forge `extensions/discovery` | Consul, etcd or mDNS. On Kubernetes, copy the Endpoints-based lookup from `extensions/consensus/discovery/kubernetes.go`. Always set a stable instance ID. |
| Control-plane events and node RPC (key revoked, role changed, kick, admin aggregation) | forge `extensions/conduit` on the JetStream provider | Directly, on the same NATS. Not on the message path. |
| Coordinator shape, bounded per-client send queue, dedup | forge `extensions/streaming` (`coordinator/`, `sendqueue.go`, `dedup.go`) | Copy the patterns, not the package. |
| Admin UI | forge `extensions/dashboard` contract | Directly. |
| Bridges to external brokers, synthetic probes, test clients | forge `extensions/mqtt` | Directly (3.1.1 only). MQTT 5 tests use `paho.golang`. |
| Tenancy model, user/app JWTs, service accounts | authsome | JWT verify via golang-jwt with authsome claims; service accounts as backend principals. |
| Device credentials | keysmith | Extension registered in-process; `ValidateKey` behind the broker's cache; plugin hooks feed conduit events. |
| Roles and role metadata | warden | Extension registered in-process; coarse grants, filter matching in the broker; role hooks feed conduit events. |
| Key and role admin screens | forge-dashboard `plugin-keysmith`, `plugin-warden` | Reused as-is; `plugin-tether` links to them. |
| Metering, entitlements, plans | ledger | Aggregated `Meter` every 60 s; `Entitled` at CONNECT and every 60 s. |
| Audit | chronicle | Batched events. |
| Control-plane DB | grove | SQLite or Postgres, shared with keysmith and warden in one app. Single-node sessions use mochi's Badger hook, not grove KV (grove's Badger driver has no transaction support and adds nothing here). |
| Per-tenant secrets and config | vault | Phase 3 onward. |
| Large payload claim-check, message archive | trove | Phase 3 claim-check (S3 or local, compress and encrypt middleware). Phase 4 opt-in per-tenant archive of the log to object storage, for twin history replay. |
| Webhooks from topics | relay | Phase 4. Tenant rule = filter → relay event type. Filtered output only. |
| Background jobs (archive batches, retention sweeps, invoice runs) | dispatch | Phase 3 onward. |
| Tenant alerts (device offline, quota reached) | herald | Phase 4. |
| Cert reload pattern for listeners | bastion `security/tls.go` | Copy the pattern. |
| Do not use | forge `consensus` (followers don't apply entries; auto-snapshot crashes), `cache` (memory only), `events`, `queue`; grove CRDT (document sync, not a route table); sentinel, shield, cortex, weave; nexus beyond reading its Redis limiter | |

## Phases

**Phase 0: spikes (gate for everything else).** Throwaway code; answers recorded in this document.

1. mochi hook surface: namespacing client IDs and topics inbound, stripping outbound (`OnPacketEncode`), gating PUBACK on an async commit, injecting log messages for local delivery without re-publishing them, loading one session at CONNECT and evicting it after disconnect, and writing to and deleting from mochi's retained store from outside (for the KV-mirrored cache). List anything that needs a patch.
2. Encoding: lossless for all valid MQTT topics and client IDs, valid as both NATS subjects and KV keys, wildcards intact; round-trip fuzz test.
3. NATS interest at scale: 1M distinct filters across 10 nodes, plus a 100k-client reconnect storm. Measure interest propagation time and memory. Record the decision (interest routing or per-tenant fallback) here.
4. JetStream on the pinned version: R3 commit latency on target hardware; session-key CAS rate during a storm; per-message TTL with expected-last-subject-sequence on KV streams; limit markers reaching watchers; duplicate-window behaviour for QoS 2.

**Phase 1: single node.** mochi plus tenancy hooks, keysmith and authsome auth, warden ACLs, keysmith and warden extensions registered in-process with their event hooks, per-node rate limits, mochi's Badger storage hook, HTTP publish and admin API including device provisioning, dashboard contract and the first cut of `plugin-tether` (overview, devices, clients, sessions), health and metrics, MQTT over WebSocket via mochi's listener. TwinOS development runs against this.

**Phase 2: one cell.** NATS (embedded for small clusters, external for production), QoS 0 interest routing, the QoS 1/2 log with node consumers, sessions and cursors, ownership with CAS, takeover, liveness and self-fencing, wills on node loss, shared subscriptions, retained KV with mochi cache, conduit control events. Chaos and partition test suite and the load tests from the success criteria.

**Phase 3: product.** ledger metering and entitlements, chronicle audit, the rest of `plugin-tether` (subscriptions, retained, quotas, cell nodes, polled live rates), connection-share budget splitting, trove claim-check, vault per-tenant config, multiple cells with SNI routing and MQTT 5 redirect, tenant placement tooling, large-tenant node pinning.

**Phase 4: extras.** relay webhooks, outbound bridges, trove archive and replay, mTLS client auth, MQTT 5 re-auth, herald alerts, protocol gateways if demand appears.

## Testing

- Unit: encoding (fuzz), ACL trie, budget split, cursor advancement over non-matching messages, takeover state machine, QoS 2 pending-entry flow.
- Conformance: Paho interop suite for 3.1.1 and 5.0 against a single node and against a 3-node cell.
- Chaos (phase 2, nightly in CI): kill -9 of a broker node during QoS 1 load, asserting zero acknowledged loss on persistent sessions; kill -9 of a NATS server; network partitions with netem/toxiproxy asserting single serving owner, no minority-side acks, and self-fencing before takeover; slow-consumer injection asserting no cursor skips.
- Load: emqtt-bench or a Go harness on the forge mqtt client, against the success-criteria numbers.

## Upstream follow-ups (not blockers)

- warden: an MQTT-style topic matcher, so filters could live as warden resources directly.
- keysmith: a built-in validation cache with hook-driven invalidation.
- authsome: a verify-only JWT format (no signing key required).
- forge router: WebSocket subprotocol negotiation (`Sec-WebSocket-Protocol: mqtt`), so MQTT over WS could share the app's HTTP listener.
- forge discovery: a Kubernetes backend that lists pods, not Services.

## Prior art

- EMQX: replicated route table (Mria), durable sessions over a shared replicated store with per-session iterators. The cursor design here follows the same idea.
- NATS server's built-in MQTT support: maps MQTT onto NATS subjects and JetStream. Not used as-is because, as far as we know, it covers 3.1.1 rather than 5.0, its tenancy is NATS accounts rather than our stack, and it has no hook surface for keysmith, warden or ledger.
- VerneMQ, HiveMQ: clustered brokers with plugin systems; licensing and ecosystem fit rule them out as the base.

## Left to the builder

- Package layout inside the repo, as long as the forge extension is importable on its own.
- The exact encoding marker (phase 0 settles it; it must be lossless and KV-safe).
- Batch sizes, intervals, lease and margin values marked as defaults above.
- The dashboard plugin's visual design, within forge-dashboard's existing kit and plugin conventions.
- Load-test harness choice.
