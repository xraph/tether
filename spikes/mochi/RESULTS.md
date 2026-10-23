# mochi cluster hooks: results

Phase 0, task 1, against mochi-mqtt/server v2.7.9 and paho.golang v0.23.0, Go 1.26.9. All four questions come out yes, and none of them needs a mochi patch, so there's nothing in `patches/`. Two of the yeses come with conditions you have to build in, and one of them (question 4) changes how the QoS 1/2 path should be written.

To reproduce, from this directory:

```sh
GOWORK=off go test -race -short -v ./...   # questions 1 to 3, and the QoS 2 check
GOWORK=off go run . -runs 3                # question 4's table
```

Machine for every number below: Apple M3 Max, 16 cores, 128 GiB RAM (137438953472 bytes), macOS, shared with other work. It was very busy: the 1-minute load average sat between 70 and 117 during the measurements, on 16 cores. Each table is the median of 3 runs.

## 1. Loading a session at CONNECT

Yes. A hook can build the session in `OnSessionEstablish` and mochi takes it over as if it had been there all along.

The hook builds an offline client the way mochi's own `loadClients` restores one from storage: `srv.NewClient(nil, listener, id, false)`, protocol version, `Clean = false` and the session expiry from the store, each subscription into both `srv.Topics.Subscribe` and `cl.State.Subscriptions`, each inflight into `cl.State.Inflight`, `Info.Subscriptions` and `Info.Inflight` bumped, then `Stop` (so it's closed, with a disconnect time) and `srv.Clients.Add`. `inheritClientSession` runs right after the hook, finds that client under the same ID and takes it over.

Evidence, `TestLazySessionMaterialization`: the store holds subscription `a/#` and one QoS 1 inflight (packet ID 7). A 5.0 client connects with clean start false and gets:

- `CONNACK` with session present = true.
- The inflight resent straight after the CONNACK, with DUP set and its stored packet ID 7.
- `OnQosComplete` for packet 7 once the client acks it. That's where the cluster layer would advance the cursor.
- A new publish to `a/b` delivered at QoS 1 through the loaded subscription.
- `Info.Subscriptions` = 1 and `Info.Inflight` = 0 afterwards, so the takeover doesn't double count.

`TestLazySessionControls` covers the other two cases: nothing stored gives session present = false, and a clean start ignores the store (the hook deletes the entry, as the KV key would be).

Things to carry into Phase 2:

- The load has to run after Tether's hook namespaces `cl.ID`, in the same hook or one added after it.
- It runs on the connecting client's own goroutine, so a slow KV read delays that one CONNECT and nothing else.
- Skip it when `srv.Clients` already has the ID: that's a local takeover and mochi handles it.

Decision: load the session from KV in `OnSessionEstablish` by building an offline client the way `loadClients` does. No patch.

## 2. Evicting a session after DISCONNECT

Yes, but not with mochi's own calls. `srv.UnsubscribeClient` fires `OnUnsubscribed` and `cl.ClearInflights` fires `OnQosDropped` for every message, so any storage hook (badger, or Tether's own KV writer) would read the eviction as the client dropping its subscriptions and messages. `TestMochiDropFiresStorageEvents` shows both events firing.

`evictSession` in `main.go` does it by hand instead: `srv.Topics.Unsubscribe(filter, id)` and `cl.State.Subscriptions.Delete` for each subscription, `cl.State.Inflight.Delete` for each inflight, the two `Info` counters adjusted, then `srv.Clients.Delete(id)`. The hook flushes the session to the store first, from `OnDisconnect`.

Evidence, `TestEvictAfterDisconnect`: a persistent client with `a/#` and one unacknowledged QoS 1 message disconnects. After eviction, `srv.Clients` has no `dev-1`, `srv.Topics.Subscribers("a/b")` doesn't list it, both counters are 0, and the recorder hook saw 0 `OnUnsubscribed`, 0 `OnQosDropped` and 0 `OnClientExpired`. The store holds 1 subscription and 1 inflight. On reconnect the session loads again (session present = true), the inflight comes back first and `a/b` is delivered.

The eviction needs two guards, and each has a test that fails without it:

- The connection must still own the ID. `srv.Clients` is keyed by client ID, so deleting by ID after a takeover removes the new owner. Skip when `srv.Clients.Get(id)` returns a different client or `cl.IsTakenOver()`. `TestEvictSkipsTakenOverSession` shows the skip, then shows a plain `srv.Clients.Delete(old.ID)` removing the new connection.
- No CONNECT for the ID may be in flight. A reconnect can pass `OnSessionEstablish` while the old connection is still live (so nothing is loaded), and the old connection can then end before mochi looks the session up. Evicting at that point leaves the reconnect with an empty session. The hook counts connections between `OnSessionEstablish` and `OnSessionEstablished` under one mutex and skips eviction while the count is above 0. `TestEvictRacesReconnect` holds a CONNECT in that window: with the guard it gets session present = true and its subscription works; without it, session present = false and the subscription is gone.

Two gaps to know about. A message published for a session between its eviction and the next CONNECT isn't delivered locally, which is fine because cursor replay from the log covers it. And when mochi fails to write a CONNACK it returns without `OnSessionEstablished` or `OnDisconnect` (Tether's broker already works around the missing `OnDisconnect`), so the connecting count for that ID never drops and later evictions for it are skipped. The session then stays in memory until something clears it, so the Phase 2 code should drop the count in the same sweep that already catches those connections.

Decision: evict from `OnDisconnect` with direct `Topics`, `State` and `Clients` calls, after flushing, guarded by ID ownership and an in-flight CONNECT count. No patch.

## 3. Injecting a cluster message for local delivery

Yes, though `cl.Net.Inline` alone can't tell a cluster message apart. mochi's built-in inline client (`srv.Publish`) is also inline, and Tether's admin and HTTP device publishes go through it, and those must reach the cluster. Inject through a dedicated client, `srv.NewClient(nil, mqtt.LocalListener, "tether-cluster", true)` with protocol version 5, using `srv.InjectPacket`, and have the hook compare the client pointer.

Evidence, `TestInjectFromCluster`:

- A QoS 1 message injected with content type, a user property and a 60 s message expiry reaches a 5.0 subscriber at QoS 1 with all three properties. The hook's send-to-cluster counter stays at 0.
- `srv.Publish` (the admin path) also shows `cl.Net.Inline` = true in `OnPublish`. The counter goes to 1, as it should.
- A device's QoS 1 publish takes it to 2.
- An injected retained message lands in mochi's retained store (`srv.Topics.Messages("a/r")` has 1), and `OnRetainMessage` sees the injector, so the retained write-through to KV skips it.

Delivery still runs `OnACLCheck` for each subscriber, so the per-subscriber grants hold for cluster messages too.

Found on the way, not about clustering: mochi drops user properties from every packet it sends to a client that connected with Request Problem Information = 0, PUBLISH included. MQTT 5 ([MQTT-3.1.2-29]) only restricts packets other than PUBLISH, CONNACK and DISCONNECT. paho.golang sends 0 when you leave `RequestProblemInfo` false in `ConnectProperties`, so real devices will hit this. A hook can undo it, because mochi sets `pk.Mods.DisallowProblemInfo` before `OnPacketEncode`: clear it for PUBLISH (`keepPublishProps` in `main.go`). `TestUserPropertiesWithoutProblemInfo` shows the property lost without the hook and delivered with it. Tether's broker hook doesn't do this today, so it's a Phase 1 bug as well.

Decision: one dedicated inline injector per node, recognised by pointer in `OnPublish` and `OnRetainMessage`; add the `DisallowProblemInfo` fix to Tether's `OnPacketEncode`. No patch.

## 4. A commit inside OnPublish

Yes, it blocks, as the spec expected. mochi reads and handles each client's packets on one goroutine, so a commit inside `OnPublish` caps that client at one QoS 1 message per commit whatever its receive maximum. Other clients don't notice.

Setup (`go run .`, `runLoad` in `main.go`): `OnPublish` sleeps 2 ms for QoS 1 publishes from clients named `load-*`. That sleep really took about 2.3 ms on this machine (the "commit median" column), so the ceiling is roughly 435 msgs/s. The server's `Capabilities.ReceiveMaximum` is set per row, and paho keeps that many publishes outstanding. A separate probe client publishes QoS 0 to its own topic every 2 ms and times the round trip.

"async-ack" is the alternative: `OnPublish` starts the commit in a goroutine and returns `packets.ErrRejectPacket`, so mochi neither acks nor fans out, and the hook writes the PUBACK itself when the commit finishes, in arrival order.

First set (load average 70 to 83):

| configuration | QoS 1 msgs/s | probe p50 | probe p99 | commit median |
|---|---|---|---|---|
| baseline, probe only | | 197µs | 2.746ms | |
| blocking rm=1, 1 client | 356 | 199µs | 2.742ms | 2.317ms |
| blocking rm=10, 1 client | 399 | 209µs | 2.248ms | 2.285ms |
| blocking rm=100, 1 client | 364 | 224µs | 3.731ms | 2.339ms |
| async-ack rm=1, 1 client | 350 | 193µs | 1.995ms | 2.332ms |
| async-ack rm=10, 1 client | 3384 | 212µs | 2.709ms | 2.297ms |
| async-ack rm=100, 1 client | 23300 | 259µs | 9.557ms | 2.157ms |
| blocking rm=10, 50 clients | 17841 | 250µs | 5.984ms | 2.223ms |

Second set, later (load average 96 to 117):

| configuration | QoS 1 msgs/s | probe p50 | probe p99 | commit median |
|---|---|---|---|---|
| baseline, probe only | | 202µs | 2.053ms | |
| blocking rm=1, 1 client | 341 | 189µs | 3.392ms | 2.304ms |
| blocking rm=10, 1 client | 394 | 179µs | 2.82ms | 2.279ms |
| blocking rm=100, 1 client | 374 | 185µs | 3.247ms | 2.295ms |
| async-ack rm=1, 1 client | 372 | 176µs | 1.826ms | 2.297ms |
| async-ack rm=10, 1 client | 3417 | 202µs | 4.05ms | 2.266ms |
| async-ack rm=100, 1 client | 16696 | 293µs | 18.638ms | 2.164ms |
| blocking rm=10, 50 clients | 19190 | 215µs | 4.786ms | 2.209ms |

What the numbers say:

- Blocking: 341 to 399 msgs/s for one client at receive maximum 1, 10 and 100. The window buys nothing.
- The probe's p50 stays at about 0.2 ms under blocking load, and its p99 stays within about 1.7x a baseline that is itself noisy on this machine. The blocked client holds up nobody else.
- 50 blocked clients together reach 17.8k to 19.2k msgs/s, about 50 times one client, so the cap is per client and not per node.
- Async-ack scales with the window: about 3.4k msgs/s at receive maximum 10, 17k to 23k at 100. The probe's p99 climbs in that last row because the load generator, the broker and the probe share one busy machine in one process; p50 barely moves.

`TestBlockingOnPublish` is the short form with assertions (blocking at receive maximum 100 no faster than one per commit and no faster than at 1, async-ack at 10 more than 3x blocking, probe p99 inside a loose bound). `TestAsyncAckQoS2` checks the async-ack path for QoS 2: the hook puts the PUBREC into `cl.State.Inflight` before writing it, mochi's own PUBREL handling then answers PUBCOMP, and 100 of 100 publishes complete with the inflight counter back at 0.

If you go with async-ack, the hook owns three things mochi normally does:

- Receive maximum. mochi only spends a client's receive quota after `OnPublish` accepts the packet, so a rejected one never counts and mochi never disconnects a client that exceeds it. The hook has to count and disconnect with 0x93.
- Ack order. QoS 1 acks must go out in the order the PUBLISHes came in; `writeAcks` waits on each commit in turn.
- Everything after `OnPublish`: retained handling, `OnPublished` and local fan-out are skipped. In this design that's what you want anyway, since QoS 1/2 delivery comes from the log and retained writes go to KV.

That last point matters for the blocking variant too. If `OnPublish` returns the packet, mochi fans it out locally and the log delivers it again. From reading `processPublish` (not tested here), returning `packets.CodeSuccessIgnore` sets `pk.Ignore`, which still sends the PUBACK but skips fan-out and retained storage, so a blocking hook would use that. Blocking also stops the client's goroutine from reading PUBACKs for its own deliveries while a commit runs.

Decision: no mochi patch, and the async-ack fallback needs none either. Write the Phase 2 QoS 1/2 path as async-ack from the start (commit off the read loop, `ErrRejectPacket`, hook-written PUBACK or PUBREC in order, receive maximum enforced by the hook). Blocking at about 400 msgs/s per client would do for devices, but async-ack costs little more code and also keeps mochi's local fan-out out of the way.

## Also found

mochi's `Server.Close` deadlocked one run of the harness: `Clients.GetByListener` read-locks twice and a client's `Clients.Delete` slipped in between, the bug Tether's broker already works around. The harness and tests now wait on `srv.Listeners.ClientsWg` before closing (`closeServer`). The aborted run's numbers aren't used above.
