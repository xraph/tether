# Paho interoperability results

First run of the Eclipse Paho interoperability suite against Tether, on 2 November 2026. How to rerun it is in [README.md](README.md).

| | |
|---|---|
| Suite | `eclipse/paho.mqtt.testing` at `fdab9d2b1ce125385135e5d9f282a0ad315268cc` (31 October 2026) |
| Tether | `bd522ed75de7c9f20eaf0ffa7bc96ee1793942dc` (`test/paho-interop`, cut from main) |
| Protocol library | mochi-mqtt v2.7.9 |
| Machine | MacBook Pro, Apple M3 Max, 16 cores, 128 GiB, macOS 27.0.1 |
| Toolchain | Go 1.26.9, Python 3.11.6 |
| Auth and grants | any client into one tenant, `#` for publish and subscribe |

## Totals

| Run | Pass | Fail |
|---|---|---|
| MQTT 3.1.1, single node | 8 of 10 | 2 |
| MQTT 3.1.1, three-node cell | 8 of 10 | 2 |
| MQTT 5.0, single node | 19 of 27 | 8 |
| MQTT 5.0, three-node cell | 22 of 27 | 5 |

Both clients of a test connect to the node you point the suite at, so the cell runs above only exercise one node's MQTT listener plus the cell machinery behind it. Two extra runs sent the suite's connections round-robin across all three nodes (the "spread" columns below): 8 of 10 for 3.1.1 and 21 of 27 for 5.0.

Twelve distinct tests failed somewhere. One of them, `test_will_delay` in a cell, is a Tether bug. The other eleven come from the harness, the suite's own Python client, mochi, a feature Tether doesn't offer, or an earlier failure leaving state behind. One test that passed also showed a 21 second delivery stall in the cell, which we couldn't reproduce. The sections after the tables sort all of that out.

## MQTT 3.1.1 (`client_test.py`)

| Test | Single | Cell | Spread |
|---|---|---|---|
| testBasic | ok | ok | ok |
| test_dollar_topics | FAIL | FAIL | FAIL |
| test_keepalive | ok | ok | ok |
| test_offline_message_queueing | ok | ok | ok |
| test_overlapping_subscriptions | ok | ok | ok |
| test_redelivery_on_reconnect | ok | ok | ok |
| test_retained_messages | ok | ok | ok |
| test_subscribe_failure | FAIL | FAIL | FAIL |
| test_unsubscribe | ok | ok | ok |
| test_zero_length_clientid | ok | ok | ok |

## MQTT 5.0 (`client_test5.py`)

| Test | Single | Cell | Spread |
|---|---|---|---|
| test_assigned_clientid | ok | ok | ok |
| test_basic | ok | ok | ok |
| test_client_topic_alias | ok | ok | ok |
| test_dollar_topics | FAIL | ok | ok |
| test_flow_control1 | ok | ok | ok |
| test_flow_control2 | ERROR | ok | ERROR |
| test_keepalive | ok | ok | ok |
| test_maximum_packet_size | ok | ok | ok |
| test_offline_message_queueing | ok | ok | ok |
| test_overlapping_subscriptions | ok | ok | ok |
| test_payload_format | ok | ok | ok |
| test_publication_expiry | ok | ok | ok |
| test_redelivery_on_reconnect | ok | ok | ok |
| test_request_response | ok | ok | ok |
| test_retained_message | FAIL | ok | ok |
| test_server_keep_alive | FAIL | FAIL | FAIL |
| test_server_topic_alias | FAIL | FAIL | FAIL |
| test_session_expiry | ok | ok | ok |
| test_shared_subscriptions | ok | ok | ok |
| test_subscribe_failure | FAIL | FAIL | FAIL |
| test_subscribe_identifiers | FAIL | ok | ok |
| test_subscribe_options | ok | ok | ok |
| test_unsubscribe | ok | ok | ok |
| test_user_properties | TIMEOUT | ok | ok |
| test_will_delay | ok | TIMEOUT | TIMEOUT |
| test_will_message | ok | FAIL | FAIL |
| test_zero_length_clientid | ok | ok | ok |

A TIMEOUT means the test hung until our 120 second per-test alarm fired. The suite has no timeout of its own.

## Tether bugs

### A delayed will is lost in a cell when the will delay is at least the session expiry

Test: `test_will_delay` (5.0), third phase. Cell and spread runs. Single node is fine.

The client connects with Session Expiry Interval 2 and Will Delay Interval 5, then drops its socket. MQTT 5 says the will goes out when the delay ends or the session ends, whichever comes first, so it should arrive after about 2 seconds. In a cell it never arrives. The first two phases (delay shorter than the expiry, and expiry 0) pass.

Packets, from a capture of the test run on its own against a cell (client `myclientid`, subscriber `myclientid2`, same node):

```
C>S CONNECT  ... 05 11 00000002 (session expiry 2) ... 05 18 00000005 (will delay 5) ... "test_will_delay will message"
S>C CONNACK  success
   (subscriber SUBSCRIBEs to the will topic, gets SUBACK)
C>S CLOSE    (socket dropped, no DISCONNECT)
   ... nothing more, for the 110 seconds until the alarm
```

We probed it with `probe_will_delay.py` against a fresh cell (seconds until the will, or none in 12):

| Session expiry | Will delay | Result |
|---|---|---|
| 5 | 3 | 3.01 s |
| 10 | 4 | 4.00 s |
| 2 | 1 | 1.00 s |
| 0 | 5 | 0.02 s |
| 3 | 5 | 3.01 s |
| 2 | 2 | 2.02 s once, none four times |
| 3 | 3 | 3.01 s once, none once |
| 2 | 5 | none, every time |
| 1 | 5 | none, every time |
| 4 | 8 | none |

On a single node the will arrived in all five tries (inputs 2/2, 2/2, 2/5, 1/5 and 3/3), 1.6 to 3.8 seconds after the drop. That is later than the expiry, but it arrives.

Diagnosis. `clusterWill` clamps the delay to the session expiry (`broker/clusterlive.go:466`), so the will is due at the same moment the session ends. `finish` writes the session record with that expiry as its key TTL (`broker/clustersession.go:1351`, `Sessions.Expire` at `:1638`) and schedules the will for `willAt + delay` (`:1379`). When the timer fires, `tryFire` reads the key and treats `tether.ErrNotFound` as "the session came back and cancelled the will" (`broker/clusterlive.go:1029`), so it quietly drops it. A temporary log line (reverted, not in the commit) in that branch printed `session key gone` at the moment each lost will was due, so the key had already expired by then. The TTL and the timer are racing at the same second, and the key wins more often than not. A key that outlives the will delay by a margin, or a key-expiry marker that publishes the will, would close it. Not fixed here.

### QoS 1 and 2 deliveries stalled for 21 seconds in a cell (not reproduced)

Test: `test_user_properties` (5.0), cell run after `test_dollar_topics`. We saw it twice and then could not make it happen again in six tries, with the same two tests and with a raw-socket probe that sends SUBSCRIBE, a QoS 1 PUBLISH and a QoS 2 PUBLISH in one write.

The client subscribes to `TopicA` at QoS 2 and publishes three messages right behind the SUBSCRIBE. From the second capture, relative to the publishes (the acks are from the first, where they also came straight away):

```
+0.000  C>S SUBSCRIBE, PUBLISH q0, PUBLISH q1, PUBLISH q2   (one burst)
+0.001  S>C SUBACK
+0.006  S>C PUBLISH q0 (user properties a=2, c=3)
+0.003  S>C PUBACK and PUBREC for the q1 and q2 publishes
+21.49  S>C PUBLISH q1 and PUBLISH q2
```

The test passes (it waits with no deadline), but the two messages were 21 seconds late. The broker log shows only `publish packet rejected hook=tether` for the two publishes, which is the normal cell path. A quick search turned up no 21 second constant in `broker/` or `cluster/`. Treat it as a race between the subscription becoming visible to the log reader and the append, hit only under some earlier state. It needs a debug build with Tether-level logging to chase.

## Not Tether bugs

### Harness and suite configuration

`test_subscribe_failure` (both protocols, every run). The suite wants a SUBSCRIBE to `test/nosubscribe` refused. The harness grants `#`, which covers it, and the broker answered with a granted QoS 2 (`9003000202`). Grants in Tether only allow, so no rule set can grant `#` and still refuse one topic. Skipping this test is the honest option for this harness.

`test_server_keep_alive` (5.0, every run). The suite's broker is configured with a server keep alive of 60 seconds and the test expects the CONNACK to say so. Tether's CONNACK carries only Receive Maximum 1024 (`2006000003210400`). Tether has no server keep alive setting, so this is a feature it doesn't offer. mochi supports it per client through its `OnConnect` hook.

`test_server_topic_alias` (5.0, every run). Topic aliases are off by design (`TopicAliasMaximum` 0, `broker/broker.go:216` and `broker/hook.go:434`), so the broker never assigns one. `test_client_topic_alias` passes; it checks that the broker refuses alias 0.

`test_dollar_topics` (3.1.1 every run, 5.0 single node). The test publishes QoS 1 to `$TopicA/B`. A `#` grant never covers a `$` topic, so the ACL denies it. What the client sees depends on who answers:

- 3.1.1: mochi's `processPublish` calls `DisconnectClient` (`server.go:873` in v2.7.9), which writes a DISCONNECT (`e000`) to the client before closing. MQTT 3.1.1 doesn't let a server send DISCONNECT, and the suite's client raises on it. That one is a mochi quirk. The connection does have to close, since 3.1.1 has no negative PUBACK. We didn't look for a way to close it without the DISCONNECT.
- 5.0 single node: a PUBACK with reason 0x87 and the reason string "not authorized", 21 bytes. The suite's Python client asserts a PUBACK is 2 to 4 bytes (`MQTTV5.py:1108`) and dies. The spec allows properties there, so that is the client's limit.
- 5.0 cell: PUBACK `4003000387`, 5 bytes. The client takes it and the test passes.

With `-grant '#,$TopicA/#'` the test passes on 3.1.1 and on 5.0 single node.

### The suite's Python client

`test_retained_message` and `test_user_properties` (5.0 single node). Same cause. On a single node mochi answers a QoS 1 publish with `40 12 0002 01 0e 2600016100013226000163000133`: reason code 0x01, then the publisher's two user properties copied onto the ack. The client rejects the 18-byte PUBACK and its receive thread dies. `test_retained_message` then fails on a leftover outbound message at disconnect, and `test_user_properties` waits forever for messages the client can no longer read. In the cell the PUBACK is `40 02 0003` and both tests pass.

`test_flow_control2` (5.0, single and spread; passed once in the cell). The broker does the right thing: after 1024 PUBRECs it sends DISCONNECT 0x93 "receive maximum exceeded" (`e01d931b1f0018...`) and closes. The client answers each PUBREC with a PUBREL, and gets `BrokenPipeError` writing to the closed socket before it reads the DISCONNECT. It's a race in the test, and it failed in three of the four runs we made (single node, single node on its own, spread).

### Cascades

`test_subscribe_identifiers` (5.0 single node). `test_retained_message` fails before its `cleanRetained()` call, so three retained messages are still there when this test subscribes, and it counts 3 messages where it expects 1. The test passes on its own and in the cell.

`test_will_message` (5.0 cell and spread). It runs straight after `test_will_delay` has hung for 120 seconds. In the failing run the broker published the will 3.0 seconds after the CONNECT (a QoS 2 PUBLISH to the subscriber), and the suite's client never sent PUBREC. The test passes by itself, and also passed once when the preceding hang was cut to 30 seconds, so it isn't a Tether fault, but we haven't pinned down why the client stays silent. It should pass once the delayed will is fixed and the hang goes away.

## Other things we saw

mochi's PUBACK for a QoS 1 publish carries reason code 0x01 (Granted QoS 1) on a single node. 0x01 isn't a valid PUBACK reason. It comes from `packets.QosCodes` in `server.go:942`. Tether could set mochi's `Compatibilities.NoInheritedPropertiesOnAck` to stop the user property echo, and nothing in `broker/` sets it today. The reason code is mochi's to fix. Cell mode builds its own acks and sends a plain success, so the two modes disagree on the wire.

One `shared: pull failed ... no responders available for request` warning on node n3 mid-run in the spread 5.0 run, at WARN level. Every test around it passed.

The mochi-global `$SYS` and per-tenant namespacing failures that the design notes predicted never showed up: the suite doesn't test either.

3.1.1 `test_offline_message_queueing` prints that the server doesn't queue QoS 0 messages for offline clients, and `test_overlapping_subscriptions` prints that it sends one message for all overlapping subscriptions. Both are informational and both tests pass.
