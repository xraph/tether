# MQTT interop: not run yet

The Eclipse Paho interoperability suite has not been run against Tether. Phase 1 shipped without it, and it moves to Phase 2's gate. Nothing in this repo downloads or runs it, because it's third-party Python code, and Rex decides when it gets run.

Until it runs, what we know about protocol behaviour comes from Tether's own tests: paho.mqtt.golang (3.1.1) and paho.golang (5.0) clients against a real broker, plus raw packet tests where a client library would hide what the broker sent. Those cover what Tether changes. They don't cover the corners of MQTT that mochi handles on its own, which is most of what the suite tests.

## What to run

The suite lives in [eclipse/paho.mqtt.testing](https://github.com/eclipse/paho.mqtt.testing), under `interoperability/`:

- `client_test.py` for MQTT 3.1.1
- `client_test5.py` for MQTT 5

Point both at a running Tether broker. Check each script's `--help` for the host and port flags of the version you check out, and pin the commit you ran in the results.

## Before you run it

The suite connects without a username or password, and the `tether` binary refuses that: every client has to present a keysmith key or an authsome JWT. So you need a small Go harness that builds a `broker.Broker` with `auth.NewStatic`, mapping the suite's client IDs to one tenant, and a `PermLoader` that grants `#` for publish and subscribe. `broker/testutil_test.go` builds brokers the same way.

Expect a few failures that are Tether's design and not bugs:

- Topic aliases are off (`TopicAliasMaximum` is 0), so tests that use them will see the alias refused.
- Mochi's global `$SYS/broker/...` topics are hidden from every client. A tenant only sees its own `$SYS/...`.
- Topics and client IDs are namespaced per tenant on the server. A client never sees the prefix, but a test that compares server-side IDs will.

Write down every other failure, with the test name and the packets involved, and decide for each one whether it's mochi, Tether's hook or the test.
