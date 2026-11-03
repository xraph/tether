# Paho interoperability run

This directory has what you need to run the Eclipse Paho interoperability suite against Tether, on one node or on a three-node cell. The results of the first run are in [RESULTS.md](RESULTS.md).

Nothing here runs with `go test ./...`. The harness is a plain `main` package, and the Python files only run when you start them.

## Rerun it

You need Go 1.26, Python 3 (the suite uses nothing outside the standard library) and about fifteen minutes. Clone the suite somewhere that isn't this repo:

```sh
git clone --depth 1 https://github.com/eclipse/paho.mqtt.testing /path/to/paho-testing
git -C /path/to/paho-testing log -1 --format=%H    # write this down
```

Then run all four combinations from the repo root:

```sh
GOWORK=off interop/run.sh /path/to/paho-testing /path/to/out
```

That builds the harness, then runs `client_test.py` (MQTT 3.1.1) and `client_test5.py` (MQTT 5.0) against a single node and against a cell. Each run leaves `<name>.out` (the suite's output, with one `RESULT` line per test at the end) and `<name>.broker.log` in the output directory. Add `spread` as a third argument and the cell runs go through a round-robin proxy, so the suite's two clients end up on different nodes.

The suite is third-party code. The scripts here run it with `python3 -I` from a directory that isn't its own, and `run_suite.py` adds the suite to the import path itself. Do the same if you run it by hand.

## The pieces

`main.go` starts the broker. With `-mode single` you get one node. With `-mode cell` you get three, each with its own embedded NATS server and JetStream store in a temp directory, wired the way the Forge extension wires a node (control bus first, members after the broker). It prints one `node n1 mqtt 127.0.0.1:18831` line per node, then `ready`, and runs until SIGINT or SIGTERM.

Flags worth knowing:

| Flag | Default | What it does |
|---|---|---|
| `-mode` | `single` | `single` or `cell` |
| `-port` | `18831` | MQTT port of the first node; the others take the next ports |
| `-nats-port` | `14222` | NATS client port of the first cell node; routes listen 100 above |
| `-grant` | `#` | comma-separated filters granted for publish and subscribe |
| `-log-level` | `info` | `debug` is very noisy; mochi prints whole packets |

The suite sends no username and no password, so the harness authenticates any client into one tenant with `auth.NewStatic` (keyed on the empty username) and grants everything in `-grant`. A `#` grant never covers a topic that starts with `$`. That matters for `test_dollar_topics`: pass `-grant '#,$TopicA/#'` and it passes.

`run_suite.py` imports one of the suite's test modules and runs it with a per-test timeout (120 seconds by default). It prints `RESULT <ok|FAIL|ERROR|TIMEOUT|skip> <test>` for every test. The suite's own `__main__` blocks parse the host and port in ways that fight with unittest, which is why this wrapper exists. Use `--only test_name` (repeatable) to run a few tests, and `--timeout` to shorten the wait.

`spread_proxy.py` relays MQTT and logs every packet as hex with its direction. Point the suite at it when you need to see what the broker sent. Give it several target ports and it rotates connections across them.

`probe_will_delay.py` reproduces the lost delayed will described in RESULTS.md. Start `main.go` in cell mode, then run `python3 -I probe_will_delay.py /path/to/paho-testing 18831 2 5` (session expiry 2 seconds, will delay 5 seconds). Run it against `-mode single` too and compare.

## Things that trip people up

Each run needs a fresh broker. The suite leaves retained messages and sessions behind, and `test_retained_message` fails before it cleans up, which then breaks `test_subscribe_identifiers`. `run.sh` starts a new broker for every run.

`test_subscribe_failure` can't pass with this harness. It wants a topic (`test/nosubscribe`) that the broker refuses, and Tether's grants only allow, so a `#` grant can't carve out an exception.

Lint the harness with `golangci-lint run ./interop/...` from the repo root.
