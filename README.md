# Tether

Tether is a multi-tenant MQTT broker for Go. Every client that connects gets resolved to a tenant and a device, devices sign in with [keysmith](https://github.com/xraph/keysmith) keys, and [warden](https://github.com/xraph/warden) roles decide which topics each one can publish to and subscribe to. You can run it as a single binary, or mount it in your own [Forge](https://github.com/xraph/forge) app as an extension. The protocol work is done by [mochi-mqtt](https://github.com/mochi-mqtt/server), and Tether adds the tenancy around it.

Module path: `github.com/xraph/tether`. MIT licensed.

Status: Phase 1, one node. It runs and the tests pass, but nothing has been released yet, so pin a commit.

## Quick start

You need Go 1.26 or later. From a checkout:

```bash
go build -o tether ./cmd/tether
export TETHER_ADMIN_TOKEN="$(openssl rand -hex 32)"
./tether
```

That starts MQTT on `:1883` and the HTTP API and dashboard on `:8080`. Everything it stores lands under `./data`: a SQLite file for tenants, devices and keys, and a Badger directory for sessions and retained messages. Stop it with Ctrl-C and it shuts down cleanly.

No token set? It refuses to start and tells you so. On a laptop you can pass `--insecure-admin` instead, which leaves the admin API and Tether's dashboard pages open to anyone who can reach port 8080, with every tenant in reach. Don't do that on a server. The flag opens the dashboard pages even when a token is set: with both, the REST API still asks for the token, but Tether's dashboard pages answer anonymous requests as an operator. `tether --version` prints the version.

Create a tenant. The `app_id` and `external_id` are yours: the pair maps a Tether tenant to whatever your own system calls the customer.

```bash
curl -s -X POST localhost:8080/tether/v1/tenants \
  -H "Authorization: Bearer $TETHER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"app_id":"aapp_demo","external_id":"acme","name":"Acme"}'
```

The reply has the tenant's `id`, which looks like `ttnt_01m5...`. Next comes a role. A role carries topic grants, and `%c` and `%u` expand to the client ID and the username when the device connects. Tether owns the roles of its tenants, so you create one with the same admin token (warden's own routes are switched off in the binary, and nothing here needs them):

```bash
curl -s -X POST localhost:8080/tether/v1/tenants/$TENANT_ID/roles \
  -H "Authorization: Bearer $TETHER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"slug":"sensor","publish":["sensors/%c/up"],"subscribe":["sensors/%c/#"]}'
```

Only `slug` is required. You can also pass a `name` and a `parent_slug` to inherit another role's grants, list a tenant's roles with `GET .../roles`, replace a role's topics later with `PUT .../roles/sensor/topics`, and remove one with `DELETE .../roles/sensor` once no device holds it.

Then give the tenant a device with that role, pinned to the client ID it will connect with:

```bash
curl -s -X POST localhost:8080/tether/v1/tenants/$TENANT_ID/devices \
  -H "Authorization: Bearer $TETHER_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"pump","client_id":"pump-1","roles":["sensor"]}'
```

`%c` only keeps devices apart when they're pinned like this, because an unpinned device can connect with any client ID it likes, another device's included, so give unpinned devices grants on `%u` (the key ID) instead.

You get back a `username` (a keysmith key ID, `akey_...`) and a `password` (the raw key, `tdk_live_...`). This is the only time you see the password. Tether stores a hash, so if the device loses it you rotate the key with `POST .../devices/$DEVICE_ID/rotate`. A device with no role can connect, and that's all it can do.

Now connect with any MQTT client, MQTT 3.1.1 or 5. The username and password are the ones from the device reply:

```bash
mosquitto_pub -h localhost -p 1883 -i pump-1 \
  -u "$USERNAME" -P "$PASSWORD" \
  -t sensors/pump-1/up -m 21.5
```

Topics are yours to name. Tether adds the tenant prefix on the way in and takes it off on the way out, so two tenants can both use `sensors/pump-1/up` and never see each other. Devices that can't speak MQTT can post to `POST /tether/v1/publish` with HTTP Basic auth, using the same username and password.

Revoke a device with `POST .../devices/$DEVICE_ID/revoke` and its live connection drops within moments. For a device pinned to a client ID, revoking or deleting it also deletes its session. An unpinned device's sessions just run out on their expiry, since Tether doesn't know which client IDs it used. Narrow a role's topics and any subscription the device no longer has the right to is removed on the spot.

## Use Tether as a Forge extension

The binary is a small Forge app, and you can build your own. The extension needs three things in one app: a `*grove.DB` in the container, keysmith, and warden. Tether registers a plugin with both so that revoking a key or editing a role reaches live connections, which means you have to build keysmith and warden through the extension's helpers.

```go
package main

import (
	"context"
	"log"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	keysmithext "github.com/xraph/keysmith/extension"
	"github.com/xraph/vessel"
	wardenext "github.com/xraph/warden/extension"

	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/store/sqlite"
)

func main() {
	drv := sqlitedriver.New()
	if err := drv.Open(context.Background(), sqlite.DSN("./tether.db")); err != nil {
		log.Fatal(err)
	}

	db, err := grove.Open(drv)
	if err != nil {
		log.Fatal(err)
	}

	app := forge.New(forge.WithAppName("my-app"))

	// Keysmith, warden and Tether all find this database in the container.
	if err := vessel.Provide(app.Container(), func() (*grove.DB, error) { return db, nil }); err != nil {
		log.Fatal(err)
	}

	t := extension.New(extension.WithAdminMiddleware(myAdminAuth))

	for _, x := range []forge.Extension{
		t.Keysmith(keysmithext.WithDisableRoutes()),
		t.Warden(wardenext.WithDisableRoutes()),
		t,
	} {
		if err := app.RegisterExtension(x); err != nil {
			log.Fatal(err)
		}
	}

	log.Fatal(app.Run())
}
```

`myAdminAuth` is a `forge.Middleware` of yours. Tether adds no admin authentication of its own, and `Start` fails with `ErrAdminAuthRequired` when the admin routes are on and you gave it neither a middleware nor `WithInsecureAdminRoutes()`. If keysmith or warden were built some other way, `Start` fails with `ErrKeysmithNotWired` or `ErrWardenNotWired`. `WithoutKeysmithEvents()` and `WithoutWardenEvents()` turn those checks off, at the price of live connections ignoring key and role changes until they reconnect.

Add `dashboard.NewExtension()` from `github.com/xraph/forge/extensions/dashboard` if you want the Tether pages. Tether publishes 24 dashboard intents (session info, overview, tenants, devices, roles and their topic grants, live clients, sessions, retained messages), and it answers all of them `UNAUTHENTICATED` unless the request carries a signed-in user. The dashboard's auth is off by default, so out of the box you see the plugin and its refusal. Turn the dashboard's auth on, or pass `WithInsecureDashboard()` if only people you trust can reach it.

A signed-in user only sees their own tenant. Tether takes it from the session the way keysmith does: a `tenant_id` claim if your auth provider sets one, otherwise the org of the forge scope your auth middleware puts on the request (authsome does this for you). That `(app, org)` pair has to match a tenant's `app_id` and `external_id`. Ask for another tenant and you get `PERMISSION_DENIED`, and a user whose session names no tenant is refused outright. If the people on your dashboard run the whole node, pass `WithDashboardOperators()` (or set `dashboard_operators: true`) and every signed-in user becomes an operator: any tenant, plus creating, changing and deleting tenants. `WithInsecureDashboard()` implies it, since an anonymous caller has no tenant to be held to.

Metrics go through the app's `forge.Metrics`: `tether_connections`, `tether_messages_in_total`, `tether_messages_out_total`, `tether_auth_failures_total{reason}` and `tether_acl_denials_total{op}`. Each package has an `llm.txt` with its full surface.

## Users and apps over JWT

Devices aren't the only clients. A person or a service account can connect with a JWT that authsome issued, as long as you point Tether at authsome's keys:

```yaml
tether:
  jwt:
    jwks_url: "https://auth.example.com/.well-known/jwks.json"
    issuer: "https://auth.example.com"
    audience: ["tether"]
    # algorithms: [RS256, EdDSA, ES256]   # the default
    # refresh_interval: 10m               # the default
    # leeway: 30s                         # the default
```

In your own app that's `extension.WithJWT(auth.JWTConfig{...})`. Leave the block out and nothing changes: only device keys connect.

The client sends the JWT as the MQTT password and the token's `sub` as the username. A password with three base64url segments that starts `ey` goes to the JWT check, anything else goes to keysmith, so one broker takes both. Tether checks the signature against the JWKS, then `exp`, `nbf`, `iss` and `aud`. It checks `iss` and `aud` itself because authsome's own validator doesn't, and a token minted for another service would otherwise pass. HMAC algorithms and `alg: none` are refused, and so is a config that lists them.

The tenant comes from the token's `app_id` and `org_id`, the same pair a keysmith key carries. Warden roles assigned to the user (or service account) decide the topics, with `%u` expanding to the `sub`. Connections close when the token expires, with reason code 0xA0, and a role change reaches the open connection the way it does for a device.

Some tokens are refused on purpose. Agents and workloads can't connect yet. A token bound to a DPoP key needs a proof MQTT can't carry, and one issued to a third-party OAuth client has a scope Tether can't enforce.

One prerequisite, and it's on the authsome side: today authsome doesn't put `org_id` or `env_id` on the tokens it issues. Tether refuses a token without `org_id` (the error names the claim), so this works the day authsome stamps it and not before. The device publish endpoint (`POST <base>/v1/publish`) stays keysmith-only either way.

## Configuration

The binary reads `--config <path>`, or `./tether.yaml` when that exists, and uses the defaults when it doesn't. Leave a key out and it keeps its default. [`tether.example.yaml`](tether.example.yaml) lists every key with a comment, and a test checks that it matches the defaults.

```yaml
server:
  http_address: ":8080"
  sqlite_path: "./data/tether.db"

tether:
  base_path: "/tether"
  broker:
    tcp_address: ":1883"
    websocket_address: ""
    data_dir: "./data/sessions"
    max_session_expiry: 168h
    max_packet_size: 1048576
    max_topic_length: 1024
    max_inflight: 1024
```

| Key | Default | What it does |
| --- | --- | --- |
| `server.http_address` | `:8080` | Admin API, device publish endpoint and dashboard |
| `server.sqlite_path` | `./data/tether.db` | Tenants, devices, keys and roles. The binary only does SQLite |
| `tether.base_path` | `/tether` | Prefix for every REST route |
| `tether.disable_routes` | `false` | Mounts no REST routes. The broker still runs |
| `tether.grove_database` | empty | Name of the grove database to use when the container holds more than one. The binary has only one |
| `tether.dashboard_operators` | `false` | Makes every signed-in dashboard user an operator, with every tenant in reach. Off, a user sees only their own tenant |
| `tether.broker.tcp_address` | `:1883` | Plain MQTT. Empty turns it off |
| `tether.broker.tls` | off | MQTT over TLS: `address`, `cert_file` and `key_file` |
| `tether.broker.websocket_address` | off | MQTT over WebSocket, subprotocol `mqtt`, any path |
| `tether.broker.data_dir` | `./data/sessions` | Sessions, subscriptions, in-flight and retained messages. Empty keeps them in memory |
| `tether.broker.max_session_expiry` | `168h` | The longest session a client may ask for |
| `tether.broker.max_packet_size` | `1048576` | Largest MQTT packet, and the largest HTTP publish payload |
| `tether.broker.max_topic_length` | `1024` | A longer topic is refused with reason code 0x90 |
| `tether.broker.max_inflight` | `1024` | QoS 1 and 2 messages in flight per client, an offline session's queue included. Past it, messages for that client are dropped |
| `tether.jwt` | off | authsome JWT clients: `jwks_url`, `issuer`, `audience`, `algorithms`, `refresh_interval`, `leeway`. See above |

Environment variables are not a config source here. The one exception is `TETHER_ADMIN_TOKEN`, which is read from the environment and never from the file, so the secret stays out of anything you commit. The `dashboard:` section is the Forge dashboard's own, and it's read from the same file.

Per-tenant limits go in the tenant itself, under `limits`: `max_connections`, `messages_per_second`, `burst`, `max_session_expiry` and `max_sessions`. Zero means unlimited, or the broker default for the session expiry, and the session expiry is a duration in nanoseconds in the JSON. `max_sessions` counts open connections plus the sessions persistent clients left behind, so a tenant can't fill the node with offline sessions; a device coming back to its own session is never refused by it. The message rate is per tenant per node. A tenant's `max_session_expiry` can only lower the broker's cap: a 5.0 client that asks for more is told the shorter value in its CONNACK, and a 3.1.1 persistent session simply gets the cap as its expiry.

In your own app the same `tether:` block goes under `extensions.tether` (or a top-level `tether`) in the Forge config, and the options `WithConfig` and `WithBrokerConfig` layer over it. The grove database can be PostgreSQL there: the extension looks at the driver name and picks the SQLite or PostgreSQL store.

## What Phase 1 does

- One node. mochi serves MQTT 3.1.1 and 5 over TCP, TLS and WebSocket, with tenancy enforced in hooks: client IDs and topics are namespaced per tenant on the server, and a tenant's `$SYS` topics are its own.
- Devices authenticate with keysmith keys, users and service accounts can use authsome JWTs, and warden roles with `%c` and `%u` placeholders decide publish and subscribe rights. Topic aliases are off, so an ACL check always sees the real topic.
- Revoking a key, changing a role or disabling a tenant reaches connections that are already open.
- Sessions, subscriptions, in-flight messages and retained messages persist in Badger and survive a restart.
- Per-tenant caps on connections, sessions and session expiry, publish rate limits, a 1 MiB packet limit, a 1 KiB topic limit, and 1024 in-flight messages per client.
- An admin REST API (tenants, devices, roles and their topics, live clients, sessions, retained messages, stats), an HTTP publish endpoint for devices, and the dashboard contract.

## What it doesn't do yet

- Clustering. There is one node. NATS-backed routing, shared durable sessions, takeover and cells are Phase 2, and the design lives in [`_project_files/TETHER-DESIGN.md`](_project_files/TETHER-DESIGN.md).
- Per-tenant caps on retained messages and on subscriptions. A tenant can hold as many of either as its devices' grants allow, bounded only by the packet and topic limits.
- Metering, entitlements and audit trails (Phase 3), webhooks, outbound bridges, client certificate auth and MQTT 5 re-authentication (Phase 4).

## Development

```bash
GOWORK=off go test -race -count=1 ./...
golangci-lint run ./...
```

The PostgreSQL store tests skip unless `TETHER_TEST_POSTGRES` points at a server. [AGENTS.md](AGENTS.md) has the rest, and issues and focused pull requests are welcome.

## License

MIT. See [LICENSE](LICENSE).
