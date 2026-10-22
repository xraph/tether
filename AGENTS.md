# Working in tether

Tether is a multi-tenant MQTT broker in the xraph forgery stack. You import it as a library, and a Forge extension (under `extension/`) mounts it in a Forge app. It embeds mochi-mqtt for the protocol, keysmith for device keys and warden for topic permissions. The design lives in `_project_files/TETHER-DESIGN.md`, and when this file and the design disagree, the design wins.

Module path: `github.com/xraph/tether`. One `go.mod` covers the repo. It's `go 1.26.0` with `toolchain go1.26.9`, and the license is MIT with Rex Raphael as the copyright holder, which is what every new package in the stack gets.

## Commands

If a parent directory has a `go.work`, run every Go command with `GOWORK=off`. Otherwise the workspace governs the module and you get confusing resolution errors.

```sh
GOWORK=off go test ./...                  # everything
GOWORK=off go test -race -count=1 ./...   # what you want before a push
GOWORK=off go vet ./...
golangci-lint run ./...                   # v2, configured by .golangci.yml
gofmt -s -w . && goimports -w -local github.com/xraph/tether .
GOWORK=off go mod tidy                    # CI fails if this changes go.mod or go.sum
```

The Makefile wraps the same things: `make test`, `make test-race`, `make vet`, `make lint`, `make fmt`, `make check` and `make coverage`. There's no `cmd/` directory, so there is nothing to build into a binary.

CI is `.github/workflows/ci.yml`. It calls the shared `xraph/workflows` `go-ci.yml@v1` with Go 1.26 on ubuntu, and adds a goimports format check on top.

## Layout

The root package is `tether`: the version, the sentinel errors and, as it lands, the broker itself. `id/` holds the TypeID types. The planned shape follows relay and keysmith: `store/` with `memory`, `sqlite`, `postgres` and a shared `storetest` suite, and `extension/` with `extension/contract/` for the dashboard contract. Every package with a public surface gets an `llm.txt` written for agents, the way forge does, so update it in the same commit when the surface changes.

## Conventions

IDs are TypeIDs, and there's one `id.ID` struct rather than a type per entity, copied from keysmith's `id/id.go`. Tenants use the prefix `ttnt` and devices use `tdev`. `TenantID` and `DeviceID` are aliases for `ID`, so the prefix check happens in `ParseTenantID` and `ParseDeviceID`, not in the type system.

Sentinel errors live in the root `errors.go` and read `errors.New("tether: ...")`. A store backend wraps them as `tether/<backend>: <op>: %w`, so callers can use `errors.Is` whatever backend sits underneath.

Logging goes through `github.com/xraph/go-utils/log`, which is forge's logger. Mochi wants an `*slog.Logger`, so give it one.

### Auth and permissions

The way an MQTT client proves who it is stays the same everywhere. The username is the keysmith key ID (`akey_...`) and the password is the raw key. Device keys use the keysmith prefix `tdk`, the environment `live`, and carry the device ID in the metadata key `tether_device_id`.

Permissions come from warden. A role holds its topic rules in the metadata key `tether`, as `{"publish": [...], "subscribe": [...]}`, and the placeholders `%c` (the raw client ID) and `%u` (the username) expand inside those filters.

Tether's mochi hook has to be the only hook that provides `OnConnectAuthenticate` and `OnACLCheck`. Mochi ORs the results of every hook that does and stops at the first `true`, so a second hook would quietly open a door. Don't add one.

Every call into keysmith or warden sets all three pieces of tenant scope: `keysmith.WithTenant`, `warden.WithTenant`, and `forge.WithScope(ctx, forge.NewOrgScope(app, tenant))`. They disagree on which one wins, so setting only one is a bug waiting for a tenant who shares a key ID.

### Protocol defaults

Topic aliases are off. Set `Capabilities.TopicAliasMaximum = 0` on the server and `cl.Properties.Props.TopicAliasMaximum = 0` on each client. Aliases would let a client name a topic the ACL check never saw.

The defaults, until a config says otherwise: the session expiry cap is 7 days, the maximum packet size is 1 MiB, keysmith results are cached for 60 seconds, tenant resolution is cached for 30 seconds, and a topic can be at most 1 KiB long. Reject a longer one with reason code 0x90.

### Lint

`.golangci.yml` is the relay config with the import prefix changed. It runs gosec, errorlint and errname among others, and gosec and errcheck are off in `_test.go` files. Group imports as standard library, third party, then `github.com/xraph/tether/...`.

### Commits

Commits follow Conventional Commits, with a header of at most 100 characters and a lowercase subject. Don't add a `Co-Authored-By` trailer, and don't put Claude or Anthropic attribution in a commit, a PR or a comment. Prose that ships in the repo, README and docs included, is written in Rex's voice and never contains an em dash.
