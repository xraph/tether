# Tether

Tether is a multi-tenant MQTT broker for Go. You import it as a library, and there's a Forge extension for when you want it mounted in a Forge app. Every client that connects gets resolved to a tenant and a device, devices authenticate with keysmith keys, and warden roles decide which topics each one can publish to and subscribe to.

Module path: `github.com/xraph/tether`. MIT licensed.

Status: in development. Nothing here is ready to run yet.
