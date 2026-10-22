// Package tether is a multi-tenant MQTT broker for Go.
//
// Tether embeds an MQTT server, resolves every connecting client to a tenant
// and a device, and keeps each tenant's topics isolated from the others. You
// import it as a library, and there is a Forge extension for mounting it in a
// Forge app.
//
// Devices authenticate with keysmith keys (the MQTT username is the key ID and
// the password is the raw key), and warden roles decide which topics a device
// may publish to and subscribe to.
//
// The project is in development. This package holds the version and the
// sentinel errors; the broker, stores and extension live in subpackages as
// they land.
package tether
