package extension

import "github.com/xraph/tether/broker"

// DefaultBasePath is where the REST API mounts when the config names no
// base path.
const DefaultBasePath = "/tether"

// Config configures the Tether extension. It loads from "extensions.tether"
// (or "tether") in the app's config, layered over DefaultConfig and under
// the options passed to New.
type Config struct {
	// BasePath prefixes every REST route: the admin API under
	// <base>/v1/tenants and the device endpoint <base>/v1/publish.
	BasePath string `json:"base_path" yaml:"base_path"`

	// DisableRoutes mounts no REST routes at all. The broker still runs.
	DisableRoutes bool `json:"disable_routes" yaml:"disable_routes"`

	// GroveDatabase names the grove database in the DI container the store
	// is built on. Empty uses the container's default one. WithStore
	// overrides both.
	GroveDatabase string `json:"grove_database" yaml:"grove_database"`

	// Broker configures the MQTT listeners and protocol limits.
	Broker broker.Config `json:"broker" yaml:"broker"`

	// RequireConfig makes Register fail when the app's config has no
	// tether section.
	RequireConfig bool `json:"-" yaml:"-"`
}

// DefaultConfig returns the base path "/tether" and broker.DefaultConfig
// (TCP on :1883, 1 MiB packets, 7 day session cap, 1 KiB topics).
func DefaultConfig() Config {
	return Config{
		BasePath: DefaultBasePath,
		Broker:   broker.DefaultConfig(),
	}
}
