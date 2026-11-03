package extension

import (
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
)

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

	// DisableMigrate stops Start from migrating the store, for a host that
	// runs Tether's migration group itself. Migrate and Rollback then refuse
	// to run. WithDisableMigrate sets the same thing.
	DisableMigrate bool `json:"disable_migrate" yaml:"disable_migrate"`

	// Broker configures the MQTT listeners and protocol limits.
	Broker broker.Config `json:"broker" yaml:"broker"`

	// JWT turns on authsome JWT authentication next to device keys: a CONNECT
	// whose password is a JWT is checked against the identity provider's
	// JWKS, any other password goes to keysmith. The zero value leaves it off.
	// See auth.JWTConfig for the fields.
	JWT auth.JWTConfig `json:"jwt" yaml:"jwt"`

	// DashboardOperators makes every signed-in dashboard user an operator:
	// they may act on any tenant and create, change and delete tenants.
	// Without it a dashboard user is confined to the tenant their session
	// names. WithDashboardOperators sets the same thing.
	DashboardOperators bool `json:"dashboard_operators" yaml:"dashboard_operators"`

	// Cluster makes this node one broker of a cell over NATS and JetStream
	// (package cluster): sessions, QoS 1 and 2 messages, retained messages
	// and shared subscriptions live in the cell, so a client can connect to
	// any node. Off while cluster.enabled is false, the default, and then
	// the broker runs alone as before. See cluster.Config for the keys;
	// log_max_age (default 7 days) is how long an offline session's
	// messages wait in the log. WithCluster sets the same thing.
	Cluster cluster.Config `json:"cluster" yaml:"cluster"`

	// RequireConfig makes Register fail when the app's config has no
	// tether section.
	RequireConfig bool `json:"-" yaml:"-"`
}

// DefaultConfig returns the base path "/tether", broker.DefaultConfig (TCP on
// :1883, 1 MiB packets, 7 day session cap, 1 KiB topics) and
// cluster.DefaultConfig (off).
func DefaultConfig() Config {
	return Config{
		BasePath: DefaultBasePath,
		Broker:   broker.DefaultConfig(),
		Cluster:  cluster.DefaultConfig(),
	}
}
