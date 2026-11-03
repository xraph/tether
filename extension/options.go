package extension

import (
	"github.com/xraph/forge"

	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/metering"
	"github.com/xraph/tether/store"
)

// Option configures the extension.
type Option func(*Extension)

// WithConfig replaces the whole config. Fields the app's config file sets
// win over the ones here that equal DefaultConfig.
func WithConfig(cfg Config) Option {
	return func(e *Extension) { e.config = cfg }
}

// WithBrokerConfig sets the broker's listeners and limits.
func WithBrokerConfig(cfg broker.Config) Option {
	return func(e *Extension) { e.config.Broker = cfg }
}

// WithCluster runs the broker as one node of a cell, configured by cfg. It
// turns cfg.Enabled on: passing it means you want the cell. Start connects
// to the cell's NATS (or starts the embedded server), provisions its streams
// and buckets, and joins; see cluster.Config for the keys and the README for
// the embedded and external shapes. The "cluster" block of the config file
// sets the same thing. Mode, Heartbeat, Lease, LogMaxAge, PullHeartbeat and
// LoadInterval left zero take cluster.DefaultConfig's, so a literal with only
// the fields you need passes Validate.
func WithCluster(cfg cluster.Config) Option {
	return func(e *Extension) {
		def := cluster.DefaultConfig()

		if cfg.Mode == "" {
			cfg.Mode = def.Mode
		}

		if cfg.Heartbeat == 0 {
			cfg.Heartbeat = def.Heartbeat
		}

		if cfg.Lease == 0 {
			cfg.Lease = def.Lease
		}

		if cfg.LogMaxAge == 0 {
			cfg.LogMaxAge = def.LogMaxAge
		}

		if cfg.PullHeartbeat == 0 {
			cfg.PullHeartbeat = def.PullHeartbeat
		}

		if cfg.LoadInterval == 0 {
			cfg.LoadInterval = def.LoadInterval
		}

		cfg.Enabled = true
		e.config.Cluster = cfg
	}
}

// WithMetering sends each tenant's usage to ledger, configured by cfg.
// Enabled false leaves metering off. A zero Interval takes the default
// (60 s), and Features with every key empty take metering.DefaultConfig's
// five, so WithMetering(metering.Config{Enabled: true}) meters everything
// under the tether.* keys. Name any key and the others stay as you gave
// them: an empty one turns that meter off. The app must register ledger's
// extension, or Start fails with ErrLedgerRequired. The "metering" block of
// the config file sets the same thing, and there a block with every key
// empty fails Start (metering.Config.Validate).
func WithMetering(cfg metering.Config) Option {
	return func(e *Extension) {
		def := metering.DefaultConfig()

		if cfg.Interval == 0 {
			cfg.Interval = def.Interval
		}

		if cfg.Features == (metering.Features{}) {
			cfg.Features = def.Features
		}

		e.config.Metering = cfg
	}
}

// WithEntitlements checks each tenant's ledger plan at CONNECT and every
// interval, configured by cfg: a tenant missing any of cfg.Features is
// refused, and disconnected, with 0x97. Enabled false leaves it off. An empty
// feature list and zero durations take metering.DefaultEntitlementConfig's.
// The app must register ledger's extension, or Start fails with
// ErrLedgerRequired. The "entitlements" block of the config file sets the
// same thing.
func WithEntitlements(cfg metering.EntitlementConfig) Option {
	return func(e *Extension) {
		def := metering.DefaultEntitlementConfig()

		if len(cfg.Features) == 0 {
			cfg.Features = def.Features
		}

		if cfg.Interval == 0 {
			cfg.Interval = def.Interval
		}

		if cfg.CheckTimeout == 0 {
			cfg.CheckTimeout = def.CheckTimeout
		}

		if cfg.FailOpen == 0 {
			cfg.FailOpen = def.FailOpen
		}

		e.config.Entitlements = cfg
	}
}

// WithAudit records the audit trail in chronicle, configured by cfg. Enabled
// false leaves it off; an empty Events list and zero numbers take
// audit.DefaultConfig's. The app must register chronicle's extension, or
// Start fails with ErrChronicleRequired. The "audit" block of the config file
// sets the same thing.
func WithAudit(cfg audit.Config) Option {
	return func(e *Extension) { e.config.Audit = cfg }
}

// WithJWT lets users and service accounts connect with an authsome JWT as the
// MQTT password, next to device keys. The username must be the token's sub.
// The JWT carries the tenant (app_id and org_id) and warden roles assigned to
// the user decide the topics. Without it only keysmith device keys connect.
// The "jwt" block of the config file sets the same thing; the option wins
// where it differs from the empty default. A config with any field set is
// validated at Start, which fails if it is incomplete.
//
// POST <base>/v1/publish stays device-only.
func WithJWT(cfg auth.JWTConfig) Option {
	return func(e *Extension) { e.config.JWT = cfg }
}

// WithStore uses s for tenants and devices instead of a grove database from
// the container. The extension migrates it on Start (unless WithDisableMigrate)
// and never closes it. A WithStore store never goes to the central migrator,
// since its database need not be the one the migrator runs on.
func WithStore(s store.Store) Option {
	return func(e *Extension) { e.store = s }
}

// WithGroveDatabase names the grove database in the DI container to build
// the store on. Without it (and without WithStore) the container's default
// grove database is used.
func WithGroveDatabase(name string) Option {
	return func(e *Extension) { e.config.GroveDatabase = name }
}

// WithDisableMigrate stops Start from migrating the store. Use it when the
// host runs the store's grove group (postgres.Migrations or
// sqlite.Migrations) some other way. The disable_migrate config key sets the
// same thing.
func WithDisableMigrate() Option {
	return func(e *Extension) { e.config.DisableMigrate = true }
}

// WithAdminMiddleware wraps the admin routes in mw, in order. This is how
// the admin API gets authenticated: Tether adds no admin auth of its own.
// The device endpoint POST <base>/v1/publish never gets these; it
// authenticates the device with HTTP Basic itself.
func WithAdminMiddleware(mw ...forge.Middleware) Option {
	return func(e *Extension) { e.adminMiddleware = append(e.adminMiddleware, mw...) }
}

// WithInsecureAdminRoutes mounts the admin routes with no admin middleware.
// Anyone who reaches the HTTP port can then create tenants and devices and
// read their credentials. It is for tests and for apps that put the whole
// router behind their own gate. Without it, Start fails with
// ErrAdminAuthRequired when routes are on and WithAdminMiddleware was not
// given.
func WithInsecureAdminRoutes() Option {
	return func(e *Extension) { e.insecureAdminRoutes = true }
}

// WithoutKeysmithEvents lets Start run although the keysmith extension was
// not built with (*Extension).Keysmith. Revoking, suspending or rotating a
// device key then leaves live MQTT connections alone, and the cached
// validation keeps a revoked key working for up to a minute.
func WithoutKeysmithEvents() Option {
	return func(e *Extension) { e.noKeysmithEvents = true }
}

// WithoutWardenEvents lets Start run although the warden plugin could not be
// registered (warden built without (*Extension).Warden and with no plugin
// registry). Role and assignment changes then reach live MQTT connections
// only when they reconnect.
func WithoutWardenEvents() Option {
	return func(e *Extension) { e.noWardenEvents = true }
}

// WithLogger sets the logger the extension, the broker and mochi log
// through. Without it they use the app's logger.
func WithLogger(l forge.Logger) Option {
	return func(e *Extension) { e.logger = l }
}

// WithInsecureDashboard serves the dashboard intents to requests that carry
// no authenticated user. Without it they answer UNAUTHENTICATED, which is
// what you get when the dashboard's own auth is off, the same refusal the
// REST admin API gives without WithAdminMiddleware. Use it only when the
// dashboard is reachable by trusted callers alone.
//
// It implies WithDashboardOperators: an anonymous caller has no session to
// take a tenant from, so it is served as an operator.
func WithInsecureDashboard() Option {
	return func(e *Extension) { e.insecureDashboard = true }
}

// WithDashboardOperators makes every signed-in dashboard user an operator,
// who may act on any tenant and create, change and delete tenants. Without
// it a dashboard user is confined to the one tenant their session names (a
// tenant_id claim, or the org of the forge Scope the host's auth middleware
// sets), mapped to a Tether tenant; a user with no such tenant is refused.
// The config key dashboard_operators: true does the same.
func WithDashboardOperators() Option {
	return func(e *Extension) { e.config.DashboardOperators = true }
}
