package extension

import (
	"github.com/xraph/forge"

	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
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
// sets the same thing.
func WithCluster(cfg cluster.Config) Option {
	return func(e *Extension) {
		cfg.Enabled = true
		e.config.Cluster = cfg
	}
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
// the container. The extension migrates it on Start and never closes it.
func WithStore(s store.Store) Option {
	return func(e *Extension) { e.store = s }
}

// WithGroveDatabase names the grove database in the DI container to build
// the store on. Without it (and without WithStore) the container's default
// grove database is used.
func WithGroveDatabase(name string) Option {
	return func(e *Extension) { e.config.GroveDatabase = name }
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
