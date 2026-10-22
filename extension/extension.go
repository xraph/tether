// Package extension mounts Tether in a Forge app: the MQTT broker, its REST
// API, and the keysmith and warden plugins that make live connections react
// to key and role changes.
//
// Build keysmith and warden through the extension so the plugins are
// registered with their engines:
//
//	t := extension.New(extension.WithAdminMiddleware(adminAuth))
//	app.RegisterExtension(t.Keysmith())
//	app.RegisterExtension(t.Warden())
//	app.RegisterExtension(t)
//
// Tether depends on both, so Forge starts them first. Start then builds the
// store (WithStore, else the container's grove database), the services, the
// broker and the routes.
package extension

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/xraph/forge"
	dashboard "github.com/xraph/forge/extensions/dashboard"
	"github.com/xraph/grove"
	"github.com/xraph/keysmith"
	keysmithext "github.com/xraph/keysmith/extension"
	"github.com/xraph/vessel"
	"github.com/xraph/warden"
	wardenplugin "github.com/xraph/warden/plugin"

	"github.com/xraph/tether"
	"github.com/xraph/tether/api"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/events"
	"github.com/xraph/tether/limits"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/postgres"
	"github.com/xraph/tether/store/sqlite"
)

// ExtensionName is the name Tether registers with Forge.
const ExtensionName = "tether"

// ExtensionDescription is the human-readable description.
const ExtensionDescription = "Multi-tenant MQTT broker with keysmith device keys and warden topic permissions"

// ExtensionVersion is the extension's version, Tether's.
const ExtensionVersion = tether.Version

var (
	// ErrKeysmithNotWired is returned by Start when the app's keysmith
	// extension was not built with (*Extension).Keysmith and
	// WithoutKeysmithEvents was not set.
	ErrKeysmithNotWired = errors.New("tether: keysmith was built without the tether plugin; " +
		"register (*Extension).Keysmith() instead of keysmith's extension, or pass WithoutKeysmithEvents")

	// ErrWardenNotWired is returned by Start when the warden plugin could
	// not be registered (warden built without (*Extension).Warden and with
	// no plugin registry) and WithoutWardenEvents was not set.
	ErrWardenNotWired = errors.New("tether: the warden engine has no plugin registry for the tether plugin; " +
		"register (*Extension).Warden() instead of warden's extension, or pass WithoutWardenEvents")

	// ErrAdminAuthRequired is returned by Start when routes are on and
	// neither WithAdminMiddleware nor WithInsecureAdminRoutes was given.
	ErrAdminAuthRequired = errors.New("tether: the admin API needs authentication; " +
		"pass WithAdminMiddleware, or WithInsecureAdminRoutes to mount it open, or disable routes")
)

var (
	_ forge.Extension                = (*Extension)(nil)
	_ dashboard.DashboardStatusAware = (*Extension)(nil)
)

// Extension is Tether as a Forge extension.
type Extension struct {
	*forge.BaseExtension

	config Config

	// Set by options.
	store               store.Store
	adminMiddleware     []forge.Middleware
	insecureAdminRoutes bool
	insecureDashboard   bool
	noKeysmithEvents    bool
	noWardenEvents      bool
	logger              forge.Logger

	// The plugins exist from New, so Keysmith and Warden can hand them to
	// the engines before Tether starts.
	ksPlugin *events.KeysmithPlugin
	wdPlugin *events.WardenPlugin

	// ksExts are the keysmith extensions Keysmith built, guarded by mu.
	ksExts []*keysmithext.Extension

	// lifecycle serializes Start and Stop, so a second call (vessel starts
	// every Starter it resolves, and *Extension is one) waits for the first
	// and then finds it done.
	lifecycle sync.Mutex

	// Built by Start, read by the accessors.
	mu       sync.RWMutex
	broker   *broker.Broker
	services *api.Services
	active   store.Store
	started  bool
}

// New returns the extension. Options override DefaultConfig; the app's
// config file (extensions.tether or tether) is layered in by Register.
func New(opts ...Option) *Extension {
	e := &Extension{
		BaseExtension: forge.NewBaseExtension(ExtensionName, ExtensionVersion, ExtensionDescription),
		config:        DefaultConfig(),
		ksPlugin:      events.NewKeysmithPlugin(),
		wdPlugin:      events.NewWardenPlugin(nil),
	}
	e.SetDependencies([]string{"keysmith", "warden"})

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Broker returns the broker, nil before Start.
func (e *Extension) Broker() *broker.Broker {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.broker
}

// Services returns what the REST handlers run on, nil before Start.
func (e *Extension) Services() *api.Services {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.services
}

// Register loads the config and puts the extension in the container.
// Everything else waits for Start, once keysmith and warden have started.
func (e *Extension) Register(app forge.App) error {
	if err := e.BaseExtension.Register(app); err != nil {
		return err
	}

	if e.logger != nil {
		e.SetLogger(e.logger)
	}

	cfg := DefaultConfig()
	if err := e.LoadConfig(ExtensionName, &cfg, e.config, DefaultConfig(), e.config.RequireConfig); err != nil {
		if e.config.RequireConfig {
			return fmt.Errorf("tether: failed to load required config: %w", err)
		}

		// The loader may have written part of the file into cfg, or left it
		// at the defaults: either way the options would be lost. The
		// programmatic config (DefaultConfig with the options applied) is
		// what the host asked for.
		e.Logger().Warn("tether: config section unreadable, using the programmatic config", forge.F("error", err.Error()))

		cfg = e.config
	}

	if cfg.BasePath == "" {
		cfg.BasePath = DefaultBasePath
	}

	e.config = cfg

	if err := vessel.Provide(app.Container(), func() (*Extension, error) { return e, nil }); err != nil {
		return fmt.Errorf("tether: register extension in container: %w", err)
	}

	return nil
}

// Start wires and starts Tether. In order: the admin auth check, the
// keysmith engine from the container (which must be the one the extension
// from Keysmith owns, so the plugin is on it), the warden engine and its
// plugin (registered now if Warden did not), the store (migrated),
// the services, the broker with the plugins bound to its reactor, the
// routes, and finally the broker's listeners.
//
// Start is idempotent: on an extension that is already started it returns nil
// and does nothing. That matters because *Extension is provided to the
// container, and resolving it there (vessel.Inject[*extension.Extension]) makes
// vessel start it once more.
func (e *Extension) Start(ctx context.Context) error {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()

	e.mu.RLock()
	already := e.started
	e.mu.RUnlock()

	if already {
		return nil
	}

	if !e.config.DisableRoutes && len(e.adminMiddleware) == 0 && !e.insecureAdminRoutes {
		return ErrAdminAuthRequired
	}

	ks, wd, err := e.engines()
	if err != nil {
		return err
	}

	st, err := e.openStore(ctx)
	if err != nil {
		return err
	}

	slogger := newSlogLogger(e.Logger())

	tenants := service.NewTenants(st)
	authn := auth.NewKeysmith(ks, tenants, st)

	connectAuth, err := e.connectAuth(authn, tenants)
	if err != nil {
		return err
	}

	loader := perm.NewLoader(wd, perm.WithLogger(slogger))
	limiter := limits.New()

	b, err := broker.New(e.config.Broker, broker.Deps{
		Auth:    connectAuth,
		Perms:   loader,
		Limits:  limiter,
		Logger:  slogger,
		Metrics: newBrokerMetrics(e.Metrics()),
	})
	if err != nil {
		return fmt.Errorf("tether: %w", err)
	}

	// Bound before the broker listens, so no connection can miss a change.
	// Until Start the reactor's broker calls do nothing.
	reactor := events.NewReactor(authn.Evict, b)
	e.ksPlugin.Bind(reactor)
	e.wdPlugin.Bind(reactor)

	services := &api.Services{
		Tenants: tenants,
		Devices: service.NewDevices(st, tenants, ks, service.NewWardenRoles(wd),
			service.WithSessions(b), service.WithLogger(slogger)),
		Roles:             perm.NewRoles(wd),
		Broker:            b,
		Auth:              authn,
		Perms:             loader,
		Limits:            limiter,
		Logger:            slogger,
		MaxPublishPayload: int(e.config.Broker.MaxPacketSize),
	}

	if !e.config.DisableRoutes {
		if err := e.mountRoutes(*services); err != nil {
			e.unbind()

			return err
		}
	}

	if err := b.Start(ctx); err != nil {
		e.unbind()

		return fmt.Errorf("tether: start broker: %w", err)
	}

	e.mu.Lock()
	e.broker, e.services, e.active, e.started = b, services, st, true
	e.mu.Unlock()

	e.MarkStarted()
	e.Logger().Info("tether: broker started", forge.F("mqtt", addrString(b)), forge.F("base_path", e.config.BasePath))

	return nil
}

// connectAuth returns the authenticator MQTT CONNECT uses: keysmith alone, or
// with the JWT authenticator in front when the config has a jwt block.
func (e *Extension) connectAuth(ks auth.Authenticator, tenants auth.TenantResolver) (auth.Authenticator, error) {
	if e.config.JWT.IsZero() {
		return ks, nil
	}

	j, err := auth.NewJWT(e.config.JWT, tenants)
	if err != nil {
		return nil, fmt.Errorf("tether: jwt config: %w", err)
	}

	return auth.Chain(j, ks), nil
}

// engines returns the keysmith and warden engines from the container. The
// keysmith engine must carry Tether's plugin (see keysmithWired), and the
// warden plugin is wired to the warden engine.
func (e *Extension) engines() (ks *keysmith.Engine, wd *warden.Engine, err error) {
	c := e.App().Container()

	ks, err = vessel.Inject[*keysmith.Engine](c)
	if err != nil {
		return nil, nil, fmt.Errorf("tether: keysmith engine not in the container (register the keysmith extension): %w", err)
	}

	if !e.noKeysmithEvents && !e.keysmithWired(ks) {
		return nil, nil, ErrKeysmithNotWired
	}

	wd, err = vessel.Inject[*warden.Engine](c)
	if err != nil {
		return nil, nil, fmt.Errorf("tether: warden engine not in the container (register the warden extension): %w", err)
	}

	if werr := e.wireWarden(wd); werr != nil {
		return nil, nil, werr
	}

	return ks, wd, nil
}

// keysmithWired reports whether ks is the engine of a keysmith extension that
// Keysmith built. The plugin's own Wired flag only says Keysmith was called;
// a host can call it and register another keysmith, whose engine never sees
// the plugin. An extension that has not registered has no engine, so it
// never matches.
func (e *Extension) keysmithWired(ks *keysmith.Engine) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return slices.ContainsFunc(e.ksExts, func(x *keysmithext.Extension) bool { return x.Engine() == ks })
}

// openStore resolves the store and migrates it.
func (e *Extension) openStore(ctx context.Context) (store.Store, error) {
	st, err := e.resolveStore()
	if err != nil {
		return nil, err
	}

	if err := st.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("tether: migrate store: %w", err)
	}

	return st, nil
}

// wireWarden makes sure the warden plugin reaches wd. The engine's plugin
// registry is the authority, not Wired: when Warden built the extension
// that put wd in the container, the plugin is already in it; otherwise
// (warden built some other way, or a second warden extension won) it is
// registered now. An engine with no registry cannot take it.
func (e *Extension) wireWarden(wd *warden.Engine) error {
	reg := wd.Plugins()
	if reg == nil {
		if e.noWardenEvents {
			return nil
		}

		return ErrWardenNotWired
	}

	if !slices.Contains(reg.Plugins(), wardenplugin.Plugin(e.wdPlugin)) {
		reg.Register(e.wdPlugin)
	}

	e.wdPlugin.MarkWired()

	return nil
}

// resolveStore returns WithStore's store, else one built on the grove
// database in the container: the one GroveDatabase names, or the default.
func (e *Extension) resolveStore() (store.Store, error) {
	if e.store != nil {
		return e.store, nil
	}

	c := e.App().Container()

	var (
		db  *grove.DB
		err error
	)

	if name := e.config.GroveDatabase; name != "" {
		db, err = vessel.InjectNamed[*grove.DB](c, name)
		if err != nil {
			return nil, fmt.Errorf("tether: grove database %q not in the container: %w", name, err)
		}
	} else {
		db, err = vessel.Inject[*grove.DB](c)
		if err != nil {
			return nil, fmt.Errorf("tether: no store: pass WithStore or register a grove database: %w", err)
		}
	}

	switch driver := db.Driver().Name(); driver {
	case "sqlite":
		return sqlite.New(db), nil
	case "pg":
		return postgres.New(db), nil
	default:
		return nil, fmt.Errorf("tether: unsupported grove driver %q (want sqlite or pg)", driver)
	}
}

// mountRoutes mounts the admin API on a group with the admin middleware and
// POST /v1/publish on a plain group, both under the base path.
//
// The middleware goes on with Use, not forge.WithGroupMiddleware: api
// registers its routes on nested groups, and forge v1.12.10's Group copies
// the parent's Use middleware but not its group-option middleware, so the
// admin routes would be mounted open.
func (e *Extension) mountRoutes(s api.Services) error {
	r := e.App().Router()

	admin := r.Group(e.config.BasePath)
	admin.Use(e.adminMiddleware...)

	public := r.Group(e.config.BasePath)

	if err := api.Register(admin, public, s); err != nil {
		return fmt.Errorf("tether: mount routes: %w", err)
	}

	return nil
}

// unbind detaches the plugins from a reactor whose broker is not running.
func (e *Extension) unbind() {
	e.ksPlugin.Bind(nil)
	e.wdPlugin.Bind(nil)
}

// Stop unbinds the plugins and stops the broker, which disconnects every
// client. The store is left open: Tether did not open it. Stopping an
// extension that is not started returns nil.
func (e *Extension) Stop(ctx context.Context) error {
	e.lifecycle.Lock()
	defer e.lifecycle.Unlock()

	e.mu.RLock()
	running := e.started
	e.mu.RUnlock()

	if !running {
		return nil
	}

	e.unbind()

	e.mu.Lock()
	b := e.broker
	e.started = false
	e.mu.Unlock()

	e.MarkStopped()

	if b == nil {
		return nil
	}

	if err := b.Stop(ctx); err != nil {
		return fmt.Errorf("tether: %w", err)
	}

	return nil
}

// Health fails when the broker is not running or the store does not answer.
func (e *Extension) Health(ctx context.Context) error {
	e.mu.RLock()
	b, st := e.broker, e.active
	e.mu.RUnlock()

	if b == nil || !b.Running() {
		return errors.New("tether: broker not running")
	}

	if err := st.Ping(ctx); err != nil {
		return fmt.Errorf("tether: store: %w", err)
	}

	return nil
}

// DashboardStatus implements dashboard.DashboardStatusAware: configured once
// Start has succeeded, until Stop.
func (e *Extension) DashboardStatus() dashboard.DashboardStatus {
	e.mu.RLock()
	defer e.mu.RUnlock()

	st := dashboard.DashboardStatus{Version: ExtensionVersion, Configured: e.started}
	if !e.started {
		st.Message = "Tether has not started."
	}

	return st
}

// addrString is the broker's TCP address for the log, or "" with no TCP
// listener.
func addrString(b *broker.Broker) string {
	if a := b.Addr(); a != nil {
		return a.String()
	}

	return ""
}
