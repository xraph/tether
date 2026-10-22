package extension_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pahov3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	dashboard "github.com/xraph/forge/extensions/dashboard"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	keysmithext "github.com/xraph/keysmith/extension"
	"github.com/xraph/vessel"
	"github.com/xraph/warden"
	wardenext "github.com/xraph/warden/extension"
	"github.com/xraph/warden/role"

	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/store/sqlite"
	"github.com/xraph/tether/tenant"
)

// waitTimeout bounds every wait. Nothing sleeps: tests wait on channels.
const waitTimeout = 5 * time.Second

const (
	testApp = "aapp_1"
	testOrg = "aorg_1"
)

var _ dashboard.DashboardStatusAware = (*extension.Extension)(nil)

// newApp returns a quiet forge app with a grove SQLite database (a file in a
// temp dir) in its container, the way a host registers one.
func newApp(t *testing.T) forge.App {
	t.Helper()

	return newAppWith(t, forge.WithEnableConfigAutoDiscovery(false))
}

// newAppWith is newApp with app options of its own (a config file).
func newAppWith(t *testing.T, opts ...forge.AppOption) forge.App {
	t.Helper()

	sdb := sqlitedriver.New()
	require.NoError(t, sdb.Open(t.Context(), sqlite.DSN(filepath.Join(t.TempDir(), "app.db"))))

	db, err := grove.Open(sdb)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	app := forge.New(append([]forge.AppOption{
		forge.WithAppName("tether-test"),
		forge.WithAppLogger(forge.NewNoopLogger()),
	}, opts...)...)
	require.NoError(t, vessel.Provide(app.Container(), func() (*grove.DB, error) { return db, nil }))

	return app
}

// loopback is the default broker config on a random loopback port.
func loopback() broker.Config {
	cfg := broker.DefaultConfig()
	cfg.TCPAddress = "127.0.0.1:0"

	return cfg
}

// boot registers exts with app and starts it, stopping it at cleanup.
func boot(t *testing.T, app forge.App, exts ...forge.Extension) error {
	t.Helper()

	for _, x := range exts {
		require.NoError(t, app.RegisterExtension(x))
	}

	err := app.Start(t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = app.Stop(ctx)
	})

	return err
}

// env is a started app with keysmith, warden and tether, and the router
// behind an httptest server.
type env struct {
	t    *testing.T
	base string
	app  forge.App
	ext  *extension.Extension
	srv  *httptest.Server
	wd   *warden.Engine
}

// startApp boots the whole stack on app.
func startApp(t *testing.T, app forge.App, plainWarden bool, opts ...extension.Option) *env {
	t.Helper()

	ext := extension.New(opts...)

	var wd forge.Extension
	if plainWarden {
		wd = wardenext.New(wardenext.WithDisableRoutes())
	} else {
		wd = ext.Warden(wardenext.WithDisableRoutes())
	}

	require.NoError(t, boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), wd, ext))

	eng, err := vessel.Inject[*warden.Engine](app.Container())
	require.NoError(t, err)

	srv := httptest.NewServer(app.Router())
	t.Cleanup(srv.Close)

	return &env{t: t, base: "/tether", app: app, ext: ext, srv: srv, wd: eng}
}

// start boots the whole stack. Warden is built with the tether helper, or
// with plain wardenext.New when plainWarden is set, so the plugin has to be
// registered at Start.
func start(t *testing.T, plainWarden bool) *env {
	t.Helper()

	return startApp(t, newApp(t), plainWarden, extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))
}

// do sends a request with an optional JSON body and decodes a JSON reply.
func (e *env) do(method, path string, body any, want int) map[string]any {
	e.t.Helper()

	var rd io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(e.t, err)

		rd = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(e.t.Context(), method, e.srv.URL+path, rd)
	require.NoError(e.t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	require.Equal(e.t, want, resp.StatusCode, "%s %s: %s", method, path, raw)

	var m map[string]any
	if len(raw) > 0 {
		require.NoError(e.t, json.Unmarshal(raw, &m), "body: %s", raw)
	}

	return m
}

// tenant creates the tenant testOrg through the REST API and returns its ID.
func (e *env) tenant() string {
	e.t.Helper()

	m := e.do(http.MethodPost, e.base+"/v1/tenants", map[string]any{"app_id": testApp, "external_id": testOrg, "name": "Acme"}, http.StatusCreated)

	return m["id"].(string)
}

// role creates an empty warden role (warden's own routes are off) and sets
// its topics through Tether's REST API.
func (e *env) role(tenantID, slug string, pub, sub []string) {
	e.t.Helper()

	require.NoError(e.t, e.wd.Store().CreateRole(scope.With(e.t.Context(), testApp, testOrg), &role.Role{
		TenantID: testOrg, AppID: testApp, Name: slug, Slug: slug,
	}))

	e.do(http.MethodPut, e.base+"/v1/tenants/"+tenantID+"/roles/"+slug+"/topics",
		map[string]any{"publish": pub, "subscribe": sub}, http.StatusOK)
}

// device is a provisioned device.
type device struct{ id, username, password string }

func (e *env) device(tenantID string, roles ...string) device {
	e.t.Helper()

	m := e.do(http.MethodPost, e.base+"/v1/tenants/"+tenantID+"/devices", map[string]any{"name": "dev", "roles": roles}, http.StatusCreated)
	d := m["device"].(map[string]any)

	return device{id: d["id"].(string), username: m["username"].(string), password: m["password"].(string)}
}

// client is a connected MQTT 3.1.1 client.
type client struct {
	pahov3.Client
	msgs chan [2]string
	lost chan error
}

func (e *env) connect(d device, clientID string) (*client, error) {
	e.t.Helper()

	c := &client{msgs: make(chan [2]string, 16), lost: make(chan error, 1)}

	opts := pahov3.NewClientOptions().
		AddBroker("tcp://" + e.ext.Broker().Addr().String()).
		SetClientID(clientID).
		SetUsername(d.username).
		SetPassword(d.password).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(waitTimeout).
		SetConnectionLostHandler(func(_ pahov3.Client, err error) { c.lost <- err })

	c.Client = pahov3.NewClient(opts)

	tok := c.Connect()
	if !tok.WaitTimeout(waitTimeout) {
		return nil, context.DeadlineExceeded
	}

	if err := tok.Error(); err != nil {
		return nil, err
	}

	e.t.Cleanup(func() { c.Disconnect(0) })

	return c, nil
}

func (c *client) subscribe(t *testing.T, filter string) {
	t.Helper()

	tok := c.Subscribe(filter, 1, func(_ pahov3.Client, m pahov3.Message) {
		c.msgs <- [2]string{m.Topic(), string(m.Payload())}
	})
	require.True(t, tok.WaitTimeout(waitTimeout), "subscribe timed out")
	require.NoError(t, tok.Error())
}

func (c *client) publish(t *testing.T, topic, payload string) {
	t.Helper()

	tok := c.Publish(topic, 1, false, payload)
	require.True(t, tok.WaitTimeout(waitTimeout), "publish timed out")
	require.NoError(t, tok.Error())
}

func (c *client) next(t *testing.T) [2]string {
	t.Helper()

	select {
	case m := <-c.msgs:
		return m
	case <-time.After(waitTimeout):
		require.FailNow(t, "no message received")

		return [2]string{}
	}
}

// subscriptions lists the client's subscriptions as the admin API reports
// them.
func (e *env) subscriptions(tenantID, clientID string) []string {
	e.t.Helper()

	m := e.do(http.MethodGet, e.base+"/v1/tenants/"+tenantID+"/clients", nil, http.StatusOK)

	for _, it := range m["items"].([]any) {
		ci := it.(map[string]any)
		if ci["client_id"] != clientID {
			continue
		}

		raw, _ := ci["subscriptions"].([]any)
		subs := make([]string, 0, len(raw))

		for _, s := range raw {
			subs = append(subs, s.(string))
		}

		return subs
	}

	require.FailNow(e.t, "client not listed", clientID)

	return nil
}

func TestExtensionBootsWithKeysmithAndWarden(t *testing.T) {
	e := start(t, false)

	tid := e.tenant()
	e.role(tid, "sensor", []string{"sensors/%c/up"}, []string{"sensors/%c/#"})
	d := e.device(tid, "sensor")

	c, err := e.connect(d, "pump-1")
	require.NoError(t, err)

	c.subscribe(t, "sensors/pump-1/#")
	c.publish(t, "sensors/pump-1/up", "21.5")

	assert.Equal(t, [2]string{"sensors/pump-1/up", "21.5"}, c.next(t))

	require.NoError(t, e.ext.Health(t.Context()))
	assert.True(t, e.ext.DashboardStatus().Configured)
	assert.NotNil(t, e.ext.Services())
	assert.Equal(t, []string{"keysmith", "warden"}, e.ext.Dependencies())
}

// Revoking a device through REST fires the keysmith plugin, whose reactor
// disconnects the device's live connection.
func TestRevokeDisconnectsLiveClient(t *testing.T) {
	e := start(t, false)

	tid := e.tenant()
	e.role(tid, "sensor", []string{"up"}, nil)
	d := e.device(tid, "sensor")

	c, err := e.connect(d, "dev-1")
	require.NoError(t, err)

	e.do(http.MethodPost, e.base+"/v1/tenants/"+tid+"/devices/"+d.id+"/revoke", map[string]any{"reason": "lost"}, http.StatusNoContent)

	select {
	case <-c.lost:
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the revoked device is still connected")
	}

	_, err = e.connect(d, "dev-1")
	require.Error(t, err, "a revoked key cannot connect again")
}

// Narrowing a role's topics through REST fires the warden plugin, whose
// reactor reloads the device's grants and drops the subscription they no
// longer allow. Both run inside the PUT, so there is nothing to wait for.
func TestRoleChangeDropsForbiddenSubscription(t *testing.T) {
	for _, tc := range []struct {
		name  string
		plain bool
	}{
		{"warden built by the helper", false},
		{"plugin registered at start", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := start(t, tc.plain)

			tetherPlugins := 0

			for _, p := range e.wd.Plugins().Plugins() {
				if p.Name() == "tether" {
					tetherPlugins++
				}
			}

			require.Equal(t, 1, tetherPlugins, "the plugin is registered exactly once")

			tid := e.tenant()
			e.role(tid, "reader", nil, []string{"a/#", "b/#"})
			d := e.device(tid, "reader")

			c, err := e.connect(d, "dev-1")
			require.NoError(t, err)

			c.subscribe(t, "a/#")
			c.subscribe(t, "b/#")
			require.ElementsMatch(t, []string{"a/#", "b/#"}, e.subscriptions(tid, "dev-1"))

			e.do(http.MethodPut, e.base+"/v1/tenants/"+tid+"/roles/reader/topics",
				map[string]any{"publish": []string{}, "subscribe": []string{"a/#"}}, http.StatusOK)

			assert.Equal(t, []string{"a/#"}, e.subscriptions(tid, "dev-1"))
		})
	}
}

func TestStartFailsWhenKeysmithNotWired(t *testing.T) {
	app := newApp(t)
	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))

	err := boot(t, app,
		keysmithext.New(keysmithext.WithDisableRoutes()),
		ext.Warden(wardenext.WithDisableRoutes()),
		ext,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, extension.ErrKeysmithNotWired)
	assert.False(t, ext.DashboardStatus().Configured)
}

func TestStartWithoutKeysmithEvents(t *testing.T) {
	app := newApp(t)
	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()), extension.WithoutKeysmithEvents())

	require.NoError(t, boot(t, app,
		keysmithext.New(keysmithext.WithDisableRoutes()),
		ext.Warden(wardenext.WithDisableRoutes()),
		ext,
	))
	assert.True(t, ext.Broker().Running())
}

// A warden engine with no plugin registry (no audit log, no cache, no
// plugin) cannot take the plugin at Start.
func TestStartFailsWhenWardenCannotTakeThePlugin(t *testing.T) {
	bare := func() forge.Extension {
		return wardenext.New(wardenext.WithConfig(wardenext.Config{
			DisableRoutes: true,
			Auth:          wardenext.AuthConfig{RequireIdentity: true},
		}))
	}

	t.Run("refused", func(t *testing.T) {
		app := newApp(t)
		ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))

		err := boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), bare(), ext)
		require.Error(t, err)
		assert.ErrorIs(t, err, extension.ErrWardenNotWired)
	})

	t.Run("opted out", func(t *testing.T) {
		app := newApp(t)
		ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()), extension.WithoutWardenEvents())

		require.NoError(t, boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), bare(), ext))
		assert.True(t, ext.Broker().Running())
	})
}

func TestStartFailsWithoutAdminAuth(t *testing.T) {
	app := newApp(t)
	ext := extension.New(extension.WithBrokerConfig(loopback()))

	err := boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext)
	require.Error(t, err)
	assert.ErrorIs(t, err, extension.ErrAdminAuthRequired)
}

// The admin group gets the admin middleware; the device endpoint does not.
func TestAdminMiddlewareGuardsAdminRoutesOnly(t *testing.T) {
	deny := func(next forge.Handler) forge.Handler {
		return func(c forge.Context) error {
			if c.Request().Header.Get("X-Admin") != "yes" {
				return forge.NewHTTPError(http.StatusUnauthorized, "admin only")
			}

			return next(c)
		}
	}

	app := newApp(t)
	ext := extension.New(extension.WithBrokerConfig(loopback()), extension.WithAdminMiddleware(deny))
	require.NoError(t, boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext))

	srv := httptest.NewServer(app.Router())
	t.Cleanup(srv.Close)

	get := func(path string, hdr ...string) int {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+path, nil)
		require.NoError(t, err)

		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusUnauthorized, get("/tether/v1/tenants"))
	assert.Equal(t, http.StatusOK, get("/tether/v1/tenants", "X-Admin", "yes"))

	// The publish endpoint answers its own 401 (Basic auth), not the
	// admin middleware's.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/tether/v1/publish", strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("WWW-Authenticate"), "Basic challenge from the publish route")
}

// promScrape is what forge's metrics collector offers for a scrape.
type promScrape interface{ PrometheusHandler() http.Handler }

// metricValue reads a sample from the app's Prometheus scrape. Forge
// prefixes app metrics with "forge_".
func metricValue(t *testing.T, app forge.App, series string) string {
	t.Helper()

	p, ok := app.Metrics().(promScrape)
	require.True(t, ok, "the app's metrics cannot be scraped")

	rec := httptest.NewRecorder()
	p.PrometheusHandler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil))

	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if name, value, ok := strings.Cut(line, " "); ok && name == "forge_"+series {
			return value
		}
	}

	require.FailNow(t, "metric not found", "%s in\n%s", series, rec.Body.String())

	return ""
}

func TestMetricsRegistered(t *testing.T) {
	e := start(t, false)

	tid := e.tenant()
	e.role(tid, "sensor", []string{"up"}, nil)
	d := e.device(tid, "sensor")

	c, err := e.connect(d, "dev-1")
	require.NoError(t, err)

	c.publish(t, "up", "1")

	_, err = e.connect(device{username: d.username, password: "tdk_live_wrong"}, "dev-2")
	require.Error(t, err)

	assert.Equal(t, "1", metricValue(t, e.app, extension.MetricConnections))
	assert.Equal(t, "1", metricValue(t, e.app, extension.MetricMessagesIn))
	assert.Equal(t, "1", metricValue(t, e.app, extension.MetricAuthFailures+`{reason="bad_credentials"}`))
}

// The tether section of the app's config file sets the base path and the
// broker, and the HTTP publish cap follows the broker's packet size.
func TestConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	yaml := "extensions:\n  tether:\n    base_path: /mqtt\n    broker:\n      tcp_address: 127.0.0.1:0\n      max_packet_size: 2048\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o600))

	app := newAppWith(t, forge.WithConfigSearchPaths(dir), forge.WithEnableAppScopedConfig(false))
	e := startApp(t, app, false, extension.WithInsecureAdminRoutes())

	require.NotNil(t, e.ext.Broker().Addr(), "the file's TCP address")
	assert.Equal(t, 2048, e.ext.Services().MaxPublishPayload)

	e.base = "/mqtt"
	tid := e.tenant()

	e.role(tid, "sensor", []string{"up"}, nil)
	d := e.device(tid, "sensor")

	publish := func(n int) int {
		body, err := json.Marshal(map[string]any{"topic": "up", "payload": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, n))})
		require.NoError(t, err)

		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.srv.URL+"/mqtt/v1/publish", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.SetBasicAuth(d.username, d.password)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()

		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, publish(2048))
	assert.Equal(t, http.StatusRequestEntityTooLarge, publish(2049))
}

// WithStore wins over the grove database in the container.
func TestWithStoreWins(t *testing.T) {
	st := memory.New()
	e := startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()), extension.WithStore(st))

	e.tenant()

	got, err := st.ListTenants(t.Context(), tenant.ListFilter{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, testOrg, got[0].ExternalID)
}

func TestStopStopsTheBroker(t *testing.T) {
	e := start(t, false)
	require.True(t, e.ext.Broker().Running())

	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	require.NoError(t, e.app.Stop(ctx))
	assert.False(t, e.ext.Broker().Running())
	assert.Error(t, e.ext.Health(t.Context()))
}
