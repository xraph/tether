package main

import (
	"bytes"
	"context"
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
	"github.com/xraph/vessel"

	"github.com/xraph/tether"
	"github.com/xraph/tether/extension"
)

// waitTimeout bounds every wait. Nothing sleeps: tests wait on channels.
const waitTimeout = 5 * time.Second

const (
	testToken = "s3cret-admin-token"
	testApp   = "aapp_1"
	testOrg   = "aorg_1"
)

// testConfig is the defaults, on a temp data dir and a random loopback port.
func testConfig(t *testing.T) config {
	t.Helper()

	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Server.SQLitePath = filepath.Join(dir, "data", "tether.db")
	cfg.Tether.Broker.DataDir = filepath.Join(dir, "data", "sessions")
	cfg.Tether.Broker.TCPAddress = "127.0.0.1:0"

	return cfg
}

// started is an app built by newApp and started in this process.
type started struct {
	app forge.App
	ext *extension.Extension
	srv *httptest.Server
}

func start(t *testing.T, cfg config, token string, insecure bool) *started {
	t.Helper()

	app, closeDB, err := newApp(cfg, token, insecure,
		forge.WithAppLogger(forge.NewNoopLogger()),
		forge.WithEnableConfigAutoDiscovery(false),
	)
	require.NoError(t, err)

	t.Cleanup(closeDB)

	require.NoError(t, app.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = app.Stop(ctx)
	})

	ext, err := vessel.Inject[*extension.Extension](app.Container())
	require.NoError(t, err)

	srv := httptest.NewServer(app.Router())
	t.Cleanup(srv.Close)

	return &started{app: app, ext: ext, srv: srv}
}

// do sends a request with an optional bearer token and JSON body, and decodes
// a JSON reply.
func (s *started) do(t *testing.T, method, path, token string, body any, want int) map[string]any {
	t.Helper()

	var rd io.Reader

	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)

		rd = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, s.srv.URL+path, rd)
	require.NoError(t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, want, resp.StatusCode, "%s %s: %s", method, path, raw)

	var m map[string]any
	if len(raw) > 0 && strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		require.NoError(t, json.Unmarshal(raw, &m), "body: %s", raw)
	}

	return m
}

// The binary's whole stack boots from its config: a tenant and a device come
// from the admin API behind the token, and the device round-trips a message
// over MQTT with the key keysmith issued.
func TestBinaryBootsAndServes(t *testing.T) {
	cfg := testConfig(t)
	s := start(t, cfg, testToken, false)

	// Without the token the admin API is closed.
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.srv.URL+"/tether/v1/tenants",
		strings.NewReader(`{"app_id":"aapp_1","external_id":"aorg_1","name":"Acme"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	_ = resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "Bearer", resp.Header.Get("WWW-Authenticate"))

	// With it, a tenant and a device.
	tenantRes := s.do(t, http.MethodPost, "/tether/v1/tenants", testToken,
		map[string]any{"app_id": testApp, "external_id": testOrg, "name": "Acme"}, http.StatusCreated)
	tid := tenantRes["id"].(string)

	// Warden's REST routes are off in the binary, so the role comes from
	// Tether's own API, with its topics.
	s.do(t, http.MethodPost, "/tether/v1/tenants/"+tid+"/roles", testToken,
		map[string]any{
			"slug": "sensor", "name": "Sensor",
			"publish": []string{"sensors/%c/up"}, "subscribe": []string{"sensors/%c/#"},
		}, http.StatusCreated)

	dev := s.do(t, http.MethodPost, "/tether/v1/tenants/"+tid+"/devices", testToken,
		map[string]any{"name": "pump", "roles": []string{"sensor"}}, http.StatusCreated)

	msgs := make(chan [2]string, 4)

	opts := pahov3.NewClientOptions().
		AddBroker("tcp://" + s.ext.Broker().Addr().String()).
		SetClientID("pump-1").
		SetUsername(dev["username"].(string)).
		SetPassword(dev["password"].(string)).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(waitTimeout)

	c := pahov3.NewClient(opts)
	tok := c.Connect()
	require.True(t, tok.WaitTimeout(waitTimeout), "connect timed out")
	require.NoError(t, tok.Error())

	t.Cleanup(func() { c.Disconnect(0) })

	tok = c.Subscribe("sensors/pump-1/#", 1, func(_ pahov3.Client, m pahov3.Message) {
		msgs <- [2]string{m.Topic(), string(m.Payload())}
	})
	require.True(t, tok.WaitTimeout(waitTimeout), "subscribe timed out")
	require.NoError(t, tok.Error())

	tok = c.Publish("sensors/pump-1/up", 1, false, "21.5")
	require.True(t, tok.WaitTimeout(waitTimeout), "publish timed out")
	require.NoError(t, tok.Error())

	select {
	case m := <-msgs:
		assert.Equal(t, [2]string{"sensors/pump-1/up", "21.5"}, m)
	case <-time.After(waitTimeout):
		require.FailNow(t, "no message received")
	}

	// The data dir the config names was created and used.
	assert.FileExists(t, cfg.Server.SQLitePath)
	assert.DirExists(t, cfg.Tether.Broker.DataDir)

	// Keysmith's own routes are off.
	s.do(t, http.MethodGet, "/keysmith/v1/keys", testToken, nil, http.StatusNotFound)

	// The dashboard is mounted and has discovered Tether's contributor.
	req, err = http.NewRequestWithContext(t.Context(), http.MethodGet, s.srv.URL+"/dashboard/api/dashboard/v1/capabilities", nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, string(body), `"name":"tether"`)
}

// A wrong token, a wrong scheme and an empty bearer all get 401 with the
// challenge header, and the right one gets through.
func TestAdminMiddleware(t *testing.T) {
	s := start(t, testConfig(t), testToken, false)

	for name, header := range map[string]string{
		"wrong token":  "Bearer nope",
		"wrong length": "Bearer " + testToken + "x",
		"basic scheme": "Basic " + testToken,
		"empty bearer": "Bearer ",
		"bare token":   testToken,
	} {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, s.srv.URL+"/tether/v1/tenants", nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", header)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			_ = resp.Body.Close()

			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			assert.Equal(t, "Bearer", resp.Header.Get("WWW-Authenticate"))
		})
	}

	s.do(t, http.MethodGet, "/tether/v1/tenants", testToken, nil, http.StatusOK)
}

// --insecure-admin boots with no token and leaves the admin API open.
func TestInsecureAdminNeedsNoToken(t *testing.T) {
	s := start(t, testConfig(t), "", true)

	s.do(t, http.MethodGet, "/tether/v1/tenants", "", nil, http.StatusOK)
}

// Without a token and without --insecure-admin the process refuses to start,
// and says what to set.
func TestRunRefusesWithoutToken(t *testing.T) {
	getenv := func(string) string { return "" }

	err := run([]string{"--config", filepath.Join(t.TempDir(), "absent.yaml")}, getenv, io.Discard, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TETHER_ADMIN_TOKEN")
	assert.Contains(t, err.Error(), "--insecure-admin")
}

// --version prints to stdout, and starts nothing.
func TestVersionGoesToStdout(t *testing.T) {
	var stdout, stderr bytes.Buffer

	require.NoError(t, run([]string{"--version"}, func(string) string { return "" }, &stdout, &stderr))
	assert.Equal(t, "tether "+tether.Version+"\n", stdout.String())
	assert.Empty(t, stderr.String())
}

// An explicit --config that does not exist is an error. The default path
// being absent is not.
func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()

	_, err := loadConfig(filepath.Join(dir, "absent.yaml"), true)
	require.Error(t, err)

	cfg, err := loadConfig(filepath.Join(dir, "absent.yaml"), false)
	require.NoError(t, err)
	assert.Equal(t, defaultConfig(), cfg)

	path := filepath.Join(dir, "tether.yaml")
	yaml := "server:\n  http_address: :9090\n  sqlite_path: /var/lib/tether/t.db\ntether:\n  base_path: /mqtt\n  broker:\n    tcp_address: :2883\n    max_session_expiry: 24h\n"
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	cfg, err = loadConfig(path, true)
	require.NoError(t, err)
	assert.Equal(t, ":9090", cfg.Server.HTTPAddress)
	assert.Equal(t, "/var/lib/tether/t.db", cfg.Server.SQLitePath)
	assert.Equal(t, "/mqtt", cfg.Tether.BasePath)
	assert.Equal(t, ":2883", cfg.Tether.Broker.TCPAddress)
	assert.Equal(t, 24*time.Hour, cfg.Tether.Broker.MaxSessionExpiry)
	assert.Equal(t, uint32(1<<20), cfg.Tether.Broker.MaxPacketSize, "a key the file omits keeps its default")
	assert.Equal(t, defaultDataDir, cfg.Tether.Broker.DataDir)
}

// tether.example.yaml documents every key with its default, so loading it
// must give back the defaults.
func TestExampleConfigMatchesDefaults(t *testing.T) {
	cfg, err := loadConfig(filepath.Join("..", "..", "tether.example.yaml"), true)
	require.NoError(t, err)
	assert.Equal(t, defaultConfig(), cfg)
}
