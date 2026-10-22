package api_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pahov3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"
	"github.com/xraph/warden/role"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/api"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/limits"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store/memory"
)

// waitTimeout bounds every wait. Nothing sleeps: tests wait on channels.
const waitTimeout = 5 * time.Second

const testApp = "aapp_1"

// env is the whole stack behind the API: stores, keysmith, warden, the
// services, a running broker and the API mounted on a Forge router.
type env struct {
	t       *testing.T
	srv     *httptest.Server
	tenants *service.Tenants
	devices *service.Devices
	wd      *warden.Engine
	broker  *broker.Broker
	limiter *limits.Limiter
}

func newEnv(t *testing.T) *env {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)

	km, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()))
	require.NoError(t, err)

	wd, err := warden.NewEngine(warden.WithStore(wmemory.New()))
	require.NoError(t, err)

	devices := service.NewDevices(st, tenants, km, service.NewWardenRoles(wd))
	authn := auth.NewKeysmith(km, tenants, st)
	loader := perm.NewLoader(wd)
	limiter := limits.New()

	cfg := broker.DefaultConfig()
	cfg.TCPAddress = "127.0.0.1:0"

	b, err := broker.New(cfg, broker.Deps{Auth: authn, Perms: loader, Limits: limiter})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	app := forge.New(forge.WithAppLogger(forge.NewNoopLogger()), forge.WithEnableConfigAutoDiscovery(false))
	router := app.Router()

	require.NoError(t, api.Register(router.Group("/tether"), router.Group("/tether"), api.Services{
		Tenants: tenants,
		Devices: devices,
		Roles:   perm.NewRoles(wd),
		Broker:  b,
		Auth:    authn,
		Perms:   loader,
		Limits:  limiter,
	}))

	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	return &env{t: t, srv: srv, tenants: tenants, devices: devices, wd: wd, broker: b, limiter: limiter}
}

// reply is a response with its body read.
type reply struct {
	*http.Response
	body []byte
	t    testing.TB
}

// json decodes the body into a map.
func (r reply) json() map[string]any {
	r.t.Helper()

	var m map[string]any

	require.NoError(r.t, json.Unmarshal(r.body, &m), "body: %s", r.body)

	return m
}

// do sends a request with an optional JSON body.
func (e *env) do(method, path string, body any, hdr ...string) reply {
	e.t.Helper()

	var rd io.Reader

	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		raw, err := json.Marshal(body)
		require.NoError(e.t, err)

		rd = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(e.t.Context(), method, e.srv.URL+path, rd)
	require.NoError(e.t, err)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)

	return reply{Response: resp, body: raw, t: e.t}
}

// expect fails unless r has status want.
func expect(t *testing.T, r reply, want int) reply {
	t.Helper()
	require.Equal(t, want, r.StatusCode, "body: %s", r.body)

	return r
}

// createTenant creates a tenant through the API.
func (e *env) createTenant(external string, limits map[string]any) string {
	e.t.Helper()

	body := map[string]any{"app_id": testApp, "external_id": external, "name": "tenant " + external}
	if limits != nil {
		body["limits"] = limits
	}

	r := expect(e.t, e.do(http.MethodPost, "/tether/v1/tenants", body), http.StatusCreated)

	return r.json()["id"].(string)
}

// device is a provisioned device.
type device struct {
	id, username, password string
}

func (e *env) provision(tenantID, name string, extra map[string]any) device {
	e.t.Helper()

	body := map[string]any{"name": name}
	for k, v := range extra {
		body[k] = v
	}

	r := expect(e.t, e.do(http.MethodPost, "/tether/v1/tenants/"+tenantID+"/devices", body), http.StatusCreated)
	m := r.json()
	d := m["device"].(map[string]any)

	return device{id: d["id"].(string), username: m["username"].(string), password: m["password"].(string)}
}

// createRole creates an empty warden role in the tenant.
func (e *env) createRole(external, slug string) {
	e.t.Helper()

	require.NoError(e.t, e.wd.Store().CreateRole(scope.With(e.t.Context(), testApp, external), &role.Role{
		TenantID: external, AppID: testApp, Name: slug, Slug: slug,
	}))
}

// grant creates a role with the given grants (through the API) and gives it
// to the device.
func (e *env) grant(tenantID, external string, d device, slug string, pub, sub []string) {
	e.t.Helper()

	e.createRole(external, slug)

	expect(e.t, e.do(http.MethodPut, "/tether/v1/tenants/"+tenantID+"/roles/"+slug+"/topics",
		map[string]any{"publish": pub, "subscribe": sub}), http.StatusOK)
	expect(e.t, e.do(http.MethodPut, "/tether/v1/tenants/"+tenantID+"/devices/"+d.id+"/roles",
		map[string]any{"roles": []string{slug}}), http.StatusOK)
}

// mqtt is a connected MQTT 3.1.1 client.
type mqtt struct {
	pahov3.Client
	msgs chan msg
	lost chan error
}

type msg struct{ Topic, Payload string }

// connect connects the device over MQTT with its credentials.
func (e *env) connect(d device, clientID string) *mqtt {
	e.t.Helper()

	c, err := e.tryConnect(d.username, d.password, clientID)
	require.NoError(e.t, err)

	return c
}

func (e *env) tryConnect(user, pass, clientID string) (*mqtt, error) {
	e.t.Helper()

	m := &mqtt{msgs: make(chan msg, 16), lost: make(chan error, 1)}

	opts := pahov3.NewClientOptions().
		AddBroker("tcp://" + e.broker.Addr().String()).
		SetClientID(clientID).
		SetUsername(user).
		SetPassword(pass).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(waitTimeout).
		SetConnectionLostHandler(func(_ pahov3.Client, err error) { m.lost <- err })

	m.Client = pahov3.NewClient(opts)

	tok := m.Connect()
	if !tok.WaitTimeout(waitTimeout) {
		return nil, context.DeadlineExceeded
	}

	if err := tok.Error(); err != nil {
		return nil, err
	}

	e.t.Cleanup(func() { m.Disconnect(0) })

	return m, nil
}

func (m *mqtt) subscribe(t *testing.T, filter string) {
	t.Helper()

	tok := m.Subscribe(filter, 1, func(_ pahov3.Client, p pahov3.Message) {
		m.msgs <- msg{Topic: p.Topic(), Payload: string(p.Payload())}
	})
	require.True(t, tok.WaitTimeout(waitTimeout), "subscribe timed out")
	require.NoError(t, tok.Error())
}

func (m *mqtt) publishRetained(t *testing.T, topic, payload string) {
	t.Helper()

	tok := m.Publish(topic, 1, true, payload)
	require.True(t, tok.WaitTimeout(waitTimeout), "publish timed out")
	require.NoError(t, tok.Error())
}

func (m *mqtt) next(t *testing.T) msg {
	t.Helper()

	select {
	case x := <-m.msgs:
		return x
	case <-time.After(waitTimeout):
		require.FailNow(t, "no message received")

		return msg{}
	}
}

func (m *mqtt) requireLost(t *testing.T) {
	t.Helper()

	select {
	case <-m.lost:
	case <-time.After(waitTimeout):
		require.FailNow(t, "the connection was not closed")
	}
}

func (m *mqtt) requireAlive(t *testing.T) {
	t.Helper()

	select {
	case err := <-m.lost:
		require.FailNow(t, "the connection was closed", "%v", err)
	default:
	}

	require.True(t, m.IsConnectionOpen())
}

func tenantPath(tenantID string) string { return "/tether/v1/tenants/" + tenantID }

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

func TestTenantsCRUD(t *testing.T) {
	e := newEnv(t)

	r := expect(t, e.do(http.MethodPost, "/tether/v1/tenants", map[string]any{
		"app_id": testApp, "external_id": "aorg_1", "name": "Acme",
		"limits": map[string]any{"max_connections": 5, "messages_per_second": 2.5},
	}), http.StatusCreated)

	created := r.json()
	tenantID := created["id"].(string)
	assert.True(t, strings.HasPrefix(tenantID, "ttnt_"))
	assert.Equal(t, true, created["enabled"])
	assert.Equal(t, "Acme", created["name"])
	assert.InDelta(t, 5, created["limits"].(map[string]any)["max_connections"], 0)

	got := expect(t, e.do(http.MethodGet, tenantPath(tenantID), nil), http.StatusOK).json()
	assert.Equal(t, created["id"], got["id"])
	assert.Equal(t, "aorg_1", got["external_id"])

	e.createTenant("aorg_2", nil)

	l := expect(t, e.do(http.MethodGet, "/tether/v1/tenants", nil), http.StatusOK).json()
	assert.Len(t, l["items"], 2)

	l = expect(t, e.do(http.MethodGet, "/tether/v1/tenants?limit=1&offset=1", nil), http.StatusOK).json()
	assert.Len(t, l["items"], 1)

	upd := expect(t, e.do(http.MethodPatch, tenantPath(tenantID), map[string]any{
		"name": "Acme Corp", "limits": map[string]any{"max_connections": 9},
	}), http.StatusOK).json()
	assert.Equal(t, "Acme Corp", upd["name"])
	assert.Equal(t, true, upd["enabled"], "an absent field is untouched")
	assert.InDelta(t, 9, upd["limits"].(map[string]any)["max_connections"], 0)

	expect(t, e.do(http.MethodDelete, tenantPath(tenantID), nil), http.StatusNoContent)
	expect(t, e.do(http.MethodGet, tenantPath(tenantID), nil), http.StatusNotFound)
}

func TestTenantsEmptyListIsAnArray(t *testing.T) {
	e := newEnv(t)

	r := expect(t, e.do(http.MethodGet, "/tether/v1/tenants", nil), http.StatusOK)
	assert.JSONEq(t, `{"items":[]}`, string(r.body))
}

func TestTenantsRejectBadInput(t *testing.T) {
	e := newEnv(t)
	e.createTenant("aorg_1", nil)

	tests := []struct {
		name   string
		method string
		path   string
		body   any
		want   int
	}{
		{"missing app id", http.MethodPost, "/tether/v1/tenants", map[string]any{"external_id": "x"}, http.StatusBadRequest},
		{"missing external id", http.MethodPost, "/tether/v1/tenants", map[string]any{"app_id": testApp}, http.StatusBadRequest},
		{"duplicate external id", http.MethodPost, "/tether/v1/tenants", map[string]any{"app_id": "aapp_2", "external_id": "aorg_1"}, http.StatusConflict},
		{"negative limit", http.MethodPost, "/tether/v1/tenants", map[string]any{"app_id": testApp, "external_id": "n", "limits": map[string]any{"burst": -1}}, http.StatusBadRequest},
		{"not json", http.MethodPost, "/tether/v1/tenants", []byte("{nope"), http.StatusBadRequest},
		{"malformed id", http.MethodGet, "/tether/v1/tenants/not-an-id", nil, http.StatusBadRequest},
		{"device id as tenant id", http.MethodGet, "/tether/v1/tenants/" + id.NewDeviceID().String(), nil, http.StatusBadRequest},
		{"unknown tenant", http.MethodGet, "/tether/v1/tenants/" + id.NewTenantID().String(), nil, http.StatusNotFound},
		{"patch unknown tenant", http.MethodPatch, "/tether/v1/tenants/" + id.NewTenantID().String(), map[string]any{"name": "x"}, http.StatusNotFound},
		{"delete unknown tenant", http.MethodDelete, "/tether/v1/tenants/" + id.NewTenantID().String(), nil, http.StatusNotFound},
		{"negative limit param", http.MethodGet, "/tether/v1/tenants?limit=-1", nil, http.StatusBadRequest},
		{"limit over the cap", http.MethodGet, "/tether/v1/tenants?limit=100000", nil, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expect(t, e.do(tt.method, tt.path, tt.body), tt.want)
		})
	}
}

func TestTenantDisableDisconnectsLiveClient(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	d := e.provision(tid, "pump", nil)

	c := e.connect(d, "pump-1")
	c.requireAlive(t)

	expect(t, e.do(http.MethodPatch, tenantPath(tid), map[string]any{"enabled": false}), http.StatusOK)
	c.requireLost(t)

	_, err := e.tryConnect(d.username, d.password, "pump-1")
	require.Error(t, err, "a disabled tenant's devices are refused")
}

func TestTenantPatchGivesLiveClientsNewLimits(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	d := e.provision(tid, "pump", nil)
	e.grant(tid, "aorg_1", d, "pub", []string{"t/#"}, []string{"t/#"})

	c := e.connect(d, "pump-1")
	c.subscribe(t, "t/#")

	// A burst of 1 at a trickle: the second publish from the live connection
	// is dropped by the new limits, which it could not be before the PATCH.
	expect(t, e.do(http.MethodPatch, tenantPath(tid), map[string]any{
		"limits": map[string]any{"messages_per_second": 0.001, "burst": 1},
	}), http.StatusOK)

	require.True(t, c.Publish("t/a", 0, false, "1").WaitTimeout(waitTimeout))
	require.True(t, c.Publish("t/b", 0, false, "2").WaitTimeout(waitTimeout))

	assert.Equal(t, "t/a", c.next(t).Topic)

	select {
	case m := <-c.msgs:
		require.FailNow(t, "the second publish passed the new limit", "%+v", m)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestTenantDeleteDisconnectsLiveClients(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	d := e.provision(tid, "pump", nil)
	c := e.connect(d, "pump-1")

	// A tenant with a device is not deleted, and its connection is left alone.
	expect(t, e.do(http.MethodDelete, tenantPath(tid), nil), http.StatusConflict)
	expect(t, e.do(http.MethodGet, tenantPath(tid), nil), http.StatusOK)
	c.requireAlive(t)

	// Without a device (removed under the connection, as no key events are
	// wired in this test) the delete goes through and ends the connection.
	tidParsed, err := id.ParseTenantID(tid)
	require.NoError(t, err)
	didParsed, err := id.ParseDeviceID(d.id)
	require.NoError(t, err)
	require.NoError(t, e.devices.Delete(t.Context(), tidParsed, didParsed))

	expect(t, e.do(http.MethodDelete, tenantPath(tid), nil), http.StatusNoContent)
	c.requireLost(t)
}

// ---------------------------------------------------------------------------
// Devices
// ---------------------------------------------------------------------------

func TestDevicesLifecycle(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid) + "/devices"

	d := e.provision(tid, "pump", map[string]any{"client_id": "pump-1"})
	assert.True(t, strings.HasPrefix(d.id, "tdev_"))
	assert.True(t, strings.HasPrefix(d.username, "akey_"))
	assert.NotEmpty(t, d.password)

	got := expect(t, e.do(http.MethodGet, base+"/"+d.id, nil), http.StatusOK).json()
	assert.Equal(t, d.id, got["id"])
	assert.Equal(t, tid, got["tenant_id"])
	assert.Equal(t, d.username, got["key_id"])
	assert.Equal(t, "pump-1", got["client_id"])

	l := expect(t, e.do(http.MethodGet, base, nil), http.StatusOK).json()
	assert.Len(t, l["items"], 1)

	// The device connects with what provisioning returned.
	e.connect(d, "pump-1")

	rot := expect(t, e.do(http.MethodPost, base+"/"+d.id+"/rotate", map[string]any{"grace_seconds": 60}), http.StatusOK)
	assert.Equal(t, "no-store", rot.Header.Get("Cache-Control"))

	rotated := rot.json()
	assert.Equal(t, d.username, rotated["username"], "the username stays")
	assert.NotEmpty(t, rotated["password"])
	assert.NotEqual(t, d.password, rotated["password"])

	expect(t, e.do(http.MethodPost, base+"/"+d.id+"/revoke", map[string]any{"reason": "lost"}), http.StatusNoContent)

	// Revoking keeps the record. Rotating a revoked device is refused.
	expect(t, e.do(http.MethodGet, base+"/"+d.id, nil), http.StatusOK)
	r := e.do(http.MethodPost, base+"/"+d.id+"/rotate", nil)
	assert.GreaterOrEqual(t, r.StatusCode, 400)
	assert.Less(t, r.StatusCode, 500, "body: %s", r.body)

	expect(t, e.do(http.MethodDelete, base+"/"+d.id, nil), http.StatusNoContent)
	expect(t, e.do(http.MethodGet, base+"/"+d.id, nil), http.StatusNotFound)
	expect(t, e.do(http.MethodDelete, base+"/"+d.id, nil), http.StatusNotFound)
}

func TestRevokeWithoutBody(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	d := e.provision(tid, "pump", nil)

	expect(t, e.do(http.MethodPost, tenantPath(tid)+"/devices/"+d.id+"/revoke", nil), http.StatusNoContent)
}

func TestProvisionReturnsPasswordOnce(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid) + "/devices"

	r := expect(t, e.do(http.MethodPost, base, map[string]any{"name": "pump"}), http.StatusCreated)
	assert.Equal(t, "no-store", r.Header.Get("Cache-Control"))

	m := r.json()
	password := m["password"].(string)
	require.NotEmpty(t, password)

	deviceID := m["device"].(map[string]any)["id"].(string)

	for _, path := range []string{base + "/" + deviceID, base} {
		got := expect(t, e.do(http.MethodGet, path, nil), http.StatusOK)

		assert.NotContains(t, string(got.body), password, "GET %s must not show the key", path)
		assert.NotContains(t, strings.ToLower(string(got.body)), "password", "GET %s", path)
	}

	// Revoke and delete answer with nothing to leak either.
	assert.NotContains(t, string(e.do(http.MethodPost, base+"/"+deviceID+"/revoke", nil).body), password)
}

func TestProvisionValidation(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid) + "/devices"

	tests := []struct {
		name string
		path string
		body any
		want int
	}{
		{"no name", base, map[string]any{}, http.StatusBadRequest},
		{"client id with a slash", base, map[string]any{"name": "x", "client_id": "a/b"}, http.StatusBadRequest},
		{"unknown role", base, map[string]any{"name": "x", "roles": []string{"ghost"}}, http.StatusBadRequest},
		{"unknown tenant", tenantPath(id.NewTenantID().String()) + "/devices", map[string]any{"name": "x"}, http.StatusNotFound},
		{"malformed tenant", "/tether/v1/tenants/zzz/devices", map[string]any{"name": "x"}, http.StatusBadRequest},
		{"unknown tenant list", tenantPath(id.NewTenantID().String()) + "/devices", nil, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method := http.MethodPost
			if tt.body == nil {
				method = http.MethodGet
			}

			expect(t, e.do(method, tt.path, tt.body), tt.want)
		})
	}
}

func TestDeviceRolesAndTopics(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid)
	d := e.provision(tid, "pump", nil)

	e.createRole("aorg_1", "sensor")

	topics := base + "/roles/sensor/topics"

	// A new role has no grants.
	assert.JSONEq(t, `{"publish":[],"subscribe":[]}`, string(expect(t, e.do(http.MethodGet, topics, nil), http.StatusOK).body))

	put := expect(t, e.do(http.MethodPut, topics, map[string]any{
		"publish": []string{"dev/%u/up"}, "subscribe": []string{"dev/%u/cmd/#"},
	}), http.StatusOK).json()
	assert.Equal(t, []any{"dev/%u/up"}, put["publish"])

	got := expect(t, e.do(http.MethodGet, topics, nil), http.StatusOK).json()
	assert.Equal(t, []any{"dev/%u/cmd/#"}, got["subscribe"])

	expect(t, e.do(http.MethodPut, topics, map[string]any{"publish": []string{"a/#/b"}}), http.StatusBadRequest)
	expect(t, e.do(http.MethodGet, base+"/roles/ghost/topics", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodPut, base+"/roles/ghost/topics", map[string]any{}), http.StatusNotFound)

	dev := expect(t, e.do(http.MethodPut, base+"/devices/"+d.id+"/roles", map[string]any{"roles": []string{"sensor"}}), http.StatusOK).json()
	assert.Equal(t, []any{"sensor"}, dev["roles"])

	dev = expect(t, e.do(http.MethodPut, base+"/devices/"+d.id+"/roles", map[string]any{"roles": []string{}}), http.StatusOK).json()
	assert.Empty(t, dev["roles"])

	expect(t, e.do(http.MethodPut, base+"/devices/"+d.id+"/roles", map[string]any{}), http.StatusBadRequest)
	expect(t, e.do(http.MethodPut, base+"/devices/"+d.id+"/roles", map[string]any{"roles": []string{"ghost"}}), http.StatusBadRequest)
}

// ---------------------------------------------------------------------------
// Clients, sessions, retained, stats
// ---------------------------------------------------------------------------

func TestClientsAndStats(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid)

	d := e.provision(tid, "pump", nil)
	e.grant(tid, "aorg_1", d, "all", []string{"#"}, []string{"#"})

	c := e.connect(d, "pump-1")
	c.subscribe(t, "t/#")

	l := expect(t, e.do(http.MethodGet, base+"/clients", nil), http.StatusOK).json()
	require.Len(t, l["items"], 1)

	ci := l["items"].([]any)[0].(map[string]any)
	assert.Equal(t, "pump-1", ci["client_id"])
	assert.Equal(t, d.username, ci["username"])
	assert.Equal(t, d.id, ci["device_id"])
	assert.Equal(t, true, ci["connected"])
	assert.Equal(t, []any{"t/#"}, ci["subscriptions"])

	st := expect(t, e.do(http.MethodGet, base+"/stats", nil), http.StatusOK).json()
	assert.InDelta(t, 1, st["connected"], 0)
	assert.InDelta(t, 1, st["subscriptions"], 0)

	expect(t, e.do(http.MethodPost, base+"/clients/pump-1/kick", nil), http.StatusNoContent)
	c.requireLost(t)
	expect(t, e.do(http.MethodPost, base+"/clients/pump-1/kick", nil), http.StatusNotFound)

	// A clean session ended with the connection, so there is none to delete.
	expect(t, e.do(http.MethodDelete, base+"/sessions/pump-1", nil), http.StatusNotFound)
}

func TestDeleteSessionDisconnectsClient(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	d := e.provision(tid, "pump", nil)

	c := e.connect(d, "pump-1")

	expect(t, e.do(http.MethodDelete, tenantPath(tid)+"/sessions/pump-1", nil), http.StatusNoContent)
	c.requireLost(t)
}

func TestRetainedAdmin(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid) + "/retained"

	d := e.provision(tid, "pump", nil)
	e.grant(tid, "aorg_1", d, "all", []string{"#"}, []string{"#"})

	c := e.connect(d, "pump-1")
	c.publishRetained(t, "state/a", "on")
	c.publishRetained(t, "state/b", "off")
	c.publishRetained(t, "other/c", "x")

	l := expect(t, e.do(http.MethodGet, base, nil), http.StatusOK).json()
	assert.Len(t, l["items"], 3, "no filter lists everything")

	l = expect(t, e.do(http.MethodGet, base+"?filter=state/%23", nil), http.StatusOK).json()
	require.Len(t, l["items"], 2)
	assert.Equal(t, "state/a", l["items"].([]any)[0].(map[string]any)["topic"])
	assert.InDelta(t, 2, l["items"].([]any)[0].(map[string]any)["size"], 0)

	expect(t, e.do(http.MethodGet, base+"?filter=a/%23/b", nil), http.StatusBadRequest)

	expect(t, e.do(http.MethodDelete, base+"?topic=state/a", nil), http.StatusNoContent)
	expect(t, e.do(http.MethodDelete, base+"?topic=state/a", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodDelete, base, nil), http.StatusBadRequest)
	expect(t, e.do(http.MethodDelete, base+"?topic=state/%2B", nil), http.StatusBadRequest)

	l = expect(t, e.do(http.MethodGet, base+"?filter=state/%23", nil), http.StatusOK).json()
	assert.Len(t, l["items"], 1)
}

func TestBrokerStoppedIs503(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)
	d := e.provision(tid, "pump", nil)
	e.grant(tid, "aorg_1", d, "all", []string{"#"}, []string{"#"})

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	require.NoError(t, e.broker.Stop(ctx))

	base := tenantPath(tid)
	tests := []struct {
		method, path string
	}{
		{http.MethodGet, base + "/clients"},
		{http.MethodPost, base + "/clients/x/kick"},
		{http.MethodDelete, base + "/sessions/x"},
		{http.MethodGet, base + "/retained"},
		{http.MethodDelete, base + "/retained?topic=a"},
		{http.MethodGet, base + "/stats"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			expect(t, e.do(tt.method, tt.path, nil), http.StatusServiceUnavailable)
		})
	}

	r := e.publish(d.username, d.password, map[string]any{"topic": "a", "payload": ""})
	expect(t, r, http.StatusServiceUnavailable)

	// The store-backed routes keep working.
	expect(t, e.do(http.MethodGet, base, nil), http.StatusOK)
	expect(t, e.do(http.MethodGet, base+"/devices", nil), http.StatusOK)
	expect(t, e.do(http.MethodPatch, base, map[string]any{"name": "still editable"}), http.StatusOK)
}

// ---------------------------------------------------------------------------
// Tenant scope
// ---------------------------------------------------------------------------

// Every route that takes a device or client under a tenant must treat an ID
// that belongs to another tenant as missing, and leave it alone.
func TestTenantScopeInPath(t *testing.T) {
	e := newEnv(t)
	a := e.createTenant("aorg_1", nil)
	b := e.createTenant("aorg_2", nil)

	devB := e.provision(b, "b-pump", map[string]any{"client_id": "b-1"})
	e.grant(b, "aorg_2", devB, "all", []string{"#"}, []string{"#"})

	cb := e.connect(devB, "b-1")
	cb.publishRetained(t, "secret/state", "b-only")

	pathA := tenantPath(a)
	dev := pathA + "/devices/" + devB.id

	expect(t, e.do(http.MethodGet, dev, nil), http.StatusNotFound)
	expect(t, e.do(http.MethodPost, dev+"/rotate", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodPost, dev+"/revoke", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodPut, dev+"/roles", map[string]any{"roles": []string{}}), http.StatusNotFound)
	expect(t, e.do(http.MethodDelete, dev, nil), http.StatusNotFound)

	l := expect(t, e.do(http.MethodGet, pathA+"/devices", nil), http.StatusOK).json()
	assert.Empty(t, l["items"], "tenant A lists none of B's devices")

	// B's client, session and retained message are invisible and untouchable
	// from A's path.
	l = expect(t, e.do(http.MethodGet, pathA+"/clients", nil), http.StatusOK).json()
	assert.Empty(t, l["items"])

	expect(t, e.do(http.MethodPost, pathA+"/clients/b-1/kick", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodDelete, pathA+"/sessions/b-1", nil), http.StatusNotFound)

	l = expect(t, e.do(http.MethodGet, pathA+"/retained", nil), http.StatusOK).json()
	assert.Empty(t, l["items"])
	expect(t, e.do(http.MethodDelete, pathA+"/retained?topic=secret/state", nil), http.StatusNotFound)

	st := expect(t, e.do(http.MethodGet, pathA+"/stats", nil), http.StatusOK).json()
	assert.InDelta(t, 0, st["connected"], 0)
	assert.InDelta(t, 0, st["retained"], 0)

	// Role grants are per tenant too: A has no such role.
	e.createRole("aorg_2", "b-role")
	expect(t, e.do(http.MethodGet, pathA+"/roles/b-role/topics", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodPut, pathA+"/roles/b-role/topics", map[string]any{"publish": []string{"#"}}), http.StatusNotFound)

	// Nothing of B's changed.
	cb.requireAlive(t)
	expect(t, e.do(http.MethodGet, tenantPath(b)+"/devices/"+devB.id, nil), http.StatusOK)
	l = expect(t, e.do(http.MethodGet, tenantPath(b)+"/retained", nil), http.StatusOK).json()
	assert.Len(t, l["items"], 1)

	// A client ID that spells another tenant's namespace is just a client ID.
	expect(t, e.do(http.MethodPost, pathA+"/clients/"+b+"%2Fb-1/kick", nil), http.StatusNotFound)
	cb.requireAlive(t)
}

// ---------------------------------------------------------------------------
// POST /v1/publish
// ---------------------------------------------------------------------------

// publish posts to /v1/publish with HTTP Basic.
func (e *env) publish(user, pass string, body any, hdr ...string) reply {
	e.t.Helper()

	hdr = append(hdr, "Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))

	return e.do(http.MethodPost, "/tether/v1/publish", body, hdr...)
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// publisher is a tenant with a device that may publish under "up/" and
// subscribe to "up/#", and a connected subscriber.
func publisher(t *testing.T, e *env, limits map[string]any) (device, *mqtt, string) {
	t.Helper()

	tid := e.createTenant("aorg_1", limits)
	d := e.provision(tid, "pump", nil)
	e.grant(tid, "aorg_1", d, "up", []string{"up/%u/#", "up/open/#"}, []string{"up/#"})

	sub := e.provision(tid, "sub", nil)
	e.grant(tid, "aorg_1", sub, "listen", nil, []string{"up/#"})

	c := e.connect(sub, "listener")
	c.subscribe(t, "up/#")

	return d, c, tid
}

func TestPublishDeliversToSubscribers(t *testing.T) {
	e := newEnv(t)
	d, listener, tid := publisher(t, e, nil)

	r := expect(t, e.publish(d.username, d.password, map[string]any{
		"topic": "up/open/reading", "payload": b64("21.5"), "qos": 1,
	}), http.StatusOK)
	assert.Equal(t, true, r.json()["published"])

	assert.Equal(t, msg{Topic: "up/open/reading", Payload: "21.5"}, listener.next(t))

	// The username placeholder expands for the device's own key ID.
	expect(t, e.publish(d.username, d.password, map[string]any{
		"topic": "up/" + d.username + "/x", "payload": b64("mine"),
	}), http.StatusOK)
	assert.Equal(t, "mine", listener.next(t).Payload)

	// A retained publish is listed by the admin API.
	expect(t, e.publish(d.username, d.password, map[string]any{
		"topic": "up/open/last", "payload": b64("kept"), "retain": true,
	}), http.StatusOK)
	assert.Equal(t, "kept", listener.next(t).Payload)

	l := expect(t, e.do(http.MethodGet, tenantPath(tid)+"/retained", nil), http.StatusOK).json()
	require.Len(t, l["items"], 1)
	assert.Equal(t, "up/open/last", l["items"].([]any)[0].(map[string]any)["topic"])
}

func TestPublishDeniedOutsideGrant(t *testing.T) {
	e := newEnv(t)
	d, listener, _ := publisher(t, e, nil)

	for _, topic := range []string{"up/other/x", "elsewhere", "up"} {
		r := expect(t, e.publish(d.username, d.password, map[string]any{"topic": topic, "payload": b64("x")}), http.StatusForbidden)
		assert.NotContains(t, string(r.body), d.password)
	}

	// A device with no role at all may publish nothing.
	tid := e.createTenant("aorg_9", nil)
	bare := e.provision(tid, "bare", nil)
	expect(t, e.publish(bare.username, bare.password, map[string]any{"topic": "up/open/x", "payload": ""}), http.StatusForbidden)

	// A grant that needs the client ID cannot match without one.
	e.createRole("aorg_9", "byclient")
	expect(t, e.do(http.MethodPut, tenantPath(tid)+"/roles/byclient/topics", map[string]any{"publish": []string{"c/%c/up"}}), http.StatusOK)
	expect(t, e.do(http.MethodPut, tenantPath(tid)+"/devices/"+bare.id+"/roles", map[string]any{"roles": []string{"byclient"}}), http.StatusOK)
	expect(t, e.publish(bare.username, bare.password, map[string]any{"topic": "c/bare/up", "payload": ""}), http.StatusForbidden)
	expect(t, e.publish(bare.username, bare.password, map[string]any{"topic": "c/%c/up", "payload": ""}), http.StatusForbidden)

	select {
	case m := <-listener.msgs:
		require.FailNow(t, "a denied publish was delivered", "%+v", m)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestPublishRateLimited(t *testing.T) {
	e := newEnv(t)
	d, listener, _ := publisher(t, e, map[string]any{"messages_per_second": 0.001, "burst": 1})

	body := map[string]any{"topic": "up/open/x", "payload": b64("1")}

	expect(t, e.publish(d.username, d.password, body), http.StatusOK)
	assert.Equal(t, "1", listener.next(t).Payload)

	r := expect(t, e.publish(d.username, d.password, body), http.StatusTooManyRequests)
	assert.NotEmpty(t, r.Header.Get("Retry-After"))
}

func TestPublishAuthentication(t *testing.T) {
	e := newEnv(t)
	d, _, tid := publisher(t, e, nil)

	body := map[string]any{"topic": "up/open/x", "payload": ""}

	t.Run("no credentials", func(t *testing.T) {
		r := expect(t, e.do(http.MethodPost, "/tether/v1/publish", body), http.StatusUnauthorized)
		assert.Contains(t, r.Header.Get("WWW-Authenticate"), "Basic")
	})

	t.Run("wrong password", func(t *testing.T) {
		r := expect(t, e.publish(d.username, "tdk_live_nope", body), http.StatusUnauthorized)
		assert.Contains(t, r.Header.Get("WWW-Authenticate"), "Basic")
	})

	t.Run("password of another key", func(t *testing.T) {
		other := e.provision(tid, "other", nil)
		expect(t, e.publish(d.username, other.password, body), http.StatusUnauthorized)
	})

	t.Run("unknown username", func(t *testing.T) {
		expect(t, e.publish("akey_nobody", d.password, body), http.StatusUnauthorized)
	})

	t.Run("failures say the same thing", func(t *testing.T) {
		a := e.publish(d.username, "tdk_live_nope", body)
		b := e.publish("akey_nobody", d.password, body)
		assert.JSONEq(t, string(a.body), string(b.body))
	})

	t.Run("disabled tenant", func(t *testing.T) {
		expect(t, e.do(http.MethodPatch, tenantPath(tid), map[string]any{"enabled": false}), http.StatusOK)
		expect(t, e.publish(d.username, d.password, body), http.StatusForbidden)
	})
}

func TestPublishValidation(t *testing.T) {
	e := newEnv(t)
	d, _, _ := publisher(t, e, nil)

	big := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, api.MaxPublishPayload+1))
	edge := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'x'}, api.MaxPublishPayload))

	tests := []struct {
		name string
		body any
		want int
	}{
		{"bad base64", map[string]any{"topic": "up/open/x", "payload": "!!not base64!!"}, http.StatusBadRequest},
		{"url safe base64", map[string]any{"topic": "up/open/x", "payload": "_-8="}, http.StatusBadRequest},
		{"qos 2", map[string]any{"topic": "up/open/x", "payload": "", "qos": 2}, http.StatusBadRequest},
		{"qos negative", map[string]any{"topic": "up/open/x", "payload": "", "qos": -1}, http.StatusBadRequest},
		{"qos is a string", []byte(`{"topic":"up/open/x","payload":"","qos":"1"}`), http.StatusBadRequest},
		{"not json", []byte("topic=x"), http.StatusBadRequest},
		{"empty body", []byte(""), http.StatusBadRequest},
		{"wildcard topic", map[string]any{"topic": "up/#", "payload": ""}, http.StatusBadRequest},
		{"empty topic", map[string]any{"topic": "", "payload": ""}, http.StatusBadRequest},
		{"payload one byte over", map[string]any{"topic": "up/open/x", "payload": big}, http.StatusRequestEntityTooLarge},
		{"payload at the limit", map[string]any{"topic": "up/open/x", "payload": edge}, http.StatusOK},
		{"body far over", []byte(`{"topic":"up/open/x","payload":"` + strings.Repeat("A", 3<<20) + `"}`), http.StatusRequestEntityTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expect(t, e.publish(d.username, d.password, tt.body), tt.want)
		})
	}

	t.Run("sys topics", func(t *testing.T) {
		r := e.publish(d.username, d.password, map[string]any{"topic": "$SYS/x", "payload": ""})
		assert.GreaterOrEqual(t, r.StatusCode, 400)
		assert.Less(t, r.StatusCode, 500)
	})
}

func TestPublishIsIdempotent(t *testing.T) {
	e := newEnv(t)
	d, listener, _ := publisher(t, e, nil)

	body := map[string]any{"topic": "up/open/x", "payload": b64("once")}

	first := expect(t, e.publish(d.username, d.password, body, "Idempotency-Key", "k-1"), http.StatusOK)
	assert.Equal(t, "once", listener.next(t).Payload)

	again := expect(t, e.publish(d.username, d.password, body, "Idempotency-Key", "k-1"), http.StatusOK)
	assert.JSONEq(t, string(first.body), string(again.body))
	assert.Empty(t, again.Header.Get("Idempotency-Skipped"), "the key was honoured")

	// A different key publishes again, and the replay delivered nothing: the
	// next message is the second publish's, not a third.
	expect(t, e.publish(d.username, d.password, map[string]any{"topic": "up/open/x", "payload": b64("twice")}, "Idempotency-Key", "k-2"), http.StatusOK)
	assert.Equal(t, "twice", listener.next(t).Payload)

	// The same key with another body is refused, not replayed.
	r := e.publish(d.username, d.password, map[string]any{"topic": "up/open/x", "payload": b64("different")}, "Idempotency-Key", "k-1")
	assert.Equal(t, http.StatusUnprocessableEntity, r.StatusCode, "body: %s", r.body)
}

func TestPublishIdempotencyDoesNotSkipAuthentication(t *testing.T) {
	e := newEnv(t)
	d, listener, _ := publisher(t, e, nil)

	body := map[string]any{"topic": "up/open/x", "payload": b64("once")}

	expect(t, e.publish(d.username, d.password, body, "Idempotency-Key", "k-1"), http.StatusOK)
	assert.Equal(t, "once", listener.next(t).Payload)

	// Someone who knows the username and the key but not the password gets no
	// replay of the stored response.
	expect(t, e.publish(d.username, "tdk_live_wrong", body, "Idempotency-Key", "k-1"), http.StatusUnauthorized)
}

func TestPublishBodyOverTheIdempotencyDefault(t *testing.T) {
	e := newEnv(t)
	d, _, _ := publisher(t, e, nil)

	// 900 KiB decoded is about 1.2 MiB of JSON, over forge's own 1 MiB cap
	// for a keyed body. It must still go through.
	payload := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{'y'}, 900<<10))

	expect(t, e.publish(d.username, d.password, map[string]any{"topic": "up/open/big", "payload": payload}, "Idempotency-Key", "big-1"), http.StatusOK)
}

// Controller ruling R16. A device pinned to a client ID publishes over HTTP
// with that ID as %c, so "devices/%c/up" is its own topic and no one else's.
// An unpinned device has no client ID, so a %c grant stays unusable for it.
func TestPublishPinnedDeviceUsesItsClientIDGrant(t *testing.T) {
	e := newEnv(t)
	tid := e.createTenant("aorg_1", nil)

	pinned := e.provision(tid, "pinned", map[string]any{"client_id": "pump-7"})
	e.grant(tid, "aorg_1", pinned, "own", []string{"devices/%c/up"}, nil)

	sub := e.provision(tid, "sub", nil)
	e.grant(tid, "aorg_1", sub, "listen", nil, []string{"devices/#"})

	listener := e.connect(sub, "listener")
	listener.subscribe(t, "devices/#")

	expect(t, e.publish(pinned.username, pinned.password, map[string]any{
		"topic": "devices/pump-7/up", "payload": b64("mine"),
	}), http.StatusOK)
	assert.Equal(t, msg{Topic: "devices/pump-7/up", Payload: "mine"}, listener.next(t))

	for _, topic := range []string{"devices/pump-8/up", "devices/%c/up", "devices/pump-7/down"} {
		expect(t, e.publish(pinned.username, pinned.password, map[string]any{"topic": topic, "payload": ""}), http.StatusForbidden)
	}

	// The pin still holds on MQTT: another client ID is refused at CONNECT.
	_, err := e.tryConnect(pinned.username, pinned.password, "pump-8")
	require.Error(t, err)

	// An unpinned device with the same kind of grant publishes nowhere
	// through it: it has no client ID to expand.
	free := e.provision(tid, "free", nil)
	e.grant(tid, "aorg_1", free, "own2", []string{"devices/%c/up"}, nil)

	for _, topic := range []string{"devices/pump-7/up", "devices//up", "devices/%c/up"} {
		expect(t, e.publish(free.username, free.password, map[string]any{"topic": topic, "payload": ""}), http.StatusForbidden)
	}

	// Only the HTTP path skips the pin: this device still connects as pump-7.
	c := e.connect(pinned, "pump-7")
	c.requireAlive(t)
}
