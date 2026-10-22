package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	pahov3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	"github.com/xraph/forge/extensions/dashboard/contract/loader"
	"github.com/xraph/keysmith"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"
	"github.com/xraph/warden/role"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/api"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/limits"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store/memory"
)

// waitTimeout bounds every wait. Nothing sleeps: tests wait on channels.
const waitTimeout = 5 * time.Second

const testApp = "aapp_1"

// harness registers the contract against a real dispatcher and a real
// service stack (memory store, keysmith and warden engines, a broker on a
// loopback port), the way the dashboard does. Tests dispatch envelopes and
// read the raw JSON back, so they check the field names the React plugin
// receives and not a Go struct that might be tagged differently.
type harness struct {
	t    *testing.T
	disp *dispatcher.Dispatcher
	svc  *api.Services
	wd   *warden.Engine
}

// newHarness builds the stack. With running false the broker is built and
// never started, which is what an admin sees while the broker is down.
func newHarness(t *testing.T, running bool) *harness {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)

	km, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()))
	require.NoError(t, err)

	wd, err := warden.NewEngine(warden.WithStore(wmemory.New()))
	require.NoError(t, err)

	authn := auth.NewKeysmith(km, tenants, st)
	loader := perm.NewLoader(wd)
	limiter := limits.New()

	cfg := broker.DefaultConfig()
	cfg.TCPAddress = "127.0.0.1:0"

	b, err := broker.New(cfg, broker.Deps{Auth: authn, Perms: loader, Limits: limiter})
	require.NoError(t, err)

	if running {
		require.NoError(t, b.Start(t.Context()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			defer cancel()

			_ = b.Stop(ctx)
		})
	}

	svc := &api.Services{
		Tenants: tenants,
		Devices: service.NewDevices(st, tenants, km, service.NewWardenRoles(wd)),
		Roles:   perm.NewRoles(wd),
		Broker:  b,
		Auth:    authn,
		Perms:   loader,
		Limits:  limiter,
	}

	disp := dispatcher.New(nil)
	require.NoError(t, Register(disp, dashcontract.NewRegistry(), dashcontract.NewWardenRegistry(),
		Deps{Services: func() *api.Services { return svc }}))

	return &harness{t: t, disp: disp, svc: svc, wd: wd}
}

// registered returns the manifest's intents that have a handler. It asks the
// dispatcher with an empty input, which every handler refuses (no tenant, no
// app ID), so nothing is changed; only an intent with no handler answers
// "not registered".
func (h *harness) registered() map[string]bool {
	h.t.Helper()

	m, err := loader.Load(bytes.NewReader(manifestYAML), "tether/contract/manifest.yaml")
	require.NoError(h.t, err)

	out := map[string]bool{}

	for _, in := range m.Intents {
		_, err := h.dispatch(dashcontract.Kind(in.Kind), in.Name, map[string]any{})
		if err != nil && strings.Contains(err.Error(), "not registered") {
			continue
		}

		out[in.Name] = true
	}

	return out
}

// dispatch sends input the way the React client does: a query carries it in
// Params, a command in Payload. The dispatcher decodes both, through
// different paths, and a test that only used Payload would never exercise
// the one every query takes.
func (h *harness) dispatch(kind dashcontract.Kind, intent string, input any) (json.RawMessage, error) {
	h.t.Helper()

	raw, err := json.Marshal(input)
	require.NoError(h.t, err)

	req := dashcontract.Request{
		Envelope:      "v1",
		Kind:          kind,
		Contributor:   ContributorName,
		Intent:        intent,
		IntentVersion: 1,
	}

	if kind == dashcontract.KindQuery {
		var params map[string]any
		require.NoError(h.t, json.Unmarshal(raw, &params))

		req.Params = params
	} else {
		req.Payload = raw
	}

	out, _, err := h.disp.Dispatch(h.t.Context(), req, dashcontract.Principal{})

	return out, err
}

// query and command return the decoded JSON object a handler answered with.
func (h *harness) query(intent string, input any) (map[string]any, error) {
	h.t.Helper()

	return h.object(dashcontract.KindQuery, intent, input)
}

func (h *harness) command(intent string, input any) (map[string]any, error) {
	h.t.Helper()

	return h.object(dashcontract.KindCommand, intent, input)
}

func (h *harness) object(kind dashcontract.Kind, intent string, input any) (map[string]any, error) {
	h.t.Helper()

	out, err := h.dispatch(kind, intent, input)
	if err != nil {
		return nil, err
	}

	var m map[string]any
	require.NoError(h.t, json.Unmarshal(out, &m), "%s returned %s", intent, out)

	return m, nil
}

// mustQuery and mustCommand are query and command for the calls that must
// succeed.
func (h *harness) mustQuery(intent string, input any) map[string]any {
	h.t.Helper()

	m, err := h.query(intent, input)
	require.NoError(h.t, err, intent)

	return m
}

func (h *harness) mustCommand(intent string, input any) map[string]any {
	h.t.Helper()

	m, err := h.command(intent, input)
	require.NoError(h.t, err, intent)

	return m
}

// code is the contract code of err, or "" when it is not a contract error.
func code(err error) dashcontract.ErrorCode {
	var ce *dashcontract.Error
	if errors.As(err, &ce) {
		return ce.Code
	}

	return ""
}

// requireCode fails unless err is a contract error with the given code.
func requireCode(t *testing.T, want dashcontract.ErrorCode, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, want, code(err), "error: %v", err)
}

// createTenant creates a tenant and returns its ID.
func (h *harness) createTenant(external string) string {
	h.t.Helper()

	m := h.mustCommand("tenants.create", map[string]any{
		"appId": testApp, "externalId": external, "name": "tenant " + external,
	})

	return m["id"].(string)
}

// provision provisions a device and returns the credentials answer.
func (h *harness) provision(tenantID, name string) map[string]any {
	h.t.Helper()

	return h.mustCommand("devices.provision", map[string]any{"tenantId": tenantID, "name": name})
}

// createRole creates an empty warden role in the tenant's external ID.
func (h *harness) createRole(external, slug string) {
	h.t.Helper()

	require.NoError(h.t, h.wd.Store().CreateRole(scope.With(h.t.Context(), testApp, external), &role.Role{
		TenantID: external, AppID: testApp, Name: slug, Slug: slug,
	}))
}

func TestTenantsLifecycle(t *testing.T) {
	h := newHarness(t, true)

	created := h.mustCommand("tenants.create", map[string]any{
		"appId": testApp, "externalId": "acme", "name": "Acme",
		"limits": map[string]any{"maxConnections": 5, "messagesPerSecond": 2.5, "burst": 3, "maxSessionExpirySeconds": 3600},
	})

	tid := created["id"].(string)
	assert.True(t, strings.HasPrefix(tid, "ttnt_"), tid)
	assert.Equal(t, testApp, created["appId"])
	assert.Equal(t, "acme", created["externalId"])
	assert.Equal(t, "Acme", created["name"])
	assert.Equal(t, true, created["enabled"])
	assert.NotEmpty(t, created["createdAt"])
	assert.NotEmpty(t, created["updatedAt"])
	assert.Equal(t, map[string]any{
		"maxConnections": float64(5), "messagesPerSecond": 2.5, "burst": float64(3), "maxSessionExpirySeconds": float64(3600),
	}, created["limits"])

	// The wire is camelCase only.
	for _, snake := range []string{"app_id", "external_id", "created_at"} {
		assert.NotContains(t, created, snake)
	}

	got := h.mustQuery("tenants.get", map[string]any{"tenantId": tid})
	assert.Equal(t, created, got)

	list := h.mustQuery("tenants.list", map[string]any{})
	items := list["items"].([]any)
	require.Len(t, items, 1)
	assert.Equal(t, tid, items[0].(map[string]any)["id"])

	// A reused external ID is a conflict, not a second tenant.
	_, err := h.command("tenants.create", map[string]any{"appId": testApp, "externalId": "acme"})
	requireCode(t, dashcontract.CodeConflict, err)

	_, err = h.command("tenants.create", map[string]any{"appId": testApp})
	requireCode(t, dashcontract.CodeBadRequest, err)

	_, err = h.command("tenants.create", map[string]any{
		"appId": testApp, "externalId": "neg", "limits": map[string]any{"burst": -1},
	})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// An update changes what it names. An empty name is a name, so it is
	// not mistaken for "absent".
	upd := h.mustCommand("tenants.update", map[string]any{"tenantId": tid, "enabled": false})
	assert.Equal(t, false, upd["enabled"])
	assert.Equal(t, "Acme", upd["name"])
	assert.Equal(t, created["limits"], upd["limits"])

	upd = h.mustCommand("tenants.update", map[string]any{"tenantId": tid, "name": ""})
	assert.Equal(t, "", upd["name"])
	assert.Equal(t, false, upd["enabled"], "the enabled flag was not named and must not change")

	upd = h.mustCommand("tenants.update", map[string]any{
		"tenantId": tid, "enabled": true, "limits": map[string]any{"maxConnections": 9},
	})
	assert.Equal(t, true, upd["enabled"])
	assert.Equal(t, map[string]any{
		"maxConnections": float64(9), "messagesPerSecond": float64(0), "burst": float64(0), "maxSessionExpirySeconds": float64(0),
	}, upd["limits"])

	_, err = h.command("tenants.update", map[string]any{"tenantId": tid, "limits": map[string]any{"maxConnections": -1}})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// A tenant with a device cannot be deleted, and nothing changes.
	dev := h.provision(tid, "d1")
	_, err = h.command("tenants.delete", map[string]any{"tenantId": tid})
	requireCode(t, dashcontract.CodeConflict, err)
	h.mustQuery("tenants.get", map[string]any{"tenantId": tid})

	h.mustCommand("devices.delete", map[string]any{"tenantId": tid, "deviceId": dev["device"].(map[string]any)["id"]})

	ack := h.mustCommand("tenants.delete", map[string]any{"tenantId": tid})
	assert.Equal(t, true, ack["ok"])

	_, err = h.query("tenants.get", map[string]any{"tenantId": tid})
	requireCode(t, dashcontract.CodeNotFound, err)
}

func TestTenantIDErrors(t *testing.T) {
	h := newHarness(t, true)

	// Well formed but unknown is NOT_FOUND, malformed is BAD_REQUEST.
	_, err := h.query("tenants.get", map[string]any{"tenantId": "ttnt_01h2xcejqtf2nbrexx3vqjhp41"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.query("tenants.get", map[string]any{"tenantId": "nope"})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// A device ID is not a tenant ID.
	_, err = h.query("tenants.get", map[string]any{"tenantId": "tdev_01h2xcejqtf2nbrexx3vqjhp41"})
	requireCode(t, dashcontract.CodeBadRequest, err)
}

func TestTenantsListPaging(t *testing.T) {
	h := newHarness(t, true)

	for _, ext := range []string{"a", "b", "c"} {
		h.createTenant(ext)
	}

	first := h.mustQuery("tenants.list", map[string]any{"limit": 2})
	assert.Len(t, first["items"], 2)
	assert.Equal(t, float64(2), first["limit"])
	assert.Equal(t, float64(0), first["offset"])

	second := h.mustQuery("tenants.list", map[string]any{"limit": 2, "offset": 2})
	assert.Len(t, second["items"], 1)

	def := h.mustQuery("tenants.list", map[string]any{})
	assert.Len(t, def["items"], 3)
	assert.Equal(t, float64(api.DefaultPageSize), def["limit"])

	_, err := h.query("tenants.list", map[string]any{"limit": -1})
	requireCode(t, dashcontract.CodeBadRequest, err)

	_, err = h.query("tenants.list", map[string]any{"limit": api.MaxPageSize + 1})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// An empty list is [] and not null.
	empty := newHarness(t, true)
	none := empty.mustQuery("tenants.list", map[string]any{})
	assert.Equal(t, []any{}, none["items"])
}

// Contract principals carry no tenant claims, so a tenant-scoped intent with
// no tenantId has nothing to scope to. It must refuse, not widen to every
// tenant. This runs over the whole manifest, so a new intent added without
// the check fails here.
func TestTenantScopedIntentsRefuseWithoutTenantID(t *testing.T) {
	h := newHarness(t, true)
	h.createTenant("acme") // there is a tenant a careless default could pick.

	m, err := loader.Load(bytes.NewReader(manifestYAML), "tether/contract/manifest.yaml")
	require.NoError(t, err)

	open := map[string]bool{"overview.stats": true, "tenants.list": true, "tenants.create": true}
	denied := 0

	for _, in := range m.Intents {
		_, derr := h.dispatch(dashcontract.Kind(in.Kind), in.Name, map[string]any{})
		if open[in.Name] {
			// tenants.create refuses for want of an appId, not a tenant.
			assert.NotEqual(t, dashcontract.CodePermissionDenied, code(derr), in.Name)

			continue
		}

		denied++

		requireCode(t, dashcontract.CodePermissionDenied, derr)
	}

	assert.Equal(t, 17, denied)

	// An empty string is no tenant either.
	_, err = h.query("devices.list", map[string]any{"tenantId": ""})
	requireCode(t, dashcontract.CodePermissionDenied, err)
}

func TestProvisionReturnsThePasswordOnce(t *testing.T) {
	h := newHarness(t, true)
	tid := h.createTenant("acme")

	out, err := h.dispatch(dashcontract.KindCommand, "devices.provision", map[string]any{"tenantId": tid, "name": "sensor-1", "clientId": "sensor-1"})
	require.NoError(t, err)

	var creds map[string]any
	require.NoError(t, json.Unmarshal(out, &creds))

	password, _ := creds["password"].(string)
	username, _ := creds["username"].(string)
	require.NotEmpty(t, password)
	assert.True(t, strings.HasPrefix(username, "akey_"), username)

	// The raw key appears once in the answer: under "password", not also in
	// the device record.
	assert.Equal(t, 1, strings.Count(string(out), password), "answer: %s", out)

	dev := creds["device"].(map[string]any)
	assert.Equal(t, username, dev["keyId"])
	assert.Equal(t, tid, dev["tenantId"])
	assert.Equal(t, "sensor-1", dev["name"])
	assert.Equal(t, "sensor-1", dev["clientId"])
	assert.True(t, strings.HasPrefix(dev["id"].(string), "tdev_"))
	assert.NotContains(t, dev, "password")

	// Nothing that reads a device shows the password.
	did := dev["id"].(string)

	got, err := h.dispatch(dashcontract.KindQuery, "devices.get", map[string]any{"tenantId": tid, "deviceId": did})
	require.NoError(t, err)
	assert.NotContains(t, string(got), password)
	assert.NotContains(t, string(got), `"password"`)

	list, err := h.dispatch(dashcontract.KindQuery, "devices.list", map[string]any{"tenantId": tid})
	require.NoError(t, err)
	assert.NotContains(t, string(list), password)
	assert.NotContains(t, string(list), `"password"`)

	// A rotation gives a different key, once, and the same username
	// fields. The old key keeps working only for the grace period, which
	// is keysmith's business.
	rot, err := h.dispatch(dashcontract.KindCommand, "devices.rotate", map[string]any{"tenantId": tid, "deviceId": did, "graceSeconds": 30})
	require.NoError(t, err)

	var rotated map[string]any
	require.NoError(t, json.Unmarshal(rot, &rotated))

	newPassword, _ := rotated["password"].(string)
	require.NotEmpty(t, newPassword)
	assert.NotEqual(t, password, newPassword)
	assert.Equal(t, 1, strings.Count(string(rot), newPassword), "answer: %s", rot)
	assert.NotEmpty(t, rotated["username"])
	assert.Equal(t, did, rotated["device"].(map[string]any)["id"])

	_, err = h.command("devices.rotate", map[string]any{"tenantId": tid, "deviceId": did, "graceSeconds": -1})
	requireCode(t, dashcontract.CodeBadRequest, err)
}

func TestDevicesRevokeDeleteAndTenantScope(t *testing.T) {
	h := newHarness(t, true)
	a := h.createTenant("acme")
	b := h.createTenant("beta")

	da := h.provision(a, "a1")["device"].(map[string]any)["id"].(string)
	h.provision(b, "b1")

	// The other tenant cannot see, change or remove it, and gets the same
	// answer as for a device that does not exist.
	_, err := h.query("devices.get", map[string]any{"tenantId": b, "deviceId": da})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("devices.revoke", map[string]any{"tenantId": b, "deviceId": da})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("devices.delete", map[string]any{"tenantId": b, "deviceId": da})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("devices.rotate", map[string]any{"tenantId": b, "deviceId": da})
	requireCode(t, dashcontract.CodeNotFound, err)

	listB := h.mustQuery("devices.list", map[string]any{"tenantId": b})["items"].([]any)
	require.Len(t, listB, 1)
	assert.NotEqual(t, da, listB[0].(map[string]any)["id"])

	// A list for an unknown tenant is NOT_FOUND, not an empty list.
	_, err = h.query("devices.list", map[string]any{"tenantId": "ttnt_01h2xcejqtf2nbrexx3vqjhp41"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.query("devices.get", map[string]any{"tenantId": a, "deviceId": "bad"})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// Revoke keeps the record; a second rotate of a revoked key conflicts.
	h.mustCommand("devices.revoke", map[string]any{"tenantId": a, "deviceId": da, "reason": "lost"})
	h.mustQuery("devices.get", map[string]any{"tenantId": a, "deviceId": da})

	_, err = h.command("devices.rotate", map[string]any{"tenantId": a, "deviceId": da})
	requireCode(t, dashcontract.CodeConflict, err)

	h.mustCommand("devices.delete", map[string]any{"tenantId": a, "deviceId": da})

	_, err = h.query("devices.get", map[string]any{"tenantId": a, "deviceId": da})
	requireCode(t, dashcontract.CodeNotFound, err)
}

func TestRolesAndDeviceRoles(t *testing.T) {
	h := newHarness(t, true)
	tid := h.createTenant("acme")
	h.createRole("acme", "sensor")

	rules := h.mustQuery("roles.topics.get", map[string]any{"tenantId": tid, "roleSlug": "sensor"})
	assert.Equal(t, []any{}, rules["publish"])
	assert.Equal(t, []any{}, rules["subscribe"])
	assert.Equal(t, "sensor", rules["roleSlug"])

	set := h.mustCommand("roles.topics.set", map[string]any{
		"tenantId": tid, "roleSlug": "sensor",
		"publish": []string{"devices/%c/up"}, "subscribe": []string{"devices/%c/down", "cmd/+"},
	})
	assert.Equal(t, []any{"devices/%c/up"}, set["publish"])
	assert.Equal(t, []any{"devices/%c/down", "cmd/+"}, set["subscribe"])

	got := h.mustQuery("roles.topics.get", map[string]any{"tenantId": tid, "roleSlug": "sensor"})
	assert.Equal(t, set, got)

	// Both lists are named: an omitted one would clear a grant nobody meant
	// to touch. [] clears on purpose.
	_, err := h.command("roles.topics.set", map[string]any{"tenantId": tid, "roleSlug": "sensor", "publish": []string{"a"}})
	requireCode(t, dashcontract.CodeBadRequest, err)

	cleared := h.mustCommand("roles.topics.set", map[string]any{
		"tenantId": tid, "roleSlug": "sensor", "publish": []string{}, "subscribe": []string{},
	})
	assert.Equal(t, []any{}, cleared["publish"])

	_, err = h.command("roles.topics.set", map[string]any{
		"tenantId": tid, "roleSlug": "sensor", "publish": []string{"a/#/b"}, "subscribe": []string{},
	})
	requireCode(t, dashcontract.CodeBadRequest, err)

	_, err = h.query("roles.topics.get", map[string]any{"tenantId": tid, "roleSlug": "missing"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("roles.topics.set", map[string]any{
		"tenantId": tid, "roleSlug": "missing", "publish": []string{}, "subscribe": []string{},
	})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.query("roles.topics.get", map[string]any{"tenantId": tid})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// Another tenant has no such role, whatever the first one has.
	other := h.createTenant("beta")

	_, err = h.query("roles.topics.get", map[string]any{"tenantId": other, "roleSlug": "sensor"})
	requireCode(t, dashcontract.CodeNotFound, err)

	// Device roles.
	did := h.provision(tid, "d1")["device"].(map[string]any)["id"].(string)

	dev := h.mustCommand("devices.roles.set", map[string]any{"tenantId": tid, "deviceId": did, "roles": []string{"sensor"}})
	assert.Equal(t, []any{"sensor"}, dev["roles"])

	dev = h.mustCommand("devices.roles.set", map[string]any{"tenantId": tid, "deviceId": did, "roles": []string{}})
	assert.Empty(t, dev["roles"])

	_, err = h.command("devices.roles.set", map[string]any{"tenantId": tid, "deviceId": did})
	requireCode(t, dashcontract.CodeBadRequest, err)

	_, err = h.command("devices.roles.set", map[string]any{"tenantId": tid, "deviceId": did, "roles": []string{"nope"}})
	requireCode(t, dashcontract.CodeBadRequest, err)
}

// Everything that calls the broker answers UNAVAILABLE while it is stopped,
// the same rule the REST API follows, so an absent answer is never read as
// "no clients" or "no such client".
func TestBrokerIntentsAreUnavailableWhileStopped(t *testing.T) {
	h := newHarness(t, false)
	tid := h.createTenant("acme")

	_, err := h.query("overview.stats", map[string]any{"tenantId": tid})
	requireCode(t, dashcontract.CodeUnavailable, err)

	_, err = h.query("overview.stats", map[string]any{})
	requireCode(t, dashcontract.CodeUnavailable, err)

	_, err = h.query("clients.list", map[string]any{"tenantId": tid})
	requireCode(t, dashcontract.CodeUnavailable, err)

	_, err = h.query("retained.list", map[string]any{"tenantId": tid})
	requireCode(t, dashcontract.CodeUnavailable, err)

	for intent, in := range map[string]map[string]any{
		"clients.kick":    {"tenantId": tid, "clientId": "c1"},
		"sessions.delete": {"tenantId": tid, "clientId": "c1"},
		"retained.delete": {"tenantId": tid, "topic": "a/b"},
	} {
		_, err = h.command(intent, in)
		requireCode(t, dashcontract.CodeUnavailable, err)
	}

	// The store-only ones do not need the broker.
	h.mustQuery("tenants.get", map[string]any{"tenantId": tid})
	h.mustCommand("tenants.update", map[string]any{"tenantId": tid, "name": "Acme"})
	h.provision(tid, "d1")
}

func TestServicesNotReadyIsUnavailable(t *testing.T) {
	disp := dispatcher.New(nil)
	require.NoError(t, Register(disp, dashcontract.NewRegistry(), dashcontract.NewWardenRegistry(),
		Deps{Services: func() *api.Services { return nil }}))

	for _, tc := range []struct {
		kind   dashcontract.Kind
		intent string
	}{
		{dashcontract.KindQuery, "tenants.list"},
		{dashcontract.KindQuery, "overview.stats"},
		{dashcontract.KindCommand, "tenants.create"},
		{dashcontract.KindQuery, "devices.list"},
	} {
		_, _, err := disp.Dispatch(t.Context(), dashcontract.Request{
			Envelope: "v1", Kind: tc.kind, Contributor: ContributorName, Intent: tc.intent, IntentVersion: 1,
			Params:  map[string]any{"tenantId": "ttnt_01h2xcejqtf2nbrexx3vqjhp41"},
			Payload: json.RawMessage(`{"tenantId":"ttnt_01h2xcejqtf2nbrexx3vqjhp41","appId":"a","externalId":"b"}`),
		}, dashcontract.Principal{})
		requireCode(t, dashcontract.CodeUnavailable, err)
	}
}

func TestRegisterNeedsServices(t *testing.T) {
	err := Register(dispatcher.New(nil), dashcontract.NewRegistry(), dashcontract.NewWardenRegistry(), Deps{})
	require.Error(t, err)
}

func TestRetainedIntents(t *testing.T) {
	h := newHarness(t, true)
	a := h.createTenant("acme")
	b := h.createTenant("beta")

	aid, bid := tenantKey(t, h, a), tenantKey(t, h, b)

	require.NoError(t, h.svc.Broker.Publish(t.Context(), aid, "sensors/t1", []byte("21"), 1, true))
	require.NoError(t, h.svc.Broker.Publish(t.Context(), aid, "sensors/t2", []byte("2200"), 0, true))
	require.NoError(t, h.svc.Broker.Publish(t.Context(), bid, "sensors/other", []byte("x"), 0, true))

	all := h.mustQuery("retained.list", map[string]any{"tenantId": a})["items"].([]any)
	require.Len(t, all, 2)
	assert.Equal(t, map[string]any{"topic": "sensors/t1", "size": float64(2), "qos": float64(1)}, all[0])
	assert.Equal(t, "sensors/t2", all[1].(map[string]any)["topic"])

	one := h.mustQuery("retained.list", map[string]any{"tenantId": a, "filter": "sensors/t1"})["items"].([]any)
	assert.Len(t, one, 1)

	_, err := h.query("retained.list", map[string]any{"tenantId": a, "filter": "a/#/b"})
	requireCode(t, dashcontract.CodeBadRequest, err)

	// The other tenant's topic is not reachable through this tenant.
	_, err = h.command("retained.delete", map[string]any{"tenantId": a, "topic": "sensors/other"})
	requireCode(t, dashcontract.CodeNotFound, err)

	ack := h.mustCommand("retained.delete", map[string]any{"tenantId": a, "topic": "sensors/t1"})
	assert.Equal(t, true, ack["ok"])

	left := h.mustQuery("retained.list", map[string]any{"tenantId": a})["items"].([]any)
	require.Len(t, left, 1)
	assert.Equal(t, "sensors/t2", left[0].(map[string]any)["topic"])

	still := h.mustQuery("retained.list", map[string]any{"tenantId": b})["items"].([]any)
	assert.Len(t, still, 1)

	_, err = h.command("retained.delete", map[string]any{"tenantId": a, "topic": "sensors/t1"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("retained.delete", map[string]any{"tenantId": a})
	requireCode(t, dashcontract.CodeBadRequest, err)
}

// tenantKey is the tenant's ID string, the form the broker takes.
func tenantKey(t *testing.T, h *harness, tenantID string) string {
	t.Helper()

	m := h.mustQuery("tenants.get", map[string]any{"tenantId": tenantID})

	return m["id"].(string)
}

// mqttClient is a connected MQTT 3.1.1 client.
type mqttClient struct {
	pahov3.Client
	lost chan error
}

// connect connects a provisioned device over MQTT.
func (h *harness) connect(creds map[string]any, clientID string) *mqttClient {
	h.t.Helper()

	m := &mqttClient{lost: make(chan error, 1)}

	opts := pahov3.NewClientOptions().
		AddBroker("tcp://" + h.svc.Broker.Addr().String()).
		SetClientID(clientID).
		SetUsername(creds["username"].(string)).
		SetPassword(creds["password"].(string)).
		SetProtocolVersion(4).
		SetCleanSession(false).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(waitTimeout).
		SetConnectionLostHandler(func(_ pahov3.Client, err error) { m.lost <- err })

	m.Client = pahov3.NewClient(opts)

	tok := m.Connect()
	require.True(h.t, tok.WaitTimeout(waitTimeout), "connect timed out")
	require.NoError(h.t, tok.Error())

	h.t.Cleanup(func() { m.Disconnect(0) })

	return m
}

func TestClientsStatsAndKick(t *testing.T) {
	h := newHarness(t, true)
	a := h.createTenant("acme")
	b := h.createTenant("beta")

	empty := h.mustQuery("clients.list", map[string]any{"tenantId": a})
	assert.Equal(t, []any{}, empty["items"])

	ca := h.provision(a, "a1")
	cb := h.provision(b, "b1")

	client := h.connect(ca, "dev-a")
	h.connect(cb, "dev-b")

	clients := h.mustQuery("clients.list", map[string]any{"tenantId": a})["items"].([]any)
	require.Len(t, clients, 1)

	c := clients[0].(map[string]any)
	assert.Equal(t, "dev-a", c["clientId"])
	assert.Equal(t, ca["username"], c["username"])
	assert.Equal(t, ca["device"].(map[string]any)["id"], c["deviceId"])
	assert.Equal(t, true, c["connected"])
	assert.Equal(t, float64(4), c["protocolVersion"])
	assert.NotEmpty(t, c["connectedAt"])
	assert.NotContains(t, c, "client_id")

	// Retained messages count in the stats.
	aid, bid := tenantKey(t, h, a), tenantKey(t, h, b)
	require.NoError(t, h.svc.Broker.Publish(t.Context(), aid, "r/1", []byte("x"), 0, true))
	require.NoError(t, h.svc.Broker.Publish(t.Context(), bid, "r/1", []byte("x"), 0, true))
	require.NoError(t, h.svc.Broker.Publish(t.Context(), bid, "r/2", []byte("x"), 0, true))

	one := h.mustQuery("overview.stats", map[string]any{"tenantId": a})
	assert.Equal(t, a, one["tenantId"])
	assert.Equal(t, "tenant", one["scope"])
	assert.Equal(t, float64(1), one["connected"])
	assert.Equal(t, float64(1), one["retained"])
	assert.NotContains(t, one, "tenants")

	// With no tenantId it is the node: every tenant added up.
	node := h.mustQuery("overview.stats", map[string]any{})
	assert.Equal(t, "node", node["scope"])
	assert.Equal(t, float64(2), node["tenants"])
	assert.Equal(t, float64(2), node["connected"])
	assert.Equal(t, float64(2), node["sessions"])
	assert.Equal(t, float64(3), node["retained"])
	assert.NotContains(t, node, "tenantId")

	_, err := h.query("overview.stats", map[string]any{"tenantId": "ttnt_01h2xcejqtf2nbrexx3vqjhp41"})
	requireCode(t, dashcontract.CodeNotFound, err)

	// Another tenant's client is not reachable through this tenant.
	_, err = h.command("clients.kick", map[string]any{"tenantId": b, "clientId": "dev-a"})
	requireCode(t, dashcontract.CodeNotFound, err)

	// Kick drops the connection and keeps the session (clean session off).
	h.mustCommand("clients.kick", map[string]any{"tenantId": a, "clientId": "dev-a"})

	select {
	case <-client.lost:
	case <-time.After(waitTimeout):
		t.Fatal("the kicked client was not disconnected")
	}

	kept := h.mustQuery("clients.list", map[string]any{"tenantId": a})["items"].([]any)
	require.Len(t, kept, 1)
	assert.Equal(t, false, kept[0].(map[string]any)["connected"])

	h.mustCommand("sessions.delete", map[string]any{"tenantId": a, "clientId": "dev-a"})

	gone := h.mustQuery("clients.list", map[string]any{"tenantId": a})
	assert.Equal(t, []any{}, gone["items"])

	_, err = h.command("sessions.delete", map[string]any{"tenantId": a, "clientId": "dev-a"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("clients.kick", map[string]any{"tenantId": a})
	requireCode(t, dashcontract.CodeBadRequest, err)
}
