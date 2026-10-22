package contract

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/loader"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/tenant"
)

// scopedFixture is two tenants, acme and beta, with a device and a role in
// beta, so an intent aimed at beta has something real to find.
type scopedFixture struct {
	acme, beta     *tenant.Tenant
	betaDevice     string
	betaRole       string
	betaClientID   string
	betaRetainedAt string
}

func newScopedFixture(t *testing.T, h *harness) scopedFixture {
	t.Helper()

	ctx := t.Context()

	acme, err := h.svc.Tenants.Create(ctx, service.CreateTenantInput{AppID: testApp, ExternalID: "org_acme", Name: "Acme"})
	require.NoError(t, err)

	beta, err := h.svc.Tenants.Create(ctx, service.CreateTenantInput{AppID: testApp, ExternalID: "org_beta", Name: "Beta"})
	require.NoError(t, err)

	_, err = h.svc.Roles.Create(ctx, beta, perm.RoleInput{Slug: "sensor", Rules: acl.Rules{Publish: []string{"a/#"}}})
	require.NoError(t, err)

	creds, err := h.svc.Devices.Provision(ctx, beta.ID, service.ProvisionInput{Name: "b1", ClientID: "b1"})
	require.NoError(t, err)

	require.NoError(t, h.svc.Broker.Publish(ctx, beta.ID.String(), "r/1", []byte("x"), 0, true))

	return scopedFixture{
		acme: acme, beta: beta,
		betaDevice:     creds.Device.ID.String(),
		betaRole:       "sensor",
		betaClientID:   "b1",
		betaRetainedAt: "r/1",
	}
}

// aimedAt is an input every intent accepts, naming beta and its records.
func (f scopedFixture) aimedAt() map[string]any {
	return map[string]any{
		"tenantId":   f.beta.ID.String(),
		"deviceId":   f.betaDevice,
		"roleSlug":   f.betaRole,
		"slug":       "new-role",
		"clientId":   f.betaClientID,
		"topic":      f.betaRetainedAt,
		"appId":      testApp,
		"externalId": "org_new",
		"name":       "x",
		"roles":      []string{},
		"publish":    []string{},
		"subscribe":  []string{},
	}
}

// signedIn is a signed-in user with no tenant of its own.
func signedIn() dashcontract.Principal {
	return dashcontract.PrincipalFor(&dashauth.UserInfo{Subject: "user-1"})
}

// asAcmeMember makes the harness dispatch as a signed-in user whose session
// the host scoped to the org org_acme, the way authsome's middleware does.
func (h *harness) asAcmeMember() {
	h.principal = signedIn()
	sc := forge.NewOrgScope(testApp, "org_acme")
	h.scope = &sc
}

func manifestIntents(t *testing.T) []struct{ kind, name string } {
	t.Helper()

	m, err := loader.Load(bytes.NewReader(manifestYAML), manifestSource)
	require.NoError(t, err)

	out := make([]struct{ kind, name string }, 0, len(m.Intents))
	for _, in := range m.Intents {
		out = append(out, struct{ kind, name string }{string(in.Kind), in.Name})
	}

	return out
}

// A tenant user reaches nothing of another tenant through any intent. The
// table is the whole manifest, so a new intent that forgets the scope check
// fails here.
func TestTenantUserCannotReachAnotherTenant(t *testing.T) {
	h := newHarnessWith(t, true, Deps{})
	f := newScopedFixture(t, h)
	h.asAcmeMember()

	intents := manifestIntents(t)
	require.Len(t, intents, 24)

	for _, in := range intents {
		out, err := h.dispatch(dashcontract.Kind(in.kind), in.name, f.aimedAt())

		switch in.name {
		case "tenants.list":
			// No tenantId to refuse: the list holds the caller's tenant only.
			require.NoError(t, err, in.name)
			assert.Contains(t, string(out), f.acme.ID.String())
			assert.NotContains(t, string(out), f.beta.ID.String())
		case "session.info":
			require.NoError(t, err, in.name)
			assert.JSONEq(t, `{"operator":false,"tenantId":"`+f.acme.ID.String()+`"}`, string(out))
		default:
			assert.Equal(t, dashcontract.CodePermissionDenied, code(err), "%s: %v", in.name, err)
		}
	}

	// Nothing of beta moved.
	_, err := h.svc.Devices.Get(t.Context(), f.beta.ID, mustDeviceID(t, f.betaDevice))
	require.NoError(t, err)

	got, err := h.svc.Tenants.Get(t.Context(), f.beta.ID)
	require.NoError(t, err)
	assert.Equal(t, "Beta", got.Name)
	assert.Len(t, h.svc.Broker.Retained(f.beta.ID.String(), "#"), 1)
}

// The same requests from an operator are served: the scope check is what
// refused them above, not the inputs.
func TestOperatorReachesEveryTenant(t *testing.T) {
	h := newHarnessWith(t, true, Deps{Operators: true})
	f := newScopedFixture(t, h)
	h.asAcmeMember()

	for _, in := range manifestIntents(t) {
		_, err := h.dispatch(dashcontract.Kind(in.kind), in.name, f.aimedAt())
		assert.NotEqual(t, dashcontract.CodePermissionDenied, code(err), "%s: %v", in.name, err)
	}

	info := h.mustQuery("session.info", map[string]any{})
	assert.Equal(t, true, info["operator"])
	assert.Equal(t, f.acme.ID.String(), info["tenantId"])
}

// A signed-in user the host gave no tenant (no org in its scope, no tenant
// claim), or an org Tether has no tenant for, is refused everything.
func TestTenantlessPrincipalIsRefused(t *testing.T) {
	h := newHarnessWith(t, true, Deps{})
	f := newScopedFixture(t, h)

	appOnly := forge.NewAppScope(testApp)
	unknownOrg := forge.NewOrgScope(testApp, "org_nobody")
	otherApp := forge.NewOrgScope("aapp_other", "org_acme")

	for name, sc := range map[string]*forge.Scope{
		"no scope":    nil,
		"app only":    &appOnly,
		"unknown org": &unknownOrg,
		"other app":   &otherApp,
	} {
		h.principal = signedIn()
		h.scope = sc

		for _, in := range manifestIntents(t) {
			_, err := h.dispatch(dashcontract.Kind(in.kind), in.name, f.aimedAt())
			assert.Equal(t, dashcontract.CodePermissionDenied, code(err), "%s: %s: %v", name, in.name, err)
		}
	}
}

// A tenant user names its own tenant or none at all: an omitted tenantId is
// the caller's tenant, so the plugin works without knowing the ID up front.
// Managing tenants themselves is for operators.
func TestTenantUserWorksInItsOwnTenant(t *testing.T) {
	h := newHarnessWith(t, true, Deps{})
	f := newScopedFixture(t, h)
	h.asAcmeMember()

	acme := f.acme.ID.String()

	info := h.mustQuery("session.info", map[string]any{})
	assert.Equal(t, map[string]any{"operator": false, "tenantId": acme}, info)

	assert.Equal(t, acme, h.mustQuery("tenants.get", map[string]any{})["id"])
	assert.Equal(t, acme, h.mustQuery("tenants.get", map[string]any{"tenantId": acme})["id"])

	stats := h.mustQuery("overview.stats", map[string]any{})
	assert.Equal(t, "tenant", stats["scope"])
	assert.Equal(t, acme, stats["tenantId"])

	list := h.mustQuery("tenants.list", map[string]any{})["items"].([]any)
	require.Len(t, list, 1)
	assert.Equal(t, acme, list[0].(map[string]any)["id"])
	assert.Equal(t, []any{}, h.mustQuery("tenants.list", map[string]any{"offset": 1})["items"])

	creds := h.mustCommand("devices.provision", map[string]any{"name": "a1"})
	assert.Equal(t, acme, creds["device"].(map[string]any)["tenantId"])

	devices := h.mustQuery("devices.list", map[string]any{})["items"].([]any)
	require.Len(t, devices, 1)

	h.mustCommand("roles.create", map[string]any{"slug": "reader", "subscribe": []string{"x/#"}})
	assert.Len(t, h.mustQuery("roles.list", map[string]any{})["items"], 1)
	assert.Equal(t, []any{}, h.mustQuery("clients.list", map[string]any{})["items"])

	// The tenant itself is the operators' to change.
	_, err := h.command("tenants.update", map[string]any{"tenantId": acme, "name": "Mine now"})
	requireCode(t, dashcontract.CodePermissionDenied, err)

	_, err = h.command("tenants.delete", map[string]any{"tenantId": acme})
	requireCode(t, dashcontract.CodePermissionDenied, err)

	_, err = h.command("tenants.create", map[string]any{"appId": testApp, "externalId": "org_mine"})
	requireCode(t, dashcontract.CodePermissionDenied, err)

	// A malformed tenantId is still a bad request, not a scope question.
	_, err = h.query("devices.list", map[string]any{"tenantId": "nope"})
	requireCode(t, dashcontract.CodeBadRequest, err)
}

// A tenant_id claim on the principal names the tenant when the host sets
// one, before any Scope. A claim that is present but unusable refuses and
// never falls back to the Scope.
func TestTenantClaimNamesTheTenant(t *testing.T) {
	h := newHarnessWith(t, true, Deps{})
	f := newScopedFixture(t, h)

	h.principal = dashcontract.PrincipalFor(&dashauth.UserInfo{
		Subject: "user-1",
		Claims:  map[string]any{"tenant_id": "org_beta", "app_id": testApp},
	})

	info := h.mustQuery("session.info", map[string]any{})
	assert.Equal(t, f.beta.ID.String(), info["tenantId"])

	// The claim wins over a Scope naming another org.
	h.asAcmeMember()
	h.principal = dashcontract.PrincipalFor(&dashauth.UserInfo{
		Subject: "user-1",
		Claims:  map[string]any{"tenant_id": "org_beta"},
	})
	assert.Equal(t, f.beta.ID.String(), h.mustQuery("session.info", map[string]any{})["tenantId"])

	h.principal = dashcontract.PrincipalFor(&dashauth.UserInfo{
		Subject: "user-1",
		Claims:  map[string]any{"tenant_id": 42},
	})

	_, err := h.query("session.info", map[string]any{})
	requireCode(t, dashcontract.CodePermissionDenied, err)

	_, err = h.query("devices.list", map[string]any{})
	requireCode(t, dashcontract.CodePermissionDenied, err)
}

// WithInsecureDashboard serves anonymous callers, and they are operators:
// with no user there is no tenant to confine them to.
func TestAnonymousAllowedImpliesOperator(t *testing.T) {
	h := newHarnessWith(t, true, Deps{AllowAnonymous: true})
	h.principal = dashcontract.Principal{}

	assert.Equal(t, map[string]any{"operator": true}, h.mustQuery("session.info", map[string]any{}))

	tid := h.createTenant("acme")
	assert.Len(t, h.mustQuery("tenants.list", map[string]any{})["items"], 1)
	h.provision(tid, "d1")
}

func mustDeviceID(t *testing.T, raw string) id.DeviceID {
	t.Helper()

	d, err := id.ParseDeviceID(raw)
	require.NoError(t, err)

	return d
}
