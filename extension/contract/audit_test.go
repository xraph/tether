package contract

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/audit"
)

// auditSpy is an audit.Emitter that keeps every event.
type auditSpy struct {
	mu  sync.Mutex
	evs []audit.Event
}

func (s *auditSpy) Emit(ev audit.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.evs = append(s.evs, ev)
}

func (s *auditSpy) all() []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]audit.Event(nil), s.evs...)
}

func (s *auditSpy) of(action string) []audit.Event {
	var out []audit.Event

	for _, ev := range s.all() {
		if ev.Action == action {
			out = append(out, ev)
		}
	}

	return out
}

// TestCommandsAudited: every command is recorded with the dashboard user as
// the actor, a failed one as a failure; queries never are.
func TestCommandsAudited(t *testing.T) {
	h := newHarness(t, true)
	spy := &auditSpy{}
	h.svc.Audit = spy

	a := h.createTenant("acme")
	h.createRole("acme", "reader")

	creds := h.provision(a, "a1")
	da := creds["device"].(map[string]any)["id"].(string)

	h.mustCommand("tenants.update", map[string]any{"tenantId": a, "name": "Acme"})
	h.mustCommand("devices.roles.set", map[string]any{"tenantId": a, "deviceId": da, "roles": []string{"reader"}})
	h.mustCommand("roles.topics.set", map[string]any{"tenantId": a, "roleSlug": "reader", "publish": []string{}, "subscribe": []string{"a/#"}})
	h.mustCommand("devices.rotate", map[string]any{"tenantId": a, "deviceId": da})
	h.mustCommand("devices.revoke", map[string]any{"tenantId": a, "deviceId": da})
	h.mustCommand("roles.create", map[string]any{"tenantId": a, "slug": "writer"})
	h.mustCommand("roles.delete", map[string]any{"tenantId": a, "roleSlug": "writer"})

	_, err := h.command("clients.kick", map[string]any{"tenantId": a, "clientId": "nobody"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("sessions.delete", map[string]any{"tenantId": a, "clientId": "nobody"})
	requireCode(t, dashcontract.CodeNotFound, err)

	_, err = h.command("retained.delete", map[string]any{"tenantId": a, "topic": "a/b"})
	requireCode(t, dashcontract.CodeNotFound, err)

	h.mustQuery("devices.list", map[string]any{"tenantId": a})

	h.mustCommand("devices.delete", map[string]any{"tenantId": a, "deviceId": da})
	h.mustCommand("tenants.delete", map[string]any{"tenantId": a})

	for _, action := range []string{
		audit.ActionTenantCreate, audit.ActionTenantUpdate, audit.ActionTenantDelete,
		audit.ActionDeviceProvision, audit.ActionDeviceRotate, audit.ActionDeviceRevoke,
		audit.ActionDeviceDelete, audit.ActionDeviceRoles,
		audit.ActionRoleCreate, audit.ActionRoleDelete, audit.ActionRoleTopics,
		audit.ActionClientKick, audit.ActionSessionDelete, audit.ActionRetainedDelete,
	} {
		evs := spy.of(action)
		require.Len(t, evs, 1, action)

		ev := evs[0]
		assert.Equal(t, "admin-1", ev.Actor, action)
		assert.Equal(t, a, ev.TenantID, action)
		assert.Equal(t, h.svc.Broker.NodeID(), ev.Node, action)
	}

	assert.Len(t, spy.all(), 14, "one event per command, none for queries")

	prov := spy.of(audit.ActionDeviceProvision)[0]
	assert.Equal(t, audit.OutcomeSuccess, prov.Outcome)
	assert.Equal(t, da, prov.Subject)

	kick := spy.of(audit.ActionClientKick)[0]
	assert.Equal(t, audit.OutcomeFailure, kick.Outcome)
	assert.Equal(t, "nobody", kick.ClientID)
	assert.NotEmpty(t, kick.Reason)

	assert.Equal(t, "writer", spy.of(audit.ActionRoleCreate)[0].Meta[audit.MetaTarget])

	del := spy.of(audit.ActionTenantDelete)[0]
	assert.Equal(t, testApp, del.Meta[audit.MetaAppID], "read before the delete")
	assert.Equal(t, "acme", del.Meta[audit.MetaExternalID])
	assert.Equal(t, "a/b", spy.of(audit.ActionRetainedDelete)[0].Meta[audit.MetaTarget])
}

// TestScopeRefusalNotAudited: a command the gate refuses never ran, so it
// records nothing.
func TestScopeRefusalNotAudited(t *testing.T) {
	h := newHarnessWith(t, true, Deps{})
	spy := &auditSpy{}
	h.svc.Audit = spy

	_, err := h.command("devices.provision", map[string]any{"tenantId": "ttnt_01h2xcejqtf2nbrexx3vqjhp41", "name": "x"})
	requireCode(t, dashcontract.CodePermissionDenied, err)

	assert.Empty(t, spy.all())
}

// A failed tenants.create carries no host pair (it goes to the trail with no
// tenant), and tenants.update has the REST shape: enabled when the change
// set it, and limits_changed always.
func TestTenantEventsShape(t *testing.T) {
	h := newHarness(t, true)
	spy := &auditSpy{}
	h.svc.Audit = spy

	a := h.createTenant("acme")

	_, err := h.command("tenants.create", map[string]any{"appId": testApp, "externalId": "acme", "name": "again"})
	require.Error(t, err)

	created := spy.of(audit.ActionTenantCreate)
	require.Len(t, created, 2)
	assert.Equal(t, testApp, created[0].Meta[audit.MetaAppID])
	assert.Equal(t, "acme", created[0].Meta[audit.MetaExternalID])

	assert.Equal(t, audit.OutcomeFailure, created[1].Outcome)
	assert.Empty(t, created[1].TenantID)
	assert.NotContains(t, created[1].Meta, audit.MetaAppID)
	assert.NotContains(t, created[1].Meta, audit.MetaExternalID)

	h.mustCommand("tenants.update", map[string]any{"tenantId": a, "enabled": false, "limits": map[string]any{"maxConnections": 5}})
	h.mustCommand("tenants.update", map[string]any{"tenantId": a, "name": "Acme"})

	updated := spy.of(audit.ActionTenantUpdate)
	require.Len(t, updated, 2)
	assert.Equal(t, false, updated[0].Meta["enabled"])
	assert.Equal(t, true, updated[0].Meta["limits_changed"])
	assert.NotContains(t, updated[1].Meta, "enabled")
	assert.Equal(t, false, updated[1].Meta["limits_changed"])
}
