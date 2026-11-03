package api_test

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	forgeauth "github.com/xraph/forge/extensions/auth"

	"github.com/xraph/tether/api"
	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/id"
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

// of returns the events of action, in order.
func (s *auditSpy) of(action string) []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []audit.Event

	for _, ev := range s.evs {
		if ev.Action == action {
			out = append(out, ev)
		}
	}

	return out
}

// asOperator is admin middleware that authenticates every call as op-1, on
// the forge context the way forge's auth extension does, or on the request's
// context.
func asOperator(onRequest bool) forge.Middleware {
	return func(next forge.Handler) forge.Handler {
		return func(c forge.Context) error {
			ac := &forgeauth.AuthContext{Subject: "op-1"}

			if onRequest {
				c.WithContext(forgeauth.WithContext(c.Context(), ac))
			} else {
				c.Set("auth_context", ac)
			}

			return next(c)
		}
	}
}

func TestAdminActionsAudited(t *testing.T) {
	for _, onRequest := range []bool{false, true} {
		name := "forge context"
		if onRequest {
			name = "request context"
		}

		t.Run(name, func(t *testing.T) {
			spy := &auditSpy{}
			e := newEnvMW(t, func(s *api.Services) { s.Audit = spy }, asOperator(onRequest))

			tid := e.createTenant("aorg_1", nil)
			base := tenantPath(tid)

			d := e.provision(tid, "pump", map[string]any{"client_id": "pump-1"})
			c := e.connect(d, "pump-1")

			expect(t, e.do(http.MethodPost, base+"/clients/pump-1/kick", nil), http.StatusNoContent)
			c.requireLost(t)
			expect(t, e.do(http.MethodPost, base+"/devices/"+d.id+"/revoke", nil), http.StatusNoContent)

			missing := id.NewDeviceID().String()
			expect(t, e.do(http.MethodDelete, base+"/devices/"+missing, nil), http.StatusNotFound)

			created := spy.of(audit.ActionTenantCreate)
			require.Len(t, created, 1)
			assert.Equal(t, tid, created[0].TenantID)

			prov := spy.of(audit.ActionDeviceProvision)
			require.Len(t, prov, 1)
			assert.Equal(t, d.id, prov[0].Subject)
			assert.Equal(t, "pump-1", prov[0].ClientID)

			kick := spy.of(audit.ActionClientKick)
			require.Len(t, kick, 1)
			assert.Equal(t, "pump-1", kick[0].ClientID)

			revoke := spy.of(audit.ActionDeviceRevoke)
			require.Len(t, revoke, 1)
			assert.Equal(t, d.id, revoke[0].Subject)
			assert.Equal(t, d.id, revoke[0].Meta[audit.MetaTarget])

			for _, ev := range []audit.Event{created[0], prov[0], kick[0], revoke[0]} {
				assert.Equal(t, "op-1", ev.Actor, ev.Action)
				assert.Equal(t, audit.OutcomeSuccess, ev.Outcome, ev.Action)
				assert.Equal(t, tid, ev.TenantID, ev.Action)
				assert.Equal(t, e.broker.NodeID(), ev.Node, ev.Action)
				assert.Empty(t, ev.Reason, ev.Action)
			}

			del := spy.of(audit.ActionDeviceDelete)
			require.Len(t, del, 1)
			assert.Equal(t, audit.OutcomeFailure, del[0].Outcome)
			assert.Equal(t, "op-1", del[0].Actor)
			assert.Equal(t, missing, del[0].Subject)
			assert.NotEmpty(t, del[0].Reason)

			// Reads are never audited.
			expect(t, e.do(http.MethodGet, base+"/devices", nil), http.StatusOK)

			spy.mu.Lock()
			n := len(spy.evs)
			spy.mu.Unlock()
			assert.Equal(t, 5, n, "one event per change")
		})
	}
}

// TestAdminAuditCoversEveryChange: every state-changing route emits its
// action, and a call with no auth context has no actor.
func TestAdminAuditCoversEveryChange(t *testing.T) {
	spy := &auditSpy{}
	e := newEnvWith(t, func(s *api.Services) { s.Audit = spy })

	tid := e.createTenant("aorg_1", nil)
	base := tenantPath(tid)
	d := e.provision(tid, "pump", nil)

	expect(t, e.do(http.MethodPatch, base, map[string]any{"name": "renamed"}), http.StatusOK)
	expect(t, e.do(http.MethodPost, base+"/roles", map[string]any{"slug": "reader"}), http.StatusCreated)
	expect(t, e.do(http.MethodPut, base+"/roles/reader/topics",
		map[string]any{"publish": []string{}, "subscribe": []string{"a/#"}}), http.StatusOK)
	expect(t, e.do(http.MethodPut, base+"/devices/"+d.id+"/roles", map[string]any{"roles": []string{"reader"}}), http.StatusOK)
	expect(t, e.do(http.MethodPost, base+"/devices/"+d.id+"/rotate", nil), http.StatusOK)
	expect(t, e.do(http.MethodPut, base+"/devices/"+d.id+"/roles", map[string]any{"roles": []string{}}), http.StatusOK)
	expect(t, e.do(http.MethodDelete, base+"/roles/reader", nil), http.StatusNoContent)
	expect(t, e.do(http.MethodDelete, base+"/sessions/nobody", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodDelete, base+"/retained?topic=a/b", nil), http.StatusNotFound)
	expect(t, e.do(http.MethodDelete, base+"/devices/"+d.id, nil), http.StatusNoContent)
	expect(t, e.do(http.MethodDelete, base, nil), http.StatusNoContent)

	for _, action := range []string{
		audit.ActionTenantCreate, audit.ActionTenantUpdate, audit.ActionTenantDelete,
		audit.ActionDeviceProvision, audit.ActionDeviceRotate, audit.ActionDeviceRoles, audit.ActionDeviceDelete,
		audit.ActionRoleCreate, audit.ActionRoleTopics, audit.ActionRoleDelete,
		audit.ActionSessionDelete, audit.ActionRetainedDelete,
	} {
		evs := spy.of(action)
		require.NotEmpty(t, evs, action)

		for _, ev := range evs {
			assert.Empty(t, ev.Actor, action)
			assert.Equal(t, tid, ev.TenantID, action)
		}
	}

	assert.Len(t, spy.of(audit.ActionDeviceRoles), 2)

	// The deleted tenant's pair is on its event: the audit writer can no
	// longer look it up.
	del := spy.of(audit.ActionTenantDelete)[0]
	assert.Equal(t, audit.OutcomeSuccess, del.Outcome)
	assert.Equal(t, testApp, del.Meta[audit.MetaAppID])
	assert.Equal(t, "aorg_1", del.Meta[audit.MetaExternalID])
	assert.Equal(t, "reader", spy.of(audit.ActionRoleCreate)[0].Meta[audit.MetaTarget])
	assert.Equal(t, "a/b", spy.of(audit.ActionRetainedDelete)[0].Meta[audit.MetaTarget])
	assert.Equal(t, audit.OutcomeFailure, spy.of(audit.ActionSessionDelete)[0].Outcome)
	assert.Equal(t, "nobody", spy.of(audit.ActionSessionDelete)[0].ClientID)
}

// A failed admin.tenant.create carries no host pair, so the audit writer
// files it with no tenant: a caller can't write into another tenant's trail
// by asking for its pair. A successful one carries the pair it created.
func TestTenantCreateFailureCarriesNoPair(t *testing.T) {
	spy := &auditSpy{}
	e := newEnvWith(t, func(s *api.Services) { s.Audit = spy })

	tid := e.createTenant("aorg_1", nil)

	body := map[string]any{"app_id": testApp, "external_id": "aorg_1", "name": "again"}
	expect(t, e.do(http.MethodPost, "/tether/v1/tenants", body), http.StatusConflict)

	evs := spy.of(audit.ActionTenantCreate)
	require.Len(t, evs, 2)

	assert.Equal(t, audit.OutcomeSuccess, evs[0].Outcome)
	assert.Equal(t, tid, evs[0].TenantID)
	assert.Equal(t, testApp, evs[0].Meta[audit.MetaAppID])
	assert.Equal(t, "aorg_1", evs[0].Meta[audit.MetaExternalID])

	assert.Equal(t, audit.OutcomeFailure, evs[1].Outcome)
	assert.Empty(t, evs[1].TenantID)
	assert.NotContains(t, evs[1].Meta, audit.MetaAppID)
	assert.NotContains(t, evs[1].Meta, audit.MetaExternalID)
}

// admin.tenant.update has the dashboard's shape: enabled when the change set
// it, and limits_changed always.
func TestTenantUpdateEventShape(t *testing.T) {
	spy := &auditSpy{}
	e := newEnvWith(t, func(s *api.Services) { s.Audit = spy })

	tid := e.createTenant("aorg_1", nil)

	expect(t, e.do(http.MethodPatch, tenantPath(tid), map[string]any{"enabled": false, "limits": map[string]any{"max_connections": 5}}), http.StatusOK)
	expect(t, e.do(http.MethodPatch, tenantPath(tid), map[string]any{"name": "renamed"}), http.StatusOK)

	evs := spy.of(audit.ActionTenantUpdate)
	require.Len(t, evs, 2)

	assert.Equal(t, false, evs[0].Meta["enabled"])
	assert.Equal(t, true, evs[0].Meta["limits_changed"])

	assert.NotContains(t, evs[1].Meta, "enabled")
	assert.Equal(t, false, evs[1].Meta["limits_changed"])
}
