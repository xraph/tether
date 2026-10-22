package contract

import (
	"context"
	"errors"

	"github.com/xraph/forge"
	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether"
	"github.com/xraph/tether/api"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

// Claim keys a host can put on the dashboard principal to name the caller's
// tenant. They are the ones keysmith's contract reads: tenant_id is the
// host's tenant (the org), app_id its app.
const (
	tenantClaim = "tenant_id"
	appClaim    = "app_id"
)

// gate decides which tenants a caller may act on. An operator may act on
// any tenant and manage tenants themselves. Anyone else is a tenant user,
// confined to the one tenant their session maps to.
type gate struct {
	sp       ServicesProvider
	operator bool
}

func newGate(deps Deps) *gate {
	// An anonymous caller has no session to take a tenant from, so serving
	// anonymous callers at all means serving them as operators.
	return &gate{sp: deps.Services, operator: deps.Operators || deps.AllowAnonymous}
}

// own resolves the caller's own Tether tenant, the way keysmith's contract
// resolves its tenant:
//
//  1. A tenant_id claim on the principal, with the app from an app_id claim
//     or else from the forge Scope. A claim that is present but unusable
//     refuses here and never falls back to the Scope.
//  2. The org and app of the forge Scope on ctx. The host's auth middleware
//     sets it server side from the session it authenticated (authsome sets
//     forge.NewOrgScope(app, org)); nothing the browser sends can write it.
//  3. Otherwise there is no tenant: PERMISSION_DENIED.
//
// The (app, org) pair then maps to a Tether tenant through
// Tenants.ResolveTenant. An org Tether has no tenant for is PERMISSION_DENIED
// too: the caller has nothing here to act on.
func (g *gate) own(ctx context.Context, s *api.Services, p dashcontract.Principal) (*tenant.Tenant, error) {
	app, org, err := hostTenant(ctx, p)
	if err != nil {
		return nil, err
	}

	t, err := s.Tenants.ResolveTenant(ctx, app, org)
	if errors.Is(err, tether.ErrNotFound) {
		return nil, denied("your organization has no Tether tenant")
	}

	if err != nil {
		return nil, fail(s, "resolve tenant", err)
	}

	return t, nil
}

// hostTenant returns the host's (app, org) for the caller, or
// PERMISSION_DENIED when nothing names one.
func hostTenant(ctx context.Context, p dashcontract.Principal) (app, org string, err error) {
	sc, hasScope := forge.ScopeFrom(ctx)

	if raw, present := p.Claims[tenantClaim]; present {
		claimed, ok := raw.(string)
		if !ok || claimed == "" {
			return "", "", denied("the tenant claim is present but unusable")
		}

		app = sc.AppID()

		if raw, present := p.Claims[appClaim]; present {
			if app, ok = raw.(string); !ok || app == "" {
				return "", "", denied("the app claim is present but unusable")
			}
		}

		if app == "" {
			return "", "", denied("no app in scope for the tenant claim")
		}

		return app, claimed, nil
	}

	if hasScope && sc.OrgID() != "" && sc.AppID() != "" {
		return sc.AppID(), sc.OrgID(), nil
	}

	return "", "", denied("no tenant in scope for this session")
}

// tenantFor returns the tenantId an intent acts on, given the one the caller
// sent (raw, possibly empty).
//
// A tenant user gets their own tenant: an empty raw means it, and any other
// tenant is PERMISSION_DENIED. An operator gets raw as sent, or their own
// tenant when they sent none and have one; with neither it is
// PERMISSION_DENIED, never "every tenant". A malformed raw is BAD_REQUEST
// for both.
func (g *gate) tenantFor(ctx context.Context, s *api.Services, p dashcontract.Principal, raw string) (string, error) {
	var want id.TenantID

	if raw != "" {
		tid, err := id.ParseTenantID(raw)
		if err != nil {
			return "", badRequest("malformed tenantId")
		}

		want = tid
	}

	if g.operator {
		if raw != "" {
			return raw, nil
		}

		if t, err := g.own(ctx, s, p); err == nil {
			return t.ID.String(), nil
		}

		return "", denied("tenantId is required")
	}

	t, err := g.own(ctx, s, p)
	if err != nil {
		return "", err
	}

	if raw != "" && want != t.ID {
		return "", denied("that tenant is outside your scope")
	}

	return t.ID.String(), nil
}

// operatorOnly refuses a tenant user before h runs: creating, changing and
// deleting tenants is for operators.
func operatorOnly[I, O any](g *gate, h handlerFunc[I, O]) handlerFunc[I, O] {
	return func(ctx context.Context, in I, p dashcontract.Principal) (O, error) {
		if !g.operator {
			var zero O

			return zero, denied("managing tenants is for operators")
		}

		return h(ctx, in, p)
	}
}

// tenantInput is an intent input that names a tenant.
type tenantInput[I any] interface {
	*I
	tenantRef() *string
}

// scoped runs h with the input's tenantId replaced by the tenant the caller
// may act on (see tenantFor). Every tenant-scoped intent is registered
// through it, so h itself never decides scope.
func scoped[I any, PI tenantInput[I], O any](g *gate, h handlerFunc[I, O]) handlerFunc[I, O] {
	return func(ctx context.Context, in I, p dashcontract.Principal) (O, error) {
		var zero O

		s, err := services(g.sp)
		if err != nil {
			return zero, err
		}

		ref := PI(&in).tenantRef()

		tid, err := g.tenantFor(ctx, s, p, *ref)
		if err != nil {
			return zero, err
		}

		*ref = tid

		return h(ctx, in, p)
	}
}

func denied(msg string) *dashcontract.Error {
	return &dashcontract.Error{Code: dashcontract.CodePermissionDenied, Message: msg}
}

// SessionInfo is the answer of session.info: whether the caller is an
// operator, and the caller's own tenant when it has one. The plugin reads it
// on load to pick its pages.
type SessionInfo struct {
	Operator bool   `json:"operator"`
	TenantID string `json:"tenantId,omitempty"`
}

// sessionInfoHandler is session.info. A tenant user with no tenant is
// PERMISSION_DENIED, like every other intent; an operator without one is
// answered with no tenantId.
func sessionInfoHandler(g *gate) handlerFunc[struct{}, SessionInfo] {
	return func(ctx context.Context, _ struct{}, p dashcontract.Principal) (SessionInfo, error) {
		s, err := services(g.sp)
		if err != nil {
			return SessionInfo{}, err
		}

		t, err := g.own(ctx, s, p)

		switch {
		case err == nil:
			return SessionInfo{Operator: g.operator, TenantID: t.ID.String()}, nil
		case g.operator:
			return SessionInfo{Operator: true}, nil
		default:
			return SessionInfo{}, err
		}
	}
}

// tenantRef methods let scoped reach the tenantId of each input.

func (in *TenantRef) tenantRef() *string           { return &in.TenantID }
func (in *UpdateTenantInput) tenantRef() *string   { return &in.TenantID }
func (in *DeviceListInput) tenantRef() *string     { return &in.TenantID }
func (in *DeviceRef) tenantRef() *string           { return &in.TenantID }
func (in *ProvisionInput) tenantRef() *string      { return &in.TenantID }
func (in *RotateInput) tenantRef() *string         { return &in.TenantID }
func (in *RevokeInput) tenantRef() *string         { return &in.TenantID }
func (in *SetDeviceRolesInput) tenantRef() *string { return &in.TenantID }
func (in *RoleRef) tenantRef() *string             { return &in.TenantID }
func (in *CreateRoleInput) tenantRef() *string     { return &in.TenantID }
func (in *SetTopicsInput) tenantRef() *string      { return &in.TenantID }
func (in *ClientRef) tenantRef() *string           { return &in.TenantID }
func (in *RetainedListInput) tenantRef() *string   { return &in.TenantID }
func (in *RetainedRef) tenantRef() *string         { return &in.TenantID }
