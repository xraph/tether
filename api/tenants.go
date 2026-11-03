package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/tether"
	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/tenant"
)

// createTenantRequest is the body of POST /v1/tenants.
type createTenantRequest struct {
	AppID      string        `json:"app_id"`
	ExternalID string        `json:"external_id"`
	Name       string        `json:"name,omitempty"`
	Limits     tenant.Limits `json:"limits,omitempty"`
}

// pageRequest binds the paging query of the list routes.
type pageRequest struct {
	Limit  int `query:"limit" optional:"true"`
	Offset int `query:"offset" optional:"true"`
}

// tenantPath binds the tenant ID of a path.
type tenantPath struct {
	TenantID string `path:"tenantId"`
}

// updateTenantRequest is PATCH /v1/tenants/:tenantId. Absent fields stay as
// they are.
type updateTenantRequest struct {
	TenantID string         `path:"tenantId"`
	Name     *string        `json:"name"`
	Enabled  *bool          `json:"enabled"`
	Limits   *tenant.Limits `json:"limits"`
}

func (a *API) registerTenants(r forge.Router) error {
	g := r.Group("/v1/tenants", forge.WithGroupTags("tenants"))

	return errors.Join(
		g.POST("", a.createTenant, forge.WithName("createTenant"), forge.WithSummary("Create a tenant")),
		g.GET("", a.listTenants, forge.WithName("listTenants"), forge.WithSummary("List tenants")),
		g.GET("/:tenantId", a.getTenant, forge.WithName("getTenant"), forge.WithSummary("Get a tenant")),
		g.PATCH("/:tenantId", audited(a, tenantUpdated, a.updateTenant), forge.WithName("updateTenant"),
			forge.WithSummary("Update a tenant's name, enabled flag or limits")),
		g.DELETE("/:tenantId", a.deleteTenant, forge.WithName("deleteTenant"),
			forge.WithSummary("Delete a tenant that has no devices")),
	)
}

// createTenant records its own audit event: the tenant is known only once
// it exists, and the response is written here, not returned.
func (a *API) createTenant(c forge.Context, req *createTenantRequest) (*empty, error) {
	ev := audit.Event{Action: audit.ActionTenantCreate, Meta: map[string]any{
		audit.MetaAppID: req.AppID, audit.MetaExternalID: req.ExternalID,
	}}

	err := a.create(c, req, &ev)
	a.record(c, ev, err)

	return written(err)
}

// create creates the tenant and writes it, and names it in ev.
func (a *API) create(c forge.Context, req *createTenantRequest, ev *audit.Event) error {
	if err := validateLimits(req.Limits); err != nil {
		return mapError(err)
	}

	t, err := a.Tenants.Create(c.Context(), service.CreateTenantInput{
		AppID:      req.AppID,
		ExternalID: req.ExternalID,
		Name:       req.Name,
		Limits:     req.Limits,
	})
	if err != nil {
		return a.fail("create tenant", err)
	}

	ev.TenantID = t.ID.String()
	ev.Meta[audit.MetaTarget] = ev.TenantID

	return c.JSON(http.StatusCreated, t)
}

func (a *API) listTenants(c forge.Context, req *pageRequest) (*list[*tenant.Tenant], error) {
	limit, offset, err := page(req)
	if err != nil {
		return nil, mapError(err)
	}

	ts, err := a.Tenants.List(c.Context(), tenant.ListFilter{Limit: limit, Offset: offset})
	if err != nil {
		return nil, a.fail("list tenants", err)
	}

	out := items(ts)

	return &out, nil
}

func (a *API) getTenant(c forge.Context, req *tenantPath) (*tenant.Tenant, error) {
	return a.tenant(c, req.TenantID)
}

func (a *API) updateTenant(c forge.Context, req *updateTenantRequest) (*tenant.Tenant, error) {
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	if req.Limits != nil {
		if err = validateLimits(*req.Limits); err != nil {
			return nil, mapError(err)
		}
	}

	updated, err := a.Tenants.Update(c.Context(), tn.ID, service.UpdateTenantInput{
		Name:    req.Name,
		Enabled: req.Enabled,
		Limits:  req.Limits,
	})
	if err != nil {
		return nil, a.fail("update tenant", err)
	}

	// Live connections keep the tenant they connected with until told:
	// this gives them the new limits, or disconnects them when it is
	// disabled. It is harmless while the broker is stopped.
	a.Broker.UpdateTenant(updated)

	return updated, nil
}

// deleteTenant records its own audit event: the tenant's host pair goes on
// it before the delete, since the audit writer can no longer look it up.
func (a *API) deleteTenant(c forge.Context, req *tenantPath) (*empty, error) {
	ev := tenantDeleted(req)

	err := a.removeTenant(c, req, &ev)
	a.record(c, ev, err)

	return written(err)
}

// removeTenant deletes the tenant and ends its connections, and puts its
// host pair on ev.
func (a *API) removeTenant(c forge.Context, req *tenantPath, ev *audit.Event) error {
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return err
	}

	ev.Meta[audit.MetaAppID], ev.Meta[audit.MetaExternalID] = tn.AppID, tn.ExternalID

	// Delete refuses while the tenant has devices, and then nothing changed:
	// connections are only touched once the tenant is really gone.
	if err := a.Tenants.Delete(c.Context(), tn.ID); err != nil {
		return a.fail("delete tenant", err)
	}

	gone := *tn
	gone.Enabled = false

	a.Broker.UpdateTenant(&gone)

	return c.NoContent(http.StatusNoContent)
}

// tenantUpdated describes PATCH /v1/tenants/:tenantId.
func tenantUpdated(req *updateTenantRequest) audit.Event {
	ev := audit.Event{Action: audit.ActionTenantUpdate, TenantID: req.TenantID, Meta: audit.Target(req.TenantID)}
	if req.Enabled != nil {
		ev.Meta["enabled"] = *req.Enabled
	}

	return ev
}

// tenantDeleted describes DELETE /v1/tenants/:tenantId.
func tenantDeleted(req *tenantPath) audit.Event {
	return audit.Event{Action: audit.ActionTenantDelete, TenantID: req.TenantID, Meta: audit.Target(req.TenantID)}
}

// tenant loads the tenant a path names: malformed is 400, unknown is 404.
func (a *API) tenant(c forge.Context, raw string) (*tenant.Tenant, error) {
	tid, err := parseTenantID(raw)
	if err != nil {
		return nil, mapError(err)
	}

	tn, err := a.Tenants.Get(c.Context(), tid)
	if err != nil {
		return nil, a.fail("get tenant", err)
	}

	return tn, nil
}

func parseTenantID(raw string) (id.TenantID, error) {
	tid, err := id.ParseTenantID(raw)
	if err != nil {
		return id.TenantID{}, fmt.Errorf("%w: malformed tenant ID", tether.ErrInvalidInput)
	}

	return tid, nil
}

func parseDeviceID(raw string) (id.DeviceID, error) {
	did, err := id.ParseDeviceID(raw)
	if err != nil {
		return id.DeviceID{}, fmt.Errorf("%w: malformed device ID", tether.ErrInvalidInput)
	}

	return did, nil
}

// page returns the limit and offset of a list request: DefaultPageSize when
// no limit is given, an error when either is negative or the limit passes
// MaxPageSize.
func page(req *pageRequest) (limit, offset int, err error) {
	switch {
	case req.Limit < 0, req.Offset < 0:
		return 0, 0, fmt.Errorf("%w: limit and offset must not be negative", tether.ErrInvalidInput)
	case req.Limit > MaxPageSize:
		return 0, 0, fmt.Errorf("%w: limit must be at most %d", tether.ErrInvalidInput, MaxPageSize)
	case req.Limit == 0:
		return DefaultPageSize, req.Offset, nil
	}

	return req.Limit, req.Offset, nil
}

// validateLimits rejects limits tenant.Limits.Validate refuses: negative
// ones, which the limiter would read as "unlimited", and a session expiry cap
// under a second.
func validateLimits(l tenant.Limits) error {
	if err := l.Validate(); err != nil {
		return fmt.Errorf("%w: %w", tether.ErrInvalidInput, err)
	}

	return nil
}

// parseDevicePath parses the tenant and device IDs of a path.
func parseDevicePath(rawTenant, rawDevice string) (tid id.TenantID, did id.DeviceID, err error) {
	if tid, err = parseTenantID(rawTenant); err != nil {
		return id.TenantID{}, id.DeviceID{}, err
	}

	if did, err = parseDeviceID(rawDevice); err != nil {
		return id.TenantID{}, id.DeviceID{}, err
	}

	return tid, did, nil
}
