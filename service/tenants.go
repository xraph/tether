// Package service holds Tether's tenant management on top of a store.
package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/tether"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/tenant"
)

// resolveTTL is how long ResolveTenant serves a tenant from memory.
const resolveTTL = 30 * time.Second

// CreateTenantInput describes a tenant to create. AppID and ExternalID name
// the host application's own tenant and are required.
type CreateTenantInput struct {
	AppID      string
	ExternalID string
	Name       string
	Limits     tenant.Limits
}

// UpdateTenantInput patches a tenant. A nil field is left unchanged.
type UpdateTenantInput struct {
	Name    *string
	Enabled *bool
	Limits  *tenant.Limits
}

// Tenants manages tenants and resolves the host application's (app, external)
// pair to one.
//
// ResolveTenant keeps a 30 s cache that Update and Delete evict. The cache is
// per process: when several broker nodes share a store, a change made through
// one node reaches the others within 30 s, or at once when every node calls
// Evict for it (a cell's control bus, events.EvictTenantsOnUpdate).
type Tenants struct {
	store store.Store
	now   func() time.Time

	// createMu serialises Create, so two creates in this process cannot both
	// pass the external ID check.
	createMu sync.Mutex

	mu    sync.Mutex
	cache map[externalKey]cacheEntry
	// gen counts evictions. A resolve that started before an eviction must
	// not write its stale result into the cache after it.
	gen uint64
}

type externalKey struct{ appID, externalID string }

type cacheEntry struct {
	tenant  tenant.Tenant
	expires time.Time
}

// NewTenants returns a tenant service over s.
func NewTenants(s store.Store) *Tenants {
	return &Tenants{store: s, now: time.Now, cache: make(map[externalKey]cacheEntry)}
}

// Create stores a new, enabled tenant. A missing AppID or ExternalID wraps
// tether.ErrInvalidInput. An ExternalID already used by another tenant, under
// this app or any other, wraps tether.ErrConflict: keysmith and warden isolate
// by tenant ID alone, so two tenants sharing an ExternalID would share keys
// and roles.
//
// The check reads every tenant and is not atomic across processes: broker
// nodes that create tenants on one store at the same moment can both pass it.
// Create tenants through one node, or let the host application guarantee that
// its tenant IDs are unique across apps.
func (s *Tenants) Create(ctx context.Context, in CreateTenantInput) (*tenant.Tenant, error) {
	if in.AppID == "" {
		return nil, fmt.Errorf("%w: app ID is required", tether.ErrInvalidInput)
	}

	if in.ExternalID == "" {
		return nil, fmt.Errorf("%w: external ID is required", tether.ErrInvalidInput)
	}

	s.createMu.Lock()
	defer s.createMu.Unlock()

	if err := s.checkExternalIDFree(ctx, in.ExternalID); err != nil {
		return nil, err
	}

	t := &tenant.Tenant{
		ID:         id.NewTenantID(),
		AppID:      in.AppID,
		ExternalID: in.ExternalID,
		Name:       in.Name,
		Enabled:    true,
		Limits:     in.Limits,
	}

	if err := s.store.CreateTenant(ctx, t); err != nil {
		return nil, err
	}

	return t, nil
}

// externalScanPage is how many tenants checkExternalIDFree reads at a time.
const externalScanPage = 200

// checkExternalIDFree returns an error wrapping tether.ErrConflict when any
// tenant already uses externalID.
func (s *Tenants) checkExternalIDFree(ctx context.Context, externalID string) error {
	for offset := 0; ; offset += externalScanPage {
		page, err := s.store.ListTenants(ctx, tenant.ListFilter{Limit: externalScanPage, Offset: offset})
		if err != nil {
			return err
		}

		for _, t := range page {
			if t.ExternalID == externalID {
				return fmt.Errorf("%w: external ID %q is already used by tenant %s under app %q",
					tether.ErrConflict, externalID, t.ID, t.AppID)
			}
		}

		if len(page) < externalScanPage {
			return nil
		}
	}
}

// Get returns the tenant with the given ID.
func (s *Tenants) Get(ctx context.Context, tenantID id.TenantID) (*tenant.Tenant, error) {
	return s.store.GetTenant(ctx, tenantID)
}

// List returns tenants ordered by creation time.
func (s *Tenants) List(ctx context.Context, f tenant.ListFilter) ([]*tenant.Tenant, error) {
	return s.store.ListTenants(ctx, f)
}

// Update applies the set fields of in to the tenant and evicts it from the
// resolve cache.
func (s *Tenants) Update(ctx context.Context, tenantID id.TenantID, in UpdateTenantInput) (*tenant.Tenant, error) {
	t, err := s.store.GetTenant(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	if in.Name != nil {
		t.Name = *in.Name
	}

	if in.Enabled != nil {
		t.Enabled = *in.Enabled
	}

	if in.Limits != nil {
		t.Limits = *in.Limits
	}

	// Evict on both sides of the write: before, so a failed write does not
	// leave a reader on a value we were about to replace, and after, so a
	// resolve that read the old row mid-write cannot repopulate the cache.
	s.evict(tenantID)

	err = s.store.UpdateTenant(ctx, t)

	s.evict(tenantID)

	if err != nil {
		return nil, err
	}

	return t, nil
}

// Delete removes the tenant. It returns an error wrapping tether.ErrConflict
// while the tenant still has devices.
func (s *Tenants) Delete(ctx context.Context, tenantID id.TenantID) error {
	n, err := s.store.CountDevices(ctx, tenantID)
	if err != nil {
		return err
	}

	if n > 0 {
		return fmt.Errorf("%w: tenant %s still has %d devices", tether.ErrConflict, tenantID, n)
	}

	err = s.store.DeleteTenant(ctx, tenantID)

	s.evict(tenantID)

	return err
}

// ResolveTenant returns the tenant mapped to (appID, externalID), served from
// a 30 s cache. A tenant that does not exist wraps tether.ErrNotFound and is
// not cached. A disabled tenant is returned as is: the caller checks Enabled.
// The returned value is a copy the caller may modify.
func (s *Tenants) ResolveTenant(ctx context.Context, appID, externalID string) (*tenant.Tenant, error) {
	k := externalKey{appID, externalID}

	s.mu.Lock()

	if e, ok := s.cache[k]; ok && s.now().Before(e.expires) {
		s.mu.Unlock()

		cp := e.tenant

		return &cp, nil
	}

	gen := s.gen
	s.mu.Unlock()

	t, err := s.store.GetTenantByExternal(ctx, appID, externalID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()

	if s.gen == gen {
		s.cache[k] = cacheEntry{tenant: *t, expires: s.now().Add(resolveTTL)}
	}

	s.mu.Unlock()

	cp := *t

	return &cp, nil
}

// Evict drops the tenant from the resolve cache. Update and Delete do it
// themselves. When several broker nodes share the store, each node calls it
// for a change another node made (events.EvictTenantsOnUpdate on the cell's
// control bus), so the change does not wait out the 30 s cache there.
func (s *Tenants) Evict(tenantID id.TenantID) {
	s.evict(tenantID)
}

// evict drops every cached entry for the tenant.
func (s *Tenants) evict(tenantID id.TenantID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gen++

	for k, e := range s.cache {
		if e.tenant.ID == tenantID {
			delete(s.cache, k)
		}
	}
}
