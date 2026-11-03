package extension

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

// tenantCacheTTL is how long tenantCache serves a tenant from memory, the
// same as service.Tenants' resolve cache.
const tenantCacheTTL = 30 * time.Second

// tenantRef is the part of a tenant that metering and audit send to other
// systems: its Tether ID and the host application's (app, external) pair.
type tenantRef struct {
	ID         string
	AppID      string
	ExternalID string
}

type tenantCacheEntry struct {
	ref     tenantRef
	expires time.Time
}

// tenantCache serves tenantRef by Tether tenant ID from memory for 30 s.
// service.Tenants.Get reads the store every time, and metering and audit look
// a tenant up on every window or event, so both wrap one cache.
//
// Evict it wherever service.Tenants is evicted (tenant.updated on the control
// bus), so a changed external ID reaches billing and the audit trail as
// quickly as it reaches ResolveTenant. A lookup error is never cached.
type tenantCache struct {
	get func(ctx context.Context, tenantID id.TenantID) (*tenant.Tenant, error)
	ttl time.Duration
	now func() time.Time

	mu      sync.Mutex
	entries map[string]tenantCacheEntry
	// gen counts evictions. A lookup that started before an eviction must not
	// write its stale result into the cache after it.
	gen uint64
}

// newTenantCache returns a cache over get, which is service.Tenants.Get.
func newTenantCache(get func(ctx context.Context, tenantID id.TenantID) (*tenant.Tenant, error)) *tenantCache {
	return &tenantCache{
		get: get, ttl: tenantCacheTTL, now: time.Now,
		entries: make(map[string]tenantCacheEntry),
	}
}

// lookup returns the tenant's ID, app ID and external ID.
func (c *tenantCache) lookup(ctx context.Context, tenantID string) (tenantRef, error) {
	now := c.now()

	c.mu.Lock()

	if e, ok := c.entries[tenantID]; ok && now.Before(e.expires) {
		c.mu.Unlock()

		return e.ref, nil
	}

	gen := c.gen

	c.mu.Unlock()

	tid, err := id.ParseTenantID(tenantID)
	if err != nil {
		return tenantRef{}, fmt.Errorf("tether: tenant %q: %w", tenantID, err)
	}

	t, err := c.get(ctx, tid)
	if err != nil {
		return tenantRef{}, err
	}

	ref := tenantRef{ID: t.ID.String(), AppID: t.AppID, ExternalID: t.ExternalID}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.gen == gen {
		c.entries[tenantID] = tenantCacheEntry{ref: ref, expires: now.Add(c.ttl)}
	}

	return ref, nil
}

// evict drops the tenant's entry.
func (c *tenantCache) evict(tenantID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.gen++

	delete(c.entries, tenantID)
}
