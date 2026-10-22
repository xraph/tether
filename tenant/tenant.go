// Package tenant defines the Tether tenant model and its store interface.
package tenant

import (
	"context"
	"time"

	"github.com/xraph/tether/id"
)

// Limits caps what one tenant may do. A zero value means "use the broker
// default", except MaxConnections where zero means unlimited.
//
// Limits holds plain value types only, so stores can persist it as JSON.
type Limits struct {
	// MaxConnections is the most concurrent MQTT connections the tenant may
	// hold. Zero means unlimited.
	MaxConnections int `json:"max_connections,omitempty"`

	// MessagesPerSecond is the sustained publish rate per connection. Zero
	// means the broker default.
	MessagesPerSecond float64 `json:"messages_per_second,omitempty"`

	// Burst is the publish burst allowance on top of MessagesPerSecond. Zero
	// means the broker default.
	Burst int `json:"burst,omitempty"`

	// MaxSessionExpiry caps the session expiry a client may request. Zero
	// means the broker default.
	MaxSessionExpiry time.Duration `json:"max_session_expiry,omitempty"`
}

// Tenant is one isolated customer of the broker. The (AppID, ExternalID) pair
// is unique and maps the tenant to the host application's own identifier.
type Tenant struct {
	ID         id.TenantID `json:"id"`
	AppID      string      `json:"app_id"`
	ExternalID string      `json:"external_id"`
	Name       string      `json:"name"`
	Enabled    bool        `json:"enabled"`
	Limits     Limits      `json:"limits"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

// ListFilter pages through tenants. Limit zero means no limit. Negative
// values are invalid.
type ListFilter struct {
	Limit  int
	Offset int
}

// Store persists tenants.
//
// Errors wrap the sentinels in the root tether package: a missing record
// wraps tether.ErrNotFound, a duplicate (AppID, ExternalID) wraps
// tether.ErrConflict, and a nil record or zero ID wraps tether.ErrInvalidInput.
type Store interface {
	// CreateTenant stores t. It sets CreatedAt and UpdatedAt on t when they
	// are zero, and normalises set ones to UTC at microsecond precision.
	// An existing tenant ID also returns tether.ErrConflict.
	CreateTenant(ctx context.Context, t *Tenant) error

	// GetTenant returns the tenant with the given ID.
	GetTenant(ctx context.Context, tenantID id.TenantID) (*Tenant, error)

	// GetTenantByExternal returns the tenant mapped to the host
	// application's (appID, externalID) pair.
	GetTenantByExternal(ctx context.Context, appID, externalID string) (*Tenant, error)

	// UpdateTenant replaces the stored tenant with t, keeps the stored
	// CreatedAt, and sets UpdatedAt to now. Moving the tenant to an
	// (AppID, ExternalID) pair another tenant holds returns tether.ErrConflict.
	UpdateTenant(ctx context.Context, t *Tenant) error

	// DeleteTenant removes the tenant. It does not cascade to devices; the
	// service layer refuses to delete a tenant that still has any.
	DeleteTenant(ctx context.Context, tenantID id.TenantID) error

	// ListTenants returns tenants ordered by CreatedAt ascending, then ID.
	ListTenants(ctx context.Context, f ListFilter) ([]*Tenant, error)
}
