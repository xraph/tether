// Package tenant defines the Tether tenant model and its store interface.
package tenant

import (
	"context"
	"errors"
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

	// MessagesPerSecond is the sustained publish rate of the whole tenant on
	// one broker node: every connection of the tenant on that node draws from
	// one budget. Zero means the broker default, and the broker has no default
	// rate, so zero means unlimited.
	MessagesPerSecond float64 `json:"messages_per_second,omitempty"`

	// Burst is how many publishes the tenant may send at once on one node,
	// on top of MessagesPerSecond. Zero means MessagesPerSecond rounded up,
	// and at least one.
	Burst int `json:"burst,omitempty"`

	// MaxSessionExpiry caps the session expiry a client may request, in
	// whole seconds, and only lowers the broker's own cap. A 5.0 client
	// that asks for more is told the capped value in its CONNACK; a 3.1.1
	// persistent session gets the cap as its expiry. Zero means the
	// broker's cap. A change reaches connections made after it.
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

// Validate reports whether t can be stored: it must be non-nil and carry an
// ID. The returned error names the problem and is not a tether sentinel, so
// this package does not import the root package. A store wraps it with
// tether.ErrInvalidInput.
func (t *Tenant) Validate() error {
	if t == nil {
		return errors.New("tenant is required")
	}

	if t.ID.IsNil() {
		return errors.New("tenant ID is required")
	}

	return nil
}

// ListFilter pages through tenants. Limit zero means no limit. Negative
// values are invalid.
type ListFilter struct {
	Limit  int
	Offset int
}

// Validate reports whether the filter is usable: Limit and Offset must not be
// negative. A store wraps the error with tether.ErrInvalidInput.
func (f ListFilter) Validate() error {
	if f.Limit < 0 {
		return errors.New("limit must not be negative")
	}

	if f.Offset < 0 {
		return errors.New("offset must not be negative")
	}

	return nil
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
