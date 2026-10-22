// Package device defines the Tether device model and its store interface.
package device

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/tether/id"
)

// Device is one MQTT client identity inside a tenant. A device authenticates
// with a keysmith key: the MQTT username is KeyID and the password is the raw
// key.
//
// Roles and Metadata hold plain JSON-compatible values, so stores can persist
// them as JSON.
type Device struct {
	ID       id.DeviceID `json:"id"`
	TenantID id.TenantID `json:"tenant_id"`
	Name     string      `json:"name"`

	// KeyID is the keysmith key ID (akey_...). It is unique across all
	// tenants and must not be empty.
	KeyID string `json:"key_id"`

	// ClientID pins the device to one MQTT client ID. Empty means any
	// client ID is accepted.
	ClientID string `json:"client_id,omitempty"`

	Roles     []string       `json:"roles,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// Validate reports whether d can be stored: it must be non-nil and carry a
// device ID, a tenant ID and a key ID. The returned error names the problem
// and is not a tether sentinel, so this package does not import the root
// package. A store wraps it with tether.ErrInvalidInput.
func (d *Device) Validate() error {
	switch {
	case d == nil:
		return errors.New("device is required")
	case d.ID.IsNil():
		return errors.New("device ID is required")
	case d.TenantID.IsNil():
		return errors.New("tenant ID is required")
	case d.KeyID == "":
		return errors.New("key ID is required")
	}

	return nil
}

// ListFilter pages through the devices of one tenant. TenantID is required:
// a zero TenantID is invalid input, never "all tenants". Limit zero means no
// limit. Negative values are invalid.
type ListFilter struct {
	TenantID id.TenantID
	Limit    int
	Offset   int
}

// Validate reports whether the filter is usable: TenantID must be set, and
// Limit and Offset must not be negative. A store wraps the error with
// tether.ErrInvalidInput.
func (f ListFilter) Validate() error {
	if f.TenantID.IsNil() {
		return errors.New("tenant ID is required")
	}

	if f.Limit < 0 {
		return errors.New("limit must not be negative")
	}

	if f.Offset < 0 {
		return errors.New("offset must not be negative")
	}

	return nil
}

// Store persists devices.
//
// Errors wrap the sentinels in the root tether package: a missing record
// wraps tether.ErrNotFound, a duplicate KeyID wraps tether.ErrConflict, and
// a nil record, zero ID, zero TenantID or empty KeyID wraps
// tether.ErrInvalidInput. A store does not check that the tenant exists; the
// service layer does.
type Store interface {
	// CreateDevice stores d. It sets CreatedAt and UpdatedAt on d when they
	// are zero, and normalises set ones to UTC at microsecond precision.
	// Device IDs are globally unique, so an ID that already exists
	// under any tenant returns tether.ErrConflict.
	CreateDevice(ctx context.Context, d *Device) error

	// GetDevice returns the device. A device that exists under another
	// tenant is reported as tether.ErrNotFound.
	GetDevice(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) (*Device, error)

	// GetDeviceByKey returns the device that owns the keysmith key ID.
	GetDeviceByKey(ctx context.Context, keyID string) (*Device, error)

	// UpdateDevice replaces the stored device identified by (d.TenantID,
	// d.ID) with d, keeps the stored CreatedAt, and sets UpdatedAt to now.
	// Moving the device to a KeyID another device holds returns
	// tether.ErrConflict.
	UpdateDevice(ctx context.Context, d *Device) error

	// DeleteDevice removes the device and frees its KeyID.
	DeleteDevice(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) error

	// ListDevices returns the tenant's devices ordered by CreatedAt
	// ascending, then ID. Devices of other tenants never appear.
	ListDevices(ctx context.Context, f ListFilter) ([]*Device, error)

	// CountDevices returns how many devices the tenant has.
	CountDevices(ctx context.Context, tenantID id.TenantID) (int, error)
}
