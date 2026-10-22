package postgres

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xraph/grove"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

// The JSON columns are JSONB and the models hold them as the raw JSON text,
// so the store, not the driver, decides how a nil slice or map is stored
// (JSON null, not SQL NULL).

type tenantModel struct {
	grove.BaseModel `grove:"table:tether_tenants"`

	ID         string    `grove:"id,pk"`
	AppID      string    `grove:"app_id,notnull"`
	ExternalID string    `grove:"external_id,notnull"`
	Name       string    `grove:"name,notnull"`
	Enabled    bool      `grove:"enabled,notnull"`
	Limits     string    `grove:"limits,notnull"` // JSON object
	CreatedAt  time.Time `grove:"created_at,notnull"`
	UpdatedAt  time.Time `grove:"updated_at,notnull"`
}

type deviceModel struct {
	grove.BaseModel `grove:"table:tether_devices"`

	ID        string    `grove:"id,pk"`
	TenantID  string    `grove:"tenant_id,notnull"`
	Name      string    `grove:"name,notnull"`
	KeyID     string    `grove:"key_id,notnull"`
	ClientID  string    `grove:"client_id,notnull"`
	Roles     string    `grove:"roles,notnull"`    // JSON array, or null
	Metadata  string    `grove:"metadata,notnull"` // JSON object, or null
	CreatedAt time.Time `grove:"created_at,notnull"`
	UpdatedAt time.Time `grove:"updated_at,notnull"`
}

// normalize returns t as UTC truncated to microseconds, the precision
// timestamptz holds.
func normalize(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

func tenantToModel(t *tenant.Tenant, created, updated time.Time) (*tenantModel, error) {
	limits, err := json.Marshal(t.Limits)
	if err != nil {
		return nil, fmt.Errorf("limits: %w", err)
	}

	return &tenantModel{
		ID:         t.ID.String(),
		AppID:      t.AppID,
		ExternalID: t.ExternalID,
		Name:       t.Name,
		Enabled:    t.Enabled,
		Limits:     string(limits),
		CreatedAt:  created,
		UpdatedAt:  updated,
	}, nil
}

func tenantFromModel(m *tenantModel) (*tenant.Tenant, error) {
	tid, err := id.ParseTenantID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("tenant ID %q: %w", m.ID, err)
	}

	var limits tenant.Limits
	if err := json.Unmarshal([]byte(m.Limits), &limits); err != nil {
		return nil, fmt.Errorf("limits of %s: %w", m.ID, err)
	}

	return &tenant.Tenant{
		ID:         tid,
		AppID:      m.AppID,
		ExternalID: m.ExternalID,
		Name:       m.Name,
		Enabled:    m.Enabled,
		Limits:     limits,
		CreatedAt:  m.CreatedAt.UTC(),
		UpdatedAt:  m.UpdatedAt.UTC(),
	}, nil
}

func deviceToModel(d *device.Device, created, updated time.Time) (*deviceModel, error) {
	// A nil slice or map encodes as JSON null and an empty one as [] or {},
	// so the difference survives a round trip.
	roles, err := json.Marshal(d.Roles)
	if err != nil {
		return nil, fmt.Errorf("roles: %w", err)
	}

	metadata, err := json.Marshal(d.Metadata)
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}

	return &deviceModel{
		ID:        d.ID.String(),
		TenantID:  d.TenantID.String(),
		Name:      d.Name,
		KeyID:     d.KeyID,
		ClientID:  d.ClientID,
		Roles:     string(roles),
		Metadata:  string(metadata),
		CreatedAt: created,
		UpdatedAt: updated,
	}, nil
}

func deviceFromModel(m *deviceModel) (*device.Device, error) {
	did, err := id.ParseDeviceID(m.ID)
	if err != nil {
		return nil, fmt.Errorf("device ID %q: %w", m.ID, err)
	}

	tid, err := id.ParseTenantID(m.TenantID)
	if err != nil {
		return nil, fmt.Errorf("tenant ID %q of device %s: %w", m.TenantID, m.ID, err)
	}

	var roles []string
	if err := json.Unmarshal([]byte(m.Roles), &roles); err != nil {
		return nil, fmt.Errorf("roles of %s: %w", m.ID, err)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(m.Metadata), &metadata); err != nil {
		return nil, fmt.Errorf("metadata of %s: %w", m.ID, err)
	}

	return &device.Device{
		ID:        did,
		TenantID:  tid,
		Name:      m.Name,
		KeyID:     m.KeyID,
		ClientID:  m.ClientID,
		Roles:     roles,
		Metadata:  metadata,
		CreatedAt: m.CreatedAt.UTC(),
		UpdatedAt: m.UpdatedAt.UTC(),
	}, nil
}
