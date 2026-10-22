package contract

import (
	"fmt"
	"math"
	"time"

	"github.com/xraph/tether"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/tenant"
)

// The types in this file are the wire shapes of the intents. The React
// plugin consumes them, so the JSON names are camelCase and stable; change
// one and the plugin's types change with it. llm.txt lists them per intent.

// Ack answers a command with nothing to return.
type Ack struct {
	OK bool `json:"ok"`
}

// Limits caps what one tenant may do. Zero means the broker default, and
// unlimited for maxConnections and messagesPerSecond (see tenant.Limits).
type Limits struct {
	MaxConnections          int     `json:"maxConnections"`
	MessagesPerSecond       float64 `json:"messagesPerSecond"`
	Burst                   int     `json:"burst"`
	MaxSessionExpirySeconds int64   `json:"maxSessionExpirySeconds"`
}

// Tenant is one isolated customer of the broker.
type Tenant struct {
	ID         string    `json:"id"`
	AppID      string    `json:"appId"`
	ExternalID string    `json:"externalId"`
	Name       string    `json:"name"`
	Enabled    bool      `json:"enabled"`
	Limits     Limits    `json:"limits"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Device is one MQTT client identity inside a tenant. It never carries the
// key's secret; only Credentials does, once.
type Device struct {
	ID        string         `json:"id"`
	TenantID  string         `json:"tenantId"`
	Name      string         `json:"name"`
	KeyID     string         `json:"keyId"`
	ClientID  string         `json:"clientId,omitempty"`
	Roles     []string       `json:"roles"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

// Credentials is the answer of devices.provision and devices.rotate: the
// device, the MQTT username (its keysmith key ID) and the raw key, the MQTT
// password. It is shown once and kept nowhere.
type Credentials struct {
	Device   Device `json:"device"`
	Username string `json:"username"`
	Password string `json:"password"`
}

// Client is one of a tenant's sessions on this node.
type Client struct {
	ClientID             string     `json:"clientId"`
	Username             string     `json:"username,omitempty"`
	DeviceID             string     `json:"deviceId,omitempty"`
	Remote               string     `json:"remote,omitempty"`
	Connected            bool       `json:"connected"`
	ProtocolVersion      int        `json:"protocolVersion"`
	Subscriptions        []string   `json:"subscriptions"`
	ConnectedAt          *time.Time `json:"connectedAt,omitempty"`
	SessionExpirySeconds uint32     `json:"sessionExpirySeconds"`
}

// Retained is one retained message, without its payload.
type Retained struct {
	Topic string `json:"topic"`
	Size  int    `json:"size"`
	QoS   int    `json:"qos"`
}

// Stats is the answer of overview.stats. Scope is "tenant" with a TenantID,
// or "node" with Tenants, the number of tenants, and every other count
// summed over them.
type Stats struct {
	Scope         string `json:"scope"`
	TenantID      string `json:"tenantId,omitempty"`
	Tenants       *int   `json:"tenants,omitempty"`
	Connected     int    `json:"connected"`
	Sessions      int    `json:"sessions"`
	Subscriptions int    `json:"subscriptions"`
	Retained      int    `json:"retained"`
	MessagesIn    uint64 `json:"messagesIn"`
	MessagesOut   uint64 `json:"messagesOut"`
}

// TopicRules is a role's topic grants. Each list is never null.
type TopicRules struct {
	RoleSlug  string   `json:"roleSlug"`
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// Role is a role of a tenant with its topic grants. Each list is never null.
type Role struct {
	Slug       string   `json:"slug"`
	Name       string   `json:"name"`
	ParentSlug string   `json:"parentSlug,omitempty"`
	Publish    []string `json:"publish"`
	Subscribe  []string `json:"subscribe"`
}

// Lists. Paged lists echo the effective limit and offset, so a client knows
// the page size when it sent none: a full page may have a next one.
type (
	// TenantList is the answer of tenants.list.
	TenantList struct {
		Items  []Tenant `json:"items"`
		Limit  int      `json:"limit"`
		Offset int      `json:"offset"`
	}

	// DeviceList is the answer of devices.list.
	DeviceList struct {
		Items  []Device `json:"items"`
		Limit  int      `json:"limit"`
		Offset int      `json:"offset"`
	}

	// ClientList is the answer of clients.list.
	ClientList struct {
		Items []Client `json:"items"`
	}

	// RoleList is the answer of roles.list.
	RoleList struct {
		Items []Role `json:"items"`
	}

	// RetainedList is the answer of retained.list.
	RetainedList struct {
		Items []Retained `json:"items"`
	}
)

// Inputs. Params (queries) and payload (commands) decode into the same
// struct, so an input names its tenant the same way everywhere.

// PageInput pages tenants.list. Zero limit means the default page.
type PageInput struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// TenantRef names a tenant: tenants.get, tenants.delete, clients.list.
type TenantRef struct {
	TenantID string `json:"tenantId"`
}

// StatsInput is overview.stats. An empty TenantID is the node total.
type StatsInput struct {
	TenantID string `json:"tenantId"`
}

// CreateTenantInput is tenants.create.
type CreateTenantInput struct {
	AppID      string `json:"appId"`
	ExternalID string `json:"externalId"`
	Name       string `json:"name"`
	Limits     Limits `json:"limits"`
}

// UpdateTenantInput is tenants.update. A field left out stays as it is; a
// present Limits replaces all four limits.
type UpdateTenantInput struct {
	TenantID string  `json:"tenantId"`
	Name     *string `json:"name"`
	Enabled  *bool   `json:"enabled"`
	Limits   *Limits `json:"limits"`
}

// DeviceListInput is devices.list.
type DeviceListInput struct {
	TenantID string `json:"tenantId"`
	Limit    int    `json:"limit"`
	Offset   int    `json:"offset"`
}

// DeviceRef names a device: devices.get, devices.delete.
type DeviceRef struct {
	TenantID string `json:"tenantId"`
	DeviceID string `json:"deviceId"`
}

// ProvisionInput is devices.provision.
type ProvisionInput struct {
	TenantID  string     `json:"tenantId"`
	Name      string     `json:"name"`
	ClientID  string     `json:"clientId"`
	Roles     []string   `json:"roles"`
	ExpiresAt *time.Time `json:"expiresAt"`
}

// RotateInput is devices.rotate. The old key keeps working for
// GraceSeconds; zero takes keysmith's default.
type RotateInput struct {
	TenantID     string `json:"tenantId"`
	DeviceID     string `json:"deviceId"`
	GraceSeconds int64  `json:"graceSeconds"`
}

// RevokeInput is devices.revoke.
type RevokeInput struct {
	TenantID string `json:"tenantId"`
	DeviceID string `json:"deviceId"`
	Reason   string `json:"reason"`
}

// SetDeviceRolesInput is devices.roles.set. Roles is the device's whole set
// of role slugs: required, and [] removes them all.
type SetDeviceRolesInput struct {
	TenantID string   `json:"tenantId"`
	DeviceID string   `json:"deviceId"`
	Roles    []string `json:"roles"`
}

// RoleRef names a role: roles.topics.get and roles.delete.
type RoleRef struct {
	TenantID string `json:"tenantId"`
	RoleSlug string `json:"roleSlug"`
}

// CreateRoleInput is roles.create. Name defaults to the slug, ParentSlug
// names a role of the same tenant to inherit from, and the lists are the
// role's topic grants (both optional, empty grants nothing).
type CreateRoleInput struct {
	TenantID   string   `json:"tenantId"`
	Slug       string   `json:"slug"`
	Name       string   `json:"name"`
	ParentSlug string   `json:"parentSlug"`
	Publish    []string `json:"publish"`
	Subscribe  []string `json:"subscribe"`
}

// SetTopicsInput is roles.topics.set. It replaces both lists, so both are
// required; [] clears one.
type SetTopicsInput struct {
	TenantID  string   `json:"tenantId"`
	RoleSlug  string   `json:"roleSlug"`
	Publish   []string `json:"publish"`
	Subscribe []string `json:"subscribe"`
}

// ClientRef names a client: clients.kick, sessions.delete.
type ClientRef struct {
	TenantID string `json:"tenantId"`
	ClientID string `json:"clientId"`
}

// RetainedListInput is retained.list. An empty Filter is "#", every topic
// but $SYS.
type RetainedListInput struct {
	TenantID string `json:"tenantId"`
	Filter   string `json:"filter"`
}

// RetainedRef names a retained topic: retained.delete.
type RetainedRef struct {
	TenantID string `json:"tenantId"`
	Topic    string `json:"topic"`
}

// maxSeconds is the most seconds a time.Duration can hold.
const maxSeconds = math.MaxInt64 / int64(time.Second)

// seconds converts a count of seconds from the wire to a Duration. A negative
// count, or one too large to represent, is invalid input.
func seconds(name string, n int64) (time.Duration, error) {
	if n < 0 || n > maxSeconds {
		return 0, fmt.Errorf("%w: %s must be between 0 and %d", tether.ErrInvalidInput, name, maxSeconds)
	}

	return time.Duration(n) * time.Second, nil
}

// toLimits converts wire limits to tenant limits. Negative limits are
// invalid: the limiter would read them as "unlimited".
func toLimits(l Limits) (tenant.Limits, error) {
	if l.MaxConnections < 0 || l.MessagesPerSecond < 0 || l.Burst < 0 {
		return tenant.Limits{}, fmt.Errorf("%w: limits must not be negative", tether.ErrInvalidInput)
	}

	expiry, err := seconds("maxSessionExpirySeconds", l.MaxSessionExpirySeconds)
	if err != nil {
		return tenant.Limits{}, err
	}

	return tenant.Limits{
		MaxConnections:    l.MaxConnections,
		MessagesPerSecond: l.MessagesPerSecond,
		Burst:             l.Burst,
		MaxSessionExpiry:  expiry,
	}, nil
}

func fromLimits(l tenant.Limits) Limits {
	return Limits{
		MaxConnections:          l.MaxConnections,
		MessagesPerSecond:       l.MessagesPerSecond,
		Burst:                   l.Burst,
		MaxSessionExpirySeconds: int64(l.MaxSessionExpiry / time.Second),
	}
}

func fromTenant(t *tenant.Tenant) Tenant {
	return Tenant{
		ID:         t.ID.String(),
		AppID:      t.AppID,
		ExternalID: t.ExternalID,
		Name:       t.Name,
		Enabled:    t.Enabled,
		Limits:     fromLimits(t.Limits),
		CreatedAt:  t.CreatedAt,
		UpdatedAt:  t.UpdatedAt,
	}
}

func fromDevice(d *device.Device) Device {
	roles := d.Roles
	if roles == nil {
		roles = []string{}
	}

	return Device{
		ID:        d.ID.String(),
		TenantID:  d.TenantID.String(),
		Name:      d.Name,
		KeyID:     d.KeyID,
		ClientID:  d.ClientID,
		Roles:     roles,
		Metadata:  d.Metadata,
		CreatedAt: d.CreatedAt,
		UpdatedAt: d.UpdatedAt,
	}
}

func fromRole(r *perm.RoleInfo) Role {
	return Role{
		Slug:       r.Slug,
		Name:       r.Name,
		ParentSlug: r.ParentSlug,
		Publish:    strs(r.Publish),
		Subscribe:  strs(r.Subscribe),
	}
}

func fromClient(c broker.ClientInfo) Client {
	subs := c.Subscriptions
	if subs == nil {
		subs = []string{}
	}

	out := Client{
		ClientID:             c.ClientID,
		Username:             c.Username,
		DeviceID:             c.DeviceID,
		Remote:               c.Remote,
		Connected:            c.Connected,
		ProtocolVersion:      int(c.ProtocolVersion),
		Subscriptions:        subs,
		SessionExpirySeconds: c.SessionExpiry,
	}

	if !c.ConnectedAt.IsZero() {
		at := c.ConnectedAt
		out.ConnectedAt = &at
	}

	return out
}

// strs returns s, or an empty list for nil, so the JSON has [] and not null.
func strs(s []string) []string {
	if s == nil {
		return []string{}
	}

	return s
}
