package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xraph/keysmith"
	kid "github.com/xraph/keysmith/id"
	"github.com/xraph/keysmith/key"
	"github.com/xraph/keysmith/rotation"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/tenant"
)

const (
	// KeyPrefix is the keysmith prefix of every device key.
	KeyPrefix = "tdk"

	// MetadataDeviceID is the key metadata field that holds the Tether device
	// ID the key was issued for.
	MetadataDeviceID = "tether_device_id"

	// cleanupTimeout bounds the compensation steps that run after a failure.
	cleanupTimeout = 10 * time.Second
)

// KeyManager is the part of keysmith's engine that Devices uses. It is
// satisfied by *keysmith.Engine.
type KeyManager interface {
	CreateKey(ctx context.Context, in *keysmith.CreateKeyInput) (*key.CreateResult, error)
	RotateKey(ctx context.Context, keyID kid.KeyID, reason rotation.Reason, opts ...keysmith.RotateOption) (*key.CreateResult, error)
	RevokeKey(ctx context.Context, keyID kid.KeyID, reason string) error
}

// RoleAssigner assigns warden roles to API keys. It is satisfied by
// *WardenRoles (NewWardenRoles). tenantID is the warden tenant, which is the
// Tether tenant's ExternalID.
//
// AssignRoleToAPIKey must be a no-op for a role the key already holds, and
// UnassignOtherRolesFromAPIKey must leave the roles in keep assigned: SetRoles
// relies on both so a role a device keeps is never unassigned, not even for a
// moment.
type RoleAssigner interface {
	AssignRoleToAPIKey(ctx context.Context, tenantID, keyID, roleSlug string) error
	UnassignRoleFromAPIKey(ctx context.Context, tenantID, keyID string) error
	UnassignOtherRolesFromAPIKey(ctx context.Context, tenantID, keyID string, keep []string) error
}

// ProvisionInput describes a device to create. Name is required. ClientID, if
// set, pins the device to one MQTT client ID and must be a single topic level:
// it becomes "%c" in the device's grants.
type ProvisionInput struct {
	Name      string
	ClientID  string
	Roles     []string
	ExpiresAt *time.Time
}

// Credentials is what a device connects with. Username is the keysmith key ID
// and Password the raw key. Password is shown once: Provision and Rotate
// return it and nothing stores it.
type Credentials struct {
	Device   *device.Device
	Username string
	Password string
}

// Devices manages the devices of a tenant: the keysmith key a device
// authenticates with, the warden roles that decide what it may publish and
// subscribe to, and the record that ties them to a tenant.
//
// Every key and role call runs under the tenant's own scope (its AppID and
// ExternalID), whatever scope the caller's context carries. Devices only ever
// get keys this service created, and there is no way to attach an existing
// key.
//
// The auth layer caches a validated key for a short time. After Revoke,
// Delete or Rotate with a zero grace, call (*auth.Keysmith).Evict with the
// device's KeyID so the change takes effect at once.
type Devices struct {
	store   device.Store
	tenants *Tenants
	keys    KeyManager
	roles   RoleAssigner
}

// NewDevices returns a device service. s holds the records, t resolves a
// tenant ID to the scope for keysmith and warden, k issues keys and r assigns
// roles.
func NewDevices(s device.Store, t *Tenants, k KeyManager, r RoleAssigner) *Devices {
	return &Devices{store: s, tenants: t, keys: k, roles: r}
}

// Provision creates a device: a keysmith key (prefix "tdk", environment
// "live", metadata "tether_device_id"), the requested roles on that key, and
// the device record, in that order. If a role cannot be assigned or the record
// cannot be stored, the roles are unassigned and the key is revoked before the
// error is returned.
//
// A missing Name, an invalid ClientID or an empty role slug wraps
// tether.ErrInvalidInput, and an unknown tenant wraps tether.ErrNotFound.
func (s *Devices) Provision(ctx context.Context, tenantID id.TenantID, in ProvisionInput) (*Credentials, error) {
	if strings.TrimSpace(in.Name) == "" {
		return nil, fmt.Errorf("%w: device name is required", tether.ErrInvalidInput)
	}

	if err := validateClientID(in.ClientID); err != nil {
		return nil, err
	}

	roles, err := cleanRoles(in.Roles)
	if err != nil {
		return nil, err
	}

	t, err := s.tenants.Get(ctx, tenantID)
	if err != nil {
		return nil, err
	}

	sctx := scope.With(ctx, t.AppID, t.ExternalID)
	deviceID := id.NewDeviceID()

	res, err := s.keys.CreateKey(sctx, &keysmith.CreateKeyInput{
		Name:        in.Name,
		Description: "Tether device " + deviceID.String(),
		Prefix:      KeyPrefix,
		Environment: key.EnvLive,
		Metadata:    map[string]any{MetadataDeviceID: deviceID.String()},
		CreatedBy:   "tether",
		ExpiresAt:   in.ExpiresAt,
	})
	if err != nil {
		return nil, fmt.Errorf("tether: create device key: %w", err)
	}

	if res == nil || res.Key == nil {
		return nil, errors.New("tether: key manager returned no key")
	}

	keyID := res.Key.ID

	// The key must belong to this tenant. If scope was not applied, keysmith
	// would have issued it somewhere else, and attaching it would let this
	// device authenticate as the wrong tenant's key.
	if res.Key.TenantID != t.ExternalID || res.Key.AppID != t.AppID {
		err := fmt.Errorf("tether: key %s was issued for (%q, %q), not for tenant %s (%q, %q)",
			keyID, res.Key.AppID, res.Key.TenantID, t.ID, t.AppID, t.ExternalID)

		return nil, errors.Join(err, s.compensate(sctx, t, keyID, false))
	}

	if err := s.assignAll(sctx, t, keyID.String(), roles); err != nil {
		return nil, errors.Join(err, s.compensate(sctx, t, keyID, true))
	}

	d := &device.Device{
		ID:       deviceID,
		TenantID: t.ID,
		Name:     in.Name,
		KeyID:    keyID.String(),
		ClientID: in.ClientID,
		Roles:    roles,
	}

	if err := s.store.CreateDevice(sctx, d); err != nil {
		return nil, errors.Join(err, s.compensate(sctx, t, keyID, true))
	}

	return &Credentials{Device: d, Username: d.KeyID, Password: res.RawKey}, nil
}

// Rotate issues a new password for the device. The username does not change.
// The old password keeps working for grace, then stops. A zero grace uses
// keysmith's default (the key's policy grace period, else 24 hours), and a
// negative grace wraps tether.ErrInvalidInput. A revoked device cannot be
// rotated.
func (s *Devices) Rotate(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID, grace time.Duration) (*Credentials, error) {
	if grace < 0 {
		return nil, fmt.Errorf("%w: grace must not be negative", tether.ErrInvalidInput)
	}

	d, t, err := s.load(ctx, tenantID, deviceID)
	if err != nil {
		return nil, err
	}

	keyID, err := kid.ParseKeyID(d.KeyID)
	if err != nil {
		return nil, fmt.Errorf("tether: device %s has a malformed key ID: %w", d.ID, err)
	}

	var opts []keysmith.RotateOption
	if grace > 0 {
		opts = append(opts, keysmith.WithGrace(grace))
	}

	res, err := s.keys.RotateKey(scope.With(ctx, t.AppID, t.ExternalID), keyID, rotation.ReasonManual, opts...)
	if err != nil {
		return nil, fmt.Errorf("tether: rotate device key: %w", err)
	}

	if res == nil {
		return nil, errors.New("tether: key manager returned no key")
	}

	return &Credentials{Device: d, Username: d.KeyID, Password: res.RawKey}, nil
}

// Revoke permanently disables the device's key. The record stays, so an
// operator still sees the device, but it can no longer connect. Revoking a
// device whose key is already revoked, or gone, succeeds.
func (s *Devices) Revoke(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID, reason string) error {
	d, t, err := s.load(ctx, tenantID, deviceID)
	if err != nil {
		return err
	}

	return s.revokeKey(scope.With(ctx, t.AppID, t.ExternalID), d, reason)
}

// Delete revokes the device's key, removes its role assignments and deletes
// the record. A key that is already revoked or gone does not stop it. If a
// step fails the record is kept, so calling Delete again finishes the job.
func (s *Devices) Delete(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) error {
	d, t, err := s.load(ctx, tenantID, deviceID)
	if err != nil {
		return err
	}

	sctx := scope.With(ctx, t.AppID, t.ExternalID)

	if err := s.revokeKey(sctx, d, "device deleted"); err != nil {
		return err
	}

	if err := s.roles.UnassignRoleFromAPIKey(sctx, t.ExternalID, d.KeyID); err != nil {
		return fmt.Errorf("tether: unassign device roles: %w", err)
	}

	return s.store.DeleteDevice(ctx, t.ID, d.ID)
}

// SetRoles replaces the device's roles with the given slugs and stores them on
// the device record. It applies only the difference: every new role is
// assigned first (a role the key already holds is a no-op), then every other
// role the key holds in the tenant's root namespace is unassigned. A role in
// both lists is never unassigned, so a connected device keeps its grants and
// subscriptions for it throughout (Broker.ReloadPrincipal runs on every
// assignment event and prunes what the grants no longer allow).
//
// If a step fails, the previous roles are put back the same way and the
// error is returned.
func (s *Devices) SetRoles(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID, roles []string) error {
	roles, err := cleanRoles(roles)
	if err != nil {
		return err
	}

	d, t, err := s.load(ctx, tenantID, deviceID)
	if err != nil {
		return err
	}

	sctx := scope.With(ctx, t.AppID, t.ExternalID)

	if err := s.applyRoles(sctx, t, d.KeyID, roles); err != nil {
		return errors.Join(err, s.restoreRoles(sctx, t, d))
	}

	updated := *d
	updated.Roles = roles

	if err := s.store.UpdateDevice(sctx, &updated); err != nil {
		return errors.Join(err, s.restoreRoles(sctx, t, d))
	}

	return nil
}

// Get returns the device. A device of another tenant wraps tether.ErrNotFound.
func (s *Devices) Get(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) (*device.Device, error) {
	return s.store.GetDevice(ctx, tenantID, deviceID)
}

// List returns the devices of the tenant named by f.TenantID.
func (s *Devices) List(ctx context.Context, f device.ListFilter) ([]*device.Device, error) {
	return s.store.ListDevices(ctx, f)
}

// load returns the device and its tenant. A device of another tenant wraps
// tether.ErrNotFound.
func (s *Devices) load(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) (*device.Device, *tenant.Tenant, error) {
	d, err := s.store.GetDevice(ctx, tenantID, deviceID)
	if err != nil {
		return nil, nil, err
	}

	t, err := s.tenants.Get(ctx, tenantID)
	if err != nil {
		return nil, nil, err
	}

	return d, t, nil
}

// revokeKey revokes d's key and treats a key that is already revoked or gone
// as done.
func (s *Devices) revokeKey(sctx context.Context, d *device.Device, reason string) error {
	keyID, err := kid.ParseKeyID(d.KeyID)
	if err != nil {
		return fmt.Errorf("tether: device %s has a malformed key ID: %w", d.ID, err)
	}

	err = s.keys.RevokeKey(sctx, keyID, reason)

	switch {
	case err == nil,
		errors.Is(err, keysmith.ErrInvalidStateTransition),
		errors.Is(err, keysmith.ErrKeyNotFound):
		return nil
	default:
		return fmt.Errorf("tether: revoke device key: %w", err)
	}
}

// assignAll assigns each role to the key, stopping at the first failure.
func (s *Devices) assignAll(sctx context.Context, t *tenant.Tenant, keyID string, roles []string) error {
	for _, slug := range roles {
		if err := s.roles.AssignRoleToAPIKey(sctx, t.ExternalID, keyID, slug); err != nil {
			return fmt.Errorf("tether: assign role %q: %w", slug, err)
		}
	}

	return nil
}

// compensate undoes a half-finished Provision: it unassigns the key's roles
// (when withRoles) and revokes the key. It runs on a context that survives the
// caller's cancellation, because the failure being handled is often the
// cancellation itself.
func (s *Devices) compensate(sctx context.Context, t *tenant.Tenant, keyID kid.KeyID, withRoles bool) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(sctx), cleanupTimeout)
	defer cancel()

	var errs []error

	if withRoles {
		if err := s.roles.UnassignRoleFromAPIKey(cctx, t.ExternalID, keyID.String()); err != nil {
			errs = append(errs, fmt.Errorf("tether: roll back: unassign roles of key %s: %w", keyID, err))
		}
	}

	if err := s.keys.RevokeKey(cctx, keyID, "provisioning failed"); err != nil {
		errs = append(errs, fmt.Errorf("tether: roll back: revoke key %s: %w", keyID, err))
	}

	return errors.Join(errs...)
}

// applyRoles makes roles the key's exact set of root namespace roles: it
// assigns them all (no-ops for held ones), then unassigns every other one.
func (s *Devices) applyRoles(sctx context.Context, t *tenant.Tenant, keyID string, roles []string) error {
	if err := s.assignAll(sctx, t, keyID, roles); err != nil {
		return err
	}

	if err := s.roles.UnassignOtherRolesFromAPIKey(sctx, t.ExternalID, keyID, roles); err != nil {
		return fmt.Errorf("tether: unassign dropped roles: %w", err)
	}

	return nil
}

// restoreRoles puts d's stored roles back on its key after a failed SetRoles,
// the same way SetRoles applies them, so a role in both sets stays assigned.
// It runs on a context that survives the caller's cancellation.
func (s *Devices) restoreRoles(sctx context.Context, t *tenant.Tenant, d *device.Device) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(sctx), cleanupTimeout)
	defer cancel()

	if err := s.applyRoles(cctx, t, d.KeyID, d.Roles); err != nil {
		return fmt.Errorf("tether: restore roles: %w", err)
	}

	return nil
}

// cleanRoles rejects empty slugs and drops repeats, keeping the first
// occurrence. It returns nil for no roles.
func cleanRoles(roles []string) ([]string, error) {
	var out []string

	seen := make(map[string]struct{}, len(roles))

	for _, r := range roles {
		if r == "" {
			return nil, fmt.Errorf("%w: role slug is empty", tether.ErrInvalidInput)
		}

		if _, dup := seen[r]; dup {
			continue
		}

		seen[r] = struct{}{}
		out = append(out, r)
	}

	return out, nil
}

// validateClientID accepts an empty ID (no pin) or a single MQTT topic level:
// no "/", "+", "#" or NUL. The ID becomes "%c" in grants, and the grant
// compiler drops any grant that would take a value outside that rule.
func validateClientID(clientID string) error {
	if strings.ContainsAny(clientID, "/+#\x00") {
		return fmt.Errorf("%w: client ID must be one topic level without '/', '+', '#' or NUL", tether.ErrInvalidInput)
	}

	return nil
}
