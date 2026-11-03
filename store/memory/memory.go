// Package memory implements store.Store in process memory. It is meant for
// tests and single-process development; nothing survives a restart.
package memory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/tenant"
)

var _ store.Store = (*Store)(nil)

// errClosed is returned by Ping once Close has been called.
var errClosed = errors.New("tether/memory: store closed")

type externalKey struct{ appID, externalID string }

// Store is an in-memory store.Store. It copies records on every read and write,
// so callers can never mutate stored values. It is safe for concurrent use.
type Store struct {
	mu sync.RWMutex

	tenants  map[string]*tenant.Tenant
	external map[externalKey]string

	// devices is keyed by device ID alone: IDs are globally unique, and a
	// lookup under the wrong tenant is reported as not found.
	devices map[string]*device.Device
	byKey   map[string]string

	closed bool
}

// New returns an empty Store.
func New() *Store {
	return &Store{
		tenants:  make(map[string]*tenant.Tenant),
		external: make(map[externalKey]string),
		devices:  make(map[string]*device.Device),
		byKey:    make(map[string]string),
	}
}

// now returns the current time as UTC truncated to microseconds, the precision
// the SQL stores round-trip.
func now() time.Time { return normalize(time.Now()) }

func normalize(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

func opErr(op string, sentinel error, detail string) error {
	if detail == "" {
		return fmt.Errorf("tether/memory: %s: %w", op, sentinel)
	}

	return fmt.Errorf("tether/memory: %s: %s: %w", op, detail, sentinel)
}

// ── Lifecycle ───────────────────────────────────────────

// Migrate is a no-op: there is no schema.
func (s *Store) Migrate(ctx context.Context) error { return ctx.Err() }

// Ping returns an error once the store is closed.
func (s *Store) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errClosed
	}

	return nil
}

// Close marks the store closed. Reads and writes keep working, so a late
// goroutine never panics, but Ping starts failing.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

// ── Tenants ─────────────────────────────────────────────

// CreateTenant implements tenant.Store.
func (s *Store) CreateTenant(ctx context.Context, t *tenant.Tenant) error {
	const op = "create tenant"

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.Validate(); err != nil {
		return opErr(op, tether.ErrInvalidInput, err.Error())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := t.ID.String()
	ext := externalKey{t.AppID, t.ExternalID}

	if _, ok := s.tenants[key]; ok {
		return opErr(op, tether.ErrConflict, "tenant ID "+key+" exists")
	}

	if _, ok := s.external[ext]; ok {
		return opErr(op, tether.ErrConflict, "app and external ID are already mapped")
	}

	ts := now()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = ts
	} else {
		t.CreatedAt = normalize(t.CreatedAt)
	}

	if t.UpdatedAt.IsZero() {
		t.UpdatedAt = ts
	} else {
		t.UpdatedAt = normalize(t.UpdatedAt)
	}

	cp := *t
	s.tenants[key] = &cp
	s.external[ext] = key

	return nil
}

// GetTenant implements tenant.Store.
func (s *Store) GetTenant(ctx context.Context, tenantID id.TenantID) (*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.tenants[tenantID.String()]
	if !ok {
		return nil, opErr("get tenant", tether.ErrNotFound, tenantID.String())
	}

	cp := *t

	return &cp, nil
}

// GetTenantByExternal implements tenant.Store.
func (s *Store) GetTenantByExternal(ctx context.Context, appID, externalID string) (*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key, ok := s.external[externalKey{appID, externalID}]
	if !ok {
		return nil, opErr("get tenant by external", tether.ErrNotFound, "")
	}

	cp := *s.tenants[key]

	return &cp, nil
}

// UpdateTenant implements tenant.Store.
func (s *Store) UpdateTenant(ctx context.Context, t *tenant.Tenant) error {
	const op = "update tenant"

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := t.Validate(); err != nil {
		return opErr(op, tether.ErrInvalidInput, err.Error())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := t.ID.String()

	old, ok := s.tenants[key]
	if !ok {
		return opErr(op, tether.ErrNotFound, key)
	}

	oldExt := externalKey{old.AppID, old.ExternalID}
	newExt := externalKey{t.AppID, t.ExternalID}

	if owner, taken := s.external[newExt]; taken && owner != key {
		return opErr(op, tether.ErrConflict, "app and external ID are already mapped")
	}

	t.CreatedAt = old.CreatedAt
	t.UpdatedAt = now()

	// Each write moves UpdatedAt forward, even past a clock behind the last
	// writer's (tenant.Store).
	if !t.UpdatedAt.After(old.UpdatedAt) {
		t.UpdatedAt = old.UpdatedAt.Add(time.Microsecond)
	}

	cp := *t
	s.tenants[key] = &cp

	if oldExt != newExt {
		delete(s.external, oldExt)
		s.external[newExt] = key
	}

	return nil
}

// DeleteTenant implements tenant.Store. It does not cascade to devices.
func (s *Store) DeleteTenant(ctx context.Context, tenantID id.TenantID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := tenantID.String()

	t, ok := s.tenants[key]
	if !ok {
		return opErr("delete tenant", tether.ErrNotFound, key)
	}

	delete(s.external, externalKey{t.AppID, t.ExternalID})
	delete(s.tenants, key)

	return nil
}

// ListTenants implements tenant.Store, ordered by CreatedAt ascending, then ID.
func (s *Store) ListTenants(ctx context.Context, f tenant.ListFilter) ([]*tenant.Tenant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := f.Validate(); err != nil {
		return nil, opErr("list tenants", tether.ErrInvalidInput, err.Error())
	}

	s.mu.RLock()
	all := make([]*tenant.Tenant, 0, len(s.tenants))

	for _, t := range s.tenants {
		cp := *t
		all = append(all, &cp)
	}
	s.mu.RUnlock()

	slices.SortFunc(all, func(a, b *tenant.Tenant) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}

		return cmp.Compare(a.ID.String(), b.ID.String())
	})

	return page(all, f.Limit, f.Offset), nil
}

// ── Devices ─────────────────────────────────────────────

// CreateDevice implements device.Store.
func (s *Store) CreateDevice(ctx context.Context, d *device.Device) error {
	const op = "create device"

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.Validate(); err != nil {
		return opErr(op, tether.ErrInvalidInput, err.Error())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := d.ID.String()

	if _, ok := s.devices[key]; ok {
		return opErr(op, tether.ErrConflict, "device ID "+key+" exists")
	}

	if _, ok := s.byKey[d.KeyID]; ok {
		return opErr(op, tether.ErrConflict, "key ID is already assigned")
	}

	ts := now()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = ts
	} else {
		d.CreatedAt = normalize(d.CreatedAt)
	}

	if d.UpdatedAt.IsZero() {
		d.UpdatedAt = ts
	} else {
		d.UpdatedAt = normalize(d.UpdatedAt)
	}

	s.devices[key] = cloneDevice(d)
	s.byKey[d.KeyID] = key

	return nil
}

// GetDevice implements device.Store.
func (s *Store) GetDevice(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) (*device.Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	d, ok := s.devices[deviceID.String()]
	if !ok || d.TenantID != tenantID {
		return nil, opErr("get device", tether.ErrNotFound, deviceID.String())
	}

	return cloneDevice(d), nil
}

// GetDeviceByKey implements device.Store.
func (s *Store) GetDeviceByKey(ctx context.Context, keyID string) (*device.Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key, ok := s.byKey[keyID]
	if !ok {
		return nil, opErr("get device by key", tether.ErrNotFound, "")
	}

	return cloneDevice(s.devices[key]), nil
}

// UpdateDevice implements device.Store.
func (s *Store) UpdateDevice(ctx context.Context, d *device.Device) error {
	const op = "update device"

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := d.Validate(); err != nil {
		return opErr(op, tether.ErrInvalidInput, err.Error())
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := d.ID.String()

	old, ok := s.devices[key]
	if !ok || old.TenantID != d.TenantID {
		return opErr(op, tether.ErrNotFound, key)
	}

	if owner, taken := s.byKey[d.KeyID]; taken && owner != key {
		return opErr(op, tether.ErrConflict, "key ID is already assigned")
	}

	d.CreatedAt = old.CreatedAt
	d.UpdatedAt = now()

	if old.KeyID != d.KeyID {
		delete(s.byKey, old.KeyID)
		s.byKey[d.KeyID] = key
	}

	s.devices[key] = cloneDevice(d)

	return nil
}

// DeleteDevice implements device.Store.
func (s *Store) DeleteDevice(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := deviceID.String()

	d, ok := s.devices[key]
	if !ok || d.TenantID != tenantID {
		return opErr("delete device", tether.ErrNotFound, key)
	}

	delete(s.byKey, d.KeyID)
	delete(s.devices, key)

	return nil
}

// ListDevices implements device.Store, ordered by CreatedAt ascending, then ID.
func (s *Store) ListDevices(ctx context.Context, f device.ListFilter) ([]*device.Device, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := f.Validate(); err != nil {
		return nil, opErr("list devices", tether.ErrInvalidInput, err.Error())
	}

	s.mu.RLock()
	all := make([]*device.Device, 0)

	for _, d := range s.devices {
		if d.TenantID == f.TenantID {
			all = append(all, cloneDevice(d))
		}
	}
	s.mu.RUnlock()

	slices.SortFunc(all, func(a, b *device.Device) int {
		if c := a.CreatedAt.Compare(b.CreatedAt); c != 0 {
			return c
		}

		return cmp.Compare(a.ID.String(), b.ID.String())
	})

	return page(all, f.Limit, f.Offset), nil
}

// CountDevices implements device.Store.
func (s *Store) CountDevices(ctx context.Context, tenantID id.TenantID) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	if tenantID.IsNil() {
		return 0, opErr("count devices", tether.ErrInvalidInput, "tenant ID is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	n := 0

	for _, d := range s.devices {
		if d.TenantID == tenantID {
			n++
		}
	}

	return n, nil
}

// ── Helpers ─────────────────────────────────────────────

// page applies offset then limit. Limit zero means no limit.
func page[T any](all []T, limit, offset int) []T {
	if offset >= len(all) {
		return []T{}
	}

	all = all[offset:]
	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}

	return all
}

func cloneDevice(d *device.Device) *device.Device {
	cp := *d
	cp.Roles = slices.Clone(d.Roles)
	cp.Metadata = cloneMap(d.Metadata)

	return &cp
}

// cloneMap deep-copies the JSON-shaped values a device carries: nested maps
// and slices. Other values are copied by assignment.
func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = cloneValue(v)
	}

	return out
}

func cloneValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneMap(x)
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = cloneValue(x[i])
		}

		return out
	case []string:
		return slices.Clone(x)
	default:
		return v
	}
}
