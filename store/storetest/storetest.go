// Package storetest holds the conformance suite every store backend must pass.
//
// The suite uses only the public store.Store interface and creates its own
// data. It never assumes an empty database and never assumes an order beyond
// the documented one: ListTenants and ListDevices return CreatedAt ascending,
// then ID.
package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/tenant"
)

// Run executes the conformance suite. newStore must return a ready store (the
// suite calls Migrate on it) and register its own cleanup with t.Cleanup.
func Run(t *testing.T, newStore func(t *testing.T) store.Store) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(t *testing.T, s store.Store)
	}{
		{"Lifecycle", testLifecycle},
		{"TenantCRUD", testTenantCRUD},
		{"TenantExternalUnique", testTenantExternalUnique},
		{"TenantNotFound", testTenantNotFound},
		{"TenantInvalidInput", testTenantInvalidInput},
		{"TenantList", testTenantList},
		{"TenantUpdateSetsUpdatedAt", testTenantUpdateSetsUpdatedAt},
		{"TenantDeleteDoesNotCascade", testTenantDeleteDoesNotCascade},
		{"DeviceCRUD", testDeviceCRUD},
		{"DeviceByKey", testDeviceByKey},
		{"DeviceKeyUnique", testDeviceKeyUnique},
		{"DeviceNotFound", testDeviceNotFound},
		{"DeviceInvalidInput", testDeviceInvalidInput},
		{"DeviceListIsTenantScoped", testDeviceListIsTenantScoped},
		{"DeviceList", testDeviceList},
		{"DeviceUpdateSetsUpdatedAt", testDeviceUpdateSetsUpdatedAt},
		{"DeviceCreateWithoutTenant", testDeviceCreateWithoutTenant},
		{"CountDevices", testCountDevices},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStore(t)
			require.NoError(t, s.Migrate(context.Background()))
			tt.fn(t, s)
		})
	}
}

// stamp returns a microsecond-precision UTC time offset from a fixed base, so
// SQL backends round-trip it exactly.
func stamp(offset time.Duration) time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Add(offset)
}

// unique returns a string no other subtest or earlier run will reuse.
func unique(prefix string) string {
	return prefix + "-" + id.NewTenantID().String()
}

func newTenant() *tenant.Tenant {
	return &tenant.Tenant{
		ID:         id.NewTenantID(),
		AppID:      unique("app"),
		ExternalID: unique("ext"),
		Name:       "Acme",
		Enabled:    true,
		Limits: tenant.Limits{
			MaxConnections:    50,
			MessagesPerSecond: 12.5,
			Burst:             25,
			MaxSessionExpiry:  2 * time.Hour,
			MaxSessions:       12,
		},
	}
}

func newDevice(tenantID id.TenantID) *device.Device {
	return &device.Device{
		ID:       id.NewDeviceID(),
		TenantID: tenantID,
		Name:     "sensor",
		KeyID:    unique("akey"),
		ClientID: "sensor-1",
		Roles:    []string{"sensor", "telemetry"},
		Metadata: map[string]any{
			"region": "eu",
			"beta":   true,
			"weight": 3.5,
			"nested": map[string]any{"tags": []any{"a", "b"}},
		},
	}
}

func mustCreateTenant(t *testing.T, s store.Store, tn *tenant.Tenant) *tenant.Tenant {
	t.Helper()
	require.NoError(t, s.CreateTenant(context.Background(), tn))

	return tn
}

func mustCreateDevice(t *testing.T, s store.Store, d *device.Device) *device.Device {
	t.Helper()
	require.NoError(t, s.CreateDevice(context.Background(), d))

	return d
}

func assertTenantEqual(t *testing.T, want, got *tenant.Tenant) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.AppID, got.AppID)
	assert.Equal(t, want.ExternalID, got.ExternalID)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.Enabled, got.Enabled)
	assert.Equal(t, want.Limits, got.Limits)
	assert.True(t, want.CreatedAt.Equal(got.CreatedAt), "CreatedAt: want %v got %v", want.CreatedAt, got.CreatedAt)
	assert.True(t, want.UpdatedAt.Equal(got.UpdatedAt), "UpdatedAt: want %v got %v", want.UpdatedAt, got.UpdatedAt)
}

func assertDeviceEqual(t *testing.T, want, got *device.Device) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.TenantID, got.TenantID)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.KeyID, got.KeyID)
	assert.Equal(t, want.ClientID, got.ClientID)
	assert.Equal(t, want.Roles, got.Roles)
	assert.Equal(t, want.Metadata, got.Metadata)
	assert.True(t, want.CreatedAt.Equal(got.CreatedAt), "CreatedAt: want %v got %v", want.CreatedAt, got.CreatedAt)
	assert.True(t, want.UpdatedAt.Equal(got.UpdatedAt), "UpdatedAt: want %v got %v", want.UpdatedAt, got.UpdatedAt)
}

func testLifecycle(t *testing.T, s store.Store) {
	ctx := context.Background()

	require.NoError(t, s.Migrate(ctx), "Migrate must be safe to repeat")
	require.NoError(t, s.Ping(ctx))
}

func testTenantCRUD(t *testing.T, s store.Store) {
	ctx := context.Background()
	before := time.Now().UTC().Add(-time.Second)

	tn := newTenant()
	require.NoError(t, s.CreateTenant(ctx, tn))
	assert.False(t, tn.CreatedAt.IsZero(), "Create sets CreatedAt")
	assert.False(t, tn.UpdatedAt.IsZero(), "Create sets UpdatedAt")
	assert.True(t, tn.CreatedAt.After(before))

	got, err := s.GetTenant(ctx, tn.ID)
	require.NoError(t, err)
	assertTenantEqual(t, tn, got)

	byExt, err := s.GetTenantByExternal(ctx, tn.AppID, tn.ExternalID)
	require.NoError(t, err)
	assertTenantEqual(t, tn, byExt)

	// The same external ID under a different app is a different tenant.
	_, err = s.GetTenantByExternal(ctx, unique("other-app"), tn.ExternalID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	// Update replaces every field, keeps CreatedAt and bumps UpdatedAt.
	created := got.CreatedAt
	upd := *got
	upd.Name = "Acme Renamed"
	upd.Enabled = false
	upd.ExternalID = unique("ext")
	upd.Limits = tenant.Limits{MaxConnections: 5}
	upd.CreatedAt = stamp(0) // ignored: Update keeps the stored CreatedAt
	require.NoError(t, s.UpdateTenant(ctx, &upd))
	assert.True(t, upd.CreatedAt.Equal(created), "Update keeps CreatedAt")
	assert.False(t, upd.UpdatedAt.Before(created))

	got, err = s.GetTenant(ctx, tn.ID)
	require.NoError(t, err)
	assertTenantEqual(t, &upd, got)
	assert.Equal(t, "Acme Renamed", got.Name)
	assert.False(t, got.Enabled)
	assert.Equal(t, tenant.Limits{MaxConnections: 5}, got.Limits)

	// The old external pair is free, the new one resolves.
	_, err = s.GetTenantByExternal(ctx, tn.AppID, tn.ExternalID)
	require.ErrorIs(t, err, tether.ErrNotFound)
	byExt, err = s.GetTenantByExternal(ctx, upd.AppID, upd.ExternalID)
	require.NoError(t, err)
	assert.Equal(t, tn.ID, byExt.ID)

	// Delete removes the tenant and frees its external pair.
	require.NoError(t, s.DeleteTenant(ctx, tn.ID))
	_, err = s.GetTenant(ctx, tn.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	again := newTenant()
	again.AppID, again.ExternalID = upd.AppID, upd.ExternalID
	require.NoError(t, s.CreateTenant(ctx, again), "a deleted tenant's external pair is reusable")

	// Mutating the caller's input or a returned tenant never reaches the store.
	fresh := newTenant()
	mustCreateTenant(t, s, fresh)
	fresh.Name = "mutated-after-create"

	read, err := s.GetTenant(ctx, fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, "Acme", read.Name)

	read.Name = "mutated-after-read"

	reread, err := s.GetTenant(ctx, fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, "Acme", reread.Name)
}

func testTenantExternalUnique(t *testing.T, s store.Store) {
	ctx := context.Background()

	first := mustCreateTenant(t, s, newTenant())

	dup := newTenant()
	dup.AppID, dup.ExternalID = first.AppID, first.ExternalID
	require.ErrorIs(t, s.CreateTenant(ctx, dup), tether.ErrConflict)

	_, err := s.GetTenant(ctx, dup.ID)
	require.ErrorIs(t, err, tether.ErrNotFound, "a conflicting create stores nothing")

	// Same external ID under another app, and another external ID under the
	// same app, are both fine.
	otherApp := newTenant()
	otherApp.ExternalID = first.ExternalID
	require.NoError(t, s.CreateTenant(ctx, otherApp))

	otherExt := newTenant()
	otherExt.AppID = first.AppID
	require.NoError(t, s.CreateTenant(ctx, otherExt))

	// Re-creating the same ID is a conflict too.
	sameID := newTenant()
	sameID.ID = first.ID
	require.ErrorIs(t, s.CreateTenant(ctx, sameID), tether.ErrConflict)

	// Updating onto another tenant's pair is a conflict and changes nothing.
	move := *otherExt
	move.ExternalID = first.ExternalID
	move.Name = "should not stick"
	require.ErrorIs(t, s.UpdateTenant(ctx, &move), tether.ErrConflict)

	got, err := s.GetTenant(ctx, otherExt.ID)
	require.NoError(t, err)
	assert.Equal(t, otherExt.ExternalID, got.ExternalID)
	assert.Equal(t, otherExt.Name, got.Name)

	// Updating a tenant without touching its pair is not a self-conflict.
	same := *got
	same.Name = "renamed"
	require.NoError(t, s.UpdateTenant(ctx, &same))
}

func testTenantNotFound(t *testing.T, s store.Store) {
	ctx := context.Background()
	missing := id.NewTenantID()

	_, err := s.GetTenant(ctx, missing)
	require.ErrorIs(t, err, tether.ErrNotFound)

	_, err = s.GetTenantByExternal(ctx, unique("app"), unique("ext"))
	require.ErrorIs(t, err, tether.ErrNotFound)

	ghost := newTenant()
	ghost.ID = missing
	require.ErrorIs(t, s.UpdateTenant(ctx, ghost), tether.ErrNotFound)

	require.ErrorIs(t, s.DeleteTenant(ctx, missing), tether.ErrNotFound)

	// An error that is not found must not also claim to be a conflict.
	assert.False(t, errors.Is(s.DeleteTenant(ctx, missing), tether.ErrConflict))
}

func testTenantInvalidInput(t *testing.T, s store.Store) {
	ctx := context.Background()

	require.ErrorIs(t, s.CreateTenant(ctx, nil), tether.ErrInvalidInput)
	require.ErrorIs(t, s.UpdateTenant(ctx, nil), tether.ErrInvalidInput)

	noID := newTenant()
	noID.ID = id.Nil
	require.ErrorIs(t, s.CreateTenant(ctx, noID), tether.ErrInvalidInput)
	require.ErrorIs(t, s.UpdateTenant(ctx, noID), tether.ErrInvalidInput)

	// A positive session expiry cap under a second would round to zero
	// seconds, which ends every session at disconnect.
	subSecond := newTenant()
	subSecond.Limits.MaxSessionExpiry = 500 * time.Millisecond
	require.ErrorIs(t, s.CreateTenant(ctx, subSecond), tether.ErrInvalidInput)

	stored := newTenant()
	require.NoError(t, s.CreateTenant(ctx, stored))

	stored.Limits.MaxSessionExpiry = time.Millisecond
	require.ErrorIs(t, s.UpdateTenant(ctx, stored), tether.ErrInvalidInput)

	_, err := s.ListTenants(ctx, tenant.ListFilter{Limit: -1})
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	_, err = s.ListTenants(ctx, tenant.ListFilter{Offset: -1})
	require.ErrorIs(t, err, tether.ErrInvalidInput)
}

func testTenantList(t *testing.T, s store.Store) {
	ctx := context.Background()

	// Create out of order with explicit CreatedAt values, so a store that
	// returns insertion order fails. a and b share a timestamp: ID breaks the tie.
	newest := newTenant()
	newest.CreatedAt = stamp(3 * time.Second)
	oldest := newTenant()
	oldest.CreatedAt = stamp(0)
	tieA := newTenant()
	tieA.CreatedAt = stamp(time.Second)
	tieB := newTenant()
	tieB.CreatedAt = stamp(time.Second)

	if tieA.ID.String() > tieB.ID.String() {
		tieA, tieB = tieB, tieA
	}

	for _, tn := range []*tenant.Tenant{newest, tieB, oldest, tieA} {
		mustCreateTenant(t, s, tn)
	}

	want := []id.TenantID{oldest.ID, tieA.ID, tieB.ID, newest.ID}

	all, err := s.ListTenants(ctx, tenant.ListFilter{})
	require.NoError(t, err)
	assert.Equal(t, want, filterTenantIDs(all, want), "CreatedAt ascending, then ID")

	// Paging walks the same sequence, whatever else is in the store.
	for i := range all {
		page, pageErr := s.ListTenants(ctx, tenant.ListFilter{Limit: 1, Offset: i})
		require.NoError(t, pageErr)
		require.Len(t, page, 1)
		assert.Equal(t, all[i].ID, page[0].ID)
	}

	limited, err := s.ListTenants(ctx, tenant.ListFilter{Limit: 2})
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, all[0].ID, limited[0].ID)
	assert.Equal(t, all[1].ID, limited[1].ID)

	offset, err := s.ListTenants(ctx, tenant.ListFilter{Offset: 1})
	require.NoError(t, err)
	require.Len(t, offset, len(all)-1)
	assert.Equal(t, all[1].ID, offset[0].ID)

	past, err := s.ListTenants(ctx, tenant.ListFilter{Offset: len(all) + 10})
	require.NoError(t, err)
	assert.Empty(t, past)
}

func filterTenantIDs(ts []*tenant.Tenant, keep []id.TenantID) []id.TenantID {
	set := make(map[string]bool, len(keep))
	for _, k := range keep {
		set[k.String()] = true
	}

	var out []id.TenantID

	for _, tn := range ts {
		if set[tn.ID.String()] {
			out = append(out, tn.ID)
		}
	}

	return out
}

func testDeviceCRUD(t *testing.T, s store.Store) {
	ctx := context.Background()
	tn := mustCreateTenant(t, s, newTenant())

	d := newDevice(tn.ID)
	require.NoError(t, s.CreateDevice(ctx, d))
	assert.False(t, d.CreatedAt.IsZero(), "Create sets CreatedAt")
	assert.False(t, d.UpdatedAt.IsZero(), "Create sets UpdatedAt")

	got, err := s.GetDevice(ctx, tn.ID, d.ID)
	require.NoError(t, err)
	assertDeviceEqual(t, d, got)

	// An empty ClientID (any client) round-trips.
	anyClient := newDevice(tn.ID)
	anyClient.ClientID = ""
	mustCreateDevice(t, s, anyClient)

	got2, err := s.GetDevice(ctx, tn.ID, anyClient.ID)
	require.NoError(t, err)
	assert.Empty(t, got2.ClientID)

	// Update replaces every field, keeps CreatedAt and bumps UpdatedAt.
	created := got.CreatedAt
	upd := *got
	upd.Name = "sensor renamed"
	upd.KeyID = unique("akey")
	upd.ClientID = "other-client"
	upd.Roles = []string{"admin"}
	upd.Metadata = map[string]any{"region": "us"}
	upd.CreatedAt = stamp(0) // ignored: Update keeps the stored CreatedAt
	require.NoError(t, s.UpdateDevice(ctx, &upd))
	assert.True(t, upd.CreatedAt.Equal(created), "Update keeps CreatedAt")
	assert.False(t, upd.UpdatedAt.Before(created))

	got, err = s.GetDevice(ctx, tn.ID, d.ID)
	require.NoError(t, err)
	assertDeviceEqual(t, &upd, got)

	// The old key is free, the new one resolves.
	_, err = s.GetDeviceByKey(ctx, d.KeyID)
	require.ErrorIs(t, err, tether.ErrNotFound)
	byKey, err := s.GetDeviceByKey(ctx, upd.KeyID)
	require.NoError(t, err)
	assert.Equal(t, d.ID, byKey.ID)

	// Delete removes the device and frees its key.
	require.NoError(t, s.DeleteDevice(ctx, tn.ID, d.ID))
	_, err = s.GetDevice(ctx, tn.ID, d.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)
	_, err = s.GetDeviceByKey(ctx, upd.KeyID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	reuse := newDevice(tn.ID)
	reuse.KeyID = upd.KeyID
	require.NoError(t, s.CreateDevice(ctx, reuse), "a deleted device's key is reusable")

	// Mutating a returned device or the caller's input never reaches the store.
	fresh := newDevice(tn.ID)
	mustCreateDevice(t, s, fresh)
	fresh.Roles[0] = "mutated-after-create"
	fresh.Metadata["region"] = "mutated-after-create"

	read, err := s.GetDevice(ctx, tn.ID, fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, "sensor", read.Roles[0])
	assert.Equal(t, "eu", read.Metadata["region"])

	read.Roles[0] = "mutated-after-read"
	read.Metadata["region"] = "mutated-after-read"

	reread, err := s.GetDevice(ctx, tn.ID, fresh.ID)
	require.NoError(t, err)
	assert.Equal(t, "sensor", reread.Roles[0])
	assert.Equal(t, "eu", reread.Metadata["region"])
}

func testDeviceByKey(t *testing.T, s store.Store) {
	ctx := context.Background()
	tnA := mustCreateTenant(t, s, newTenant())
	tnB := mustCreateTenant(t, s, newTenant())

	a := mustCreateDevice(t, s, newDevice(tnA.ID))
	b := mustCreateDevice(t, s, newDevice(tnB.ID))

	// Key lookup is global: it finds the device and says which tenant owns it.
	gotA, err := s.GetDeviceByKey(ctx, a.KeyID)
	require.NoError(t, err)
	assertDeviceEqual(t, a, gotA)
	assert.Equal(t, tnA.ID, gotA.TenantID)

	gotB, err := s.GetDeviceByKey(ctx, b.KeyID)
	require.NoError(t, err)
	assert.Equal(t, tnB.ID, gotB.TenantID)

	_, err = s.GetDeviceByKey(ctx, unique("akey"))
	require.ErrorIs(t, err, tether.ErrNotFound)

	_, err = s.GetDeviceByKey(ctx, "")
	require.ErrorIs(t, err, tether.ErrNotFound)
}

func testDeviceKeyUnique(t *testing.T, s store.Store) {
	ctx := context.Background()
	tnA := mustCreateTenant(t, s, newTenant())
	tnB := mustCreateTenant(t, s, newTenant())

	first := mustCreateDevice(t, s, newDevice(tnA.ID))

	// The key is unique across tenants, not just within one.
	for _, owner := range []id.TenantID{tnA.ID, tnB.ID} {
		dup := newDevice(owner)
		dup.KeyID = first.KeyID
		require.ErrorIs(t, s.CreateDevice(ctx, dup), tether.ErrConflict)

		_, err := s.GetDevice(ctx, owner, dup.ID)
		require.ErrorIs(t, err, tether.ErrNotFound, "a conflicting create stores nothing")
	}

	// Device IDs are globally unique: re-using one is a conflict, even under
	// another tenant.
	for _, owner := range []id.TenantID{tnA.ID, tnB.ID} {
		sameID := newDevice(owner)
		sameID.ID = first.ID
		require.ErrorIs(t, s.CreateDevice(ctx, sameID), tether.ErrConflict)
	}

	// Updating onto another device's key is a conflict and changes nothing.
	other := mustCreateDevice(t, s, newDevice(tnA.ID))
	move := *other
	move.KeyID = first.KeyID
	move.Name = "should not stick"
	require.ErrorIs(t, s.UpdateDevice(ctx, &move), tether.ErrConflict)

	got, err := s.GetDevice(ctx, tnA.ID, other.ID)
	require.NoError(t, err)
	assert.Equal(t, other.KeyID, got.KeyID)
	assert.Equal(t, other.Name, got.Name)

	// Updating a device without touching its key is not a self-conflict.
	same := *got
	same.Name = "renamed"
	require.NoError(t, s.UpdateDevice(ctx, &same))
}

func testDeviceNotFound(t *testing.T, s store.Store) {
	ctx := context.Background()
	tnA := mustCreateTenant(t, s, newTenant())
	tnB := mustCreateTenant(t, s, newTenant())
	d := mustCreateDevice(t, s, newDevice(tnA.ID))

	_, err := s.GetDevice(ctx, tnA.ID, id.NewDeviceID())
	require.ErrorIs(t, err, tether.ErrNotFound)

	// A device that exists under another tenant is not found, for every operation.
	_, err = s.GetDevice(ctx, tnB.ID, d.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	require.ErrorIs(t, s.DeleteDevice(ctx, tnB.ID, d.ID), tether.ErrNotFound)

	cross := *d
	cross.TenantID = tnB.ID
	cross.Name = "should not stick"
	require.ErrorIs(t, s.UpdateDevice(ctx, &cross), tether.ErrNotFound)

	ghost := newDevice(tnA.ID)
	require.ErrorIs(t, s.UpdateDevice(ctx, ghost), tether.ErrNotFound)
	require.ErrorIs(t, s.DeleteDevice(ctx, tnA.ID, ghost.ID), tether.ErrNotFound)

	// The failed cross-tenant calls left the device alone.
	got, err := s.GetDevice(ctx, tnA.ID, d.ID)
	require.NoError(t, err)
	assert.Equal(t, d.Name, got.Name)
}

func testDeviceInvalidInput(t *testing.T, s store.Store) {
	ctx := context.Background()
	tn := mustCreateTenant(t, s, newTenant())

	require.ErrorIs(t, s.CreateDevice(ctx, nil), tether.ErrInvalidInput)
	require.ErrorIs(t, s.UpdateDevice(ctx, nil), tether.ErrInvalidInput)

	noID := newDevice(tn.ID)
	noID.ID = id.Nil
	require.ErrorIs(t, s.CreateDevice(ctx, noID), tether.ErrInvalidInput)

	noTenant := newDevice(id.Nil)
	require.ErrorIs(t, s.CreateDevice(ctx, noTenant), tether.ErrInvalidInput)

	noKey := newDevice(tn.ID)
	noKey.KeyID = ""
	require.ErrorIs(t, s.CreateDevice(ctx, noKey), tether.ErrInvalidInput)

	_, err := s.ListDevices(ctx, device.ListFilter{})
	require.ErrorIs(t, err, tether.ErrInvalidInput, "a zero TenantID never means all tenants")

	_, err = s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID, Limit: -1})
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	_, err = s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID, Offset: -1})
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	_, err = s.CountDevices(ctx, id.Nil)
	require.ErrorIs(t, err, tether.ErrInvalidInput)
}

func testDeviceListIsTenantScoped(t *testing.T, s store.Store) {
	ctx := context.Background()
	tnA := mustCreateTenant(t, s, newTenant())
	tnB := mustCreateTenant(t, s, newTenant())
	empty := mustCreateTenant(t, s, newTenant())

	idsA := make([]id.DeviceID, 0, 3)
	idsB := make([]id.DeviceID, 0, 2)

	for range 3 {
		idsA = append(idsA, mustCreateDevice(t, s, newDevice(tnA.ID)).ID)
	}

	for range 2 {
		idsB = append(idsB, mustCreateDevice(t, s, newDevice(tnB.ID)).ID)
	}

	listA, err := s.ListDevices(ctx, device.ListFilter{TenantID: tnA.ID})
	require.NoError(t, err)
	assert.ElementsMatch(t, idsA, deviceIDs(listA))

	listB, err := s.ListDevices(ctx, device.ListFilter{TenantID: tnB.ID})
	require.NoError(t, err)
	assert.ElementsMatch(t, idsB, deviceIDs(listB))

	for _, d := range listA {
		assert.Equal(t, tnA.ID, d.TenantID, "tenant B's devices never appear in tenant A's list")
	}

	for _, d := range listB {
		assert.Equal(t, tnB.ID, d.TenantID)
	}

	none, err := s.ListDevices(ctx, device.ListFilter{TenantID: empty.ID})
	require.NoError(t, err)
	assert.Empty(t, none)

	// Paging stays inside the tenant.
	paged, err := s.ListDevices(ctx, device.ListFilter{TenantID: tnA.ID, Limit: 2, Offset: 2})
	require.NoError(t, err)
	require.Len(t, paged, 1)
	assert.Equal(t, tnA.ID, paged[0].TenantID)
}

func testDeviceList(t *testing.T, s store.Store) {
	ctx := context.Background()
	tn := mustCreateTenant(t, s, newTenant())

	newest := newDevice(tn.ID)
	newest.CreatedAt = stamp(3 * time.Second)
	oldest := newDevice(tn.ID)
	oldest.CreatedAt = stamp(0)
	tieA := newDevice(tn.ID)
	tieA.CreatedAt = stamp(time.Second)
	tieB := newDevice(tn.ID)
	tieB.CreatedAt = stamp(time.Second)

	if tieA.ID.String() > tieB.ID.String() {
		tieA, tieB = tieB, tieA
	}

	for _, d := range []*device.Device{newest, tieB, oldest, tieA} {
		mustCreateDevice(t, s, d)
	}

	want := []id.DeviceID{oldest.ID, tieA.ID, tieB.ID, newest.ID}

	all, err := s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID})
	require.NoError(t, err)
	assert.Equal(t, want, deviceIDs(all), "CreatedAt ascending, then ID")

	page, err := s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID, Limit: 2})
	require.NoError(t, err)
	assert.Equal(t, want[:2], deviceIDs(page))

	page, err = s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID, Limit: 2, Offset: 1})
	require.NoError(t, err)
	assert.Equal(t, want[1:3], deviceIDs(page))

	page, err = s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID, Offset: 3})
	require.NoError(t, err)
	assert.Equal(t, want[3:], deviceIDs(page))

	page, err = s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID, Offset: 10})
	require.NoError(t, err)
	assert.Empty(t, page)
}

func deviceIDs(ds []*device.Device) []id.DeviceID {
	out := make([]id.DeviceID, 0, len(ds))
	for _, d := range ds {
		out = append(out, d.ID)
	}

	return out
}

func testCountDevices(t *testing.T, s store.Store) {
	ctx := context.Background()
	tnA := mustCreateTenant(t, s, newTenant())
	tnB := mustCreateTenant(t, s, newTenant())

	count := func(tenantID id.TenantID) int {
		t.Helper()

		n, err := s.CountDevices(ctx, tenantID)
		require.NoError(t, err)

		return n
	}

	assert.Zero(t, count(tnA.ID), "a tenant with no devices counts zero")
	assert.Zero(t, count(id.NewTenantID()), "an unknown tenant counts zero")

	created := make([]*device.Device, 0, 3)

	for i := range 3 {
		d := mustCreateDevice(t, s, newDevice(tnA.ID))
		created = append(created, d)
		assert.Equal(t, i+1, count(tnA.ID))
	}

	mustCreateDevice(t, s, newDevice(tnB.ID))
	assert.Equal(t, 3, count(tnA.ID), "other tenants' devices are not counted")
	assert.Equal(t, 1, count(tnB.ID))

	require.NoError(t, s.DeleteDevice(ctx, tnA.ID, created[0].ID))
	assert.Equal(t, 2, count(tnA.ID))

	// A failed create does not change the count.
	dup := newDevice(tnA.ID)
	dup.KeyID = created[1].KeyID
	require.Error(t, s.CreateDevice(ctx, dup))
	assert.Equal(t, 2, count(tnA.ID), "a conflicting create is not counted")
}

// The next two tests pin "Update sets UpdatedAt to now" without sleeping: the
// record is created with an UpdatedAt far in the past, so an Update that does
// not bump it leaves a value that is not after that stamp.

func testTenantUpdateSetsUpdatedAt(t *testing.T, s store.Store) {
	ctx := context.Background()

	tn := newTenant()
	tn.CreatedAt = stamp(0)
	tn.UpdatedAt = stamp(0)
	mustCreateTenant(t, s, tn)

	got, err := s.GetTenant(ctx, tn.ID)
	require.NoError(t, err)
	require.True(t, got.UpdatedAt.Equal(stamp(0)), "Create keeps a caller-supplied UpdatedAt")

	before := time.Now().UTC().Truncate(time.Microsecond)

	upd := *got
	upd.Name = "renamed"
	require.NoError(t, s.UpdateTenant(ctx, &upd))

	assert.True(t, upd.UpdatedAt.After(stamp(0)), "Update bumps UpdatedAt, got %v", upd.UpdatedAt)
	assert.False(t, upd.UpdatedAt.Before(before), "UpdatedAt is now, got %v want >= %v", upd.UpdatedAt, before)
	assert.True(t, upd.CreatedAt.Equal(stamp(0)), "Update keeps CreatedAt")

	read, err := s.GetTenant(ctx, tn.ID)
	require.NoError(t, err)
	assert.True(t, read.UpdatedAt.Equal(upd.UpdatedAt), "read-back UpdatedAt: want %v got %v", upd.UpdatedAt, read.UpdatedAt)
	assert.True(t, read.CreatedAt.Equal(stamp(0)))
}

func testDeviceUpdateSetsUpdatedAt(t *testing.T, s store.Store) {
	ctx := context.Background()
	tn := mustCreateTenant(t, s, newTenant())

	d := newDevice(tn.ID)
	d.CreatedAt = stamp(0)
	d.UpdatedAt = stamp(0)
	mustCreateDevice(t, s, d)

	got, err := s.GetDevice(ctx, tn.ID, d.ID)
	require.NoError(t, err)
	require.True(t, got.UpdatedAt.Equal(stamp(0)), "Create keeps a caller-supplied UpdatedAt")

	before := time.Now().UTC().Truncate(time.Microsecond)

	upd := *got
	upd.Name = "renamed"
	require.NoError(t, s.UpdateDevice(ctx, &upd))

	assert.True(t, upd.UpdatedAt.After(stamp(0)), "Update bumps UpdatedAt, got %v", upd.UpdatedAt)
	assert.False(t, upd.UpdatedAt.Before(before), "UpdatedAt is now, got %v want >= %v", upd.UpdatedAt, before)
	assert.True(t, upd.CreatedAt.Equal(stamp(0)), "Update keeps CreatedAt")

	read, err := s.GetDevice(ctx, tn.ID, d.ID)
	require.NoError(t, err)
	assert.True(t, read.UpdatedAt.Equal(upd.UpdatedAt), "read-back UpdatedAt: want %v got %v", upd.UpdatedAt, read.UpdatedAt)
	assert.True(t, read.CreatedAt.Equal(stamp(0)))
}

// testTenantDeleteDoesNotCascade pins that DeleteTenant leaves devices alone.
// The service layer (not the store) refuses to delete a tenant that still has
// devices, so a backend must not add ON DELETE CASCADE or RESTRICT.
func testTenantDeleteDoesNotCascade(t *testing.T, s store.Store) {
	ctx := context.Background()
	tn := mustCreateTenant(t, s, newTenant())
	d := mustCreateDevice(t, s, newDevice(tn.ID))

	require.NoError(t, s.DeleteTenant(ctx, tn.ID), "DeleteTenant succeeds while devices exist")

	_, err := s.GetTenant(ctx, tn.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	got, err := s.GetDevice(ctx, tn.ID, d.ID)
	require.NoError(t, err, "the device survives its tenant's deletion")
	assertDeviceEqual(t, d, got)

	byKey, err := s.GetDeviceByKey(ctx, d.KeyID)
	require.NoError(t, err)
	assert.Equal(t, d.ID, byKey.ID)

	n, err := s.CountDevices(ctx, tn.ID)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	list, err := s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID})
	require.NoError(t, err)
	assert.Equal(t, []id.DeviceID{d.ID}, deviceIDs(list))
}

// testDeviceCreateWithoutTenant pins that CreateDevice does not check the
// tenant exists. The service layer does, so a backend must not add a foreign
// key from devices to tenants.
func testDeviceCreateWithoutTenant(t *testing.T, s store.Store) {
	ctx := context.Background()
	neverCreated := id.NewTenantID()

	d := newDevice(neverCreated)
	require.NoError(t, s.CreateDevice(ctx, d), "CreateDevice does not check the tenant exists")

	got, err := s.GetDevice(ctx, neverCreated, d.ID)
	require.NoError(t, err)
	assertDeviceEqual(t, d, got)

	n, err := s.CountDevices(ctx, neverCreated)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}
