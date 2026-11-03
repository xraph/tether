package service

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/tenant"
)

// countingStore counts GetTenantByExternal calls.
type countingStore struct {
	store.Store
	byExternal atomic.Int32
}

func (c *countingStore) GetTenantByExternal(ctx context.Context, appID, externalID string) (*tenant.Tenant, error) {
	c.byExternal.Add(1)

	return c.Store.GetTenantByExternal(ctx, appID, externalID)
}

func newService(t *testing.T) (*Tenants, *countingStore) {
	t.Helper()

	m := memory.New()
	t.Cleanup(func() { _ = m.Close() })

	cs := &countingStore{Store: m}

	return NewTenants(cs), cs
}

func TestCreateValidatesAndDefaultsEnabled(t *testing.T) {
	svc, _ := newService(t)
	ctx := t.Context()

	_, err := svc.Create(ctx, CreateTenantInput{ExternalID: "x"})
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	_, err = svc.Create(ctx, CreateTenantInput{AppID: "aapp_1"})
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	_, err = svc.Create(ctx, CreateTenantInput{
		AppID: "aapp_1", ExternalID: "aorg_sub", Limits: tenant.Limits{MaxSessionExpiry: 500 * time.Millisecond},
	})
	require.ErrorIs(t, err, tether.ErrInvalidInput, "a sub-second session expiry cap rounds to zero")

	got, err := svc.Create(ctx, CreateTenantInput{
		AppID: "aapp_1", ExternalID: "aorg_1", Name: "Acme",
		Limits: tenant.Limits{MaxConnections: 5},
	})
	require.NoError(t, err)
	assert.False(t, got.ID.IsNil())
	assert.Equal(t, id.PrefixTenant, got.ID.Prefix())
	assert.True(t, got.Enabled)
	assert.Equal(t, 5, got.Limits.MaxConnections)

	_, err = svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.ErrorIs(t, err, tether.ErrConflict)

	fetched, err := svc.Get(ctx, got.ID)
	require.NoError(t, err)
	assert.Equal(t, got.ID, fetched.ID)

	list, err := svc.List(ctx, tenant.ListFilter{})
	require.NoError(t, err)
	assert.Len(t, list, 1)
}

func TestUpdatePatchesOnlySetFields(t *testing.T) {
	svc, _ := newService(t)
	ctx := t.Context()

	created, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1", Name: "Acme"})
	require.NoError(t, err)

	off := false
	updated, err := svc.Update(ctx, created.ID, UpdateTenantInput{Enabled: &off})
	require.NoError(t, err)
	assert.False(t, updated.Enabled)
	assert.Equal(t, "Acme", updated.Name)

	name := "Acme Inc"
	lim := tenant.Limits{Burst: 9}
	updated, err = svc.Update(ctx, created.ID, UpdateTenantInput{Name: &name, Limits: &lim})
	require.NoError(t, err)
	assert.Equal(t, "Acme Inc", updated.Name)
	assert.Equal(t, 9, updated.Limits.Burst)
	assert.False(t, updated.Enabled)

	_, err = svc.Update(ctx, id.NewTenantID(), UpdateTenantInput{Name: &name})
	require.ErrorIs(t, err, tether.ErrNotFound)
}

func TestResolveCachedAndEvictedOnUpdate(t *testing.T) {
	svc, cs := newService(t)
	ctx := t.Context()

	created, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1", Name: "Acme"})
	require.NoError(t, err)

	first, err := svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.Equal(t, created.ID, first.ID)

	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.EqualValues(t, 1, cs.byExternal.Load(), "second resolve is served from the cache")

	// Mutating the returned value must not corrupt the cache.
	first.Enabled = false
	again, err := svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.True(t, again.Enabled)

	off := false
	_, err = svc.Update(ctx, created.ID, UpdateTenantInput{Enabled: &off})
	require.NoError(t, err)

	after, err := svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.False(t, after.Enabled, "update evicts the cached tenant")
	assert.EqualValues(t, 2, cs.byExternal.Load())
}

func TestResolveExpiresAfterTTL(t *testing.T) {
	svc, cs := newService(t)
	ctx := t.Context()

	clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	svc.now = func() time.Time { return clock }

	_, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.NoError(t, err)

	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)

	clock = clock.Add(29 * time.Second)
	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.EqualValues(t, 1, cs.byExternal.Load())

	clock = clock.Add(2 * time.Second)
	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.EqualValues(t, 2, cs.byExternal.Load())
}

func TestResolveDoesNotCacheMisses(t *testing.T) {
	svc, cs := newService(t)
	ctx := t.Context()

	_, err := svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.ErrorIs(t, err, tether.ErrNotFound)

	_, err = svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.NoError(t, err)

	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)
	assert.EqualValues(t, 2, cs.byExternal.Load())
}

func TestResolveEvictedOnDelete(t *testing.T) {
	svc, _ := newService(t)
	ctx := t.Context()

	created, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.NoError(t, err)

	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.NoError(t, err)

	require.NoError(t, svc.Delete(ctx, created.ID))

	_, err = svc.ResolveTenant(ctx, "aapp_1", "aorg_1")
	require.ErrorIs(t, err, tether.ErrNotFound)
}

func TestDeleteRefusedWhileDevicesExist(t *testing.T) {
	svc, cs := newService(t)
	ctx := t.Context()

	created, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.NoError(t, err)

	dev := &device.Device{ID: id.NewDeviceID(), TenantID: created.ID, Name: "pump", KeyID: "akey_1"}
	require.NoError(t, cs.CreateDevice(ctx, dev))

	err = svc.Delete(ctx, created.ID)
	require.ErrorIs(t, err, tether.ErrConflict)

	_, err = svc.Get(ctx, created.ID)
	require.NoError(t, err, "tenant survives a refused delete")

	require.NoError(t, cs.DeleteDevice(ctx, created.ID, dev.ID))
	require.NoError(t, svc.Delete(ctx, created.ID))

	_, err = svc.Get(ctx, created.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)
}

func TestCreateRefusesAnExternalIDUsedUnderAnotherApp(t *testing.T) {
	svc, _ := newService(t)
	ctx := t.Context()

	_, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.NoError(t, err)

	// Keysmith and warden isolate by tenant ID alone, so the same external ID
	// under a second app would share keys and roles with the first.
	_, err = svc.Create(ctx, CreateTenantInput{AppID: "aapp_2", ExternalID: "aorg_1"})
	require.ErrorIs(t, err, tether.ErrConflict)

	_, err = svc.Create(ctx, CreateTenantInput{AppID: "aapp_2", ExternalID: "aorg_2"})
	require.NoError(t, err)

	list, err := svc.List(ctx, tenant.ListFilter{})
	require.NoError(t, err)
	assert.Len(t, list, 2)
}

func TestCreateFindsAnExternalIDPastTheFirstPage(t *testing.T) {
	svc, _ := newService(t)
	ctx := t.Context()

	for i := range 250 {
		_, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: fmt.Sprintf("aorg_%d", i)})
		require.NoError(t, err)
	}

	_, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_2", ExternalID: "aorg_249"})
	require.ErrorIs(t, err, tether.ErrConflict)
}

func TestConcurrentCreatesOfOneExternalIDMakeOneTenant(t *testing.T) {
	svc, _ := newService(t)

	var wg sync.WaitGroup

	errs := make(chan error, 8)

	for i := range 8 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := svc.Create(t.Context(), CreateTenantInput{AppID: fmt.Sprintf("aapp_%d", i), ExternalID: "aorg_1"})
			errs <- err
		}()
	}

	wg.Wait()
	close(errs)

	ok := 0

	for err := range errs {
		if err == nil {
			ok++
		} else {
			require.ErrorIs(t, err, tether.ErrConflict)
		}
	}

	assert.Equal(t, 1, ok)
}

// The evict hook hears every eviction the resolve cache makes: Update, Delete
// and Evict (a cell's tenant.updated), so a cache kept beside the service
// (the extension's metering and audit lookups) is dropped on the same path.
func TestEvictHookHearsEveryEviction(t *testing.T) {
	m := memory.New()
	t.Cleanup(func() { _ = m.Close() })

	var (
		mu    sync.Mutex
		heard []id.TenantID
	)

	svc := NewTenants(m, WithEvictHook(func(tid id.TenantID) {
		mu.Lock()
		heard = append(heard, tid)
		mu.Unlock()
	}))

	ctx := t.Context()

	created, err := svc.Create(ctx, CreateTenantInput{AppID: "aapp_1", ExternalID: "aorg_1"})
	require.NoError(t, err)

	count := func() int {
		mu.Lock()
		defer mu.Unlock()

		for _, h := range heard {
			require.Equal(t, created.ID, h)
		}

		return len(heard)
	}

	require.Zero(t, count(), "Create evicts nothing")

	name := "renamed"
	_, err = svc.Update(ctx, created.ID, UpdateTenantInput{Name: &name})
	require.NoError(t, err)

	afterUpdate := count()
	require.Positive(t, afterUpdate, "Update")

	svc.Evict(created.ID)
	require.Equal(t, afterUpdate+1, count(), "Evict")

	require.NoError(t, svc.Delete(ctx, created.ID))
	require.Equal(t, afterUpdate+2, count(), "Delete")
}
