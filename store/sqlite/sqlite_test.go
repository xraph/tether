package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/sqlite"
	"github.com/xraph/tether/store/storetest"
	"github.com/xraph/tether/tenant"
)

var _ store.Store = (*sqlite.Store)(nil)

// openStore opens a fresh database file per call. It is a file, not
// ":memory:", because every pooled connection to ":memory:" would get its own
// empty database.
func openStore(t *testing.T) *sqlite.Store {
	t.Helper()

	return openStoreAt(t, filepath.Join(t.TempDir(), "tether.db"))
}

// openStoreAt opens a store with its own pool on the file at path and closes
// it when the test ends.
func openStoreAt(t *testing.T, path string) *sqlite.Store {
	t.Helper()

	sdb := sqlitedriver.New()
	require.NoError(t, sdb.Open(t.Context(), sqlite.DSN(path)))

	db, err := grove.Open(sdb)
	require.NoError(t, err)

	s := sqlite.New(db)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		t.Helper()

		return openStore(t)
	})
}

// Writes from many goroutines wait for sqlite's one write lock instead of
// failing with SQLITE_BUSY, which is what the DSN's busy timeout and
// immediate transactions are for.
func TestConcurrentWrites(t *testing.T) {
	ctx := t.Context()
	s := openStore(t)
	require.NoError(t, s.Migrate(ctx))

	tn := &tenant.Tenant{ID: id.NewTenantID(), AppID: "app", ExternalID: "ext", Enabled: true}
	require.NoError(t, s.CreateTenant(ctx, tn))

	const writers = 24

	var wg sync.WaitGroup

	for i := range writers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			d := &device.Device{ID: id.NewDeviceID(), TenantID: tn.ID, KeyID: fmt.Sprintf("akey_%d", i)}
			assert.NoError(t, s.CreateDevice(ctx, d))

			d.Name = "renamed"
			assert.NoError(t, s.UpdateDevice(ctx, d))
		}()
	}

	wg.Wait()

	n, err := s.CountDevices(ctx, tn.ID)
	require.NoError(t, err)
	assert.Equal(t, writers, n)
}

// A caller-supplied time in another zone and with sub-microsecond digits is
// stored as the same instant at microsecond precision, in UTC, and the
// caller's struct is normalised to match what a read returns.
func TestTimestampsNormalised(t *testing.T) {
	ctx := t.Context()
	s := openStore(t)
	require.NoError(t, s.Migrate(ctx))

	zone := time.FixedZone("UTC+5", 5*60*60)
	at := time.Date(2026, 3, 4, 5, 6, 7, 123456789, zone)
	want := time.Date(2026, 3, 4, 0, 6, 7, 123456000, time.UTC)

	tn := &tenant.Tenant{
		ID: id.NewTenantID(), AppID: "app", ExternalID: "ext",
		CreatedAt: at, UpdatedAt: at,
	}
	require.NoError(t, s.CreateTenant(ctx, tn))
	assert.Equal(t, want, tn.CreatedAt)
	assert.Equal(t, time.UTC, tn.CreatedAt.Location())

	got, err := s.GetTenant(ctx, tn.ID)
	require.NoError(t, err)
	assert.Equal(t, want, got.CreatedAt)
	assert.Equal(t, want, got.UpdatedAt)
	assert.Equal(t, time.UTC, got.CreatedAt.Location())

	d := &device.Device{
		ID: id.NewDeviceID(), TenantID: tn.ID, KeyID: "akey_tz",
		CreatedAt: at, UpdatedAt: at,
	}
	require.NoError(t, s.CreateDevice(ctx, d))
	assert.Equal(t, want, d.CreatedAt)

	gotDev, err := s.GetDevice(ctx, tn.ID, d.ID)
	require.NoError(t, err)
	assert.Equal(t, want, gotDev.CreatedAt)
}

// Nil and empty collections keep their difference, and values that JSON
// cannot encode are rejected as invalid input before anything is stored.
func TestJSONColumns(t *testing.T) {
	ctx := t.Context()
	s := openStore(t)
	require.NoError(t, s.Migrate(ctx))

	tn := &tenant.Tenant{ID: id.NewTenantID(), AppID: "app", ExternalID: "ext"}
	require.NoError(t, s.CreateTenant(ctx, tn))

	nilCols := &device.Device{ID: id.NewDeviceID(), TenantID: tn.ID, KeyID: "akey_nil"}
	require.NoError(t, s.CreateDevice(ctx, nilCols))

	got, err := s.GetDevice(ctx, tn.ID, nilCols.ID)
	require.NoError(t, err)
	assert.Nil(t, got.Roles)
	assert.Nil(t, got.Metadata)

	emptyCols := &device.Device{
		ID: id.NewDeviceID(), TenantID: tn.ID, KeyID: "akey_empty",
		Roles: []string{}, Metadata: map[string]any{},
	}
	require.NoError(t, s.CreateDevice(ctx, emptyCols))

	got, err = s.GetDevice(ctx, tn.ID, emptyCols.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{}, got.Roles)
	assert.Equal(t, map[string]any{}, got.Metadata)

	bad := &device.Device{
		ID: id.NewDeviceID(), TenantID: tn.ID, KeyID: "akey_bad",
		Metadata: map[string]any{"ch": make(chan int)},
	}
	err = s.CreateDevice(ctx, bad)
	require.Error(t, err)
	assert.ErrorContains(t, err, "metadata")

	_, err = s.GetDeviceByKey(ctx, "akey_bad")
	require.Error(t, err, "a rejected create stores nothing")
}

func TestPingAfterClose(t *testing.T) {
	s := openStore(t)
	require.NoError(t, s.Ping(t.Context()))
	require.NoError(t, s.Close())
	require.Error(t, s.Ping(t.Context()))
}

func TestCanceledContext(t *testing.T) {
	s := openStore(t)
	require.NoError(t, s.Migrate(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := s.GetTenant(ctx, id.NewTenantID())
	require.ErrorIs(t, err, context.Canceled)
}
