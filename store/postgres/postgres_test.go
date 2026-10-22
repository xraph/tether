package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/postgres"
	"github.com/xraph/tether/store/storetest"
	"github.com/xraph/tether/tenant"
)

var _ store.Store = (*postgres.Store)(nil)

// openStore creates a throwaway database on the server TETHER_TEST_POSTGRES
// points at, opens a store on it, and drops the database afterwards. The tests
// skip when the variable is unset. The role needs CREATEDB.
//
//	TETHER_TEST_POSTGRES=postgres://postgres:tether@127.0.0.1:5432/postgres?sslmode=disable
func openStore(t *testing.T) (*postgres.Store, *grove.DB) {
	t.Helper()

	dsn := os.Getenv("TETHER_TEST_POSTGRES")
	if dsn == "" {
		t.Skip("TETHER_TEST_POSTGRES is not set")
	}

	ctx := t.Context()

	admin := pgdriver.New()
	require.NoError(t, admin.Open(ctx, dsn))

	suffix := make([]byte, 6)
	_, err := rand.Read(suffix)
	require.NoError(t, err)

	name := "tether_" + hex.EncodeToString(suffix)
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	u.Path = "/" + name

	drv := pgdriver.New()
	require.NoError(t, drv.Open(ctx, u.String()))

	db, err := grove.Open(drv)
	require.NoError(t, err)

	s := postgres.New(db)

	t.Cleanup(func() {
		_ = s.Close()
		// t.Context() is already cancelled here, so use a fresh one.
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close()
	})

	return s, db
}

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		t.Helper()

		s, _ := openStore(t)

		return s
	})
}

func TestConcurrentWrites(t *testing.T) {
	s, _ := openStore(t)
	ctx := t.Context()
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

func TestTimestampsNormalised(t *testing.T) {
	s, _ := openStore(t)
	ctx := t.Context()
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
	assert.Equal(t, time.UTC, got.CreatedAt.Location(), "reads come back in UTC whatever the session zone is")

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

func TestJSONColumns(t *testing.T) {
	s, _ := openStore(t)
	ctx := t.Context()
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

// List order must be byte order on the ID, whatever the database's default
// collation is, because the conformance suite and the memory store break
// CreatedAt ties with Go's string comparison.
func TestIDColumnsUseByteOrder(t *testing.T) {
	s, db := openStore(t)
	ctx := t.Context()
	require.NoError(t, s.Migrate(ctx))

	pg := pgdriver.Unwrap(db)

	for _, table := range []string{"tether_tenants", "tether_devices"} {
		var collation string

		err := pg.NewRaw(
			`SELECT collation_name FROM information_schema.columns
			 WHERE table_name = $1 AND column_name = 'id'`, table,
		).Scan(ctx, &collation)
		require.NoError(t, err, table)
		assert.Equal(t, "C", collation, table)
	}
}

func TestPingAfterClose(t *testing.T) {
	s, _ := openStore(t)
	require.NoError(t, s.Ping(t.Context()))
	require.NoError(t, s.Close())
	require.Error(t, s.Ping(t.Context()))
}

func TestCanceledContext(t *testing.T) {
	s, _ := openStore(t)
	require.NoError(t, s.Migrate(t.Context()))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := s.GetTenant(ctx, id.NewTenantID())
	require.ErrorIs(t, err, context.Canceled)
}
