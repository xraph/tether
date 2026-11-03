package sqlite_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	"github.com/xraph/grove/migrate"

	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store/sqlite"
	"github.com/xraph/tether/tenant"
)

// shippedMigrations is every migration a released database may have
// recorded, as group/version/name. The orchestrator matches on group and
// version, so these must never change.
var shippedMigrations = []string{
	"tether/20261022000001/create_tenants",
	"tether/20261022000002/create_devices",
}

func migrationIDs(g *migrate.Group) []string {
	ms := g.Migrations()

	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Group+"/"+m.Version+"/"+m.Name)
	}

	return out
}

// The per-file group carries the versions and names the struct literals
// had, so a database migrated before the move sees them as applied.
func TestMigrationsKeepTheirVersionsAndNames(t *testing.T) {
	assert.Equal(t, "tether", sqlite.Migrations.Name())
	assert.Equal(t, shippedMigrations, migrationIDs(legacyMigrations()), "the legacy copy drifted from bd522ed")
	assert.Equal(t, shippedMigrations, migrationIDs(sqlite.Migrations))

	for _, m := range sqlite.Migrations.Migrations() {
		assert.NotNil(t, m.Up, m.Name)
		assert.NotNil(t, m.Down, m.Name)
		assert.True(t, m.Transactional, "%s asks for a transaction (sqlitemigrate has none, so it runs plain)", m.Name)
	}
}

func newExecutor(t *testing.T, db *grove.DB) migrate.Executor {
	t.Helper()

	exec, err := migrate.NewExecutorFor(sqlitedriver.Unwrap(db))
	require.NoError(t, err)

	return exec
}

// appliedRows is everything grove_migrations holds, with the time each row
// was written, so a migration that ran again would show up as a new row or
// a new time.
func appliedRows(t *testing.T, db *grove.DB) []string {
	t.Helper()

	applied, err := newExecutor(t, db).ListApplied(t.Context())
	require.NoError(t, err)

	out := make([]string, 0, len(applied))
	for _, a := range applied {
		out = append(out, a.Group+"/"+a.Version+"/"+a.Name+"@"+a.MigratedAt)
	}

	return out
}

// A database the bd522ed code migrated, with data in it, needs nothing from
// the per-file group: no migration applies and no row in grove_migrations
// changes.
func TestMigrateOnALegacyDatabaseIsANoOp(t *testing.T) {
	ctx := t.Context()
	s := openStore(t)
	db := s.DB()

	res, err := migrate.NewOrchestrator(newExecutor(t, db), legacyMigrations()).Migrate(ctx)
	require.NoError(t, err)
	require.Len(t, res.Applied, len(shippedMigrations))

	tn := &tenant.Tenant{ID: id.NewTenantID(), AppID: "app", ExternalID: "org", Name: "Legacy", Enabled: true}
	require.NoError(t, s.CreateTenant(ctx, tn))

	before := appliedRows(t, db)
	require.Len(t, before, len(shippedMigrations))

	res, err = migrate.NewOrchestrator(newExecutor(t, db), sqlite.Migrations).Migrate(ctx)
	require.NoError(t, err)
	assert.Empty(t, res.Applied)

	require.NoError(t, s.Migrate(ctx))
	assert.Equal(t, before, appliedRows(t, db))

	got, err := s.GetTenant(ctx, tn.ID)
	require.NoError(t, err)
	assert.Equal(t, "Legacy", got.Name)
}

// Several pools on one fresh file, migrating at once, take turns on grove's
// lock row: all succeed and each migration is recorded once.
func TestConcurrentMigrateOnAFreshDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tether.db")

	const pools = 4

	stores := make([]*sqlite.Store, pools)
	for i := range stores {
		stores[i] = openStoreAt(t, path)
	}

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make([]error, pools)
	)

	for i, s := range stores {
		wg.Go(func() {
			<-start

			errs[i] = s.Migrate(t.Context())
		})
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "pool %d", i)
	}

	rows := appliedRows(t, stores[0].DB())
	require.Len(t, rows, len(shippedMigrations))

	for i, want := range shippedMigrations {
		assert.Contains(t, rows[i], want+"@")
	}
}
