package extension_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/sqlitedriver"
	groveext "github.com/xraph/grove/extension"
	"github.com/xraph/grove/migrate"
	keysmithext "github.com/xraph/keysmith/extension"
	wardenext "github.com/xraph/warden/extension"

	"github.com/xraph/vessel"

	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/store/sqlite"
)

var _ forge.MigratableExtension = (*extension.Extension)(nil)

func tetherGroup(t *testing.T, groups []*forge.MigrationGroupInfo) *forge.MigrationGroupInfo {
	t.Helper()

	for _, g := range groups {
		if g.Name == "tether" {
			return g
		}
	}

	require.FailNow(t, "no tether group", "groups: %v", groups)

	return nil
}

func names(ms []*forge.MigrationInfo) []string {
	out := make([]string, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Version+"_"+m.Name)
	}

	return out
}

var shipped = []string{"20261022000001_create_tenants", "20261022000002_create_devices"}

// Without central migrations Start migrates the store, and the forge
// migrate commands see Tether's group through MigratableExtension.
func TestMigrationStatusAfterStart(t *testing.T) {
	e := start(t, false)
	ctx := t.Context()

	groups, err := e.ext.MigrationStatus(ctx)
	require.NoError(t, err)

	g := tetherGroup(t, groups)
	assert.Equal(t, shipped, names(g.Applied))
	assert.Empty(t, g.Pending)

	res, err := e.ext.Migrate(ctx)
	require.NoError(t, err)
	assert.Zero(t, res.Applied)
}

// WithDisableMigrate leaves the schema to the host: Start migrates nothing,
// and Migrate refuses.
func TestDisableMigrateSkipsTheStartMigration(t *testing.T) {
	e := startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(),
		extension.WithBrokerConfig(loopback()), extension.WithDisableMigrate())
	ctx := t.Context()

	groups, err := e.ext.MigrationStatus(ctx)
	require.NoError(t, err)

	g := tetherGroup(t, groups)
	assert.Empty(t, g.Applied)
	assert.Equal(t, shipped, names(g.Pending))

	_, err = e.ext.Migrate(ctx)
	require.Error(t, err)
}

// On a database tether shares, grove's Rollback undoes the newest row
// whatever its group. When that row is someone else's, Rollback says whose
// and leaves everything in place.
func TestRollbackRefusesAnotherGroupsNewestMigration(t *testing.T) {
	e := start(t, false)
	ctx := t.Context()

	db, err := vessel.Inject[*grove.DB](e.app.Container())
	require.NoError(t, err)

	other := migrate.NewGroup("other")
	other.MustRegister(&migrate.Migration{
		Name:    "noop",
		Version: "20991231000000",
		Up:      func(context.Context, migrate.Executor) error { return nil },
	})

	exec, err := migrate.NewExecutorFor(db.Driver())
	require.NoError(t, err)

	_, err = migrate.NewOrchestrator(exec, other).Migrate(ctx)
	require.NoError(t, err)

	_, err = e.ext.Rollback(ctx)
	require.ErrorContains(t, err, `belongs to group "other", not "tether"`)

	groups, err := e.ext.MigrationStatus(ctx)
	require.NoError(t, err)
	assert.Equal(t, shipped, names(tetherGroup(t, groups).Applied))
}

// A store that is not on grove has no group to report.
func TestMemoryStoreHasNoMigrationGroup(t *testing.T) {
	e := startApp(t, newApp(t), false, extension.WithInsecureAdminRoutes(),
		extension.WithBrokerConfig(loopback()), extension.WithStore(memory.New()))

	groups, err := e.ext.MigrationStatus(t.Context())
	require.NoError(t, err)
	assert.Empty(t, groups)
}

// With central migrations on the app and the grove extension, Tether's group
// goes to grove's registry in Register and runs in the one pass forge makes
// before any Start. Status through the registry shows it applied, and
// Rollback points at the central migrator.
func TestCentralMigrationsRunTheTetherGroup(t *testing.T) {
	sdb := sqlitedriver.New()
	require.NoError(t, sdb.Open(t.Context(), sqlite.DSN(filepath.Join(t.TempDir(), "app.db"))))

	app := forge.New(
		forge.WithAppName("tether-test"),
		forge.WithAppLogger(forge.NewNoopLogger()),
		forge.WithEnableConfigAutoDiscovery(false),
		forge.WithCentralMigrations(),
	)

	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))

	require.NoError(t, boot(t, app,
		groveext.New(groveext.WithDriver(sdb), groveext.WithCentralMigrations(), groveext.WithDisableRoutes()),
		ext.Keysmith(keysmithext.WithDisableRoutes()),
		ext.Warden(wardenext.WithDisableRoutes()),
		ext,
	))

	cm, ok := app.CentralMigrator()
	require.True(t, ok, "grove provides the CentralMigrator")

	groups, err := cm.StatusAll(t.Context())
	require.NoError(t, err)

	g := tetherGroup(t, groups)
	assert.Equal(t, shipped, names(g.Applied))
	assert.Empty(t, g.Pending)

	res, err := ext.Migrate(t.Context())
	require.NoError(t, err)
	assert.Zero(t, res.Applied)

	_, err = ext.Rollback(t.Context())
	require.ErrorContains(t, err, "central migrator")
}
