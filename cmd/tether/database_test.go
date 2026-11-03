package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// pgSecret is a password the tests plant in a DSN and then look for in
	// every error, so a leak fails a test.
	pgSecret = "hunter2-do-not-log"

	// pgURL points at a port nothing listens on, so opening it fails fast.
	pgURL = "postgres://tether:" + pgSecret + "@127.0.0.1:1/tetherdb?sslmode=disable&connect_timeout=2"
)

// A config that predates the database block, with only sqlite_path, still
// loads as a SQLite config and passes validation.
func TestDatabaseOldConfigStaysSQLite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tether.yaml")
	require.NoError(t, os.WriteFile(path, []byte("server:\n  sqlite_path: /var/lib/tether/t.db\n"), 0o600))

	cfg, err := loadConfig(path, true)
	require.NoError(t, err)

	drv, err := cfg.Server.Database.driver()
	require.NoError(t, err)
	assert.Equal(t, driverSQLite, drv)
	assert.Equal(t, "/var/lib/tether/t.db", cfg.Server.SQLitePath)
	assert.Empty(t, cfg.Server.Database.DSN)
	require.NoError(t, applyDatabase(&cfg, ""))

	// The defaults are SQLite too.
	cfg = defaultConfig()
	require.NoError(t, applyDatabase(&cfg, ""))
	assert.Equal(t, driverSQLite, cfg.Server.Database.Driver)
}

// The database block parses, and the file keys come through.
func TestDatabaseBlockParses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tether.yaml")
	yaml := "server:\n  database:\n    driver: postgres\n    dsn: postgres://u:p@db.internal:5432/tether\n"
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))

	cfg, err := loadConfig(path, true)
	require.NoError(t, err)
	assert.Equal(t, "postgres", cfg.Server.Database.Driver)
	assert.Equal(t, "postgres://u:p@db.internal:5432/tether", cfg.Server.Database.DSN)
	assert.Equal(t, defaultSQLitePath, cfg.Server.SQLitePath, "the SQLite path keeps its default and is unused")
	require.NoError(t, applyDatabase(&cfg, ""))
}

func TestDatabaseValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*config)
		env     string
		wantErr string // empty: valid
	}{
		{"default sqlite", func(*config) {}, "", ""},
		{"empty driver is sqlite", func(c *config) { c.Server.Database.Driver = "" }, "", ""},
		{"driver case and spaces", func(c *config) {
			c.Server.Database.Driver = " SQLite "
		}, "", ""},
		{"unknown driver", func(c *config) { c.Server.Database.Driver = "mysql" }, "", `"mysql" is not supported`},
		{"sqlite with no path", func(c *config) { c.Server.SQLitePath = "" }, "", "server.sqlite_path is empty"},
		{"postgres needs a dsn", func(c *config) { c.Server.Database.Driver = "postgres" }, "", "no DSN"},
		{"postgres with a blank dsn", func(c *config) {
			c.Server.Database.Driver = "postgres"
			c.Server.Database.DSN = "  \n"
		}, "", "no DSN"},
		{"postgres with a url", func(c *config) {
			c.Server.Database.Driver = "Postgres"
			c.Server.Database.DSN = pgURL
		}, "", ""},
		{"postgres with key value pairs", func(c *config) {
			c.Server.Database.Driver = "postgres"
			c.Server.Database.DSN = "host=db user=tether password=" + pgSecret + " dbname=tether"
		}, "", ""},
		{"postgres from the environment", func(c *config) { c.Server.Database.Driver = "postgres" }, pgURL, ""},
		{"postgres with a bad dsn", func(c *config) {
			c.Server.Database.Driver = "postgres"
			c.Server.Database.DSN = "postgres://tether:" + pgSecret + "@127.0.0.1:notaport/tetherdb"
		}, "", "does not parse"},
		{"sqlite with a dsn in the file", func(c *config) { c.Server.Database.DSN = pgURL }, "", "driver is sqlite"},
		{"sqlite with a dsn in the environment", func(*config) {}, pgURL, "driver is sqlite"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultConfig()
			tc.mutate(&cfg)

			err := applyDatabase(&cfg, tc.env)
			if tc.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.NotContains(t, err.Error(), pgSecret, "an error must never quote the DSN")
		})
	}
}

// TETHER_DATABASE_DSN beats the file, and the whitespace a secret file leaves
// at the end of a value does not reach the driver.
func TestDatabaseEnvDSNWins(t *testing.T) {
	cfg := defaultConfig()
	cfg.Server.Database.Driver = driverPostgres
	cfg.Server.Database.DSN = "postgres://file@filehost/db"

	require.NoError(t, applyDatabase(&cfg, "  postgres://env@envhost/db\n"))
	assert.Equal(t, "postgres://env@envhost/db", cfg.Server.Database.DSN)

	// An unset variable leaves the file's value alone.
	require.NoError(t, applyDatabase(&cfg, ""))
	assert.Equal(t, "postgres://env@envhost/db", cfg.Server.Database.DSN)

	cfg.Server.Database.DSN = "postgres://file@filehost/db"
	require.NoError(t, applyDatabase(&cfg, ""))
	assert.Equal(t, "postgres://file@filehost/db", cfg.Server.Database.DSN)
}

// What a log or an error may show of a DSN is the user, host, port and
// database, whichever syntax it came in.
func TestDescribeDSNHidesTheSecrets(t *testing.T) {
	for _, dsn := range []string{
		pgURL,
		"postgres://tether:" + pgSecret + "@127.0.0.1:1/tetherdb?sslpassword=" + pgSecret,
		"host=127.0.0.1 port=1 user=tether password=" + pgSecret + " dbname=tetherdb sslpassword=" + pgSecret,
	} {
		got := describeDSN(dsn)
		assert.Equal(t, "tether@127.0.0.1:1/tetherdb", got)
		assert.NotContains(t, got, pgSecret)
	}

	assert.Equal(t, "(unparseable DSN)", describeDSN("postgres://u:"+pgSecret+"@h:notaport/d"))
}

// A server that is down fails startup with the host in the message and the
// password nowhere in it.
func TestOpenPostgresErrorHidesThePassword(t *testing.T) {
	cfg := testConfig(t)
	cfg.Server.Database = databaseConfig{Driver: driverPostgres, DSN: pgURL}

	_, _, err := newApp(cfg, testToken, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tether@127.0.0.1:1/tetherdb")
	assert.NotContains(t, err.Error(), pgSecret)
}

// run refuses a PostgreSQL config with no DSN before it opens or starts
// anything, and the DSN variable reaches the check.
func TestRunPostgresConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tether.yaml")
	require.NoError(t, os.WriteFile(path, []byte("server:\n  database:\n    driver: postgres\n"), 0o600))

	env := map[string]string{adminTokenEnv: testToken}
	getenv := func(k string) string { return env[k] }

	err := run([]string{"--config", path}, getenv, io.Discard, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), databaseDSNEnv)

	// With the variable set the config passes the check and the run goes on
	// to open the database, which is down, so it fails on that and says where.
	env[databaseDSNEnv] = pgURL

	err = run([]string{"--config", path}, getenv, io.Discard, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect to postgres tether@127.0.0.1:1/tetherdb")
	assert.NotContains(t, err.Error(), pgSecret)

	// A SQLite config with the variable set is refused rather than run on a
	// local file.
	require.NoError(t, os.WriteFile(path, []byte("server:\n  sqlite_path: "+filepath.Join(dir, "t.db")+"\n"), 0o600))

	err = run([]string{"--config", path}, getenv, io.Discard, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "driver is sqlite")
	assert.NoFileExists(t, filepath.Join(dir, "t.db"))
}

// A PostgreSQL node keeps nothing of the database on disk, so it does not
// create the SQLite file's directory.
func TestPostgresSkipsTheSQLiteDir(t *testing.T) {
	cfg := testConfig(t)
	cfg.Server.SQLitePath = filepath.Join(t.TempDir(), "never", "tether.db")
	cfg.Server.Database = databaseConfig{Driver: driverPostgres, DSN: pgURL}

	_, _, err := newApp(cfg, testToken, false)
	require.Error(t, err) // the server is down

	assert.NoDirExists(t, filepath.Dir(cfg.Server.SQLitePath))
}
