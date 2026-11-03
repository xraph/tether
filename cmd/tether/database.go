package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/drivers/sqlitedriver"
	kspostgres "github.com/xraph/keysmith/store/postgres"

	"github.com/xraph/tether/store/sqlite"
)

const (
	// driverSQLite and driverPostgres are the values of server.database.driver.
	// driverPostgresAlias is accepted for driverPostgres, since libpq URLs
	// spell the scheme both ways.
	driverSQLite        = "sqlite"
	driverPostgres      = "postgres"
	driverPostgresAlias = "postgresql"

	// databaseDSNEnv names the environment variable that carries the
	// PostgreSQL connection string, so the password need not sit in the
	// config file. When both are set the variable wins.
	databaseDSNEnv = "TETHER_DATABASE_DSN"

	// openTimeout bounds opening the PostgreSQL pool, which includes the
	// first connection and the schema step. A DSN's own connect_timeout can
	// make the connection part shorter.
	openTimeout = 30 * time.Second

	// keysmithMigrationLock is the advisory lock that lets one node at a
	// time run keysmith's migration. It is the bytes of "tetherks".
	keysmithMigrationLock int64 = 0x7465746865726b73
)

// databaseConfig picks where tenants, devices, keys and roles live.
type databaseConfig struct {
	// Driver is "sqlite" (the default) or "postgres" ("postgresql" works
	// too). SQLite keeps its file at
	// server.sqlite_path, which is why that key stays where it was.
	Driver string `yaml:"driver"`

	// DSN is the PostgreSQL connection string, a postgres:// URL or
	// key=value pairs. TETHER_DATABASE_DSN overrides it. It holds the
	// password, so nothing logs it.
	DSN string `yaml:"dsn"`
}

// driver is the normalised driver name. An empty one is SQLite, so a config
// that predates the block keeps working.
func (d databaseConfig) driver() (string, error) {
	switch v := strings.ToLower(strings.TrimSpace(d.Driver)); v {
	case "", driverSQLite:
		return driverSQLite, nil
	case driverPostgres, driverPostgresAlias:
		return driverPostgres, nil
	default:
		return "", fmt.Errorf("tether: server.database.driver %q is not supported (want %s or %s)",
			d.Driver, driverSQLite, driverPostgres)
	}
}

// usesPostgres reports whether the settings pick PostgreSQL. It assumes
// validateDatabase passed, and an invalid driver reads as SQLite.
func (s serverConfig) usesPostgres() bool {
	drv, err := s.Database.driver()

	return err == nil && drv == driverPostgres
}

// dsn is the connection string without the whitespace a secret file or an
// environment variable tends to carry at its end.
func (d databaseConfig) dsn() string { return strings.TrimSpace(d.DSN) }

// applyDatabase lets TETHER_DATABASE_DSN override server.database.dsn, then
// checks the database settings. Nothing here opens a connection.
func applyDatabase(cfg *config, envDSN string) error {
	if v := strings.TrimSpace(envDSN); v != "" {
		cfg.Server.Database.DSN = v
	}

	return cfg.Server.validateDatabase()
}

// validateDatabase checks the settings for the driver they pick. A DSN beside
// the SQLite driver is an error, because the operator who set it almost
// certainly means to be on PostgreSQL, and silently running on a local file
// would split a cell's state across hosts.
func (s serverConfig) validateDatabase() error {
	drv, err := s.Database.driver()
	if err != nil {
		return err
	}

	switch drv {
	case driverPostgres:
		if s.Database.dsn() == "" {
			return errors.New("tether: server.database.driver is postgres but there is no DSN: " +
				"set server.database.dsn or " + databaseDSNEnv)
		}

		// This is the parser the driver uses, so a bad pool_* parameter
		// fails here too, and not inside Open.
		if _, err := pgxpool.ParseConfig(s.Database.dsn()); err != nil {
			return dsnError(err)
		}
	default:
		if s.SQLitePath == "" {
			return errors.New("tether: server.sqlite_path is empty")
		}

		if s.Database.dsn() != "" {
			return errors.New("tether: a database DSN is set (server.database.dsn or " + databaseDSNEnv + ") " +
				"but server.database.driver is " + driverSQLite + ": set it to postgres to use the DSN, or remove the DSN")
		}
	}

	return nil
}

// dsnError says why a DSN was refused without showing the parser's error.
// pgx quotes the connection string in it with the password masked, and the
// masking is best effort: a quoted password with an escaped quote in it
// comes out partly in clear. So the cause is classified by type and dropped.
//
// pgx also reads the files a DSN names (sslrootcert, sslcert, sslkey,
// servicefile), and a file that is missing or unreadable is not a parse
// error, so it gets its own message.
func dsnError(err error) error {
	var pathErr *fs.PathError
	if errors.As(err, &pathErr) {
		return errors.New("tether: a file the PostgreSQL DSN names (sslrootcert, sslcert, sslkey or servicefile) " +
			"could not be read: check the path and its permissions (the error is not shown, since it could quote the password)")
	}

	return errors.New("tether: the PostgreSQL DSN does not parse: " +
		"use a postgres:// URL or key=value pairs, and check any pool_* values " +
		"(the error is not shown, since it could quote the password)")
}

// openDB opens the grove database the settings name. It assumes
// validateDatabase passed.
func openDB(s serverConfig) (*grove.DB, error) {
	drv, err := s.Database.driver()
	if err != nil {
		return nil, err
	}

	if drv == driverPostgres {
		return openPostgres(s.Database.dsn())
	}

	return openSQLite(s.SQLitePath)
}

// openSQLite opens the grove SQLite database at path.
func openSQLite(path string) (*grove.DB, error) {
	drv := sqlitedriver.New()

	if err := drv.Open(context.Background(), sqlite.DSN(path)); err != nil {
		return nil, fmt.Errorf("tether: open sqlite %s: %w", path, err)
	}

	db, err := grove.Open(drv)
	if err != nil {
		return nil, fmt.Errorf("tether: open grove: %w", err)
	}

	return db, nil
}

// openPostgres opens the grove PostgreSQL pool on dsn and pings the server,
// so a wrong host or password fails startup here. Errors name the user, host and database but never the
// password.
func openPostgres(dsn string) (*grove.DB, error) {
	ctx, cancel := context.WithTimeout(context.Background(), openTimeout)
	defer cancel()

	drv := pgdriver.New()

	if err := drv.Open(ctx, dsn); err != nil {
		// A parse error carries the DSN, so it never goes out wrapped.
		// validateDatabase runs the same parser first, so this is the
		// backstop for a caller that skipped it.
		var parseErr *pgconn.ParseConfigError
		if errors.As(err, &parseErr) || errors.As(err, new(*fs.PathError)) {
			return nil, dsnError(err)
		}

		return nil, fmt.Errorf("tether: open postgres %s: %w", describeDSN(dsn), err)
	}

	db, err := grove.Open(drv)
	if err != nil {
		_ = drv.Close()

		return nil, fmt.Errorf("tether: open grove: %w", err)
	}

	// The pool connects lazily, so without this a server that is down shows
	// up as a migration error from whichever extension starts first.
	if err := db.Ping(ctx); err != nil {
		_ = db.Close()

		return nil, fmt.Errorf("tether: connect to postgres %s: %w", describeDSN(dsn), err)
	}

	if err := migrateKeysmith(ctx, db); err != nil {
		_ = db.Close()

		return nil, err
	}

	return db, nil
}

// migrateKeysmith creates keysmith's schema under an advisory lock, before
// the app starts. The keysmith extension's own migration is switched off on
// PostgreSQL (see newApp), so this is the only one.
//
// Tether's and warden's migrations go through grove's orchestrator, which
// takes its own lock, so nodes starting at once queue up. Keysmith's
// Store.Migrate is a list of plain statements with no lock, and it creates
// keysmith_usage_agg and drops it again on every run. Two nodes starting
// together can both try to create that table and one fails with a duplicate
// key on pg_type. That is not only a first-start problem: it happens on any
// start that overlaps another.
func migrateKeysmith(ctx context.Context, db *grove.DB) (err error) {
	pg := pgdriver.Unwrap(db)

	conn, err := pg.AcquireConn(ctx)
	if err != nil {
		return fmt.Errorf("tether: migrate keysmith: %w", err)
	}

	defer conn.Release()

	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", keysmithMigrationLock); err != nil {
		return fmt.Errorf("tether: migrate keysmith: take the lock: %w", err)
	}

	// The lock belongs to the session, and Release only returns the session
	// to the pool, so drop it here whatever happens. ctx may be the thing
	// that failed, so the unlock gets its own. Only this lock is dropped:
	// pg_advisory_unlock returns false when the session did not hold it,
	// which means something else released it, and that is an error too. If
	// the unlock fails, the pooled session may still hold the lock, so
	// startup fails and the caller closes the pool.
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var released bool

		uerr := conn.QueryRow(uctx, "SELECT pg_advisory_unlock($1)", keysmithMigrationLock).Scan(&released)

		switch {
		case err != nil:
			// The migration's own error is the one to report.
		case uerr != nil:
			err = fmt.Errorf("tether: migrate keysmith: release the lock: %w", uerr)
		case !released:
			err = errors.New("tether: migrate keysmith: release the lock: the session no longer held it")
		}
	}()

	if err = kspostgres.New(pg).Migrate(ctx); err != nil {
		return fmt.Errorf("tether: migrate keysmith: %w", err)
	}

	return nil
}

// describeDSN is the part of a connection string that is safe to show:
// user@host:port/database. It has no password and none of the query
// parameters (sslpassword and friends).
func describeDSN(dsn string) string {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "(unparseable DSN)"
	}

	return fmt.Sprintf("%s@%s:%d/%s", cfg.User, cfg.Host, cfg.Port, cfg.Database)
}
