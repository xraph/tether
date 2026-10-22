package sqlite

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Migrations is the grove migration group for the Tether store (SQLite). Store.Migrate
// runs it, and a host that manages migrations through the grove extension can
// register it there instead.
//
// Timestamps are INTEGER unix microseconds, not TEXT. modernc.org/sqlite
// writes a time.Time as a formatted string whose layout depends on the DSN,
// and ordering and equality on text only work while every writer agrees on
// that layout. An integer sorts and compares the same way whatever the DSN.
//
// There is deliberately no foreign key from tether_devices to tether_tenants:
// the store contract says CreateDevice does not check the tenant and
// DeleteTenant does not cascade, and the service layer enforces both.
var Migrations = newMigrations()

func newMigrations() *migrate.Group {
	g := migrate.NewGroup("tether")

	g.MustRegister(
		&migrate.Migration{
			Name:    "create_tenants",
			Version: "20261022000001",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
CREATE TABLE IF NOT EXISTS tether_tenants (
    id          TEXT PRIMARY KEY,
    app_id      TEXT NOT NULL,
    external_id TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    enabled     INTEGER NOT NULL DEFAULT 1,
    limits      TEXT NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,

    UNIQUE (app_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_tether_tenants_created ON tether_tenants (created_at, id);
`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS tether_tenants`)

				return err
			},
		},
		&migrate.Migration{
			Name:    "create_devices",
			Version: "20261022000002",
			Up: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `
CREATE TABLE IF NOT EXISTS tether_devices (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    name       TEXT NOT NULL DEFAULT '',
    key_id     TEXT NOT NULL UNIQUE,
    client_id  TEXT NOT NULL DEFAULT '',
    roles      TEXT NOT NULL DEFAULT 'null',
    metadata   TEXT NOT NULL DEFAULT 'null',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tether_devices_tenant ON tether_devices (tenant_id, created_at, id);
`)

				return err
			},
			Down: func(ctx context.Context, exec migrate.Executor) error {
				_, err := exec.Exec(ctx, `DROP TABLE IF EXISTS tether_devices`)

				return err
			},
		},
	)

	return g
}
