package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

// Migrations is the grove migration group for the Tether store (PostgreSQL).
// Store.Migrate runs it, and a host that manages migrations through the grove
// extension can register it there instead.
//
// The id columns are COLLATE "C". Lists order by created_at and then id, and
// the memory store and the conformance suite break ties with Go's byte-wise
// string comparison. A database default such as en_US.UTF-8 sorts the same
// strings differently, so the collation is pinned on the column and not left
// to the server.
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
    id          TEXT COLLATE "C" PRIMARY KEY,
    app_id      TEXT NOT NULL,
    external_id TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    limits      JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

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
    id         TEXT COLLATE "C" PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    name       TEXT NOT NULL DEFAULT '',
    key_id     TEXT NOT NULL UNIQUE,
    client_id  TEXT NOT NULL DEFAULT '',
    roles      JSONB NOT NULL DEFAULT 'null',
    metadata   JSONB NOT NULL DEFAULT 'null',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
