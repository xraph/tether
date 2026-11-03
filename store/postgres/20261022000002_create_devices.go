package postgres

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.Add(
		func(ctx context.Context, db *migrate.DB) error {
			_, err := db.Exec(ctx, `
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
		func(ctx context.Context, db *migrate.DB) error {
			_, err := db.Exec(ctx, `DROP TABLE IF EXISTS tether_devices`)

			return err
		},
	)
}
