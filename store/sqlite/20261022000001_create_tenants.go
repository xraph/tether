package sqlite

import (
	"context"

	"github.com/xraph/grove/migrate"
)

func init() {
	Migrations.Add(
		func(ctx context.Context, db *migrate.DB) error {
			_, err := db.Exec(ctx, `
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
		func(ctx context.Context, db *migrate.DB) error {
			_, err := db.Exec(ctx, `DROP TABLE IF EXISTS tether_tenants`)

			return err
		},
	)
}
