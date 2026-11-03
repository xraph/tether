package sqlite

import "github.com/xraph/grove/migrate"

// Migrations is the grove migration group for the Tether store (SQLite).
// Store.Migrate runs it through grove's orchestrator, and a host that runs
// its migrations centrally (the grove extension's WithMigrations, or the
// shared MigrationRegistry) can hand it the group instead.
//
// Each migration lives in its own file named <version>_<name>.go and joins
// the group with Migrations.Add, which reads the version and the name from
// that file name. Never rename one of those files: the orchestrator knows a
// migration by its group and version, and a database that already recorded
// 20261022000001 must find it under that version again.
//
// Timestamps are INTEGER unix microseconds, not TEXT. modernc.org/sqlite
// writes a time.Time as a formatted string whose layout depends on the DSN,
// and ordering and equality on text only work while every writer agrees on
// that layout. An integer sorts and compares the same way whatever the DSN.
//
// There is deliberately no foreign key from tether_devices to tether_tenants:
// the store contract says CreateDevice does not check the tenant and
// DeleteTenant does not cascade, and the service layer enforces both.
//
// The SQL drivers have no schema builders yet, so the migrations are raw SQL
// through db.Exec.
var Migrations = migrate.NewGroup("tether")
