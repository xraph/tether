// Package postgres implements store.Store on PostgreSQL through Grove.
//
// Open a pgdriver.PgDB, wrap it with grove.Open, and hand the grove.DB to New.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	"github.com/xraph/grove/migrate"

	// Migrate needs the postgres migration executor registered, and a host
	// that only imports this package should not have to know that.
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/tenant"
)

var _ store.Store = (*Store)(nil)

// uniqueViolation is the SQLSTATE for a unique or primary key violation.
const uniqueViolation = "23505"

// Store is a PostgreSQL-backed store.Store. It is safe for concurrent use.
type Store struct {
	db *grove.DB
	pg *pgdriver.PgDB
}

// New returns a Store on db, which must be opened with the pgdriver. Call
// Migrate before first use.
func New(db *grove.DB) *Store {
	return &Store{db: db, pg: pgdriver.Unwrap(db)}
}

// DB returns the underlying grove database.
func (s *Store) DB() *grove.DB { return s.db }

func wrapErr(op string, err error) error {
	return fmt.Errorf("tether/postgres: %s: %w", op, err)
}

func opErr(op string, sentinel error, detail string) error {
	if detail == "" {
		return fmt.Errorf("tether/postgres: %s: %w", op, sentinel)
	}

	return fmt.Errorf("tether/postgres: %s: %s: %w", op, detail, sentinel)
}

func invalidErr(op string, err error) error {
	return opErr(op, tether.ErrInvalidInput, err.Error())
}

func isNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }

// isConflict reports whether err is a unique or primary key violation.
func isConflict(err error) bool {
	var pe *pgconn.PgError

	return errors.As(err, &pe) && pe.Code == uniqueViolation
}

func now() time.Time { return normalize(time.Now()) }

// stamp returns t normalised, or the current time when t is zero.
func stamp(t, fallback time.Time) time.Time {
	if t.IsZero() {
		return fallback
	}

	return normalize(t)
}

// ── Lifecycle ───────────────────────────────────────────

// Migrate creates or upgrades the schema with the grove orchestrator. It is
// safe to call repeatedly.
func (s *Store) Migrate(ctx context.Context) error {
	executor, err := migrate.NewExecutorFor(s.pg)
	if err != nil {
		return wrapErr("create migration executor", err)
	}

	if _, err := migrate.NewOrchestrator(executor, Migrations).Migrate(ctx); err != nil {
		return wrapErr("migrate", err)
	}

	return nil
}

// Ping checks that the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.db.Ping(ctx); err != nil {
		return wrapErr("ping", err)
	}

	return nil
}

// Close closes the database.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return wrapErr("close", err)
	}

	return nil
}

// ── Tenants ─────────────────────────────────────────────

// CreateTenant implements tenant.Store.
func (s *Store) CreateTenant(ctx context.Context, t *tenant.Tenant) error {
	const op = "create tenant"

	if err := t.Validate(); err != nil {
		return invalidErr(op, err)
	}

	ts := now()
	created, updated := stamp(t.CreatedAt, ts), stamp(t.UpdatedAt, ts)

	m, err := tenantToModel(t, created, updated)
	if err != nil {
		return invalidErr(op, err)
	}

	if _, err := s.pg.NewInsert(m).Exec(ctx); err != nil {
		if isConflict(err) {
			return opErr(op, tether.ErrConflict, "tenant ID, or app and external ID, already exists")
		}

		return wrapErr(op, err)
	}

	t.CreatedAt, t.UpdatedAt = created, updated

	return nil
}

// GetTenant implements tenant.Store.
func (s *Store) GetTenant(ctx context.Context, tenantID id.TenantID) (*tenant.Tenant, error) {
	const op = "get tenant"

	m := new(tenantModel)

	if err := s.pg.NewSelect(m).Where("id = ?", tenantID.String()).Scan(ctx); err != nil {
		if isNoRows(err) {
			return nil, opErr(op, tether.ErrNotFound, tenantID.String())
		}

		return nil, wrapErr(op, err)
	}

	return decode(op, m, tenantFromModel)
}

// GetTenantByExternal implements tenant.Store.
func (s *Store) GetTenantByExternal(ctx context.Context, appID, externalID string) (*tenant.Tenant, error) {
	const op = "get tenant by external"

	m := new(tenantModel)

	err := s.pg.NewSelect(m).
		Where("app_id = ?", appID).
		Where("external_id = ?", externalID).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, opErr(op, tether.ErrNotFound, "")
		}

		return nil, wrapErr(op, err)
	}

	return decode(op, m, tenantFromModel)
}

// UpdateTenant implements tenant.Store.
func (s *Store) UpdateTenant(ctx context.Context, t *tenant.Tenant) error {
	const op = "update tenant"

	if err := t.Validate(); err != nil {
		return invalidErr(op, err)
	}

	updated := now()

	m, err := tenantToModel(t, time.Time{}, updated)
	if err != nil {
		return invalidErr(op, err)
	}

	// created_at is left out, so the stored value stays, and RETURNING hands
	// it back for the caller's struct. updated_at moves forward with every
	// write, even past a clock behind the last writer's (tenant.Store).
	var created, stamped time.Time

	err = s.pg.NewUpdate(m).
		Set("app_id = ?", m.AppID).
		Set("external_id = ?", m.ExternalID).
		Set("name = ?", m.Name).
		Set("enabled = ?", m.Enabled).
		Set("limits = ?", m.Limits).
		Set("updated_at = GREATEST(?, updated_at + interval '1 microsecond')", m.UpdatedAt).
		Where("id = ?", m.ID).
		Returning("created_at", "updated_at").
		Scan(ctx, &created, &stamped)
	if err != nil {
		switch {
		case isNoRows(err):
			return opErr(op, tether.ErrNotFound, m.ID)
		case isConflict(err):
			return opErr(op, tether.ErrConflict, "app and external ID are already mapped")
		default:
			return wrapErr(op, err)
		}
	}

	t.CreatedAt, t.UpdatedAt = created.UTC(), stamped.UTC()

	return nil
}

// DeleteTenant implements tenant.Store. It does not cascade to devices.
func (s *Store) DeleteTenant(ctx context.Context, tenantID id.TenantID) error {
	const op = "delete tenant"

	res, err := s.pg.NewDelete((*tenantModel)(nil)).Where("id = ?", tenantID.String()).Exec(ctx)
	if err != nil {
		return wrapErr(op, err)
	}

	return requireAffected(op, res, tenantID.String())
}

// ListTenants implements tenant.Store, ordered by CreatedAt ascending, then ID.
func (s *Store) ListTenants(ctx context.Context, f tenant.ListFilter) ([]*tenant.Tenant, error) {
	const op = "list tenants"

	if err := f.Validate(); err != nil {
		return nil, invalidErr(op, err)
	}

	var models []tenantModel

	q := page(s.pg.NewSelect(&models).OrderExpr(`created_at, id COLLATE "C"`), f.Limit, f.Offset)
	if err := q.Scan(ctx); err != nil && !isNoRows(err) {
		return nil, wrapErr(op, err)
	}

	out := make([]*tenant.Tenant, 0, len(models))

	for i := range models {
		t, err := decode(op, &models[i], tenantFromModel)
		if err != nil {
			return nil, err
		}

		out = append(out, t)
	}

	return out, nil
}

// ── Devices ─────────────────────────────────────────────

// CreateDevice implements device.Store.
func (s *Store) CreateDevice(ctx context.Context, d *device.Device) error {
	const op = "create device"

	if err := d.Validate(); err != nil {
		return invalidErr(op, err)
	}

	ts := now()
	created, updated := stamp(d.CreatedAt, ts), stamp(d.UpdatedAt, ts)

	m, err := deviceToModel(d, created, updated)
	if err != nil {
		return invalidErr(op, err)
	}

	if _, err := s.pg.NewInsert(m).Exec(ctx); err != nil {
		if isConflict(err) {
			return opErr(op, tether.ErrConflict, "device ID or key ID already exists")
		}

		return wrapErr(op, err)
	}

	d.CreatedAt, d.UpdatedAt = created, updated

	return nil
}

// GetDevice implements device.Store.
func (s *Store) GetDevice(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) (*device.Device, error) {
	const op = "get device"

	m := new(deviceModel)

	err := s.pg.NewSelect(m).
		Where("id = ?", deviceID.String()).
		Where("tenant_id = ?", tenantID.String()).
		Scan(ctx)
	if err != nil {
		if isNoRows(err) {
			return nil, opErr(op, tether.ErrNotFound, deviceID.String())
		}

		return nil, wrapErr(op, err)
	}

	return decode(op, m, deviceFromModel)
}

// GetDeviceByKey implements device.Store.
func (s *Store) GetDeviceByKey(ctx context.Context, keyID string) (*device.Device, error) {
	const op = "get device by key"

	m := new(deviceModel)

	if err := s.pg.NewSelect(m).Where("key_id = ?", keyID).Scan(ctx); err != nil {
		if isNoRows(err) {
			return nil, opErr(op, tether.ErrNotFound, "")
		}

		return nil, wrapErr(op, err)
	}

	return decode(op, m, deviceFromModel)
}

// UpdateDevice implements device.Store.
func (s *Store) UpdateDevice(ctx context.Context, d *device.Device) error {
	const op = "update device"

	if err := d.Validate(); err != nil {
		return invalidErr(op, err)
	}

	updated := now()

	m, err := deviceToModel(d, time.Time{}, updated)
	if err != nil {
		return invalidErr(op, err)
	}

	// The tenant is part of the match, so a device under another tenant is
	// not found. tenant_id and created_at are not in the column list.
	var created time.Time

	err = s.pg.NewUpdate(m).
		Column("name", "key_id", "client_id", "roles", "metadata", "updated_at").
		Where("id = ?", m.ID).
		Where("tenant_id = ?", m.TenantID).
		Returning("created_at").
		Scan(ctx, &created)
	if err != nil {
		switch {
		case isNoRows(err):
			return opErr(op, tether.ErrNotFound, m.ID)
		case isConflict(err):
			return opErr(op, tether.ErrConflict, "key ID is already assigned")
		default:
			return wrapErr(op, err)
		}
	}

	d.CreatedAt, d.UpdatedAt = created.UTC(), updated

	return nil
}

// DeleteDevice implements device.Store.
func (s *Store) DeleteDevice(ctx context.Context, tenantID id.TenantID, deviceID id.DeviceID) error {
	const op = "delete device"

	res, err := s.pg.NewDelete((*deviceModel)(nil)).
		Where("id = ?", deviceID.String()).
		Where("tenant_id = ?", tenantID.String()).
		Exec(ctx)
	if err != nil {
		return wrapErr(op, err)
	}

	return requireAffected(op, res, deviceID.String())
}

// ListDevices implements device.Store, ordered by CreatedAt ascending, then ID.
func (s *Store) ListDevices(ctx context.Context, f device.ListFilter) ([]*device.Device, error) {
	const op = "list devices"

	if err := f.Validate(); err != nil {
		return nil, invalidErr(op, err)
	}

	var models []deviceModel

	q := s.pg.NewSelect(&models).
		Where("tenant_id = ?", f.TenantID.String()).
		OrderExpr(`created_at, id COLLATE "C"`)

	if err := page(q, f.Limit, f.Offset).Scan(ctx); err != nil && !isNoRows(err) {
		return nil, wrapErr(op, err)
	}

	out := make([]*device.Device, 0, len(models))

	for i := range models {
		d, err := decode(op, &models[i], deviceFromModel)
		if err != nil {
			return nil, err
		}

		out = append(out, d)
	}

	return out, nil
}

// CountDevices implements device.Store.
func (s *Store) CountDevices(ctx context.Context, tenantID id.TenantID) (int, error) {
	const op = "count devices"

	if tenantID.IsNil() {
		return 0, opErr(op, tether.ErrInvalidInput, "tenant ID is required")
	}

	n, err := s.pg.NewSelect((*deviceModel)(nil)).Where("tenant_id = ?", tenantID.String()).Count(ctx)
	if err != nil {
		return 0, wrapErr(op, err)
	}

	return int(n), nil
}

// ── Helpers ─────────────────────────────────────────────

// page applies limit and offset. Limit zero means no limit, and PostgreSQL
// accepts an OFFSET on its own.
func page(q *pgdriver.SelectQuery, limit, offset int) *pgdriver.SelectQuery {
	if limit > 0 {
		q = q.Limit(limit)
	}

	if offset > 0 {
		q = q.Offset(offset)
	}

	return q
}

// requireAffected maps a write that touched no row to tether.ErrNotFound.
func requireAffected(op string, res interface{ RowsAffected() (int64, error) }, what string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return wrapErr(op, err)
	}

	if n == 0 {
		return opErr(op, tether.ErrNotFound, what)
	}

	return nil
}

func decode[M, T any](op string, m *M, fn func(*M) (*T, error)) (*T, error) {
	v, err := fn(m)
	if err != nil {
		return nil, wrapErr(op+": decode row", err)
	}

	return v, nil
}
