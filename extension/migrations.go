package extension

import (
	"context"
	"errors"
	"fmt"

	"github.com/xraph/forge"
	"github.com/xraph/grove"
	groveext "github.com/xraph/grove/extension"
	"github.com/xraph/grove/migrate"
	"github.com/xraph/vessel"

	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/postgres"
	"github.com/xraph/tether/store/sqlite"
)

var _ forge.MigratableExtension = (*Extension)(nil)

// errMigrateBeforeRegister is what the migration methods return on an
// extension the app has not registered, since the store comes from the app's
// container.
var errMigrateBeforeRegister = errors.New("tether: migrations need the extension registered with an app first")

// errCentralRollback is what Rollback returns once the extension has handed
// its group to grove's MigrationRegistry: the registry owns it then, and
// forge's migrate down goes through the registry.
var errCentralRollback = errors.New("tether: the migration group was contributed to the central migrator; " +
	"roll it back through forge's migrate down, which uses the CentralMigrator")

// migrationGroup returns the grove group and database behind st, or false
// for a store that is not on grove (memory, or one of the host's own).
func migrationGroup(st store.Store) (*migrate.Group, *grove.DB, bool) {
	switch s := st.(type) {
	case *postgres.Store:
		return postgres.Migrations, s.DB(), true
	case *sqlite.Store:
		return sqlite.Migrations, s.DB(), true
	default:
		return nil, nil, false
	}
}

// contribute hands Tether's migration group to grove's MigrationRegistry
// when the app runs central migrations, so it runs in the one ordered,
// locked pass forge makes between registering and starting extensions. It
// reports whether it did.
//
// It does nothing, and Start migrates the store itself as before, when any
// of these holds: the app has central migrations off, no registry is in the
// container (the grove extension creates it, and only with its own
// central_migrations on), the host passed WithStore (that store's database
// need not be the one the registry knows), the store is not on grove, or
// migrations are disabled. The grove database comes from the container the
// same way Start finds it, keyed by GroveDatabase as the grove extension
// keys named databases.
func (e *Extension) contribute(app forge.App) (bool, error) {
	if e.config.DisableMigrate || e.store != nil || !app.CentralMigrationsEnabled() {
		return false, nil
	}

	c := app.Container()
	if !vessel.HasType[*groveext.MigrationRegistry](c) {
		return false, nil
	}

	reg, err := vessel.Inject[*groveext.MigrationRegistry](c)
	if err != nil {
		return false, fmt.Errorf("tether: resolve the migration registry: %w", err)
	}

	st, err := e.resolveStore()
	if err != nil {
		// Start reports a missing database with the same error. Leaving the
		// group out of the registry here keeps Register from failing an app
		// that registers its database later than Tether.
		return false, nil
	}

	group, db, ok := migrationGroup(st)
	if !ok {
		return false, nil
	}

	reg.Contribute(e.config.GroveDatabase, db.Driver(), group)

	return true, nil
}

// orchestrator builds a grove orchestrator for Tether's group on the store's
// database, or returns nil when the store is not on grove. Each call gets its
// own executor, because the executor keeps the locked connection on itself.
func (e *Extension) orchestrator() (*migrate.Orchestrator, store.Store, error) {
	if e.App() == nil {
		return nil, nil, errMigrateBeforeRegister
	}

	st, err := e.resolveStore()
	if err != nil {
		return nil, nil, err
	}

	group, db, ok := migrationGroup(st)
	if !ok {
		return nil, st, nil
	}

	exec, err := migrate.NewExecutorFor(db.Driver())
	if err != nil {
		return nil, nil, fmt.Errorf("tether: create migration executor: %w", err)
	}

	return migrate.NewOrchestrator(exec, group), st, nil
}

// Migrate runs Tether's pending migrations under grove's lock. Forge's
// migrate up calls it. On a store that is not on grove it runs the store's
// own Migrate and reports nothing applied. Once the group went to the
// central migrator it does nothing, since that pass already ran it.
func (e *Extension) Migrate(ctx context.Context) (*forge.MigrationResult, error) {
	if e.config.DisableMigrate {
		return nil, errors.New("tether: migrations are disabled (disable_migrate)")
	}

	if e.contributed {
		return &forge.MigrationResult{}, nil
	}

	orch, st, err := e.orchestrator()
	if err != nil {
		return nil, err
	}

	if orch == nil {
		if err = st.Migrate(ctx); err != nil {
			return nil, fmt.Errorf("tether: migrate store: %w", err)
		}

		return &forge.MigrationResult{}, nil
	}

	r, err := orch.Migrate(ctx)
	if err != nil {
		return nil, fmt.Errorf("tether: migrate: %w", err)
	}

	res := &forge.MigrationResult{Applied: len(r.Applied)}
	for _, m := range r.Applied {
		res.Names = append(res.Names, m.Group+"/"+m.Name)
	}

	return res, nil
}

// Rollback rolls back the last applied migration, the way grove's own
// extension does: grove looks at the newest row in grove_migrations, so on a
// database other extensions share it fails when that row is not Tether's.
// Forge's migrate down calls it.
func (e *Extension) Rollback(ctx context.Context) (*forge.MigrationResult, error) {
	if e.config.DisableMigrate {
		return nil, errors.New("tether: migrations are disabled (disable_migrate)")
	}

	if e.contributed {
		return nil, errCentralRollback
	}

	orch, _, err := e.orchestrator()
	if err != nil {
		return nil, err
	}

	if orch == nil {
		return &forge.MigrationResult{}, nil
	}

	if cerr := e.checkNewestIsTethers(ctx); cerr != nil {
		return nil, cerr
	}

	r, err := orch.Rollback(ctx)
	if err != nil {
		return nil, fmt.Errorf("tether: rollback: %w", err)
	}

	res := &forge.MigrationResult{RolledBack: len(r.Rollback)}
	for _, m := range r.Rollback {
		res.Names = append(res.Names, m.Group+"/"+m.Name)
	}

	return res, nil
}

// checkNewestIsTethers refuses a rollback whose target grove would not find:
// grove undoes the newest row in grove_migrations whatever its group, and
// fails with a bare "not found in registered groups" when that row belongs
// to keysmith, warden or anyone else sharing the database. It reads without
// the lock, so it only improves the message; the orchestrator still checks
// under the lock. A database with no tracking table yet passes through.
func (e *Extension) checkNewestIsTethers(ctx context.Context) error {
	st, err := e.resolveStore()
	if err != nil {
		return err
	}

	group, db, ok := migrationGroup(st)
	if !ok {
		return nil
	}

	exec, err := migrate.NewExecutorFor(db.Driver())
	if err != nil {
		return fmt.Errorf("tether: create migration executor: %w", err)
	}

	applied, err := exec.ListApplied(ctx)
	if err != nil || len(applied) == 0 {
		// The orchestrator reports a real failure itself.
		return nil
	}

	if newest := applied[len(applied)-1]; newest.Group != group.Name() {
		return fmt.Errorf("tether: rollback: the newest migration, %s/%s_%s, belongs to group %q, not %q; "+
			"roll that group back first, or use a central migrator", newest.Group, newest.Version, newest.Name,
			newest.Group, group.Name())
	}

	return nil
}

// MigrationStatus reports Tether's group, applied and pending. A store that
// is not on grove has no group and reports none.
func (e *Extension) MigrationStatus(ctx context.Context) ([]*forge.MigrationGroupInfo, error) {
	orch, _, err := e.orchestrator()
	if err != nil {
		return nil, err
	}

	if orch == nil {
		return nil, nil
	}

	statuses, err := orch.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("tether: migration status: %w", err)
	}

	out := make([]*forge.MigrationGroupInfo, 0, len(statuses))
	for _, gs := range statuses {
		info := &forge.MigrationGroupInfo{Name: gs.Name}
		for _, ms := range gs.Applied {
			info.Applied = append(info.Applied, migrationInfo(ms))
		}

		for _, ms := range gs.Pending {
			info.Pending = append(info.Pending, migrationInfo(ms))
		}

		out = append(out, info)
	}

	return out, nil
}

func migrationInfo(ms *migrate.MigrationStatus) *forge.MigrationInfo {
	return &forge.MigrationInfo{
		Name:      ms.Migration.Name,
		Version:   ms.Migration.Version,
		Group:     ms.Migration.Group,
		Comment:   ms.Migration.Comment,
		Applied:   ms.Applied,
		AppliedAt: ms.AppliedAt,
	}
}
