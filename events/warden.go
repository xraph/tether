package events

import (
	"context"
	"sync/atomic"

	"github.com/xraph/warden/assignment"
	"github.com/xraph/warden/id"
	"github.com/xraph/warden/role"
)

// WardenPlugin is a warden plugin that reports role changes to a Handler.
//
// Role updates and deletions, and permissions attached to or detached from a
// role, report RolesChanged with the role's ID. Assignments and unassignments
// report SubjectChanged with the assignment's subject kind and ID. Warden
// emits no tenant with these, which is fine: role IDs and keysmith key IDs
// are unique across tenants.
//
// Register it when building the engine (warden.WithPlugin) or later
// (Engine.Plugins().Register). Note that warden v1.8.0's Engine.Plugins() is
// nil unless the engine was built with a plugin or a decision cache, so
// registering at construction is the dependable way. The Handler (a reactor
// over a broker that needs the engine) usually does not exist yet then:
// pass nil and Bind it later, as with KeysmithPlugin. Events before Bind are
// dropped.
//
// Every hook returns nil and recovers a panic from the Handler. Warden itself
// only logs plugin errors, but a failing hook would still be noise in its
// metrics.
type WardenPlugin struct {
	h     atomic.Pointer[handlerRef]
	wired atomic.Bool
}

// NewWardenPlugin returns a plugin that reports to h. A nil h leaves it
// unbound until Bind.
func NewWardenPlugin(h Handler) *WardenPlugin {
	p := &WardenPlugin{}
	p.Bind(h)

	return p
}

// Bind sets the Handler that receives events from now on. A nil Handler
// unbinds.
func (p *WardenPlugin) Bind(h Handler) {
	if h == nil {
		p.h.Store(nil)

		return
	}

	p.h.Store(&handlerRef{h: h})
}

// MarkWired records that the plugin was registered with a warden engine,
// at construction (warden.WithPlugin) or through Engine.Plugins().Register.
// As with KeysmithPlugin, code that receives the plugin later checks Wired
// instead of trusting that it was.
func (p *WardenPlugin) MarkWired() { p.wired.Store(true) }

// Wired reports whether MarkWired was called.
func (p *WardenPlugin) Wired() bool { return p.wired.Load() }

// Name implements warden's plugin.Plugin.
func (p *WardenPlugin) Name() string { return pluginName }

// OnRoleUpdated implements warden's plugin.RoleUpdated.
func (p *WardenPlugin) OnRoleUpdated(ctx context.Context, r *role.Role) error {
	if r != nil {
		p.rolesChanged(ctx, r.ID)
	}

	return nil
}

// OnRoleDeleted implements warden's plugin.RoleDeleted.
func (p *WardenPlugin) OnRoleDeleted(ctx context.Context, roleID id.RoleID) error {
	p.rolesChanged(ctx, roleID)

	return nil
}

// OnPermissionAttached implements warden's plugin.PermissionAttached.
func (p *WardenPlugin) OnPermissionAttached(ctx context.Context, roleID id.RoleID, _ id.PermissionID) error {
	p.rolesChanged(ctx, roleID)

	return nil
}

// OnPermissionDetached implements warden's plugin.PermissionDetached.
func (p *WardenPlugin) OnPermissionDetached(ctx context.Context, roleID id.RoleID, _ id.PermissionID) error {
	p.rolesChanged(ctx, roleID)

	return nil
}

// OnRoleAssigned implements warden's plugin.RoleAssigned.
func (p *WardenPlugin) OnRoleAssigned(ctx context.Context, a *assignment.Assignment) error {
	p.subjectChanged(ctx, a)

	return nil
}

// OnRoleUnassigned implements warden's plugin.RoleUnassigned.
func (p *WardenPlugin) OnRoleUnassigned(ctx context.Context, a *assignment.Assignment) error {
	p.subjectChanged(ctx, a)

	return nil
}

func (p *WardenPlugin) rolesChanged(ctx context.Context, roleID id.RoleID) {
	ref := p.h.Load()
	if ref == nil || roleID.IsNil() {
		return
	}

	defer recoverHandler()

	ref.h.RolesChanged(ctx, roleID.String())
}

func (p *WardenPlugin) subjectChanged(ctx context.Context, a *assignment.Assignment) {
	ref := p.h.Load()
	if ref == nil || a == nil {
		return
	}

	defer recoverHandler()

	ref.h.SubjectChanged(ctx, a.SubjectKind, a.SubjectID)
}
