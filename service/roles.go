package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xraph/warden"
	"github.com/xraph/warden/assignment"
	"github.com/xraph/warden/id"
	"github.com/xraph/warden/keysmithbridge"
	"github.com/xraph/warden/plugin"
)

// assignmentPage bounds one read of a key's assignments, as keysmithbridge
// does.
const assignmentPage = 500

// WardenRoles is the RoleAssigner over a warden engine. Assigning and
// unassigning every role go through warden's keysmithbridge. Removing all but
// some roles (UnassignOtherRolesFromAPIKey) is Tether's own, because the
// bridge can only remove every role of a key at once.
type WardenRoles struct {
	*keysmithbridge.Bridge

	eng   *warden.Engine
	actor warden.Actor
}

// NewWardenRoles returns a WardenRoles over eng. Writes are attributed to the
// actor on the context, or else to the service actor "tether".
func NewWardenRoles(eng *warden.Engine) *WardenRoles {
	return &WardenRoles{
		Bridge: keysmithbridge.New(eng),
		eng:    eng,
		actor:  warden.Actor{Kind: "service", ID: "tether", Via: "tether/service"},
	}
}

// UnassignOtherRolesFromAPIKey removes every assignment the API key keyID
// holds in the tenant's root namespace whose role is not one of the slugs in
// keep. Assignments of roles in keep are never touched. A slug in keep that
// names no role keeps nothing.
//
// It mirrors keysmithbridge's UnassignRoleFromAPIKey for each assignment it
// removes: it emits RoleUnassigned and an "assignment.deleted" audit event,
// treats an assignment someone else removed first as done, and drops the
// key's cached warden decisions once anything was removed, whatever happens
// after.
func (w *WardenRoles) UnassignOtherRolesFromAPIKey(ctx context.Context, tenantID, keyID string, keep []string) error {
	if w == nil || w.eng == nil {
		return errors.New("tether: no warden engine")
	}

	if tenantID == "" {
		return warden.ErrTenantRequired
	}

	if keyID == "" {
		return errors.New("tether: key ID is required")
	}

	st := w.eng.Store()

	kept := make(map[id.RoleID]struct{}, len(keep))

	for _, slug := range keep {
		r, err := st.GetRoleBySlug(ctx, tenantID, "", slug)
		if err != nil {
			if errors.Is(err, warden.ErrNotFound) {
				continue
			}

			return fmt.Errorf("tether: look up role %q: %w", slug, err)
		}

		kept[r.ID] = struct{}{}
	}

	actor := w.actorFor(ctx)
	removed := 0

	defer func() {
		if removed > 0 {
			w.eng.InvalidateSubject(ctx, tenantID, warden.SubjectAPIKey, keyID)
		}
	}()

	root := ""
	offset := 0

	for {
		rows, err := st.ListAssignments(ctx, &assignment.ListFilter{
			TenantID:      tenantID,
			NamespacePath: &root,
			SubjectKind:   string(warden.SubjectAPIKey),
			SubjectID:     keyID,
			Limit:         assignmentPage,
			Offset:        offset,
		})
		if err != nil {
			return fmt.Errorf("tether: list assignments for key %q: %w", keyID, err)
		}

		stayed := 0

		for _, a := range rows {
			if _, ok := kept[a.RoleID]; ok {
				stayed++

				continue
			}

			if err := st.DeleteAssignment(ctx, tenantID, a.ID); err != nil {
				if errors.Is(err, warden.ErrAssignmentNotFound) {
					continue // someone else removed it first
				}

				return fmt.Errorf("tether: remove assignment %s from key %q: %w", a.ID, keyID, err)
			}

			removed++

			if pl := w.eng.Plugins(); pl != nil {
				pl.EmitRoleUnassigned(ctx, a)
				pl.EmitAudit(ctx, plugin.Event{
					Actor: actor, At: time.Now().UTC(), Action: "assignment.deleted",
					TenantID: tenantID, EntityID: a.ID.String(), Before: a,
				})
			}
		}

		if len(rows) < assignmentPage {
			return nil
		}

		// Removed rows leave the listing; kept ones stay and are skipped.
		offset += stayed
	}
}

// actorFor prefers the actor already on the context, then the service's own.
func (w *WardenRoles) actorFor(ctx context.Context) warden.Actor {
	if a, ok := warden.ActorFromContext(ctx); ok {
		return a
	}

	return w.actor
}
