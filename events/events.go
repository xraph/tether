// Package events turns keysmith key changes and warden role changes into
// reactions on live MQTT connections.
//
// Two plugins listen to the engines: [KeysmithPlugin] for key rotation,
// revocation, suspension and expiry, and [WardenPlugin] for role, permission
// and assignment changes. Each reports what happened to a [Handler] in
// broker terms: a key is no longer valid, a role's grants changed, or a
// subject's roles changed. [NewReactor] returns the Handler that applies those
// to the broker: it evicts the key from the authenticator's cache, and
// disconnects or reloads the affected connections. In a cell,
// [NewClusterReactor] sends each change through the control bus instead, and
// every node applies it.
//
// Plugin hooks run synchronously inside the engine call that made the change.
// With [NewReactor] the reaction is done when that call returns. With
// [NewClusterReactor] the call returns once the bus has stored the change,
// and each node, this one included, applies it asynchronously when the event
// reaches it. Hooks never fail and never panic into the engine: keysmith
// stops dispatching at the first plugin error.
package events

import (
	"context"
	"time"
)

// Handler receives key and role changes from the plugins.
//
// Implementations are called from inside keysmith and warden admin calls (and
// from keysmith's ValidateKey for an expired key), so they should finish
// quickly and must not call back into the engine that called them. The
// cluster reactor ([NewClusterReactor]) waits for the control bus to store
// each change, so it can hold such a call, a device's ValidateKey included,
// for up to its publish wait (5 s) while the bus does not answer.
type Handler interface {
	// KeyInvalidated reports that the keysmith key keyID changed.
	// disconnect is true when the key may no longer be used at all
	// (revoked, suspended or expired) and false when only its secret
	// changed (rotated; the old one may still work during a grace window).
	KeyInvalidated(ctx context.Context, keyID string, disconnect bool)

	// KeyRotated reports that the keysmith key keyID was rotated with a
	// grace window: its previous secret keeps working until graceEnds, and
	// no longer. rotatedAt is a time after keysmith stored the new secret,
	// so every connection made with the previous secret predates it. A
	// rotation with no grace window is KeyInvalidated(keyID, true) instead.
	KeyRotated(ctx context.Context, keyID string, rotatedAt, graceEnds time.Time)

	// RolesChanged reports that the warden role roleID changed: updated,
	// deleted, or a permission attached or detached.
	RolesChanged(ctx context.Context, roleID string)

	// SubjectChanged reports that a role was assigned to or unassigned
	// from a warden subject. subjectKind is warden's subject kind, "api_key"
	// for a keysmith key.
	SubjectChanged(ctx context.Context, subjectKind, subjectID string)
}
