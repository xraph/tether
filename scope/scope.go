// Package scope carries a tenant's identity on a context for the three
// libraries Tether calls, which each read it from their own place.
package scope

import (
	"context"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	"github.com/xraph/warden"
)

// With returns ctx carrying (appID, tenantID) in all three mechanisms:
// keysmith.WithTenant, warden.WithTenant and forge.WithScope with an org
// scope.
//
// Keysmith reads forge's scope first and falls back to its own. Warden does
// the opposite and prefers its own explicit values. A context that holds only
// one of them gives the two libraries different answers whenever an outer
// layer set a scope, so Tether always sets all three. An existing forge scope
// on ctx is replaced.
func With(ctx context.Context, appID, tenantID string) context.Context {
	ctx = keysmith.WithTenant(ctx, appID, tenantID)
	ctx = warden.WithTenant(ctx, appID, tenantID)

	return forge.WithScope(ctx, forge.NewOrgScope(appID, tenantID))
}
