package broker

import (
	"context"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
)

// PermLoader returns the topic grants of an authenticated identity. The broker
// calls it once per CONNECT, after authentication, and compiles the rules with
// the connection's client ID and username. It calls it again for a live
// connection when the connection's role or principal changes
// (Broker.ReloadRole, Broker.ReloadPrincipal).
//
// roleIDs must name every role the rules came from, inherited roles
// included: ReloadRole finds the connections to reload by them.
//
// At CONNECT an error refuses the connection with CONNACK 0x88 (server
// unavailable). On a reload, an error wrapping auth.ErrBadCredentials,
// auth.ErrNotAuthorized, tether.ErrNotFound or tether.ErrInvalidInput means
// the principal may no longer connect, and the connection is disconnected
// with 0x87; any other error is an outage, and the connection keeps its
// previous grants.
type PermLoader interface {
	Load(ctx context.Context, id *auth.Identity) (rules acl.Rules, roleIDs []string, err error)
}
