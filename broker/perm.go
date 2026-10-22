package broker

import (
	"context"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
)

// PermLoader returns the topic grants of an authenticated identity. The broker
// calls it once per CONNECT, after authentication, and compiles the rules with
// the connection's client ID and username.
//
// An error refuses the connection with CONNACK 0x88 (server unavailable).
type PermLoader interface {
	Load(ctx context.Context, id *auth.Identity) (rules acl.Rules, roleIDs []string, err error)
}
