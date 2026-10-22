package events

import (
	"context"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether/auth"
)

// BrokerControl is the part of *broker.Broker the reactor drives.
type BrokerControl interface {
	DisconnectPrincipal(principalID string, code packets.Code) int
	ReloadRole(ctx context.Context, roleID string)
	ReloadPrincipal(ctx context.Context, principalID string)
}

// reactor is the Handler NewReactor returns.
type reactor struct {
	evict func(keyID string)
	b     BrokerControl
}

// NewReactor returns the Handler that applies key and role changes to a
// broker. evict is the authenticator's cache eviction ((*auth.Keysmith).Evict)
// and may be nil.
//
//   - KeyInvalidated with disconnect: evict the key, then disconnect every
//     connection authenticated with it (0x98 administrative action).
//   - KeyInvalidated without disconnect (a rotation): evict the key only.
//     Live connections stay; the previous secret keeps working for its grace
//     window, and the next CONNECT validates against keysmith again.
//   - RolesChanged: reload the connections whose grants came from the role.
//   - SubjectChanged for an "api_key" subject: reload that key's
//     connections. Other subject kinds are ignored, since only keys connect.
//
// Eviction comes first, so a client that reconnects after the disconnect is
// validated against keysmith, not the cache. Empty IDs are ignored.
func NewReactor(evict func(keyID string), b BrokerControl) Handler {
	return &reactor{evict: evict, b: b}
}

func (r *reactor) KeyInvalidated(_ context.Context, keyID string, disconnect bool) {
	if keyID == "" {
		return
	}

	if r.evict != nil {
		r.evict(keyID)
	}

	if disconnect {
		r.b.DisconnectPrincipal(keyID, packets.ErrAdministrativeAction)
	}
}

func (r *reactor) RolesChanged(ctx context.Context, roleID string) {
	if roleID == "" {
		return
	}

	r.b.ReloadRole(ctx, roleID)
}

func (r *reactor) SubjectChanged(ctx context.Context, subjectKind, subjectID string) {
	if subjectKind != auth.PrincipalKindAPIKey || subjectID == "" {
		return
	}

	r.b.ReloadPrincipal(ctx, subjectID)
}
