package events

import (
	"context"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/xraph/warden"
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
//   - SubjectChanged for an "api_key", "user" or "service_acct" subject:
//     reload that principal's connections. Other subject kinds are ignored,
//     since only those connect.
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
	if subjectID == "" || !connects(subjectKind) {
		return
	}

	r.b.ReloadPrincipal(ctx, subjectID)
}

// connects reports whether warden subjects of this kind can hold an MQTT
// connection: API keys (devices), users and service accounts (JWT clients).
// It names warden's kinds, which are the ones assignment events carry; see
// perm.SubjectKind for the mapping from Identity.PrincipalKind.
func connects(subjectKind string) bool {
	switch warden.SubjectKind(subjectKind) {
	case warden.SubjectAPIKey, warden.SubjectUser, warden.SubjectServiceAcct:
		return true
	default:
		return false
	}
}
