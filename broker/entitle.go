package broker

import (
	"context"
	"errors"
	"log/slog"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// ErrNotEntitled is what an Entitlement wraps when a tenant's plan does not
// cover Tether. A CONNECT refused with it gets 0x97 quota exceeded.
var ErrNotEntitled = errors.New("tether/broker: not entitled")

// Entitlement decides whether a tenant may connect. The broker asks it once
// per CONNECT, after the tenant is resolved and before the session and slot
// checks, and never on the PUBLISH path. Deps.Entitlement nil checks nothing.
//
// Allow returns nil to let the tenant in. An error wrapping ErrNotEntitled
// refuses the CONNECT with 0x97 quota exceeded (3.1.1: server unavailable)
// and AuthFailure reason not_entitled; any other error refuses it with 0x88
// server unavailable and reason unavailable. Allow runs on the connection's
// goroutine with the CONNECT's context and no broker lock held, so it may
// make a bounded remote call, but should answer from memory when it can.
type Entitlement interface {
	Allow(ctx context.Context, tenantID string) error
}

// entitled asks Deps.Entitlement about tenantID and refuses the CONNECT when
// it says no. True when there is no Entitlement.
func (h *hook) entitled(ctx context.Context, cl *mqtt.Client, tenantID string, log *slog.Logger) bool {
	e := h.b.deps.Entitlement
	if e == nil {
		return true
	}

	err := e.Allow(ctx, tenantID)

	switch {
	case err == nil:
		return true
	case errors.Is(err, ErrNotEntitled):
		return h.refuseAs(cl, log, packets.ErrQuotaExceeded, AuthReasonNotEntitled, "tenant not entitled", err)
	default:
		return h.refuseAs(cl, log, packets.ErrServerUnavailable, AuthReasonUnavailable, "entitlement check failed", err)
	}
}
