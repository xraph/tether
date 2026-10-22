package broker

import (
	"log/slog"

	mqtt "github.com/mochi-mqtt/server/v2"
)

// sessionSweepInterval is how often expireSessions runs, the same pace as
// mochi's own sweep of 5.0 sessions.
const sessionSweepInterval = 1

// expireSessions ends the offline 3.1.1 sessions whose expiry has run out.
// Mochi expires a 3.1.1 session after the broker's cap whatever the tenant
// allows, because it reads the expiry property of 5.0 sessions only. Tether
// gives every persistent 3.1.1 session its effective cap as that property
// (capSessionExpiry), and badger stores it, so this finds the ones that
// expired, restored sessions included.
//
// It takes the same tombstone DeleteSession does, so a CONNECT that would
// inherit the session at that moment is refused (0x88) or wins (the session
// is left alone).
func (b *Broker) expireSessions(srv *mqtt.Server, now int64) {
	for nsID, cl := range srv.Clients.GetAll() {
		if cl.Net.Inline {
			continue
		}

		// Read the properties only once the client is closed: mochi
		// changes them while the connection is open.
		stopped := cl.StopTime()
		if stopped == 0 || cl.Properties.ProtocolVersion >= 5 || cl.Properties.Clean {
			continue
		}

		props := cl.Properties.Props
		if !props.SessionExpiryIntervalFlag || stopped+int64(props.SessionExpiryInterval) >= now {
			continue
		}

		b.expireSession(srv, nsID, cl)
	}
}

// expireSession purges cl's session unless another connection holds the ID
// or its own connection has not finished tearing down.
func (b *Broker) expireSession(srv *mqtt.Server, nsID string, cl *mqtt.Client) {
	st, ok := b.state.beginDelete(nsID, cl)
	if !ok {
		return
	}
	defer b.state.endDelete(nsID)

	if st != nil {
		select {
		case <-st.ended:
		default:
			return
		}
	}

	if cur, ok := srv.Clients.Get(nsID); !ok || cur != cl {
		return
	}

	b.purgeSession(srv, cl)
	b.log.Info("tether: session expired", slog.String("client", nsID))
}
