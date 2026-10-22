package broker

import (
	"testing"
	"time"

	pahov3packets "github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

// A tenant's MaxSessions counts its open connections and its offline kept
// sessions. A new session over it is refused with 0x97 (3.1.1: server
// unavailable), a client coming back to its own session is not, and another
// tenant is not affected.
func TestTenantSessionLimit(t *testing.T) {
	capped := newTenant(tenant.Limits{MaxSessions: 2})
	other := newTenant(tenant.Limits{})
	b := startBroker(t, map[string]*auth.Identity{
		"dev":   ident(capped, "dev"),
		"other": ident(other, "other"),
	}, nil)
	addr := b.Addr().String()
	tid := capped.ID.String()

	// An offline persistent session counts.
	away := connect311Persistent(t, addr, "away", "dev")
	away.Disconnect(0)

	require.Eventually(t, func() bool {
		cs := b.Clients(tid)

		return len(cs) == 1 && !cs[0].Connected
	}, waitTimeout, 10*time.Millisecond)

	// So does an open clean one.
	_, ca, err := connect5(t, addr, "live", "dev", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0), ca.ReasonCode)

	_, ca, err = connect5(t, addr, "third", "dev", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)

	_, err = connect311(t, addr, "third", "dev", nil)
	require.ErrorIs(t, err, pahov3packets.ErrorRefusedServerUnavailable)

	// Coming back to an existing session is not a new one.
	back := connect311Persistent(t, addr, "away", "dev")
	t.Cleanup(func() { back.Disconnect(0) })

	// Another tenant has its own count.
	_, ca, err = connect5(t, addr, "third", "other", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0), ca.ReasonCode)

	// Deleting a session makes room.
	back.Disconnect(0)
	require.Eventually(t, func() bool {
		for _, c := range b.Clients(tid) {
			if c.ClientID == "away" {
				return !c.Connected
			}
		}

		return false
	}, waitTimeout, 10*time.Millisecond)
	require.NoError(t, b.DeleteSession(tid, "away"))

	_, ca, err = connect5(t, addr, "third", "dev", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0), ca.ReasonCode)
}

// MaxInflight caps how many QoS 1 and 2 messages mochi keeps in flight per
// client, 1024 unless the config says otherwise (mochi's own default is
// 8192).
func TestMaxInflightReachesMochi(t *testing.T) {
	b := startBroker(t, nil, nil)
	assert.Equal(t, uint16(DefaultMaxInflight), b.server.Options.Capabilities.MaximumInflight)

	cfg := testConfig()
	cfg.MaxInflight = 16
	b = startBrokerWith(t, cfg, nil, nil, nil)
	assert.Equal(t, uint16(16), b.server.Options.Capabilities.MaximumInflight)
}
