package broker

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pahov3packets "github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
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

// A takeover that was handed the old connection's slot and is then refused
// (here: a change arrived while it connected, and authorizing again
// failed) gives the slot back to the old connection, which is still open.
// Releasing it instead would let the tenant open one connection too many.
func TestRefusedTakeoverGivesTheSlotBack(t *testing.T) {
	tn := newTenant(tenant.Limits{MaxConnections: 1})
	authn := &switchAuth{ids: map[string]*auth.Identity{"dev": ident(tn, "dev")}}
	perms := newMutablePerms(map[string]acl.Rules{})
	b := startBrokerDeps(t, authn, perms)
	addr := b.Addr().String()

	old, _, err := connect5(t, addr, "dev-1", "dev", true)
	require.NoError(t, err)

	release := perms.hold("dev")
	done := make(chan *paho.Connack, 1)

	go func() {
		_, ca, _ := dial5(t, addr, &paho.Connect{
			ClientID: "dev-1", Username: "dev", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
			CleanStart: true, KeepAlive: 30,
		})
		done <- ca
	}()

	select {
	case <-perms.started:
	case <-time.After(waitTimeout):
		require.FailNow(t, "the takeover never reached Load")
	}

	// A change lands while the takeover is in Load, and authorizing again
	// will fail.
	authn.setRefuse(true)
	b.DisconnectPrincipal("akey_nobody", packets.ErrAdministrativeAction)
	release()

	select {
	case ca := <-done:
		require.NotNil(t, ca)
		assert.NotEqual(t, byte(0), ca.ReasonCode, "the takeover is refused")
	case <-time.After(waitTimeout):
		require.FailNow(t, "the takeover never finished")
	}

	authn.setRefuse(false)

	// The old connection is still there and still holds the tenant's one
	// slot, so a second client ID is over the limit.
	_, ca, err := connect5(t, addr, "dev-2", "dev", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)

	select {
	case d := <-old.disconnects:
		require.FailNow(t, "the old connection was dropped", "reason %#x", d.ReasonCode)
	default:
	}

	// When it leaves, the slot is free again.
	require.NoError(t, old.Disconnect(&paho.Disconnect{}))
	require.Eventually(t, func() bool {
		_, _, err := connect5(t, addr, "dev-3", "dev", true)

		return err == nil
	}, waitTimeout, 20*time.Millisecond)
}
