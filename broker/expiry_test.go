package broker

import (
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	pahov3 "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

const (
	day      = uint32(24 * 60 * 60)
	thirtyDs = 30 * day
)

// connect311Persistent connects a 3.1.1 client with clean session off, so
// the broker keeps its session after it disconnects.
func connect311Persistent(t *testing.T, addr, clientID, user string) pahov3.Client {
	t.Helper()

	opts := pahov3.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(clientID).
		SetUsername(user).
		SetPassword("secret").
		SetProtocolVersion(4).
		SetCleanSession(false).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(waitTimeout)

	c := pahov3.NewClient(opts)
	tok := c.Connect()
	require.True(t, tok.WaitTimeout(waitTimeout), "connect timed out")
	require.NoError(t, tok.Error())

	return c
}

// connect5Expiry connects a 5.0 client asking for the given session expiry.
func connect5Expiry(t *testing.T, addr, clientID, user string, expiry uint32) (*client5, *paho.Connack) {
	t.Helper()

	c, ca, err := dial5(t, addr, &paho.Connect{
		ClientID:     clientID,
		Username:     user,
		UsernameFlag: true,
		Password:     []byte("secret"),
		PasswordFlag: true,
		KeepAlive:    30,
		Properties:   &paho.ConnectProperties{SessionExpiryInterval: &expiry},
	})
	require.NoError(t, err)

	return c, ca
}

// pumpInfo is what Clients lists for the client pump-1 of tenantID.
func pumpInfo(t *testing.T, b *Broker, tenantID string) (ClientInfo, bool) {
	t.Helper()

	for _, c := range b.Clients(tenantID) {
		if c.ClientID == "pump-1" {
			return c, true
		}
	}

	return ClientInfo{}, false
}

// A 5.0 client asking for 30 days under a tenant capped at one day gets one
// day: in its CONNACK and in the client listing, open and offline.
func TestTenantCapsSessionExpiry5(t *testing.T) {
	tn := newTenant(tenant.Limits{MaxSessionExpiry: 24 * time.Hour})
	b := startBroker(t, map[string]*auth.Identity{"dev": ident(tn, "dev")}, nil)
	tid := tn.ID.String()

	c, ca := connect5Expiry(t, b.Addr().String(), "pump-1", "dev", thirtyDs)
	require.NotNil(t, ca.Properties)
	require.NotNil(t, ca.Properties.SessionExpiryInterval, "the CONNACK must say the broker shortened it")
	assert.Equal(t, day, *ca.Properties.SessionExpiryInterval)

	info, ok := pumpInfo(t, b, tid)
	require.True(t, ok)
	assert.Equal(t, day, info.SessionExpiry)

	// Raising it on the way out does not get past the cap either.
	thirty := thirtyDs
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0, Properties: &paho.DisconnectProperties{SessionExpiryInterval: &thirty}}))

	require.Eventually(t, func() bool {
		got, found := pumpInfo(t, b, tid)

		return found && !got.Connected
	}, waitTimeout, 10*time.Millisecond)

	info, _ = pumpInfo(t, b, tid)
	assert.Equal(t, day, info.SessionExpiry)
}

// Under the cap nothing changes, and the CONNACK says nothing about it.
func TestSessionExpiryUnderTheTenantCapIsKept(t *testing.T) {
	tn := newTenant(tenant.Limits{MaxSessionExpiry: 24 * time.Hour})
	b := startBroker(t, map[string]*auth.Identity{"dev": ident(tn, "dev")}, nil)

	_, ca := connect5Expiry(t, b.Addr().String(), "pump-1", "dev", 3600)
	if ca.Properties != nil {
		assert.Nil(t, ca.Properties.SessionExpiryInterval)
	}

	info, ok := pumpInfo(t, b, tn.ID.String())
	require.True(t, ok)
	assert.Equal(t, uint32(3600), info.SessionExpiry)
}

// A 3.1.1 persistent session gets the effective cap as its expiry, and the
// broker ends it when that runs out. A tenant without a cap keeps the
// broker's.
func TestTenantCapExpires311Session(t *testing.T) {
	short := newTenant(tenant.Limits{MaxSessionExpiry: time.Second})
	long := newTenant(tenant.Limits{})
	b := startBroker(t, map[string]*auth.Identity{
		"short": ident(short, "short"),
		"long":  ident(long, "long"),
	}, nil)

	addr := b.Addr().String()
	cs := connect311Persistent(t, addr, "pump-1", "short")
	cl := connect311Persistent(t, addr, "pump-1", "long")

	info, ok := pumpInfo(t, b, short.ID.String())
	require.True(t, ok)
	assert.Equal(t, uint32(1), info.SessionExpiry)

	info, ok = pumpInfo(t, b, long.ID.String())
	require.True(t, ok)
	assert.Equal(t, b.cfg.sessionExpirySeconds(), info.SessionExpiry)

	cs.Disconnect(0)
	cl.Disconnect(0)

	require.Eventually(t, func() bool {
		_, found := pumpInfo(t, b, short.ID.String())

		return !found
	}, 8*time.Second, 50*time.Millisecond, "the capped 3.1.1 session must expire")

	info, ok = pumpInfo(t, b, long.ID.String())
	require.True(t, ok, "the uncapped session stays")
	assert.False(t, info.Connected)
}

// A 5.0 session under a short tenant cap expires through mochi's own sweep.
func TestTenantCapExpires5Session(t *testing.T) {
	tn := newTenant(tenant.Limits{MaxSessionExpiry: time.Second})
	b := startBroker(t, map[string]*auth.Identity{"dev": ident(tn, "dev")}, nil)

	c, _ := connect5Expiry(t, b.Addr().String(), "pump-1", "dev", 3600)
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	require.Eventually(t, func() bool {
		_, found := pumpInfo(t, b, tn.ID.String())

		return !found
	}, 8*time.Second, 50*time.Millisecond)
}

// ExpirePrincipal ends the principal's connections made before the mark at
// the given time, with 0xA0, and leaves later ones alone.
func TestExpirePrincipalEndsOlderConnections(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	b := startBroker(t, map[string]*auth.Identity{"dev": ident(tn, "dev")}, nil)
	addr := b.Addr().String()

	old, _, err := connect5(t, addr, "old", "dev", true)
	require.NoError(t, err)

	mark := time.Now()

	// connectedAt is taken from the wall clock; make sure the next one is
	// after the mark.
	time.Sleep(5 * time.Millisecond)

	fresh, _, err := connect5(t, addr, "new", "dev", true)
	require.NoError(t, err)

	assert.Equal(t, 1, b.ExpirePrincipal("akey_dev", mark, time.Now().Add(200*time.Millisecond)))
	assert.Equal(t, 0, b.ExpirePrincipal("akey_dev", mark, time.Now().Add(time.Hour)), "a later end does not extend it")
	assert.Equal(t, 0, b.ExpirePrincipal("akey_nobody", mark, time.Now()))

	select {
	case d := <-old.disconnects:
		assert.Equal(t, byte(0xA0), d.ReasonCode)
	case <-time.After(waitTimeout):
		require.FailNow(t, "the older connection was not ended")
	}

	select {
	case d := <-fresh.disconnects:
		require.FailNow(t, "the newer connection was ended", "reason %#x", d.ReasonCode)
	case <-time.After(300 * time.Millisecond):
	}
}
