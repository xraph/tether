package extension_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// conn5 is an MQTT 5.0 connection that reports how the server ended it.
type conn5 struct {
	*paho.Client
	gone chan byte
}

func (e *env) connectDevice5(username, password, clientID string) *conn5 {
	e.t.Helper()

	var d net.Dialer

	nc, err := d.DialContext(e.t.Context(), "tcp", e.ext.Broker().Addr().String())
	require.NoError(e.t, err)

	c := &conn5{gone: make(chan byte, 1)}
	c.Client = paho.NewClient(paho.ClientConfig{
		Conn: nc,
		OnServerDisconnect: func(d *paho.Disconnect) {
			select {
			case c.gone <- d.ReasonCode:
			default:
			}
		},
	})

	ctx, cancel := context.WithTimeout(e.t.Context(), waitTimeout)
	defer cancel()

	ca, err := c.Connect(ctx, &paho.Connect{
		ClientID: clientID, KeepAlive: 30, CleanStart: true,
		Username: username, UsernameFlag: true,
		Password: []byte(password), PasswordFlag: true,
	})
	require.NoError(e.t, err)
	require.Equal(e.t, byte(0), ca.ReasonCode)

	e.t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{}) })

	return c
}

// A rotation with a grace window keeps the old secret working for the window
// and no longer: a connection made with it before the rotation is ended with
// 0xA0 (maximum connect time) when the window closes, and one made with the
// new secret stays.
func TestRotationGraceEndsOldConnections(t *testing.T) {
	e := start(t, false)

	tid := e.tenant()
	e.role(tid, "sensor", []string{"up"}, nil)
	d := e.device(tid, "sensor")

	old := e.connectDevice5(d.username, d.password, "old")

	m := e.do(http.MethodPost, e.base+"/v1/tenants/"+tid+"/devices/"+d.id+"/rotate",
		map[string]any{"grace_seconds": 1}, http.StatusOK)
	fresh := e.connectDevice5(d.username, m["password"].(string), "new")

	select {
	case code := <-old.gone:
		assert.Equal(t, byte(0xA0), code)
	case <-time.After(3 * time.Second):
		require.FailNow(t, "the old-secret connection outlived the grace window")
	}

	select {
	case code := <-fresh.gone:
		require.FailNow(t, "the new-secret connection was ended", "reason %#x", code)
	case <-time.After(500 * time.Millisecond):
	}
}
