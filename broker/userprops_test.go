package broker

import (
	"context"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

// User properties on a PUBLISH reach a 5.0 subscriber whatever it asked for in
// Request Problem Information. Mochi v2.7.9 drops them from every packet it
// sends a client that connected with 0, PUBLISH included, which paho.golang
// does by default; MQTT 5 ([MQTT-3.1.2-29]) only restricts the packets other
// than PUBLISH, CONNACK and DISCONNECT. OnPacketEncode undoes it.
func TestUserPropertiesReachSubscriber(t *testing.T) {
	for _, tc := range []struct {
		name        string
		problemInfo bool
	}{
		{"request problem information 0", false},
		{"request problem information 1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ta := newTenant(tenant.Limits{})
			b := startBroker(t,
				map[string]*auth.Identity{"pub": ident(ta, "pub"), "sub": ident(ta, "sub")},
				map[string]acl.Rules{
					"pub": {Publish: []string{"t"}},
					"sub": {Subscribe: []string{"t"}},
				})
			addr := b.Addr().String()

			sub, _, err := dial5(t, addr, &paho.Connect{
				ClientID:     "sub-dev",
				Username:     "sub",
				UsernameFlag: true,
				Password:     []byte("secret"),
				PasswordFlag: true,
				CleanStart:   true,
				KeepAlive:    30,
				Properties:   &paho.ConnectProperties{RequestProblemInfo: tc.problemInfo},
			})
			require.NoError(t, err)
			require.Equal(t, []byte{1}, subscribe5(t, sub, "t"))

			pub, _, err := connect5(t, addr, "pub-dev", "pub", true)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
			defer cancel()

			_, err = pub.Publish(ctx, &paho.Publish{
				Topic:   "t",
				QoS:     1,
				Payload: []byte("x"),
				Properties: &paho.PublishProperties{User: paho.UserProperties{
					{Key: "trace", Value: "abc"},
					{Key: "unit", Value: "c"},
				}},
			})
			require.NoError(t, err)

			got := recv(t, sub.msgs)
			assert.Equal(t, "t", got.Topic)
			assert.Equal(t, map[string]string{"trace": "abc", "unit": "c"}, got.User)
		})
	}
}
