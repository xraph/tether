package broker

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	pahov3packets "github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/gorilla/websocket"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	mochipackets "github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// Review Focus 1: two tenants, same raw client ID.
func TestTenantsIsolatedBySameClientID(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{
			"a-pump": ident(ta, "a-pump"), "a-sub": ident(ta, "a-sub"),
			"b-pump": ident(tb, "b-pump"), "b-sub": ident(tb, "b-sub"),
		},
		map[string]acl.Rules{
			"a-pump": {Publish: []string{"t"}},
			"a-sub":  {Subscribe: []string{"#"}},
			"b-pump": {Publish: []string{"#"}},
			"b-sub":  {Subscribe: []string{"#"}},
		})
	addr := b.Addr().String()

	aSub, _, err := connect5(t, addr, "a-listener", "a-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, aSub, "t"))

	bSub, _, err := connect5(t, addr, "b-listener", "b-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, bSub, "#"))

	aPump, _, err := connect5(t, addr, "pump-1", "a-pump", true)
	require.NoError(t, err)

	bPump, _, err := connect5(t, addr, "pump-1", "b-pump", true)
	require.NoError(t, err)

	// A's pump-1 is still connected after B's pump-1 arrived: it publishes.
	require.Equal(t, byte(0), publish5(t, aPump, "t", "x"))
	require.Equal(t, byte(0), publish5(t, aPump, "t", "y"))

	got := recv(t, aSub.msgs)
	assert.Equal(t, message{Topic: "t", Payload: "x"}, got, "A sees its own topic with the prefix stripped")
	assert.Equal(t, message{Topic: "t", Payload: "y"}, recv(t, aSub.msgs))

	// "y" reached A, so A's fan-out of "x" and "y" finished. Anything it sent
	// B would already sit in B's queue, ahead of this sentinel.
	require.Equal(t, byte(0), publish5(t, bPump, "sentinel", "z"))
	assert.Equal(t, message{Topic: "sentinel", Payload: "z"}, recv(t, bSub.msgs), "B never sees A's messages")

	for name, c := range map[string]*client5{"a-pump": aPump, "b-pump": bPump} {
		select {
		case d := <-c.disconnects:
			t.Fatalf("%s was disconnected with reason %#x", name, d.ReasonCode)
		default:
		}
	}

	cl, ok := b.server.Clients.Get(ta.ID.String() + "/pump-1")
	require.True(t, ok)
	assert.False(t, cl.Closed())

	cl, ok = b.server.Clients.Get(tb.ID.String() + "/pump-1")
	require.True(t, ok)
	assert.False(t, cl.Closed())
}

// Review Focus 1: a client calling itself "inline".
func TestInlineClientIDIsolated(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Subscribe: []string{"t"}}})

	c, _, err := connect5(t, b.Addr().String(), "inline", "a", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, c, "t"))

	inline, ok := b.server.Clients.Get("inline")
	require.True(t, ok)
	assert.True(t, inline.Net.Inline, "the inline client was not taken over")

	netCl, ok := b.server.Clients.Get(ta.ID.String() + "/inline")
	require.True(t, ok)
	assert.False(t, netCl.Net.Inline)

	got := make(chan string, 1)
	require.NoError(t, b.server.Subscribe("inline/check", 1, func(_ *mqtt.Client, _ mochipackets.Subscription, pk mochipackets.Packet) {
		got <- string(pk.Payload)
	}))
	require.NoError(t, b.server.Publish("inline/check", []byte("ok"), false, 0))

	select {
	case p := <-got:
		assert.Equal(t, "ok", p)
	case <-time.After(waitTimeout):
		t.Fatal("inline publish never reached the inline subscription")
	}

	// The broker's own publish into the tenant's space reaches the client
	// that calls itself inline, with the prefix stripped.
	require.NoError(t, b.server.Publish(topic.Namespace(ta.ID.String(), "t"), []byte("hi"), false, 0))
	assert.Equal(t, message{Topic: "t", Payload: "hi"}, recv(t, c.msgs))
}

// Review Focus 2: an alias-only PUBLISH despite topic alias maximum 0.
func TestAliasOnlyPublishClosesConnection(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	spy := &aclSpy{}
	b := startBrokerWith(t, testConfig(),
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Publish: []string{"#"}}},
		func(b *Broker) { b.aclObserver = spy.observe })

	conn := rawConnect5(t, b.Addr().String(), "aliaser", "a")

	// A normal QoS 1 publish is checked against the namespaced topic.
	pub := &packets.Publish{Topic: "t", QoS: 1, PacketID: 1, Payload: []byte("x"), Properties: &packets.Properties{}}
	_, err := pub.WriteTo(conn)
	require.NoError(t, err)

	cp := readPacket(t, conn)
	require.Equal(t, packets.PUBACK, cp.Type)
	assert.Equal(t, byte(0), cp.Content.(*packets.Puback).ReasonCode)
	assert.Contains(t, spy.all(), aclCheck{Topic: topic.Namespace(ta.ID.String(), "t"), Write: true})

	alias := uint16(1)
	pub = &packets.Publish{Topic: "", QoS: 0, Payload: []byte("y"), Properties: &packets.Properties{TopicAlias: &alias}}
	_, err = pub.WriteTo(conn)
	require.NoError(t, err)

	requireClosed(t, conn)

	for _, c := range spy.all() {
		assert.NotEmpty(t, c.Topic, "an ACL check saw an empty topic")
	}
}

// Review Focus 4: a will on a topic the client may not publish to.
func TestWillOutsideACLRefused(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Publish: []string{"status/#"}}})
	addr := b.Addr().String()

	willCP := func(clientID, willTopic string) *paho.Connect {
		return &paho.Connect{
			ClientID: clientID, Username: "a", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
			CleanStart: true, KeepAlive: 30,
			WillMessage: &paho.WillMessage{Topic: willTopic, Payload: []byte("gone"), QoS: 1},
		}
	}

	_, ca, err := dial5(t, addr, willCP("w5", "secret/x"))
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x87), ca.ReasonCode)

	_, err = connect311(t, addr, "w3", "a", &Will{Topic: "secret/x", Payload: "gone", QoS: 1})
	require.ErrorIs(t, err, pahov3packets.ErrorRefusedNotAuthorised)

	// A will inside the grants is accepted.
	_, ca, err = dial5(t, addr, willCP("w5-ok", "status/w5-ok"))
	require.NoError(t, err)
	assert.Equal(t, byte(0), ca.ReasonCode)

	_, err = connect311(t, addr, "w3-ok", "a", &Will{Topic: "status/w3-ok", Payload: "gone", QoS: 1})
	require.NoError(t, err)
}

func TestWillIsNamespaced(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a": ident(ta, "a"), "a-sub": ident(ta, "a-sub"), "b-sub": ident(tb, "b-sub")},
		map[string]acl.Rules{
			"a":     {Publish: []string{"status/#"}},
			"a-sub": {Subscribe: []string{"#"}},
			"b-sub": {Subscribe: []string{"#"}},
		})
	addr := b.Addr().String()

	aSub, _, err := connect5(t, addr, "a-sub", "a-sub", true)
	require.NoError(t, err)
	subscribe5(t, aSub, "status/#")

	bSub, _, err := connect5(t, addr, "b-sub", "b-sub", true)
	require.NoError(t, err)
	subscribe5(t, bSub, "#")

	conn := rawConnect5Will(t, addr, "dying", "a", "status/dying")
	require.NoError(t, conn.Close()) // abnormal close sends the will

	assert.Equal(t, message{Topic: "status/dying", Payload: "gone"}, recv(t, aSub.msgs))

	// Mochi sends the will before OnDisconnect drops the state, so once the
	// state is gone the will's fan-out is over. Anything it sent B would sit
	// in B's queue ahead of this sentinel.
	waitStateDropped(t, b, ta.ID.String()+"/dying")
	require.NoError(t, b.server.Publish(topic.Namespace(tb.ID.String(), "sentinel"), []byte("s"), false, 0))
	assert.Equal(t, message{Topic: "sentinel", Payload: "s"}, recv(t, bSub.msgs), "B never sees A's will")
}

// waitStateDropped waits until no live connection owns the namespaced ID,
// that is until mochi has called OnDisconnect for it.
func waitStateDropped(t *testing.T, b *Broker, nsID string) {
	t.Helper()

	deadline := time.After(waitTimeout)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for b.state.hasID(nsID) {
		select {
		case <-deadline:
			t.Fatal("connection state never dropped")
		case <-tick.C:
		}
	}
}

func TestBadCredentials(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBroker(t, map[string]*auth.Identity{"a": ident(ta, "a")}, nil)
	addr := b.Addr().String()

	_, ca, err := connect5(t, addr, "x", "nobody", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x86), ca.ReasonCode)

	_, err = connect311(t, addr, "x3", "nobody", nil)
	require.ErrorIs(t, err, pahov3packets.ErrorRefusedBadUsernameOrPassword)
}

func TestDisabledTenantRefused(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	ta.Enabled = false
	b := startBroker(t, map[string]*auth.Identity{"a": ident(ta, "a")}, nil)

	_, ca, err := connect5(t, b.Addr().String(), "x", "a", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x87), ca.ReasonCode)
}

func TestConnectionLimit(t *testing.T) {
	ta := newTenant(tenant.Limits{MaxConnections: 1})
	tb := newTenant(tenant.Limits{MaxConnections: 1})
	b := startBroker(t, map[string]*auth.Identity{"a": ident(ta, "a"), "b": ident(tb, "b")}, nil)
	addr := b.Addr().String()

	first, _, err := connect5(t, addr, "one", "a", true)
	require.NoError(t, err)

	_, ca, err := connect5(t, addr, "two", "a", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode, "quota exceeded")

	_, err = connect311(t, addr, "two-v3", "a", nil)
	require.ErrorIs(t, err, pahov3packets.ErrorRefusedServerUnavailable)

	_, _, err = connect5(t, addr, "other-tenant", "b", true)
	require.NoError(t, err, "another tenant has its own slots")

	require.NoError(t, first.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	// The slot comes back once the broker has processed the disconnect.
	deadline := time.After(waitTimeout)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()

	for {
		_, _, err = connect5(t, addr, "three", "a", true)
		if err == nil {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("slot never released: %v", err)
		case <-tick.C:
		}
	}
}

func TestTakeoverAtConnectionLimit(t *testing.T) {
	ta := newTenant(tenant.Limits{MaxConnections: 1})
	b := startBroker(t, map[string]*auth.Identity{"a": ident(ta, "a")}, nil)
	addr := b.Addr().String()

	old, _, err := connect5(t, addr, "pump-1", "a", true)
	require.NoError(t, err)

	// The same device reconnects while its old connection is still open.
	_, ca, err := connect5(t, addr, "pump-1", "a", true)
	require.NoError(t, err, "a takeover of its own client ID needs no second slot")
	assert.Equal(t, byte(0), ca.ReasonCode)

	select {
	case d := <-old.disconnects:
		assert.Equal(t, byte(0x8E), d.ReasonCode, "session taken over")
	case <-time.After(waitTimeout):
		t.Fatal("old connection was not taken over")
	}

	// The slot moved, it did not multiply: another client ID is still refused.
	_, ca, err = connect5(t, addr, "pump-2", "a", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)
}

func TestTLSListener(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	certFile, keyFile, pool := selfSigned(t, t.TempDir())

	cfg := testConfig()
	cfg.TLS = &TLSConfig{Address: "127.0.0.1:0", CertFile: certFile, KeyFile: keyFile}

	b := startBrokerWith(t, cfg,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Publish: []string{"t"}, Subscribe: []string{"t"}}}, nil)
	require.NotNil(t, b.tlsAddr)

	c, err := connect311TLS(t, "ssl://"+b.tlsAddr.String(), "tls-client", "a", nil,
		&tls.Config{RootCAs: pool, ServerName: "127.0.0.1", MinVersion: tls.VersionTLS12})
	require.NoError(t, err)

	msgs := subscribe311(t, c, "t")
	publish311(t, c, "t", "over tls")
	assert.Equal(t, message{Topic: "t", Payload: "over tls"}, recv(t, msgs))
}

func TestWebSocketListener(t *testing.T) {
	ta := newTenant(tenant.Limits{})

	cfg := testConfig()
	cfg.WebSocketAddress = freeAddr(t)

	startBrokerWith(t, cfg,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Publish: []string{"t"}, Subscribe: []string{"t"}}}, nil)
	waitListening(t, cfg.WebSocketAddress)

	d := websocket.Dialer{Subprotocols: []string{"mqtt"}, HandshakeTimeout: waitTimeout}
	ws, resp, err := d.DialContext(t.Context(), "ws://"+cfg.WebSocketAddress+"/", http.Header{})
	require.NoError(t, err)
	assert.Equal(t, "mqtt", resp.Header.Get("Sec-WebSocket-Protocol"))
	require.NoError(t, resp.Body.Close())

	// Finish an MQTT handshake on the probe before closing it, so the server
	// has taken the connection before the test (and the broker) moves on.
	var buf bytes.Buffer
	_, err = (&packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: "probe", CleanStart: true, KeepAlive: 30,
		Username: "a", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true, Properties: &packets.Properties{},
	}).WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, ws.SetReadDeadline(time.Now().Add(waitTimeout)))
	require.NoError(t, ws.WriteMessage(websocket.BinaryMessage, buf.Bytes()))

	_, frame, err := ws.ReadMessage()
	require.NoError(t, err)

	ack, err := packets.ReadPacket(bytes.NewReader(frame))
	require.NoError(t, err)
	require.Equal(t, packets.CONNACK, ack.Type)
	require.NoError(t, ws.Close())

	c, err := connect311(t, "ws://"+cfg.WebSocketAddress+"/", "ws-client", "a", nil)
	require.NoError(t, err)

	msgs := subscribe311(t, c, "t")
	publish311(t, c, "t", "over ws")
	assert.Equal(t, message{Topic: "t", Payload: "over ws"}, recv(t, msgs))
}

func TestSessionSurvivesRestart(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	ids := map[string]*auth.Identity{"keeper": ident(ta, "keeper"), "pub": ident(ta, "pub")}
	rules := map[string]acl.Rules{"keeper": {Subscribe: []string{"t"}}, "pub": {Publish: []string{"t"}}}

	cfg := testConfig()
	cfg.DataDir = t.TempDir()

	b1, err := New(cfg, Deps{Auth: auth.NewStatic(ids), Perms: fakePerms{rules: rules}, Logger: testLogger()})
	require.NoError(t, err)
	require.NoError(t, b1.Start(t.Context()))

	keeper, ca, err := connect5(t, b1.Addr().String(), "keeper", "keeper", false)
	require.NoError(t, err)
	assert.False(t, ca.SessionPresent)
	require.Equal(t, []byte{1}, subscribe5(t, keeper, "t"))

	require.NoError(t, b1.Stop(t.Context()))

	b2 := startBrokerWith(t, cfg, ids, rules, nil)
	addr := b2.Addr().String()

	// Queued while the restored session is offline.
	pub, _, err := connect5(t, addr, "pub", "pub", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publish5(t, pub, "t", "queued"))

	keeper, ca, err = connect5(t, addr, "keeper", "keeper", false)
	require.NoError(t, err)
	assert.True(t, ca.SessionPresent)
	assert.Equal(t, message{Topic: "t", Payload: "queued"}, recv(t, keeper.msgs))

	require.Equal(t, byte(0), publish5(t, pub, "t", "live"))
	assert.Equal(t, message{Topic: "t", Payload: "live"}, recv(t, keeper.msgs))
}

func TestOfflineSessionReceivesQueuedMessages(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"keeper": ident(ta, "keeper"), "pub": ident(ta, "pub")},
		map[string]acl.Rules{"keeper": {Subscribe: []string{"t"}}, "pub": {Publish: []string{"t"}}})
	addr := b.Addr().String()

	keeper, _, err := connect5(t, addr, "keeper", "keeper", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, keeper, "t"))
	require.NoError(t, keeper.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	// Wait until the broker has dropped the connection's state, so the
	// message below meets an offline session.
	waitStateDropped(t, b, ta.ID.String()+"/keeper")

	pub, _, err := connect5(t, addr, "pub", "pub", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publish5(t, pub, "t", "while away"))

	keeper, ca, err := connect5(t, addr, "keeper", "keeper", false)
	require.NoError(t, err)
	assert.True(t, ca.SessionPresent)
	assert.Equal(t, message{Topic: "t", Payload: "while away"}, recv(t, keeper.msgs))
}

func TestOverLengthPublishTopicClosesConnection(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	spy := &aclSpy{}
	cfg := testConfig()
	cfg.MaxTopicLength = 16
	b := startBrokerWith(t, cfg,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Publish: []string{"#"}}},
		func(b *Broker) { b.aclObserver = spy.observe })

	conn := rawConnect5(t, b.Addr().String(), "long", "a")

	pub := &packets.Publish{Topic: strings.Repeat("x", 17), QoS: 1, PacketID: 1, Payload: []byte("x"), Properties: &packets.Properties{}}
	_, err := pub.WriteTo(conn)
	require.NoError(t, err)

	cp := readPacket(t, conn)
	require.Equal(t, packets.DISCONNECT, cp.Type)
	assert.Equal(t, byte(0x90), cp.Content.(*packets.Disconnect).ReasonCode)
	requireClosed(t, conn)
	assert.Empty(t, spy.all(), "a rejected topic never reaches the ACL")
}

func TestInvalidSubscribeFilters(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	spy := &aclSpy{}
	cfg := testConfig()
	cfg.MaxTopicLength = 16
	b := startBrokerWith(t, cfg,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Subscribe: []string{"#"}}},
		func(b *Broker) { b.aclObserver = spy.observe })

	c, _, err := connect5(t, b.Addr().String(), "subber", "a", true)
	require.NoError(t, err)

	reasons := subscribe5(t, c, "a/#/b", strings.Repeat("x", 17), "ok/+")
	require.Len(t, reasons, 3, "the filter count is unchanged")
	assert.GreaterOrEqual(t, reasons[0], byte(0x80))
	assert.GreaterOrEqual(t, reasons[1], byte(0x80))
	assert.Equal(t, byte(1), reasons[2])

	for _, check := range spy.all() {
		assert.Equal(t, topic.Namespace(ta.ID.String(), "ok/+"), check.Topic, "only the valid filter is checked")
	}
}

func TestSubscribeOutsideTenantDenied(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a": ident(ta, "a"), "b": ident(tb, "b"), "b-sub": ident(tb, "b-sub")},
		map[string]acl.Rules{
			"a":     {Subscribe: []string{"#"}},
			"b":     {Publish: []string{"#"}},
			"b-sub": {Subscribe: []string{"#"}},
		})
	addr := b.Addr().String()

	a, _, err := connect5(t, addr, "a", "a", true)
	require.NoError(t, err)

	// Spelling out B's namespace only reaches A's own "<B>/..." topics.
	bNS := tb.ID.String()
	require.Equal(t, []byte{1, 1}, subscribe5(t, a, bNS+"/#", "$share/g/"+bNS+"/#"))

	bSub, _, err := connect5(t, addr, "b-sub", "b-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, bSub, "#"))

	bc, _, err := connect5(t, addr, "b", "b", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publish5(t, bc, "x", "secret"))
	require.Equal(t, byte(0), publish5(t, bc, "y", "secret"))

	// "y" reached B's own subscriber, so the fan-out of "x" is over: anything
	// it sent A would sit in A's queue ahead of the sentinel.
	assert.Equal(t, message{Topic: "x", Payload: "secret"}, recv(t, bSub.msgs))
	assert.Equal(t, message{Topic: "y", Payload: "secret"}, recv(t, bSub.msgs))

	// Sentinel through A's own space: published by the broker into A's
	// namespace under the literal "<B>/sentinel" name A subscribed to.
	require.NoError(t, b.server.Publish(topic.Namespace(ta.ID.String(), bNS+"/sentinel"), []byte("s"), false, 0))
	assert.Equal(t, message{Topic: bNS + "/sentinel", Payload: "s"}, recv(t, a.msgs))
}

func TestNewValidatesDeps(t *testing.T) {
	_, err := New(DefaultConfig(), Deps{Perms: fakePerms{}})
	require.Error(t, err)

	_, err = New(DefaultConfig(), Deps{Auth: auth.NewStatic(nil)})
	require.Error(t, err)

	cfg := DefaultConfig()
	cfg.TCPAddress = ""
	_, err = New(cfg, Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}})
	require.Error(t, err, "no listener")
}

func TestStartTwiceFails(t *testing.T) {
	b := startBroker(t, nil, nil)
	require.Error(t, b.Start(t.Context()))
}

// rawConnect5 opens a TCP connection, sends a 5.0 CONNECT and requires a
// successful CONNACK.
func rawConnect5(t *testing.T, addr, clientID, user string) net.Conn {
	t.Helper()

	return rawConnect(t, addr, &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: true, KeepAlive: 30,
		Username: user, UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{},
	})
}

// rawConnect5Will is rawConnect5 with a QoS 0 will "gone" on willTopic.
func rawConnect5Will(t *testing.T, addr, clientID, user, willTopic string) net.Conn {
	t.Helper()

	return rawConnect(t, addr, &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: true, KeepAlive: 30,
		Username: user, UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{}, WillProperties: &packets.Properties{},
		WillFlag: true, WillTopic: willTopic, WillMessage: []byte("gone"),
	})
}

func rawConnect(t *testing.T, addr string, cp *packets.Connect) net.Conn {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = cp.WriteTo(conn)
	require.NoError(t, err)

	ack := readPacket(t, conn)
	require.Equal(t, packets.CONNACK, ack.Type)
	require.Equal(t, byte(0), ack.Content.(*packets.Connack).ReasonCode)

	return conn
}

func readPacket(t *testing.T, conn net.Conn) *packets.ControlPacket {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(waitTimeout)))

	cp, err := packets.ReadPacket(conn)
	require.NoError(t, err)

	return cp
}

// requireClosed reads until the server closes conn, allowing a DISCONNECT
// first, and fails if it is still open after waitTimeout.
func requireClosed(t *testing.T, conn net.Conn) {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(waitTimeout)))

	for {
		cp, err := packets.ReadPacket(conn)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("connection still open")
			}

			return
		}

		require.Equal(t, packets.DISCONNECT, cp.Type, "only a DISCONNECT may precede the close")
	}
}

// writeFailListener accepts real connections whose writes always fail, so
// mochi's success CONNACK write fails and attachClient returns without
// calling OnDisconnect. closed receives once per connection mochi closes.
type writeFailListener struct {
	net.Listener

	closed chan struct{}
}

func (l *writeFailListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &writeFailConn{Conn: c, closed: l.closed}, nil
}

type writeFailConn struct {
	net.Conn

	closed chan struct{}
	once   sync.Once
}

func (c *writeFailConn) Write([]byte) (int, error) { return 0, errors.New("write refused by test") }

func (c *writeFailConn) Close() error {
	c.once.Do(func() { c.closed <- struct{}{} })

	return c.Conn.Close()
}

// A handshake that dies on the success CONNACK skips OnDisconnect in mochi.
// Its slot must come back without the client ever reconnecting.
func TestSlotReleasedWhenConnackWriteFails(t *testing.T) {
	ta := newTenant(tenant.Limits{MaxConnections: 1})
	b := startBroker(t, map[string]*auth.Identity{"a": ident(ta, "a")}, nil)

	raw, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	faulty := &writeFailListener{Listener: raw, closed: make(chan struct{}, 1)}
	require.NoError(t, b.server.AddListener(listeners.NewNet("faulty", faulty)))
	b.server.Listeners.Serve("faulty", b.server.EstablishConnection)

	conn, err := dial(t, raw.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Empty client ID: mochi assigns a random one, so this ID never returns.
	_, err = (&packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, CleanStart: true, KeepAlive: 30,
		Username: "a", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{},
	}).WriteTo(conn)
	require.NoError(t, err)

	select {
	case <-faulty.closed:
	case <-time.After(waitTimeout):
		t.Fatal("broker never closed the failed handshake")
	}

	// writeFailConn.Close signals closed inside mochi's Client.Stop, before
	// Stop cancels the client's context, so a dial now can still beat
	// Closed() and find a slot the sweep does not yet see as leaked. Wait
	// until the leaked client is registered and reports Closed.
	require.Eventually(t, func() bool {
		b.state.mu.RLock()
		defer b.state.mu.RUnlock()

		for cl, st := range b.state.byClient {
			if st.tenantID == ta.ID.String() {
				return cl.Closed()
			}
		}

		return false
	}, waitTimeout, time.Millisecond, "the leaked client never reported Closed")

	_, _, err = connect5(t, b.Addr().String(), "next", "a", true)
	require.NoError(t, err, "the leaked slot is swept and reused")

	// The sweep never frees a live connection's slot.
	_, ca, err := connect5(t, b.Addr().String(), "another", "a", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)
}
