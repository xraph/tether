package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// fastSys makes the broker publish tenant $SYS stats every 50 ms.
func fastSys(b *Broker) { b.sysInterval = 50 * time.Millisecond }

// inlineSub subscribes the broker's inline client to filter and returns the
// messages published after the call. Mochi replays retained messages to an
// inline subscription inside Subscribe; those are drained here.
//
// Mochi calls inline handlers during the fan-out, from the publishing
// goroutine, so a message received here means every earlier publish from that
// goroutine finished its fan-out. Subscribe it after the subscriptions a test
// is about, so their subscribers are part of every fan-out it sees.
func inlineSub(t *testing.T, b *Broker, filter string, subID int) <-chan message {
	t.Helper()

	ch := make(chan message, 1024)
	require.NoError(t, b.server.Subscribe(filter, subID, func(_ *mqtt.Client, _ packets.Subscription, pk packets.Packet) {
		select {
		case ch <- message{Topic: pk.TopicName, Payload: string(pk.Payload)}:
		default:
		}
	}))

	for range len(ch) {
		<-ch
	}

	return ch
}

// recvN waits for n messages on ch.
func recvN(t *testing.T, ch <-chan message, n int) {
	t.Helper()

	for range n {
		recv(t, ch)
	}
}

// Review Focus 3: "#" never matches mochi's global $SYS, the tenant's own
// $SYS, or another tenant's topics.
func TestHashSeesNoSysAndNoOtherTenant(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBrokerWith(t, testConfig(),
		map[string]*auth.Identity{"a": ident(ta, "a"), "b-pub": ident(tb, "b-pub"), "b-sub": ident(tb, "b-sub")},
		map[string]acl.Rules{
			"a":     {Subscribe: []string{"#"}},
			"b-pub": {Publish: []string{"#"}},
			"b-sub": {Subscribe: []string{"#"}},
		}, fastSys)
	addr := b.Addr().String()

	a, _, err := connect5(t, addr, "a", "a", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, a, "#"))

	mochiTicks := inlineSub(t, b, "$SYS/broker/uptime", 1)
	tenantTicks := inlineSub(t, b, topic.Namespace(ta.ID.String(), "$SYS/clients/connected"), 2)

	bSub, _, err := connect5(t, addr, "b-sub", "b-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, bSub, "#"))

	bPub, _, err := connect5(t, addr, "b-pub", "b-pub", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publish5(t, bPub, "x", "secret"))
	require.Equal(t, byte(0), publish5(t, bPub, "y", "secret"))

	// "y" reached B's subscriber, so the fan-out of "x" is over.
	assert.Equal(t, message{Topic: "x", Payload: "secret"}, recv(t, bSub.msgs))
	assert.Equal(t, message{Topic: "y", Payload: "secret"}, recv(t, bSub.msgs))

	// Two mochi $SYS ticks and two tenant $SYS ticks: the first of each has
	// finished its fan-out.
	recvN(t, mochiTicks, 2)
	recvN(t, tenantTicks, 2)

	require.NoError(t, b.server.Publish(topic.Namespace(ta.ID.String(), "sentinel"), []byte("s"), false, 0))
	assert.Equal(t, message{Topic: "sentinel", Payload: "s"}, recv(t, a.msgs), "the sentinel is A's first message")
}

// Review Focus 3: a tenant's "$SYS/#" sees its own stats only.
func TestTenantSysOnlyOwn(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBrokerWith(t, testConfig(),
		map[string]*auth.Identity{"a": ident(ta, "a"), "b": ident(tb, "b")}, nil, fastSys)
	addr := b.Addr().String()

	// No grants at all: the tenant's $SYS is readable by default.
	a, _, err := connect5(t, addr, "a", "a", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, a, "$SYS/#"))

	mochiTicks := inlineSub(t, b, "$SYS/broker/uptime", 1)

	// B is connected, so B's stats are published too.
	_, _, err = connect5(t, addr, "b", "b", true)
	require.NoError(t, err)

	var got []message

	// Two ticks of A's stats, then two of mochi's own.
	for seen := 0; seen < 2; {
		m := recv(t, a.msgs)
		got = append(got, m)

		if m.Topic == "$SYS/clients/connected" {
			seen++
		}
	}

	recvN(t, mochiTicks, 2)

	require.NoError(t, b.server.Publish(topic.Namespace(ta.ID.String(), "$SYS/zz-sentinel"), []byte("s"), false, 0))

	for {
		m := recv(t, a.msgs)
		if m.Topic == "$SYS/zz-sentinel" {
			break
		}

		got = append(got, m)
	}

	want := map[string]string{
		"$SYS/clients/connected":   "1",
		"$SYS/messages/received":   "0",
		"$SYS/messages/sent":       "",
		"$SYS/subscriptions/count": "1",
		"$SYS/retained/count":      "0",
	}

	topics := map[string]bool{}

	for _, m := range got {
		topics[m.Topic] = true

		assert.True(t, strings.HasPrefix(m.Topic, "$SYS/"), "unexpected topic %q", m.Topic)
		assert.False(t, strings.HasPrefix(m.Topic, "$SYS/broker"), "mochi's $SYS leaked: %q", m.Topic)
		assert.NotContains(t, m.Topic, tb.ID.String(), "tenant B's $SYS leaked")
		assert.NotContains(t, m.Topic, ta.ID.String(), "the prefix was not stripped")

		if v, ok := want[m.Topic]; ok && v != "" {
			assert.Equal(t, v, m.Payload, m.Topic)
		}
	}

	for name := range want {
		assert.True(t, topics[name], "never received %s", name)
	}
}

func TestPublishDeniedOutsideGrant(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"pub": ident(ta, "pub"), "sub": ident(ta, "sub")},
		map[string]acl.Rules{"pub": {Publish: []string{"allowed/#"}}, "sub": {Subscribe: []string{"#"}}})
	addr := b.Addr().String()

	sub, _, err := connect5(t, addr, "sub", "sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "#"))

	pub, _, err := connect5(t, addr, "pub", "pub", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0x87), publish5(t, pub, "secret/x", "no"))

	// The pub's packets are processed in order: the sentinel comes after the
	// denied publish, so it must be the subscriber's first message.
	require.Equal(t, byte(0), publish5(t, pub, "allowed/s", "s"))
	assert.Equal(t, message{Topic: "allowed/s", Payload: "s"}, recv(t, sub.msgs))
}

func TestSubscribeDeniedOutsideGrant(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a": ident(ta, "a")},
		map[string]acl.Rules{"a": {Subscribe: []string{"telemetry/+/temp"}}})

	c, _, err := connect5(t, b.Addr().String(), "a", "a", true)
	require.NoError(t, err)

	assert.Equal(t, []byte{0x87}, subscribe5(t, c, "telemetry/#"))
	assert.Equal(t, []byte{1, 1}, subscribe5(t, c, "telemetry/+/temp", "telemetry/pump/temp"))
}

func TestRateLimitQuotaExceeded(t *testing.T) {
	// One token, and none refills during the test, even on a slow runner.
	ta := newTenant(tenant.Limits{MessagesPerSecond: 0.01, Burst: 1})
	tb := newTenant(tenant.Limits{MessagesPerSecond: 0.01, Burst: 1})
	b := startBroker(t,
		map[string]*auth.Identity{"a": ident(ta, "a"), "a-sub": ident(ta, "a-sub"), "b": ident(tb, "b")},
		map[string]acl.Rules{
			"a":     {Publish: []string{"#"}},
			"a-sub": {Subscribe: []string{"#"}},
			"b":     {Publish: []string{"#"}},
		})
	addr := b.Addr().String()

	sub, _, err := connect5(t, addr, "a-sub", "a-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "#"))

	a, _, err := connect5(t, addr, "a", "a", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publish5(t, a, "t", "one"))
	assert.Equal(t, byte(0x97), publish5(t, a, "t", "two"), "quota exceeded")

	// Over the limit, QoS 0 is dropped too. The QoS 1 publish after it is
	// refused, which proves the QoS 0 one was processed first.
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err = a.Publish(ctx, &paho.Publish{Topic: "t", QoS: 0, Payload: []byte("three")})
	require.NoError(t, err)
	assert.Equal(t, byte(0x97), publish5(t, a, "t", "four"))

	// Another tenant's budget is its own.
	bc, _, err := connect5(t, addr, "b", "b", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0), publish5(t, bc, "t", "b-one"))

	// The broker's own publishes are never limited.
	require.NoError(t, b.server.Publish(topic.Namespace(ta.ID.String(), "t"), []byte("sentinel"), false, 0))

	assert.Equal(t, message{Topic: "t", Payload: "one"}, recv(t, sub.msgs))
	assert.Equal(t, message{Topic: "t", Payload: "sentinel"}, recv(t, sub.msgs), "nothing over the limit was delivered")
}

func TestStats(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"pub": ident(ta, "pub"), "sub": ident(ta, "sub"), "b": ident(tb, "b")},
		map[string]acl.Rules{"pub": {Publish: []string{"t", "r"}}, "sub": {Subscribe: []string{"t"}}})
	addr := b.Addr().String()

	sub, _, err := connect5(t, addr, "sub", "sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "t"))

	pub, _, err := connect5(t, addr, "pub", "pub", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publish5(t, pub, "t", "x"))
	require.Equal(t, message{Topic: "t", Payload: "x"}, recv(t, sub.msgs))

	st := b.Stats(ta.ID.String())
	assert.Equal(t, uint64(1), st.MessagesIn)
	assert.Equal(t, uint64(1), st.MessagesOut)
	assert.Equal(t, 2, st.Connected)
	assert.Equal(t, 2, st.Sessions)
	assert.Equal(t, 1, st.Subscriptions)
	assert.Equal(t, 0, st.Retained)

	// A retained message counts for its tenant only.
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err = pub.Publish(ctx, &paho.Publish{Topic: "r", QoS: 1, Retain: true, Payload: []byte("kept")})
	require.NoError(t, err)
	assert.Equal(t, 1, b.Stats(ta.ID.String()).Retained)
	assert.Equal(t, uint64(2), b.Stats(ta.ID.String()).MessagesIn)

	_, _, err = connect5(t, addr, "b", "b", true)
	require.NoError(t, err)
	assert.Equal(t, Stats{Connected: 1, Sessions: 1}, b.Stats(tb.ID.String()))
	assert.Equal(t, Stats{}, b.Stats("ttnt_nobody"))
}

func TestStatsBeforeStart(t *testing.T) {
	b, err := New(testConfig(), Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}})
	require.NoError(t, err)
	assert.Equal(t, Stats{}, b.Stats(newTenant(tenant.Limits{}).ID.String()))
	assert.Equal(t, defaultSysInterval, b.sysInterval)
	assert.Equal(t, 10*time.Second, defaultSysInterval)
}

// unitHook is a hook on a broker and a mochi server that are not started, for
// calling hook methods directly.
func unitHook(t *testing.T, limits tenant.Limits) (h *hook, srv *mqtt.Server, ta *tenant.Tenant) {
	t.Helper()

	b, err := New(testConfig(), Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}, Logger: testLogger()})
	require.NoError(t, err)

	srv = mqtt.New(&mqtt.Options{InlineClient: true, Logger: testLogger()})
	b.server = srv

	return &hook{b: b, server: srv}, srv, newTenant(limits)
}

// netClient is a mochi network client over one end of a pipe. The other end
// is returned for reading what the broker writes.
func netClient(t *testing.T, srv *mqtt.Server, nsID string, version byte) (*mqtt.Client, net.Conn) {
	t.Helper()

	server, peer := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = peer.Close()
	})

	cl := srv.NewClient(server, "tcp", nsID, false)
	cl.Properties.ProtocolVersion = version

	return cl, peer
}

// bindState gives cl live connection state in tn with rules.
func bindState(h *hook, cl *mqtt.Client, tn *tenant.Tenant, rawID string, rules acl.Rules) {
	h.b.state.add(newClientState(cl, rawID, "", ident(tn, rawID), tn,
		&grants{acl: acl.Compile(rules, acl.Vars{ClientID: rawID})},
		h.b.deps.Limits, h.b.countersFor(tn.ID.String())))
}

// Controller ruling R8: a closed client without state is an offline session.
// It may receive inside its own tenant only, and never write.
func TestACLCheckOfflineSession(t *testing.T) {
	h, srv, ta := unitHook(t, tenant.Limits{})
	tb := newTenant(tenant.Limits{})
	aNS, bNS := ta.ID.String(), tb.ID.String()

	cl, _ := netClient(t, srv, aNS+"/keeper", 5)
	cl.Stop(nil)
	require.True(t, cl.Closed())
	require.Nil(t, h.b.state.get(cl))

	assert.True(t, h.OnACLCheck(cl, topic.Namespace(aNS, "t"), false), "own tenant read")
	assert.True(t, h.OnACLCheck(cl, topic.Namespace(aNS, "$share/g/t"), false), "own tenant shared read")

	assert.False(t, h.OnACLCheck(cl, topic.Namespace(aNS, "t"), true), "no write, even in its own tenant")
	assert.False(t, h.OnACLCheck(cl, topic.Namespace(bNS, "t"), false), "another tenant's name")
	assert.False(t, h.OnACLCheck(cl, topic.Namespace(bNS, "$SYS/clients/connected"), false), "another tenant's $SYS")
	assert.False(t, h.OnACLCheck(cl, "t", false), "a name outside every namespace")
	assert.False(t, h.OnACLCheck(cl, "$SYS/broker/uptime", false), "mochi's $SYS")

	// An ID that does not carry a tenant gets nothing.
	bare, _ := netClient(t, srv, "keeper", 5)
	bare.Stop(nil)
	assert.False(t, h.OnACLCheck(bare, "keeper/t", false))

	// A live client without state is not an offline session.
	live, _ := netClient(t, srv, aNS+"/live", 5)
	assert.False(t, h.OnACLCheck(live, topic.Namespace(aNS, "t"), false))
}

// The tenant's own $SYS is readable by every client of the tenant, grants or
// not, and writable by none. Nothing else under $SYS is readable.
func TestACLCheckTenantSys(t *testing.T) {
	h, srv, ta := unitHook(t, tenant.Limits{})
	tb := newTenant(tenant.Limits{})
	aNS, bNS := ta.ID.String(), tb.ID.String()

	cl, _ := netClient(t, srv, aNS+"/dev", 5)
	bindState(h, cl, ta, "dev", acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}})

	for _, name := range []string{"$SYS/#", "$SYS/clients/connected", "$SYS/+/count", "$share/g/$SYS/#"} {
		assert.True(t, h.OnACLCheck(cl, topic.Namespace(aNS, name), false), name)
		assert.False(t, h.OnACLCheck(cl, topic.Namespace(aNS, name), true), "write "+name)
	}

	assert.False(t, h.OnACLCheck(cl, topic.Namespace(bNS, "$SYS/clients/connected"), false))
	assert.False(t, h.OnACLCheck(cl, "$SYS/broker/uptime", false))
	assert.False(t, h.OnACLCheck(cl, "$SYS/#", false))
	assert.False(t, h.OnACLCheck(cl, topic.Namespace(aNS, "$sys/clients/connected"), false), "the allowance is for $SYS exactly")

	none, _ := netClient(t, srv, aNS+"/none", 5)
	bindState(h, none, ta, "none", acl.Rules{})
	assert.True(t, h.OnACLCheck(none, topic.Namespace(aNS, "$SYS/#"), false), "no grants needed")
	assert.False(t, h.OnACLCheck(none, topic.Namespace(aNS, "t"), false))
}

// Controller ruling 1: a PUBLISH that does not strip with the receiver's
// tenant must not leave, neither its topic nor its payload nor its properties.
func TestOutboundStripFailureFailsClosed(t *testing.T) {
	h, srv, ta := unitHook(t, tenant.Limits{})
	tb := newTenant(tenant.Limits{})
	bNS := tb.ID.String()

	cl, _ := netClient(t, srv, ta.ID.String()+"/dev", 5)
	bindState(h, cl, ta, "dev", acl.Rules{Subscribe: []string{"#"}})

	pk := packets.Packet{
		FixedHeader: packets.FixedHeader{Type: packets.Publish},
		TopicName:   topic.Namespace(bNS, "secret"),
		Payload:     []byte("secret payload"),
		Properties: packets.Properties{
			ResponseTopic:   topic.Namespace(bNS, "reply"),
			CorrelationData: []byte("secret correlation"),
			User:            []packets.UserProperty{{Key: "k", Val: "secret"}},
		},
	}

	out := h.OnPacketEncode(cl, pk)
	assert.Equal(t, withheldTopic, out.TopicName)
	assert.NotContains(t, out.TopicName, bNS)
	assert.Empty(t, out.Payload)
	assert.Equal(t, packets.Properties{}, out.Properties)
	assert.True(t, cl.Closed(), "the client is stopped")
	assert.Equal(t, packets.ErrImplementationSpecificError, cl.StopCause())
	assert.Equal(t, uint64(0), h.b.Stats(ta.ID.String()).MessagesOut, "a withheld packet is not counted")

	// Through mochi's own write path, not one byte reaches the peer.
	require.NoError(t, srv.AddHook(h, nil))

	cl2, peer2 := netClient(t, srv, ta.ID.String()+"/dev2", 5)
	bindState(h, cl2, ta, "dev2", acl.Rules{Subscribe: []string{"#"}})

	read := make(chan []byte, 1)

	go func() {
		got, _ := io.ReadAll(peer2)
		read <- got
	}()

	require.Error(t, cl2.WritePacket(pk))
	_ = peer2.Close()

	select {
	case got := <-read:
		assert.Empty(t, got)
	case <-time.After(waitTimeout):
		t.Fatal("peer read never finished")
	}
}

// OnPublish error semantics (mochi-facts "PUBLISH path"): a bare
// ErrQuotaExceeded only for 5.0 QoS 1; ErrRejectPacket for everything else, or
// mochi would publish the message anyway. The inline client is never limited.
func TestOnPublishRateLimitErrors(t *testing.T) {
	h, srv, ta := unitHook(t, tenant.Limits{MessagesPerSecond: 0.001, Burst: 1})
	aNS := ta.ID.String()

	pk := func(qos byte) packets.Packet {
		return packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish, Qos: qos}, TopicName: topic.Namespace(aNS, "t")}
	}

	v5, _ := netClient(t, srv, aNS+"/v5", 5)
	bindState(h, v5, ta, "v5", acl.Rules{Publish: []string{"#"}})

	v3, _ := netClient(t, srv, aNS+"/v3", 4)
	bindState(h, v3, ta, "v3", acl.Rules{Publish: []string{"#"}})

	_, err := h.OnPublish(v5, pk(1))
	require.NoError(t, err, "within the burst")

	_, err = h.OnPublish(v5, pk(1))

	var code packets.Code

	require.True(t, errors.As(err, &code))
	assert.Equal(t, packets.ErrQuotaExceeded, err, "a bare code, mochi type-asserts it")

	for _, tc := range []struct {
		name string
		cl   *mqtt.Client
		qos  byte
	}{
		{"5.0 QoS 0", v5, 0},
		{"5.0 QoS 2", v5, 2},
		{"3.1.1 QoS 0", v3, 0},
		{"3.1.1 QoS 1", v3, 1},
		{"3.1.1 QoS 2", v3, 2},
	} {
		_, err = h.OnPublish(tc.cl, pk(tc.qos))
		assert.Equal(t, packets.ErrRejectPacket, err, tc.name)
	}

	inline := srv.NewClient(nil, "local", "inline", true)
	_, err = h.OnPublish(inline, pk(1))
	require.NoError(t, err, "the inline client is never limited")

	assert.Equal(t, uint64(1), h.b.Stats(aNS).MessagesIn, "only the accepted publish counts")
}
