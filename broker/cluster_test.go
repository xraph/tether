package broker

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// A QoS 0 message published on A reaches a subscriber of the same tenant on
// B, with its topic stripped and its user properties, for plain and "$"
// topics alike.
func TestQoS0AcrossNodes(t *testing.T) {
	c := startCluster(t, 2)

	sub, _, err := connect5(t, c.addr(1), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, sub, "a/+", "$app/#"))

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	awaitInterest(t, pub, sub)

	publish0(t, pub, "a/b", "hello", &paho.PublishProperties{
		User: paho.UserProperties{{Key: "k", Value: "v1"}, {Key: "k2", Value: "v2"}},
	})

	m := recvMsg(t, sub)
	require.Equal(t, "a/b", m.Topic)
	require.Equal(t, "hello", m.Payload)
	require.Equal(t, map[string]string{"k": "v1", "k2": "v2"}, m.User)

	publish0(t, pub, "$app/x", "dollar", nil)

	m = recvMsg(t, sub)
	require.Equal(t, "$app/x", m.Topic)
	require.Equal(t, "dollar", m.Payload)
}

// Review Focus 3 for QoS 0: two tenants with the same raw client IDs and the
// same topics on two nodes never see each other's messages. (Within a tenant
// the subscriber and the publisher have different IDs: one ID on two nodes
// is one session, which the second CONNECT takes over.)
func TestQoS0TenantIsolationAcrossNodes(t *testing.T) {
	c := startCluster(t, 2)

	subs := map[string]*client5{}
	pubs := map[string]*client5{}

	for _, user := range []string{"u1", "u2"} {
		s, _, err := connect5(t, c.addr(1), "dev-sub", user, true)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 1}, subscribe5(t, s, "#", "x/y"))

		p, _, err := connect5(t, c.addr(0), "dev-pub", user, true)
		require.NoError(t, err)

		awaitInterest(t, p, s)

		subs[user], pubs[user] = s, p
	}

	for _, user := range []string{"u1", "u2"} {
		publish0(t, pubs[user], "x/y", "from-"+user, nil)
	}

	for _, user := range []string{"u1", "u2"} {
		publish0(t, pubs[user], "x/end", "end-"+user, nil)
	}

	// Two topics, so their order is not guaranteed: "#" and "x/y" are two
	// NATS subscriptions on B, each with its own dispatch goroutine.
	for _, user := range []string{"u1", "u2"} {
		got := []message{recvMsg(t, subs[user]), recvMsg(t, subs[user])}
		require.ElementsMatch(t, []message{{Topic: "x/y", Payload: "from-" + user}, {Topic: "x/end", Payload: "end-" + user}}, got)
	}

	for _, user := range []string{"u1", "u2"} {
		noMoreMsgs(t, subs[user])
	}
}

// A subscriber whose filters overlap gets one copy of a message published on
// its own node and one of a message published on another node.
func TestQoS0NoDoubleDelivery(t *testing.T) {
	c := startCluster(t, 2)

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1, 1}, subscribe5(t, sub, "a/+", "a/#", "+/b"))

	local, _, err := connect5(t, c.addr(0), "local", "u1", true)
	require.NoError(t, err)

	remote, _, err := connect5(t, c.addr(1), "remote", "u1", true)
	require.NoError(t, err)

	awaitInterest(t, remote, sub)

	publish0(t, local, "a/b", "local", nil)
	publish0(t, remote, "a/b", "remote", nil)
	publish0(t, remote, "a/end", "remote-end", nil)

	got := map[string]int{}

	for len(got) < 3 || got["remote-end"] == 0 {
		got[recvMsg(t, sub).Payload]++
	}

	noMoreMsgs(t, sub)

	require.Equal(t, map[string]int{"local": 1, "remote": 1, "remote-end": 1}, got)
}

// What the injector publishes is never routed again: a node that only
// receives routed messages publishes nothing to the cell. The host's own
// QoS 0 publishes do go out.
func TestInjectorNotRepublished(t *testing.T) {
	c := startCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	sub, _, err := connect5(t, c.addr(1), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "a/#"))

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	awaitInterest(t, pub, sub)

	sentByA := a.cluster.published.Load()
	sentByB := b.cluster.published.Load()

	for range 5 {
		publish0(t, pub, "a/b", "routed", nil)
	}

	for range 5 {
		require.Equal(t, "routed", recvMsg(t, sub).Payload)
	}

	// A message handed to B directly, as its Router would, is injected and
	// delivered, and not published again either.
	b.deliverRouted(c.t1.ID.String(), "a/direct", []byte("direct"), cluster.Props{}, false)
	require.Equal(t, message{Topic: "a/direct", Payload: "direct"}, recvMsg(t, sub))

	require.Equal(t, sentByA+5, a.cluster.published.Load())
	require.Equal(t, sentByB, b.cluster.published.Load(), "the injector's messages went back to the cell")

	// The host's QoS 0 publish on B is routed: mochi's inline client is not
	// the injector.
	require.NoError(t, b.Publish(t.Context(), c.t1.ID.String(), "a/host", []byte("host"), 0, false))
	require.Equal(t, message{Topic: "a/host", Payload: "host"}, recvMsg(t, sub))
	require.Equal(t, sentByB+1, b.cluster.published.Load())
	require.Zero(t, a.cluster.routeErrors.Load()+b.cluster.routeErrors.Load())
}

// The host's QoS 0 publishes (Publish, DevicePublish) reach subscribers on
// other nodes. The tenant $SYS stats each node publishes do not leave it.
func TestHostPublishQoS0AcrossNodes(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}

	c := startClusterWith(t, 2, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) { b.sysInterval = 20 * time.Millisecond })
	a := c.nodes[0]

	sub, _, err := connect5(t, c.addr(1), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "h/#"))

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	awaitInterest(t, pub, sub)

	// A has a connection of t1, so it publishes t1's $SYS stats every 20 ms.
	base := a.cluster.published.Load()
	time.Sleep(10 * a.sysInterval)
	require.Equal(t, base, a.cluster.published.Load(), "$SYS stats were routed")

	require.NoError(t, a.Publish(t.Context(), t1.ID.String(), "h/admin", []byte("admin"), 0, false))
	require.NoError(t, a.DevicePublish(t.Context(), t1.ID.String(), "h/device", []byte("device"), 0, false))

	require.Equal(t, message{Topic: "h/admin", Payload: "admin"}, recvMsg(t, sub))
	require.Equal(t, message{Topic: "h/device", Payload: "device"}, recvMsg(t, sub))
}

// Without a cluster the broker is the Phase 1 broker: none of the cluster
// hook methods, no cluster state, and the storage hook when DataDir is set.
// The rest of the package's tests run it that way.
func TestNoClusterUnchanged(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	cfg := testConfig()
	cfg.DataDir = t.TempDir()

	b := startBrokerWith(t, cfg, map[string]*auth.Identity{"u": ident(ta, "u")}, nil, nil)
	require.Nil(t, b.cluster)
	require.NotNil(t, b.storage)

	h := &hook{b: b}
	for _, m := range []byte{mqtt.OnSubscribed, mqtt.OnUnsubscribed, mqtt.OnClientExpired, mqtt.OnSubscribe, mqtt.OnRetainMessage} {
		require.False(t, h.Provides(m), "hook provides %d without a cluster", m)
	}

	for _, m := range provided {
		require.True(t, h.Provides(m))
	}

	// The same hook in cluster mode provides the three route methods.
	h = &hook{b: &Broker{cluster: &clusterNode{}}}
	for _, m := range clusterProvided {
		require.True(t, h.Provides(m))
	}
}

// With a cluster, DataDir is ignored: no storage hook.
func TestClusterIgnoresDataDir(t *testing.T) {
	cfg := testConfig()
	cfg.DataDir = t.TempDir()

	c := startClusterWith(t, 1, cfg, map[string]*auth.Identity{}, fakePerms{}, nil)
	require.Nil(t, c.nodes[0].storage)
}

// New refuses a cluster without a connection or with a disabled or invalid
// config.
func TestNewClusterValidation(t *testing.T) {
	deps := func(c *Cluster) Deps {
		return Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}, Cluster: c}
	}

	_, err := New(testConfig(), deps(&Cluster{}))
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	conn := &cluster.Conn{}

	_, err = New(testConfig(), deps(&Cluster{Conn: conn}))
	require.ErrorIs(t, err, tether.ErrInvalidInput)

	ccfg := cluster.DefaultConfig()
	ccfg.Enabled = true

	_, err = New(testConfig(), deps(&Cluster{Conn: conn, Config: ccfg}))
	require.ErrorIs(t, err, tether.ErrInvalidInput)
}

// Ruling R13: the cell carries topics of at most 1024 bytes. With a longer
// MaxTopicLength, a longer PUBLISH topic is refused like any invalid one
// (5.0: DISCONNECT 0x90; 3.1.1: the connection closes) and a longer filter
// gets SUBACK 0x8F.
func TestClusterTopicLength(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	cfg := testConfig()
	cfg.MaxTopicLength = 4096

	c := startClusterWith(t, 1, cfg, map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)
	b := c.nodes[0]

	long := "a/" + strings.Repeat("x", 1500)

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{packets.ErrTopicFilterInvalid.Code, 1}, subscribe5(t, sub, long, "a/ok"))
	require.Equal(t, []string{"a/ok"}, b.cluster.routed(namespacedID(t1.ID.String(), "sub")))

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, _ = pub.Publish(ctx, &paho.Publish{Topic: long, QoS: 0, Payload: []byte("x")})

	select {
	case d := <-pub.disconnects:
		require.Equal(t, packets.ErrTopicNameInvalid.Code, d.ReasonCode)
	case <-time.After(waitTimeout):
		t.Fatal("no DISCONNECT for an over-long topic")
	}

	c3, err := connect311(t, c.addr(0), "v3", "u1", nil)
	require.NoError(t, err)

	tok := c3.Publish(long, 0, false, "x")
	tok.WaitTimeout(waitTimeout)
	eventually(t, "the 3.1.1 client to be disconnected", func() bool { return !c3.IsConnectionOpen() })

	require.Zero(t, b.cluster.published.Load())
}

// A filter whose Router subscribe fails is refused in the SUBACK (0x80), is
// not stored by mochi, gets no retained message and is counted.
func TestRouteFailureRefusesFilter(t *testing.T) {
	c := startCluster(t, 1)
	b := c.nodes[0]

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err = pub.Publish(ctx, &paho.Publish{Topic: "r/kept", QoS: 1, Retain: true, Payload: []byte("retained")})
	require.NoError(t, err)

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "ok/+"))

	// A closed Router refuses every subscribe.
	b.cluster.router.Close()

	before := b.cluster.routeErrors.Load()
	require.Equal(t, []byte{packets.ErrUnspecifiedError.Code}, subscribe5(t, sub, "r/#"))
	require.Greater(t, b.cluster.routeErrors.Load(), before)

	nsID := namespacedID(c.t1.ID.String(), "sub")
	cl, ok := b.server.Clients.Get(nsID)
	require.True(t, ok)

	_, held := cl.State.Subscriptions.Get(topic.Namespace(c.t1.ID.String(), "r/#"))
	require.False(t, held, "mochi kept the refused subscription")
	require.Equal(t, []string{"ok/+"}, b.cluster.routed(nsID))
	require.Empty(t, b.server.Topics.Subscribers(topic.Namespace(c.t1.ID.String(), "r/x")).Subscriptions)

	noMoreMsgs(t, sub)
}

// Every path that ends a subscription gives its Router reference back,
// once: UNSUBSCRIBE, a clean session ending, a persistent session going
// offline (it is evicted from mochi and lives in the cell), mochi's 5.0
// session expiry, the 3.1.1 expiry sweep, a takeover by a clean session and
// pruning after a grant is revoked.
func TestRoutesReleased(t *testing.T) {
	t1 := newTenant(tenant.Limits{MaxSessionExpiry: 2 * time.Second})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	perms := newMutablePerms(map[string]acl.Rules{"u1": all})

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")}, perms, nil)
	b := c.nodes[0]
	tid := t1.ID.String()
	nc := b.cluster.Conn.NC()

	// The tenant's log reader and its retained watch hold NATS
	// subscriptions of their own from the first connection on; count from
	// there.
	require.NoError(t, b.ensureReader(tid))
	b.cluster.ensureRetained(tid)
	waitRetainedLoaded(t, b, tid)

	base := nc.NumSubscriptions()

	routed := func(raw string) []string {
		got := b.cluster.routed(namespacedID(tid, raw))
		slices.Sort(got)

		return got
	}

	waitRouted := func(raw string, want ...string) {
		t.Helper()
		eventually(t, raw+" routes "+strings.Join(want, ","), func() bool {
			got := routed(raw)

			return len(got) == len(want) && (len(want) == 0 || slices.Equal(got, want))
		})
	}

	t.Run("unsubscribe and persistent offline", func(t *testing.T) {
		p, _, err := connect5(t, c.addr(0), "p", "u1", false)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 1}, subscribe5(t, p, "a/b", "c/+"))
		require.Equal(t, []string{"a/b", "c/+"}, routed("p"))
		require.Equal(t, base+2, nc.NumSubscriptions())

		// A second SUBSCRIBE of the same filter holds no second reference.
		require.Equal(t, []byte{1}, subscribe5(t, p, "a/b"))

		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		defer cancel()

		_, err = p.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{"c/+", "never/subscribed"}})
		require.NoError(t, err)
		require.Equal(t, []string{"a/b"}, routed("p"))
		require.Equal(t, base+1, nc.NumSubscriptions())

		// Offline, the session is evicted to the cell and its route goes.
		require.NoError(t, p.Disconnect(&paho.Disconnect{ReasonCode: 0}))
		waitRouted("p")
		// The Router call runs right after the reference goes.
		eventually(t, "the route's NATS subscription to go", func() bool { return nc.NumSubscriptions() == base })

		require.NoError(t, b.DeleteSession(tid, "p"))
		require.Empty(t, routed("p"))
	})

	t.Run("clean session ends", func(t *testing.T) {
		cl, _, err := connect5(t, c.addr(0), "clean", "u1", true)
		require.NoError(t, err)
		require.Equal(t, []byte{1}, subscribe5(t, cl, "d/#"))
		require.Equal(t, []string{"d/#"}, routed("clean"))

		require.NoError(t, cl.Disconnect(&paho.Disconnect{ReasonCode: 0}))
		waitRouted("clean")
		require.Equal(t, base, nc.NumSubscriptions())
	})

	t.Run("mochi expires a 5.0 session", func(t *testing.T) {
		cl, _ := connect5Expiry(t, c.addr(0), "exp5", "u1", 1)
		require.Equal(t, []byte{1}, subscribe5(t, cl, "e/+"))
		require.NoError(t, cl.Disconnect(&paho.Disconnect{ReasonCode: 0}))

		waitRouted("exp5")
		require.Equal(t, base, nc.NumSubscriptions())
	})

	t.Run("expiry sweep ends a 3.1.1 session", func(t *testing.T) {
		v3 := connect311Persistent(t, c.addr(0), "exp3", "u1")
		_ = subscribe311(t, v3, "f/#")
		require.Equal(t, []string{"f/#"}, routed("exp3"))
		v3.Disconnect(0)

		// The tenant caps the session at 2 s; the sweep then purges it.
		waitRouted("exp3")
	})

	t.Run("clean takeover", func(t *testing.T) {
		p, _, err := connect5(t, c.addr(0), "tk", "u1", false)
		require.NoError(t, err)
		require.Equal(t, []byte{1}, subscribe5(t, p, "g/+"))
		require.NoError(t, p.Disconnect(&paho.Disconnect{ReasonCode: 0}))

		_, ca, err := connect5(t, c.addr(0), "tk", "u1", true)
		require.NoError(t, err)
		require.False(t, ca.SessionPresent)
		waitRouted("tk")
	})

	t.Run("prune after a revoked grant", func(t *testing.T) {
		p, _, err := connect5(t, c.addr(0), "pr", "u1", true)
		require.NoError(t, err)
		require.Equal(t, []byte{1, 1}, subscribe5(t, p, "keep/+", "drop/+"))

		perms.set("u1", acl.Rules{Publish: []string{"#"}, Subscribe: []string{"keep/#"}})
		b.ReloadPrincipal(t.Context(), "akey_u1")
		require.Equal(t, []string{"keep/+"}, routed("pr"))

		perms.set("u1", all)
	})

	require.Zero(t, b.cluster.routeErrors.Load(), "a route was released twice or never taken")
}

// The route bookkeeping takes one reference per session and filter and
// releases each once, however often a release repeats.
func TestRouteRefsExactlyOnce(t *testing.T) {
	c := startCluster(t, 1)
	n := c.nodes[0].cluster
	nc := n.Conn.NC()
	base := nc.NumSubscriptions()
	tid := c.t1.ID.String()

	require.NoError(t, n.subscribe("s1", tid, "a/b"))
	require.NoError(t, n.subscribe("s1", tid, "a/b"))
	require.NoError(t, n.subscribe("s2", tid, "a/b"))
	require.Equal(t, base+1, nc.NumSubscriptions())

	n.release("s1", tid, []string{"a/b"})
	n.release("s1", tid, []string{"a/b"})
	n.releaseAll("s1", tid)
	require.Equal(t, base+1, nc.NumSubscriptions(), "s2 still holds the route")

	n.releaseAll("s2", tid)
	n.releaseAll("s2", tid)
	require.Equal(t, base, nc.NumSubscriptions())
	require.Zero(t, n.routeErrors.Load())
	require.Empty(t, n.routes)

	// A failed subscribe leaves no reference behind.
	err := n.subscribe("s3", tid, "$share/g/a")
	require.ErrorIs(t, err, tether.ErrInvalidInput)
	require.Empty(t, n.routed("s3"))
}

// Messages off NATS that no tenant client could have sent are dropped:
// a tenant ID that is not canonical, a $SYS name, an invalid name.
func TestDeliverRoutedDropsForeign(t *testing.T) {
	c := startCluster(t, 1)
	b := c.nodes[0]
	tid := c.t1.ID.String()

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, sub, "#", "$SYS/#"))

	b.deliverRouted("not-a-tenant", "a", []byte("x"), cluster.Props{}, false)
	b.deliverRouted(strings.ToUpper(tid), "a", []byte("x"), cluster.Props{}, false)
	b.deliverRouted(tid, "$SYS/clients/connected", []byte("999"), cluster.Props{}, false)
	b.deliverRouted(tid, "$sys/x", []byte("x"), cluster.Props{}, false)
	b.deliverRouted(tid, "a/+", []byte("x"), cluster.Props{}, false)
	b.deliverRouted(tid, "$share/g/a", []byte("x"), cluster.Props{}, false)
	b.deliverRouted(tid, "ok", []byte("sentinel"), cluster.Props{}, false)

	require.Equal(t, message{Topic: "ok", Payload: "sentinel"}, recvMsg(t, sub))
	noMoreMsgs(t, sub)

	// After Stop nothing is injected, and nothing fails.
	require.NoError(t, b.Stop(t.Context()))
	b.deliverRouted(tid, "ok", []byte("late"), cluster.Props{}, false)
	require.True(t, errors.Is(b.inject(tid, "ok", nil, 0, cluster.Props{}, "", 0, false), errNotInjecting))
}

// Ruling R3: the publish properties survive mochi to cluster and back.
func TestPropsRoundTrip(t *testing.T) {
	zero := byte(0)
	one := byte(1)

	for _, p := range []cluster.Props{
		{},
		{
			User:            []cluster.UserProperty{{Key: "k", Value: "1"}, {Key: "k", Value: "2"}, {Key: "", Value: "line\r\nbreak"}},
			ContentType:     "application/json",
			ResponseTopic:   "reply/to",
			CorrelationData: []byte{0, 1, 2, 0xff},
			MessageExpiry:   60,
			PayloadFormat:   &one,
		},
		{PayloadFormat: &zero},
	} {
		require.Equal(t, p, clusterProps(packetProps(p)))
	}

	pp := packets.Properties{
		User:                  []packets.UserProperty{{Key: "a", Val: "b"}, {Key: "a", Val: "c"}},
		ContentType:           "text/plain",
		ResponseTopic:         "r",
		CorrelationData:       []byte("cd"),
		MessageExpiryInterval: 5,
		PayloadFormat:         1,
		PayloadFormatFlag:     true,
	}
	require.Equal(t, pp, packetProps(clusterProps(pp)))

	// No payload format flag: the value is not carried.
	require.Nil(t, clusterProps(packets.Properties{PayloadFormat: 1}).PayloadFormat)
}

// Mochi calls OnUnsubscribed for an UNSUBSCRIBE it refused (packet
// identifier in use) without removing anything; a filter the session still
// holds keeps its route.
func TestUnsubscribedKeepsHeldFilters(t *testing.T) {
	c := startCluster(t, 1)
	b := c.nodes[0]
	tid := c.t1.ID.String()
	nsID := namespacedID(tid, "sub")

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, sub, "a/b", "c/d"))

	cl, ok := b.server.Clients.Get(nsID)
	require.True(t, ok)

	// "c/d" is gone from the session, "a/b" is not.
	cl.State.Subscriptions.Delete(topic.Namespace(tid, "c/d"))

	h := &hook{b: b, server: b.server}
	h.OnUnsubscribed(cl, packets.Packet{Filters: packets.Subscriptions{
		{Filter: topic.Namespace(tid, "a/b")},
		{Filter: topic.Namespace(tid, "c/d")},
	}})

	require.Equal(t, []string{"a/b"}, b.cluster.routed(nsID))
}

// A reload's prune can remove a subscription between mochi storing it and
// OnSubscribed taking its reference; the prune then finds nothing to
// release. OnSubscribed sees the filter gone and gives the reference back,
// so no route and no NATS interest outlive the subscription.
func TestSubscribedAfterPruneLeavesNoRoute(t *testing.T) {
	c := startCluster(t, 1)
	b := c.nodes[0]
	tid := c.t1.ID.String()
	nsID := namespacedID(tid, "sub")

	_, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)

	cl, ok := b.server.Clients.Get(nsID)
	require.True(t, ok)

	// The connection started the tenant's retained watch, which holds a
	// NATS subscription of its own; count from there.
	waitRetainedLoaded(t, b, tid)

	nc := b.cluster.Conn.NC()
	base := nc.NumSubscriptions()
	filter := topic.Namespace(tid, "x/y")

	// Mochi's Add, then the prune: gone from the session, its release found
	// no reference. OnSubscribed runs after both.
	cl.State.Subscriptions.Add(filter, packets.Subscription{Filter: filter})
	b.unsubscribe(b.server, cl, func(f string) bool { return f == filter })
	require.Empty(t, b.cluster.routed(nsID))

	h := &hook{b: b, server: b.server}
	h.OnSubscribed(cl, packets.Packet{Filters: packets.Subscriptions{{Filter: filter}}}, []byte{1})

	require.Empty(t, b.cluster.routed(nsID))
	require.Equal(t, base, nc.NumSubscriptions())
	require.Zero(t, b.cluster.routeErrors.Load())
}
