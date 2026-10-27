package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pp "github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/topic"
)

// retMsg is a PUBLISH a retClient received, with its retain flag.
type retMsg struct {
	Topic   string
	Payload string
	Retain  bool
	User    string // the value of the user property "k", if any
	Expiry  uint32 // the message expiry interval, 0 for none
}

// retClient is a 5.0 test client that keeps the retain flag of what it
// receives.
type retClient struct {
	*paho.Client
	msgs <-chan retMsg
}

// dialRet connects a clean 5.0 client of user to addr.
func dialRet(t *testing.T, addr, clientID, user string) *retClient {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)

	msgs := make(chan retMsg, 1024)
	c := paho.NewClient(paho.ClientConfig{
		Conn: conn,
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				m := retMsg{Topic: pr.Packet.Topic, Payload: string(pr.Packet.Payload), Retain: pr.Packet.Retain}

				if p := pr.Packet.Properties; p != nil {
					m.User = p.User.Get("k")

					if p.MessageExpiry != nil {
						m.Expiry = *p.MessageExpiry
					}
				}

				msgs <- m

				return true, nil
			},
		},
	})

	// Generous: a CONNECT takes the session key in the cell, which a
	// loaded machine can slow well past waitTimeout.
	ctx, cancel := context.WithTimeout(t.Context(), logWait)
	defer cancel()

	_, err = c.Connect(ctx, &paho.Connect{
		ClientID: clientID, Username: user, UsernameFlag: true, Password: []byte("secret"), PasswordFlag: true,
		CleanStart: true, KeepAlive: 30,
	})
	require.NoError(t, err)

	t.Cleanup(func() { _ = c.Disconnect(&paho.Disconnect{ReasonCode: 0}) })

	return &retClient{Client: c, msgs: msgs}
}

// sub subscribes c to filter at QoS 1, with Retain As Published when rap.
func (c *retClient) sub(t *testing.T, filter string, rap bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	sa, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1, RetainAsPublished: rap}}})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, sa.Reasons)
}

// pub publishes a retained message at qos and waits for its ack (QoS 1);
// user, when not empty, is the user property "k", expiry the message expiry
// interval when not zero.
func (c *retClient) pub(t *testing.T, name, payload string, qos byte, user string, expiry uint32) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), logWait)
	defer cancel()

	props := &paho.PublishProperties{}
	if user != "" {
		props.User = paho.UserProperties{{Key: "k", Value: user}}
	}

	if expiry > 0 {
		props.MessageExpiry = &expiry
	}

	resp, err := c.Publish(ctx, &paho.Publish{Topic: name, QoS: qos, Retain: true, Payload: []byte(payload), Properties: props})
	require.NoError(t, err)

	if resp != nil {
		require.Zero(t, resp.ReasonCode)
	}
}

// next waits for c's next message, without its message expiry (mochi gives
// every message one, its maximum when the publisher set none).
func (c *retClient) next(t *testing.T) retMsg {
	t.Helper()

	m := c.nextFull(t)
	m.Expiry = 0

	return m
}

// nextFull waits for c's next message.
func (c *retClient) nextFull(t *testing.T) retMsg {
	t.Helper()

	deadline := time.After(logWait)

	for {
		select {
		case m := <-c.msgs:
			if strings.HasPrefix(m.Topic, probePrefix) {
				continue // a late interest probe
			}

			return m
		case <-deadline:
			t.Fatal("no message received")

			return retMsg{}
		}
	}
}

// none fails when c receives anything within settle.
func (c *retClient) none(t *testing.T) {
	t.Helper()

	select {
	case m := <-c.msgs:
		t.Fatalf("unexpected message %q on %q", m.Payload, m.Topic)
	case <-time.After(settle):
	}
}

// retained waits for c's retained copy of name (retain flag set), skipping
// live copies of it, and returns its payload.
func (c *retClient) retained(t *testing.T, name string) string {
	t.Helper()

	deadline := time.After(logWait)

	for {
		select {
		case m := <-c.msgs:
			if m.Topic == name && m.Retain {
				return m.Payload
			}

			if m.Topic != name {
				t.Fatalf("unexpected message %q on %q", m.Payload, m.Topic)
			}
		case <-deadline:
			t.Fatalf("no retained message on %q", name)

			return ""
		}
	}
}

// retainedN collects c's next n retained copies (retain flag set), by topic,
// skipping live copies.
func (c *retClient) retainedN(t *testing.T, n int) map[string]string {
	t.Helper()

	out := make(map[string]string, n)
	deadline := time.After(logWait)

	for len(out) < n {
		select {
		case m := <-c.msgs:
			if m.Retain {
				out[m.Topic] = m.Payload
			}
		case <-deadline:
			t.Fatalf("%d of %d retained messages: %v", len(out), n, out)
		}
	}

	return out
}

// lag makes b's retained watch apply every report d late, as a watch that
// lags behind the store would.
func lag(t *testing.T, b *Broker, d time.Duration) {
	t.Helper()

	b.cluster.retainedApplyDelay.Store(int64(d))
	t.Cleanup(func() { b.cluster.retainedApplyDelay.Store(0) })
}

// cached reports whether node b's cache of tenantID holds the retained
// message name from its watch.
func cached(b *Broker, tenantID, name string) bool {
	return slices.Contains(b.cluster.retainedView(tenantID), topic.Namespace(tenantID, name))
}

// inMochi returns the payload mochi's store on b holds for tenantID's name.
func inMochi(b *Broker, tenantID, name string) (string, bool) {
	pk, ok := b.server.Topics.Retained.Get(topic.Namespace(tenantID, name))

	return string(pk.Payload), ok
}

// waitRetainedLoaded waits until b has loaded tenantID's retained messages.
func waitRetainedLoaded(t *testing.T, b *Broker, tenantID string) {
	t.Helper()

	eventually(t, "the tenant's retained messages to load", func() bool {
		c := b.cluster.cacheOf(tenantID)
		if c == nil {
			return false
		}

		select {
		case <-c.ready:
			return true
		default:
			return false
		}
	})
}

// kvTopics loads tenantID's retained topics from the cell's store.
func kvTopics(t *testing.T, b *Broker, tenantID string) map[string]string {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), logWait)
	defer cancel()

	msgs, err := b.cluster.Retained.Load(ctx, tenantID)
	require.NoError(t, err)

	out := make(map[string]string, len(msgs))
	for _, m := range msgs {
		out[m.Topic] = string(m.Payload)
	}

	return out
}

// Review Focus 5: a retained message set on node A is delivered to a
// SUBSCRIBE on node B right after, through the SUBACK flow. When B's first
// connection of the tenant starts the load, the SUBSCRIBE waits for it; when
// B has the tenant loaded already, with its watch lagging 300 ms, the
// SUBSCRIBE waits for the watch to catch up with the store (Sync). The
// retained flag and the user properties come through, and a client gets its
// own retained publish back on its node at once.
func TestRetainedAcrossNodes(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	pub := dialRet(t, c.addr(0), "pub", "u1")
	pub.pub(t, "r/x", "v1", 1, "a", 0)

	// B has no connection of t1 yet: this one starts the load.
	sub := dialRet(t, c.addr(1), "sub", "u1")
	sub.sub(t, "r/#", false)
	require.Equal(t, retMsg{Topic: "r/x", Payload: "v1", Retain: true, User: "a"}, sub.next(t))
	require.Zero(t, c.nodes[1].cluster.retainedWaitTimeouts.Load())

	// Warm B, its watch lagging: a SUBSCRIBE right after the PUBACK still
	// gets it.
	late := dialRet(t, c.addr(1), "late", "u1")
	h := dialRet(t, c.addr(1), "h", "u1")

	lag(t, c.nodes[1], 300*time.Millisecond)

	syncs := c.nodes[1].cluster.retainedSyncs.Load()

	pub.pub(t, "r/y", "v2", 1, "", 0)
	late.sub(t, "r/y", false)
	require.Equal(t, "v2", late.retained(t, "r/y"))
	require.Greater(t, c.nodes[1].cluster.retainedSyncs.Load(), syncs)
	require.Zero(t, c.nodes[1].cluster.retainedWaitTimeouts.Load())

	// QoS 0 too, and the publisher's own node has it at once.
	pub.pub(t, "r/z", "v3", 0, "", 0)
	pub.sub(t, "r/z", false)
	require.Equal(t, retMsg{Topic: "r/z", Payload: "v3", Retain: true}, pub.next(t))

	// A QoS 0 publish has no ack to order a SUBSCRIBE after, so wait for
	// the store.
	eventually(t, "r/z to reach B's cache", func() bool { return cached(c.nodes[1], tid, "r/z") })
	require.Equal(t, map[string]string{"r/x": "v1", "r/y": "v2", "r/z": "v3"}, kvTopics(t, c.nodes[0], tid))

	z := dialRet(t, c.addr(1), "z", "u1")
	z.sub(t, "r/z", false)
	require.Equal(t, retMsg{Topic: "r/z", Payload: "v3", Retain: true}, z.next(t))

	// The host's retained publishes go to the cell too, and a Publish that
	// returned is in every node's cache for the next SUBSCRIBE.
	require.NoError(t, c.nodes[0].Publish(t.Context(), tid, "h/q0", []byte("h0"), 0, true))
	require.NoError(t, c.nodes[0].Publish(t.Context(), tid, "h/q1", []byte("h1"), 1, true))

	h.sub(t, "h/#", false)
	require.Equal(t, map[string]string{"h/q0": "h0", "h/q1": "h1"}, h.retainedN(t, 2))

	require.Equal(t, []RetainedInfo{{Topic: "h/q0", Size: 2}, {Topic: "h/q1", Size: 2, QoS: 1}}, c.nodes[1].Retained(tid, "h/#"))
}

// A SUBSCRIBE waits for its tenant's load, up to the bound: with the load
// held 500 ms it still gets the retained message; held past the bound, it
// goes on without it and the timeout is counted.
func TestRetainedSubscribeWaitsForLoad(t *testing.T) {
	c := startCluster(t, 1)
	tid := c.t1.ID.String()

	pub := dialRet(t, c.addr(0), "pub", "u1")
	pub.pub(t, "w/x", "kept", 1, "", 0)

	// Stop the tenant's cache as the idle sweep would, and hold the load
	// the next CONNECT starts.
	b := c.nodes[0]
	n := b.cluster

	held := make(chan struct{})
	release := make(chan struct{})
	n.retainedLoadHook = func(string) {
		close(held)
		<-release
	}

	drop := func(id string, _ *retainedCache) bool { return id == tid }
	n.retire(drop)

	_, ok := inMochi(b, tid, "w/x")
	require.False(t, ok, "a stopped cache leaves mochi's store")

	sub := dialRet(t, c.addr(0), "sub", "u1")
	<-held

	subacked := make(chan error, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), logWait)
		defer cancel()

		_, err := sub.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "w/#", QoS: 1}}})
		subacked <- err
	}()

	select {
	case <-subacked:
		t.Fatal("the SUBACK came while the load was held")
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-subacked)
	require.Equal(t, retMsg{Topic: "w/x", Payload: "kept", Retain: true}, sub.next(t))
	require.Zero(t, n.retainedWaitTimeouts.Load())

	// Held past the bound: the SUBACK comes without it.
	n.retainedWait = 200 * time.Millisecond
	held2 := make(chan struct{})
	release2 := make(chan struct{})
	n.retainedLoadHook = func(string) {
		close(held2)
		<-release2
	}

	t.Cleanup(func() {
		select {
		case <-release2:
		default:
			close(release2)
		}
	})

	n.retire(drop)

	other := dialRet(t, c.addr(0), "other", "u1")
	<-held2
	other.sub(t, "w/#", false)
	require.Equal(t, uint64(1), n.retainedWaitTimeouts.Load())
	other.none(t)

	close(release2)
	waitRetainedLoaded(t, b, tid)

	_, ok = inMochi(b, tid, "w/x")
	require.True(t, ok)
}

// An empty retained publish on A, or DeleteRetained on either node, clears
// the message on every node: a SUBSCRIBE on the other node right after gets
// nothing, with that node's watch lagging 300 ms, and its cache has dropped
// it by the SUBACK.
func TestRetainedDeleteAcrossNodes(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	pub := dialRet(t, c.addr(0), "pub", "u1")
	first := dialRet(t, c.addr(1), "first", "u1")
	s := dialRet(t, c.addr(1), "s", "u1")
	onA := dialRet(t, c.addr(0), "onA", "u1")

	lag(t, c.nodes[1], 300*time.Millisecond)

	pub.pub(t, "d/x", "v", 1, "", 0)
	first.sub(t, "d/x", false)
	require.Equal(t, "v", first.retained(t, "d/x"))

	pub.pub(t, "d/x", "", 1, "", 0)
	s.sub(t, "d/#", false)
	s.none(t)
	require.False(t, cached(c.nodes[1], tid, "d/x"))

	_, ok := inMochi(c.nodes[1], tid, "d/x")
	require.False(t, ok)
	require.Empty(t, kvTopics(t, c.nodes[0], tid))

	// DeleteRetained on B for a message the host set on A: s gets the
	// message live, then the live empty message, and a SUBSCRIBE on A right
	// after gets nothing.
	require.NoError(t, c.nodes[0].Publish(t.Context(), tid, "d/y", []byte("w"), 0, true))
	require.Equal(t, retMsg{Topic: "d/y", Payload: "w"}, s.next(t))
	require.NoError(t, c.nodes[1].DeleteRetained(tid, "d/y"))
	require.Equal(t, retMsg{Topic: "d/y"}, s.next(t))

	onA.sub(t, "d/#", false)
	onA.none(t)
	require.Empty(t, c.nodes[0].Retained(tid, "#"))
	require.True(t, errors.Is(c.nodes[0].DeleteRetained(tid, "d/y"), tether.ErrNotFound))

	// A node with no cache of the tenant answers from the cell.
	require.NoError(t, c.nodes[1].Publish(t.Context(), tid, "d/z", []byte("z"), 1, true))
	require.Nil(t, c.nodes[0].cluster.cacheOf(c.t2.ID.String()))
	require.True(t, errors.Is(c.nodes[0].DeleteRetained(c.t2.ID.String(), "d/z"), tether.ErrNotFound))
	require.NoError(t, c.nodes[0].DeleteRetained(tid, "d/z"))
	require.Empty(t, kvTopics(t, c.nodes[1], tid))
}

// A 5.0 message expiry ends the retained message in the cell's store and in
// B's cache, and a subscriber in between gets what is left of the interval.
func TestRetainedExpiry(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	pub := dialRet(t, c.addr(0), "pub", "u1")
	_ = dialRet(t, c.addr(1), "watcher", "u1")

	s := dialRet(t, c.addr(1), "s", "u1")

	pub.pub(t, "e/x", "short", 1, "", 2)
	set := time.Now()

	s.sub(t, "e/#", false)

	m := s.nextFull(t)
	require.Equal(t, "short", m.Payload)
	require.True(t, m.Retain)
	require.LessOrEqual(t, m.Expiry, uint32(2))

	eventuallyLive(t, "e/x to expire from the store and from B's cache", func() bool {
		return !cached(c.nodes[1], tid, "e/x") && len(kvTopics(t, c.nodes[0], tid)) == 0
	})
	require.GreaterOrEqual(t, time.Since(set), 1500*time.Millisecond)

	_, ok := inMochi(c.nodes[1], tid, "e/x")
	require.False(t, ok)

	late := dialRet(t, c.addr(1), "late", "u1")
	late.sub(t, "e/#", false)
	late.none(t)
}

// Review Focus 3 for retained messages: two tenants with the same client IDs
// and topics keep their own retained messages in the store, in every node's
// cache, in delivery and in the admin listing.
func TestRetainedTenantIsolation(t *testing.T) {
	c := startCluster(t, 2)
	t1, t2 := c.t1.ID.String(), c.t2.ID.String()

	p1 := dialRet(t, c.addr(0), "dev", "u1")
	p2 := dialRet(t, c.addr(0), "dev", "u2")
	p1.pub(t, "r/x", "one", 1, "", 0)
	p2.pub(t, "r/x", "two", 1, "", 0)
	p2.pub(t, "r/only2", "two", 0, "", 0)

	s1 := dialRet(t, c.addr(1), "dev", "u1")
	s2 := dialRet(t, c.addr(1), "dev", "u2")

	s1.sub(t, "#", false)
	require.Equal(t, retMsg{Topic: "r/x", Payload: "one", Retain: true}, s1.next(t))
	s1.none(t)

	eventually(t, "t2's QoS 0 retained message to reach the store", func() bool { return len(kvTopics(t, c.nodes[0], t2)) == 2 })

	s2.sub(t, "#", false)

	got := []retMsg{s2.next(t), s2.next(t)}
	slices.SortFunc(got, func(a, b retMsg) int { return strings.Compare(a.Topic, b.Topic) })
	require.Equal(t, []retMsg{{Topic: "r/only2", Payload: "two", Retain: true}, {Topic: "r/x", Payload: "two", Retain: true}}, got)

	require.Equal(t, map[string]string{"r/x": "one"}, kvTopics(t, c.nodes[1], t1))
	require.Equal(t, []RetainedInfo{{Topic: "r/x", Size: 3, QoS: 1}}, c.nodes[1].Retained(t1, "#"))
	require.Len(t, c.nodes[1].Retained(t2, "#"), 2)

	for _, b := range c.nodes {
		require.Equal(t, []string{topic.Namespace(t1, "r/x")}, b.cluster.retainedView(t1))

		v, ok := inMochi(b, t1, "r/x")
		require.True(t, ok)
		require.Equal(t, "one", v)
	}

	require.True(t, errors.Is(c.nodes[1].DeleteRetained(t1, "r/only2"), tether.ErrNotFound))
}

// A "$" topic's retained message is kept under "==<tenant>" in the store, so
// "#" neither lists nor delivers it, while "$app/#" does. A $SYS entry in the
// store (no client or host can write one) is never cached, delivered or
// listed.
func TestRetainedDollarKeys(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()
	n := c.nodes[0].cluster

	pub := dialRet(t, c.addr(0), "pub", "u1")
	pub.pub(t, "$app/status", "up", 1, "", 0)
	pub.pub(t, "plain", "p", 1, "", 0)

	key := subject.RetainedKey(tid, "$app/status")
	require.True(t, strings.HasPrefix(key, "=="+tid+"."), key)

	ctx, cancel := context.WithTimeout(t.Context(), logWait)
	defer cancel()

	kv, err := n.Conn.JS().KeyValue(ctx, n.Config.RetainedBucket())
	require.NoError(t, err)

	lister, err := kv.ListKeysFiltered(ctx, subject.RetainedKeyFilter(tid, "#")...)
	require.NoError(t, err)

	var keys []string
	for k := range lister.Keys() {
		keys = append(keys, k)
	}

	require.Equal(t, []string{subject.RetainedKey(tid, "plain")}, keys)

	// Straight into the store, as only a foreign writer could.
	require.NoError(t, n.Retained.Put(ctx, tid, "$SYS/fake", cluster.LogMessage{Payload: []byte("x")}, 0))

	all := dialRet(t, c.addr(1), "all", "u1")
	all.sub(t, "#", false)
	require.Equal(t, retMsg{Topic: "plain", Payload: "p", Retain: true}, all.next(t))
	all.none(t)

	app := dialRet(t, c.addr(1), "app", "u1")
	app.sub(t, "$app/#", false)
	require.Equal(t, retMsg{Topic: "$app/status", Payload: "up", Retain: true}, app.next(t))

	sys := dialRet(t, c.addr(1), "sys", "u1")
	sys.sub(t, "$SYS/#", false)
	sys.none(t)

	require.Equal(t, []string{topic.Namespace(tid, "$app/status"), topic.Namespace(tid, "plain")}, c.nodes[1].cluster.retainedView(tid))
	require.Equal(t, []RetainedInfo{{Topic: "plain", Size: 1, QoS: 1}}, c.nodes[1].Retained(tid, "#"))
	require.Equal(t, []RetainedInfo{{Topic: "$app/status", Size: 2, QoS: 1}}, c.nodes[1].Retained(tid, "$app/#"))
	require.Empty(t, c.nodes[1].Retained(tid, "$SYS/#"))
}

// A retained will goes to the cell: QoS 0 and QoS 1 wills of clients that
// drop on A are retained on B.
func TestRetainedWillAcrossNodes(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	for _, w := range []struct {
		id  string
		qos byte
	}{{"w0", 0}, {"w1", 1}} {
		zero := uint32(0)
		conn := rawConnect(t, c.addr(0), &pp.Connect{
			ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: w.id, CleanStart: true, KeepAlive: 30,
			Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
			Properties:     &pp.Properties{SessionExpiryInterval: &zero},
			WillProperties: &pp.Properties{},
			WillFlag:       true, WillRetain: true, WillTopic: "will/" + w.id, WillQOS: w.qos, WillMessage: []byte("gone " + w.id),
		})
		_ = conn.Close()
	}

	eventually(t, "both wills in the store", func() bool { return len(kvTopics(t, c.nodes[0], tid)) == 2 })

	s := dialRet(t, c.addr(1), "s", "u1")
	s.sub(t, "will/#", false)

	got := []retMsg{s.next(t), s.next(t)}
	slices.SortFunc(got, func(a, b retMsg) int { return strings.Compare(a.Topic, b.Topic) })
	require.Equal(t, []retMsg{{Topic: "will/w0", Payload: "gone w0", Retain: true}, {Topic: "will/w1", Payload: "gone w1", Retain: true}}, got)

	// And on A, whose cache took them in too.
	a := dialRet(t, c.addr(0), "a", "u1")
	a.sub(t, "will/#", false)
	require.Len(t, []retMsg{a.next(t), a.next(t)}, 2)
}

// A live retained delivery carries the retain flag to a 5.0 subscription
// with Retain As Published and not to one without, for QoS 1 through the
// log on both nodes and for QoS 0 on the publisher's node (the Router
// carries no retain flag, so a routed QoS 0 copy arrives with it clear).
func TestRetainAsPublishedLive(t *testing.T) {
	c := startCluster(t, 2)

	pub := dialRet(t, c.addr(0), "pub", "u1")

	rapB := dialRet(t, c.addr(1), "rapB", "u1")
	plainB := dialRet(t, c.addr(1), "plainB", "u1")
	rapA := dialRet(t, c.addr(0), "rapA", "u1")
	plainA := dialRet(t, c.addr(0), "plainA", "u1")

	rapB.sub(t, "live/#", true)
	plainB.sub(t, "live/#", false)
	rapA.sub(t, "live/#", true)
	plainA.sub(t, "live/#", false)
	rapA.sub(t, "local/#", true)
	plainA.sub(t, "local/#", false)

	pub.pub(t, "live/q1", "one", 1, "", 0)

	require.Equal(t, retMsg{Topic: "live/q1", Payload: "one", Retain: true}, rapB.next(t))
	require.Equal(t, retMsg{Topic: "live/q1", Payload: "one"}, plainB.next(t))
	require.Equal(t, retMsg{Topic: "live/q1", Payload: "one", Retain: true}, rapA.next(t))
	require.Equal(t, retMsg{Topic: "live/q1", Payload: "one"}, plainA.next(t))

	// QoS 0 on the publisher's node (the Router carries no retain flag to
	// the others; B subscribes to none of this).
	pub.pub(t, "local/q0", "zero", 0, "", 0)
	require.Equal(t, retMsg{Topic: "local/q0", Payload: "zero", Retain: true}, rapA.next(t))
	require.Equal(t, retMsg{Topic: "local/q0", Payload: "zero"}, plainA.next(t))

	// A message that is not retained never carries the flag.
	pub5 := pub.Client

	ctx, cancel := context.WithTimeout(t.Context(), logWait)
	defer cancel()

	_, err := pub5.Publish(ctx, &paho.Publish{Topic: "live/plain", QoS: 1, Payload: []byte("p")})
	require.NoError(t, err)
	require.Equal(t, retMsg{Topic: "live/plain", Payload: "p"}, rapB.next(t))
}

// A sweep that retires a tenant's cache while a CONNECT of the tenant makes
// it a new one leaves the new cache whole: the old one's entries left
// mochi's store before it left the map, so its stop cannot clear what the new
// cache loaded.
func TestRetainedSweepRacingConnect(t *testing.T) {
	c := startCluster(t, 1)
	tid := c.t1.ID.String()
	b := c.nodes[0]
	n := b.cluster

	pub := dialRet(t, c.addr(0), "pub", "u1")
	pub.pub(t, "s/x", "v", 1, "", 0)
	waitRetainedLoaded(t, b, tid)

	var second *retClient

	// Between the old cache leaving the map and its watch stopping, a new
	// CONNECT makes and loads a new cache.
	n.retainedAfterRetire = func(string) {
		second = dialRet(t, c.addr(0), "second", "u1")
		waitRetainedLoaded(t, b, tid)
	}

	n.sweepRetained(map[string]int{}, time.Now().Add(time.Hour))
	n.retainedAfterRetire = nil

	require.NotNil(t, second)

	v, ok := inMochi(b, tid, "s/x")
	require.True(t, ok, "the old cache's stop cleared the new cache's entries")
	require.Equal(t, "v", v)
	require.Equal(t, []string{topic.Namespace(tid, "s/x")}, n.retainedView(tid))

	second.sub(t, "s/#", false)
	require.Equal(t, "v", second.retained(t, "s/x"))
}

// probe makes sure QoS 0 published on pub's node reaches sub's node for
// everything sub subscribed to so far (awaitInterest for retClients).
func probe(t *testing.T, pub, sub *retClient) {
	t.Helper()

	name := fmt.Sprintf("%s%d", probePrefix, probeSeq.Add(1))
	sub.sub(t, name, false)

	deadline := time.Now().Add(10 * time.Second)

	for {
		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		_, err := pub.Publish(ctx, &paho.Publish{Topic: name, QoS: 0, Payload: []byte("probe")})

		cancel()
		require.NoError(t, err)

		select {
		case m := <-sub.msgs:
			if m.Topic == name {
				return
			}

			if !strings.HasPrefix(m.Topic, probePrefix) {
				t.Fatalf("message %q on %q before the probe", m.Payload, m.Topic)
			}
		case <-time.After(20 * time.Millisecond):
		}

		if time.Now().After(deadline) {
			t.Fatal("interest did not reach the publisher's node")
		}
	}
}

// A routed QoS 0 retained publish carries the retain flag to a Retain As
// Published subscription on another node (the Router's Tether-Retain header,
// ruling R33b) and not to one without.
func TestRetainAsPublishedRouted(t *testing.T) {
	c := startCluster(t, 2)

	pub := dialRet(t, c.addr(0), "pub", "u1")
	plainB := dialRet(t, c.addr(1), "plainB", "u1")
	rapB := dialRet(t, c.addr(1), "rapB", "u1")

	plainB.sub(t, "rq/#", false)
	rapB.sub(t, "rq/#", true)
	probe(t, pub, rapB)

	pub.pub(t, "rq/x", "zero", 0, "", 0)
	require.Equal(t, retMsg{Topic: "rq/x", Payload: "zero", Retain: true}, rapB.next(t))
	require.Equal(t, retMsg{Topic: "rq/x", Payload: "zero"}, plainB.next(t))

	// Not retained: no flag.
	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err := pub.Publish(ctx, &paho.Publish{Topic: "rq/plain", QoS: 0, Payload: []byte("p")})
	require.NoError(t, err)
	require.Equal(t, retMsg{Topic: "rq/plain", Payload: "p"}, rapB.next(t))
}

// One topic's writes from one node reach the store in the order they were
// made, whatever path they take: a QoS 0 retained publish whose write is
// held in its queue, then a host retained publish of the same topic. The
// host's Publish returns only after its write, behind the held one, and the
// store and both caches keep the later message.
func TestRetainedWritesKeepTopicOrder(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()
	n := c.nodes[0].cluster

	pub := dialRet(t, c.addr(0), "pub", "u1")
	_ = dialRet(t, c.addr(1), "watcher", "u1")

	held := make(chan struct{})
	release := make(chan struct{})

	var once sync.Once

	hook := func(w *retainWrite) {
		if w.msg.Topic == "o/x" {
			once.Do(func() {
				close(held)
				<-release
			})
		}
	}
	n.retainedBeforeWrite.Store(&hook)
	t.Cleanup(func() { n.retainedBeforeWrite.Store(nil) })

	pub.pub(t, "o/x", "first", 0, "", 0)
	<-held

	done := make(chan error, 1)

	go func() { done <- c.nodes[0].Publish(context.Background(), tid, "o/x", []byte("second"), 1, true) }()

	select {
	case err := <-done:
		t.Fatalf("the later write did not wait behind the held one: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-done)

	require.Equal(t, map[string]string{"o/x": "second"}, kvTopics(t, c.nodes[0], tid))

	for i := range c.nodes {
		s := dialRet(t, c.addr(i), fmt.Sprintf("s%d", i), "u1")
		s.sub(t, "o/x", false)
		require.Equal(t, "second", s.retained(t, "o/x"))
	}
}
