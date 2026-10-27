package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/internal/chaos"
	"github.com/xraph/tether/tenant"
)

// logWait bounds the waits of the log tests, which go through JetStream and,
// in some, a fault proxy that delays every byte.
const logWait = 30 * time.Second

// startProxiedCell is startCluster with node 0 reaching NATS through a fault
// proxy (NoDiscovery is set on every node, so it cannot get around it).
func startProxiedCell(t *testing.T, nodes int, before func(*Broker)) (*testCell, *chaos.Proxy) {
	t.Helper()

	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}

	var proxy *chaos.Proxy

	c := startCellVia(t, nodes, testConfig(),
		map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")},
		fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}}, before,
		func(i int, url string) string {
			if i != 0 {
				return url
			}

			p, err := chaos.NewProxy(strings.TrimPrefix(url, "nats://"))
			require.NoError(t, err)
			t.Cleanup(p.Close)

			proxy = p

			return "nats://" + p.Addr()
		})
	c.t1, c.t2 = t1, t2

	return c, proxy
}

// subscribeQoS subscribes c to filter at qos, with No Local when noLocal.
func subscribeQoS(t *testing.T, c *client5, filter string, qos byte, noLocal bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	sa, err := c.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: qos, NoLocal: noLocal}}})
	require.NoError(t, err)
	require.Equal(t, []byte{qos}, sa.Reasons)
}

// publishQ publishes at qos (1 or 2) and returns the PUBACK or PUBREC reason.
func publishQ(t *testing.T, c *client5, name, payload string, qos byte) byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), logWait)
	defer cancel()

	resp, err := c.Publish(ctx, &paho.Publish{Topic: name, QoS: qos, Payload: []byte(payload)})
	require.NoError(t, err)

	return resp.ReasonCode
}

// recvLog waits up to logWait for the next message on c that is not a probe.
func recvLog(t *testing.T, c *client5) message {
	t.Helper()

	deadline := time.After(logWait)

	for {
		select {
		case m := <-c.msgs:
			if strings.HasPrefix(m.Topic, probePrefix) {
				continue
			}

			return m
		case <-deadline:
			t.Fatal("no message received")

			return message{}
		}
	}
}

// drainToEnd reads c until a message with payload "end" arrives and returns
// how often each payload came before it.
func drainToEnd(t *testing.T, c *client5) map[string]int {
	t.Helper()

	got := map[string]int{}

	for {
		m := recvLog(t, c)
		if m.Payload == "end" {
			return got
		}

		got[m.Payload]++
	}
}

// A QoS 1 message published on A is acked once committed and reaches a
// subscriber on B and one on A once each: A's own subscriber gets it from
// A's reader, not from mochi as well.
func TestQoS1AcrossNodes(t *testing.T) {
	c := startCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	remote, _, err := connect5(t, c.addr(1), "remote", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, remote, "a/#", 1, false)

	local, _, err := connect5(t, c.addr(0), "local", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, local, "a/#", 2, false)

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, pub, "a/b", "one", 1))
	require.Equal(t, byte(0), publishQ(t, pub, "a/c", "two", 2))
	require.Equal(t, byte(0), publishQ(t, pub, "a/end", "end", 1))

	for _, s := range []*client5{remote, local} {
		require.Equal(t, map[string]int{"one": 1, "two": 1}, drainToEnd(t, s))
		noMoreMsgs(t, s)
	}

	require.Equal(t, uint64(3), a.cluster.appends.Load())
	require.Equal(t, uint64(3), a.cluster.acks.Load())
	require.Zero(t, b.cluster.appends.Load())

	// Mochi kept no QoS 2 state for the publisher once PUBCOMP went out.
	eventually(t, "no inflight left on A", func() bool {
		return atomic.LoadInt64(&a.server.Info.Inflight) == 0
	})
	require.Empty(t, a.cluster.qos2Entries(namespacedID(c.t1.ID.String(), "pub")))
}

// Review Focus 2, half: no PUBACK goes out before the commit. Node A is cut
// from NATS by the proxy (blackhole), a QoS 1 publish gets no PUBACK within
// 2 s, and once the network heals the broker's retry (or the commit that was
// waiting) acks it and B's subscriber receives it.
//
// MQTT allows QoS 1 duplicates, and a retry after an ack that was lost could
// make one; the append is retried under the same message ID, so JetStream
// stores it once and one delivery is what we expect. The test asserts at
// least one and logs the count.
func TestPubackOnlyAfterCommit(t *testing.T) {
	c, proxy := startProxiedCell(t, 2, nil)
	a := c.nodes[0]

	sub, _, err := connect5(t, c.addr(1), "sub", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sub, "q/#", 1, false)

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	proxy.SetMode(chaos.Both, chaos.Blackhole)

	acked := make(chan byte, 1)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		resp, err := pub.Publish(ctx, &paho.Publish{Topic: "q/1", QoS: 1, Payload: []byte("held")})
		if err != nil {
			acked <- 0xff

			return
		}

		acked <- resp.ReasonCode
	}()

	select {
	case code := <-acked:
		t.Fatalf("PUBACK (0x%02x) while NATS was unreachable", code)
	case <-time.After(2 * time.Second):
	}

	require.Zero(t, a.cluster.acks.Load())
	require.Zero(t, a.cluster.appends.Load())

	proxy.SetMode(chaos.Both, chaos.Pass)

	select {
	case code := <-acked:
		require.Equal(t, byte(0), code)
	case <-time.After(logWait):
		t.Fatal("no PUBACK after the network healed")
	}

	require.Equal(t, byte(0), publishQ(t, pub, "q/end", "end", 1))

	got := drainToEnd(t, sub)
	require.GreaterOrEqual(t, got["held"], 1)
	t.Logf("delivered %d time(s)", got["held"])
}

// A QoS 2 publisher whose connection drops after the message was committed
// but before its PUBREC, and which sends it again with DUP after it
// reconnects, has it delivered once: the resend takes the pending entry's
// message ID and JetStream stores nothing new. PUBREL then clears the entry.
func TestQoS2ExactlyOnceWithDup(t *testing.T) {
	c, proxy := startProxiedCell(t, 2, nil)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	sub, _, err := connect5(t, c.addr(1), "sub", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sub, "q2/#", 2, false)

	expiry := uint32(3600)
	connect := &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: "q2pub", CleanStart: false, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{SessionExpiryInterval: &expiry},
	}

	conn := rawConnect(t, c.addr(0), connect)

	// From here every byte between A and NATS takes a second each way, so
	// the PUBREC cannot come before the client hangs up. (The CONNECT went
	// first: it starts the tenant's reader, which talks to NATS.)
	proxy.SetDelay(chaos.Both, time.Second)
	proxy.SetMode(chaos.Both, chaos.Delay)

	writePacket(t, conn, &packets.Publish{Topic: "q2/x", QoS: 2, PacketID: 7, Payload: []byte("once"), Properties: &packets.Properties{}})

	nsID := namespacedID(tid, "q2pub")
	eventually(t, "the pending entry", func() bool { return len(a.cluster.qos2Entries(nsID)) == 1 })
	msgID := a.cluster.qos2Entries(nsID)[7]

	require.NoError(t, conn.Close())

	// Committed and delivered, though the publisher never heard.
	require.Equal(t, "once", recvLog(t, sub).Payload)
	require.Zero(t, a.cluster.acks.Load())

	// The reconnect takes the session's key, so NATS is quick again; the
	// old connection's last record write goes first.
	proxy.SetMode(chaos.Both, chaos.Pass)
	eventually(t, "the old connection's session to end", func() bool { return a.cluster.sessionByID(nsID) == nil })

	conn = rawConnect(t, c.addr(0), connect)
	writePacket(t, conn, &packets.Publish{Topic: "q2/x", QoS: 2, PacketID: 7, Duplicate: true, Payload: []byte("once"), Properties: &packets.Properties{}})

	rec := readLog(t, conn)
	require.Equal(t, packets.PUBREC, rec.Type)
	require.Equal(t, uint16(7), rec.Content.(*packets.Pubrec).PacketID)
	require.Less(t, rec.Content.(*packets.Pubrec).ReasonCode, byte(0x80))
	require.Equal(t, map[uint16]string{7: msgID}, a.cluster.qos2Entries(nsID))

	writePacket(t, conn, &packets.Pubrel{PacketID: 7, Properties: &packets.Properties{}})

	comp := readLog(t, conn)
	require.Equal(t, packets.PUBCOMP, comp.Type)
	require.Empty(t, a.cluster.qos2Entries(nsID))

	// A sentinel appended after the resend proves no second copy follows.
	writePacket(t, conn, &packets.Publish{Topic: "q2/end", QoS: 1, PacketID: 8, Payload: []byte("end"), Properties: &packets.Properties{}})
	require.Equal(t, packets.PUBACK, readLog(t, conn).Type)

	require.Empty(t, drainToEnd(t, sub))
	// The original's commit came back after its connection had ended, so
	// the node counted only the deduplicated resend and the sentinel.
	require.Equal(t, uint64(2), a.cluster.appends.Load())
}

// Ruling R22: a 5.0 client that sends more unacked QoS 1 publishes than the
// server's Receive Maximum is disconnected with 0x93. The commits are held
// back by a proxy delay, so none of the three is acked in time.
func TestReceiveMaximumEnforced(t *testing.T) {
	c, proxy := startProxiedCell(t, 1, func(b *Broker) { b.receiveMaximum = 2 })

	conn, ack := rawConnectAck(t, c.addr(0), "rm")
	require.NotNil(t, ack.Properties.ReceiveMaximum)
	require.Equal(t, uint16(2), *ack.Properties.ReceiveMaximum)

	proxy.SetDelay(chaos.Both, 2*time.Second)
	proxy.SetMode(chaos.Both, chaos.Delay)

	for id := uint16(1); id <= 3; id++ {
		writePacket(t, conn, &packets.Publish{Topic: "rm/x", QoS: 1, PacketID: id, Payload: []byte("x"), Properties: &packets.Properties{}})
	}

	for {
		cp := readLog(t, conn)
		if cp.Type == packets.PUBACK {
			continue
		}

		require.Equal(t, packets.DISCONNECT, cp.Type)
		require.Equal(t, byte(0x93), cp.Content.(*packets.Disconnect).ReasonCode)

		break
	}

	requireClosed(t, conn)
}

// Ruling R22 for 3.1.1, which has no receive maximum: the client is held
// back instead of disconnected, and every publish is acked in the end, at
// QoS 1 and at QoS 2. A 3.1.1 client's QoS 2 slot frees with its PUBREC, not
// its PUBREL: the PUBREL is read by the very read loop that waits for a slot.
func TestReceiveMaximumBackpressure311(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprintf("qos %d", qos), func(t *testing.T) { backpressure311(t, qos) })
	}
}

func backpressure311(t *testing.T, qos byte) {
	c, proxy := startProxiedCell(t, 1, func(b *Broker) { b.receiveMaximum = 2 })
	a := c.nodes[0]

	v3, err := connect311(t, c.addr(0), "bp", "u1", nil)
	require.NoError(t, err)

	msgs := subscribe311(t, v3, "bp/#")

	proxy.SetDelay(chaos.Both, 300*time.Millisecond)
	proxy.SetMode(chaos.Both, chaos.Delay)

	toks := make([]interface{ WaitTimeout(time.Duration) bool }, 0, 6)
	for i := range 6 {
		toks = append(toks, v3.Publish("bp/x", qos, false, fmt.Sprint(i)))
	}

	for i, tok := range toks {
		require.True(t, tok.WaitTimeout(logWait), "publish %d not acked", i)
	}

	require.True(t, v3.IsConnectionOpen())
	require.Equal(t, uint64(6), a.cluster.acks.Load())

	for i := range 6 {
		select {
		case m := <-msgs:
			require.Equal(t, fmt.Sprint(i), m.Payload, "per-client order")
		case <-time.After(logWait):
			t.Fatalf("message %d not delivered", i)
		}
	}
}

// Ruling R23: acks go out in arrival order. The first publish (QoS 2) fails
// its first append and is retried after a backoff, while the second (QoS 1)
// commits at once; its PUBACK still waits for the first's PUBREC.
func TestAcksInArrivalOrder(t *testing.T) {
	var failed atomic.Bool

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			b.cluster.appendRetry = 300 * time.Millisecond
			b.cluster.persistQoS2 = func(string, string, uint16, string) error {
				if failed.CompareAndSwap(false, true) {
					return errors.New("session store unavailable")
				}

				return nil
			}
		})
	a := c.nodes[0]

	conn, _ := rawConnectAck(t, c.addr(0), "order")
	writePacket(t, conn, &packets.Publish{Topic: "o/1", QoS: 2, PacketID: 1, Payload: []byte("1"), Properties: &packets.Properties{}})
	writePacket(t, conn, &packets.Publish{Topic: "o/2", QoS: 1, PacketID: 2, Payload: []byte("2"), Properties: &packets.Properties{}})

	first := readLog(t, conn)
	require.Equal(t, packets.PUBREC, first.Type)
	require.Equal(t, uint16(1), first.Content.(*packets.Pubrec).PacketID)

	second := readLog(t, conn)
	require.Equal(t, packets.PUBACK, second.Type)
	require.Equal(t, uint16(2), second.Content.(*packets.Puback).PacketID)

	require.True(t, failed.Load())
	require.Equal(t, uint64(2), a.cluster.appends.Load())
}

// Review Focus 3 for the log: two tenants with the same raw client IDs and
// topics on two nodes only ever receive their own QoS 1 messages. A log
// entry's tenant comes from its subject, and a reader only reads its own
// tenant's subjects, so even a message appended for t2 straight into the log
// reaches t2 alone; one under $SYS is dropped. (Subscriber and publisher have
// different client IDs: one ID on two nodes is one session, taken over.)
func TestLogDeliveryTenantIsolation(t *testing.T) {
	c := startCluster(t, 2)
	b := c.nodes[1]

	subs := map[string]*client5{}
	pubs := map[string]*client5{}

	for _, user := range []string{"u1", "u2"} {
		s, _, err := connect5(t, c.addr(1), "dev-sub", user, true)
		require.NoError(t, err)
		subscribeQoS(t, s, "#", 1, false)

		p, _, err := connect5(t, c.addr(0), "dev-pub", user, true)
		require.NoError(t, err)

		subs[user], pubs[user] = s, p
	}

	require.ElementsMatch(t, []string{c.t1.ID.String(), c.t2.ID.String()}, b.cluster.readerTenants())

	for _, user := range []string{"u1", "u2"} {
		require.Equal(t, byte(0), publishQ(t, pubs[user], "x/y", "from-"+user, 1))
	}

	t2 := c.t2.ID.String()

	_, _, err := b.cluster.Log.Append(t.Context(), cluster.LogMessage{Tenant: t2, Topic: "$SYS/clients/connected", QoS: 1, Payload: []byte("forged")}, "")
	require.NoError(t, err)
	_, _, err = b.cluster.Log.Append(t.Context(), cluster.LogMessage{Tenant: t2, Topic: "x/direct", QoS: 1, Payload: []byte("direct-u2")}, "")
	require.NoError(t, err)

	for _, user := range []string{"u1", "u2"} {
		require.Equal(t, byte(0), publishQ(t, pubs[user], "x/end", "end", 1))
	}

	require.Equal(t, map[string]int{"from-u1": 1}, drainToEnd(t, subs["u1"]))
	require.Equal(t, map[string]int{"from-u2": 1, "direct-u2": 1}, drainToEnd(t, subs["u2"]))

	for _, user := range []string{"u1", "u2"} {
		noMoreMsgs(t, subs[user])
	}
}

// No Local holds for QoS 1 and 2 messages delivered from the log: the
// publisher's client ID travels with the message.
func TestQoS1NoLocal(t *testing.T) {
	c := startCluster(t, 1)

	self, _, err := connect5(t, c.addr(0), "self", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, self, "n/#", 1, true)

	other, _, err := connect5(t, c.addr(0), "other", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, other, "n/#", 1, false)

	require.Equal(t, byte(0), publishQ(t, self, "n/x", "mine", 1))
	require.Equal(t, byte(0), publishQ(t, other, "n/end", "end", 1))

	require.Equal(t, map[string]int{"mine": 1}, drainToEnd(t, other))
	require.Equal(t, "end", recvLog(t, self).Payload)
	noMoreMsgs(t, self)
}

// The injector's QoS 1 and 2 publishes are delivered at the subscriber's QoS
// and leave no inflight state on the injector: mochi takes an inline publish
// as already received.
func TestInjectQoS12KeepsNoState(t *testing.T) {
	c := startCluster(t, 1)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sub, "i/#", 2, false)

	for i := range 20 {
		qos := byte(1 + i%2)
		require.NoError(t, a.inject(tid, "i/x", []byte(fmt.Sprint(i)), qos, cluster.Props{}, "", 0, false))
	}

	for i := range 20 {
		require.Equal(t, fmt.Sprint(i), recvLog(t, sub).Payload)
	}

	inj := a.cluster.injector.Load()
	require.Zero(t, inj.State.Inflight.Len())
	eventually(t, "the subscriber's inflight to drain", func() bool {
		return atomic.LoadInt64(&a.server.Info.Inflight) == 0
	})
}

// A host QoS 1 publish goes through the log: every node delivers it, the
// publishing one included, and an append that fails is the caller's error.
func TestHostPublishQoS1ThroughLog(t *testing.T) {
	c, proxy := startProxiedCell(t, 2, nil)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	subs := make([]*client5, 0, 2)

	for i := range 2 {
		s, _, err := connect5(t, c.addr(i), fmt.Sprintf("s%d", i), "u1", true)
		require.NoError(t, err)
		subscribeQoS(t, s, "h/#", 1, false)

		subs = append(subs, s)
	}

	require.NoError(t, a.Publish(t.Context(), tid, "h/x", []byte("host"), 1, false))
	require.NoError(t, a.DevicePublish(t.Context(), tid, "h/end", []byte("end"), 1, false))

	for _, s := range subs {
		require.Equal(t, map[string]int{"host": 1}, drainToEnd(t, s))
	}

	proxy.SetMode(chaos.Both, chaos.Blackhole)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := a.Publish(ctx, tid, "h/x", []byte("lost"), 1, false)
	require.ErrorIs(t, err, cluster.ErrUnavailable)

	proxy.SetMode(chaos.Both, chaos.Pass)
}

// A tenant's reader stops after the idle time with no connection of the
// tenant, starts again with the next connection, and is restarted from where
// it was when it ends without being stopped (ruling R24).
func TestTenantReaderLifecycle(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			b.cluster.readerIdle = 300 * time.Millisecond
			b.cluster.readerSweep = 50 * time.Millisecond
		})
	a := c.nodes[0]
	tid := t1.ID.String()

	require.Empty(t, a.cluster.readerTenants())

	cl, _, err := connect5(t, c.addr(0), "x", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []string{tid}, a.cluster.readerTenants())

	// Held while connected, well past the idle time.
	time.Sleep(time.Second)
	require.Equal(t, []string{tid}, a.cluster.readerTenants())

	require.NoError(t, cl.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	eventually(t, "the idle reader to stop", func() bool { return len(a.cluster.readerTenants()) == 0 })

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sub, "r/#", 1, false)
	require.Equal(t, []string{tid}, a.cluster.readerTenants())

	// End the reader behind the node's back: the watcher starts a new one
	// after the last message the old one finished.
	a.cluster.readersMu.Lock()
	r := a.cluster.readers[tid].r
	a.cluster.readersMu.Unlock()

	require.Equal(t, byte(0), publishQ(t, sub, "r/1", "before", 1))
	require.Equal(t, "before", recvLog(t, sub).Payload)

	r.Stop()
	eventually(t, "the reader to restart", func() bool { return a.cluster.readerRestarts.Load() == 1 })

	require.Equal(t, byte(0), publishQ(t, sub, "r/2", "after", 1))
	require.Equal(t, "after", recvLog(t, sub).Payload)
	noMoreMsgs(t, sub)
}

// Over the tenant's rate budget a 5.0 QoS 1 publish gets PUBACK 0x97 and a
// QoS 2 one PUBREC 0x97, through the pipe, after the acks of earlier ones.
func TestQoS1QuotaExceededInOrder(t *testing.T) {
	t1 := newTenant(tenant.Limits{MessagesPerSecond: 0.1, Burst: 1})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)

	conn, _ := rawConnectAck(t, c.addr(0), "quota")

	for id := uint16(1); id <= 3; id++ {
		qos := byte(1)
		if id == 3 {
			qos = 2
		}

		writePacket(t, conn, &packets.Publish{Topic: "z", QoS: qos, PacketID: id, Payload: []byte("x"), Properties: &packets.Properties{}})
	}

	cp := readLog(t, conn)
	require.Equal(t, packets.PUBACK, cp.Type)
	require.Equal(t, uint16(1), cp.Content.(*packets.Puback).PacketID)
	require.Equal(t, byte(0), cp.Content.(*packets.Puback).ReasonCode)

	cp = readLog(t, conn)
	require.Equal(t, packets.PUBACK, cp.Type)
	require.Equal(t, uint16(2), cp.Content.(*packets.Puback).PacketID)
	require.Equal(t, byte(0x97), cp.Content.(*packets.Puback).ReasonCode)

	// A 5.0 QoS 2 over the budget gets PUBREC 0x97, also in order.
	cp = readLog(t, conn)
	require.Equal(t, packets.PUBREC, cp.Type)
	require.Equal(t, uint16(3), cp.Content.(*packets.Pubrec).PacketID)
	require.Equal(t, byte(0x97), cp.Content.(*packets.Pubrec).ReasonCode)
}

// rawConnectAck is rawConnect5 for user u1 that also returns the CONNACK.
func rawConnectAck(t *testing.T, addr, clientID string) (net.Conn, *packets.Connack) {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	writePacket(t, conn, &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: true, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{},
	})

	cp := readPacket(t, conn)
	require.Equal(t, packets.CONNACK, cp.Type)

	ack := cp.Content.(*packets.Connack)
	require.Equal(t, byte(0), ack.ReasonCode)

	return conn, ack
}

// writePacket writes one packet to conn.
func writePacket(t *testing.T, conn net.Conn, p io.WriterTo) {
	t.Helper()

	_, err := p.WriteTo(conn)
	require.NoError(t, err)
}

// readLog reads one packet from conn, waiting up to logWait.
func readLog(t *testing.T, conn net.Conn) *packets.ControlPacket {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(logWait)))

	cp, err := packets.ReadPacket(conn)
	require.NoError(t, err)

	return cp
}

// A client subscribed to its own QoS 2 topic, publishing with the packet ID
// mochi would give its next outbound delivery: the PUBREC holds that ID in
// mochi's one inflight map from the moment the PUBLISH is read, so the
// delivery of the message (from this node's own reader, racing the ack)
// gets another ID instead of being overwritten by the PUBREC. Both flows
// complete with success codes and nothing is left in flight.
//
// (mochi has the opposite collision of its own, with or without a cluster:
// a client PUBLISH reusing an ID of an outbound delivery still in flight
// deletes that delivery's entry in processPublish. The test steers clear of
// it by completing each round before the next.)
func TestQoS2SelfSubscribedKeepsDeliveries(t *testing.T) {
	c := startCluster(t, 1)
	a := c.nodes[0]
	nsID := namespacedID(c.t1.ID.String(), "self")

	conn, _ := rawConnectAck(t, c.addr(0), "self")
	writePacket(t, conn, &packets.Subscribe{PacketID: 1, Subscriptions: []packets.SubOptions{{Topic: "s/#", QoS: 2}}, Properties: &packets.Properties{}})
	require.Equal(t, packets.SUBACK, readLog(t, conn).Type)

	next := uint16(1) // mochi's next outbound packet ID for this client

	for round := range 20 {
		writePacket(t, conn, &packets.Publish{Topic: "s/x", QoS: 2, PacketID: next, Payload: []byte(fmt.Sprint(round)), Properties: &packets.Properties{}})

		var (
			rec   *packets.Pubrec
			deliv *packets.Publish
		)

		for rec == nil || deliv == nil {
			cp := readLog(t, conn)

			switch cp.Type {
			case packets.PUBREC:
				rec = cp.Content.(*packets.Pubrec)
			case packets.PUBLISH:
				deliv = cp.Content.(*packets.Publish)
			default:
				t.Fatalf("round %d: unexpected packet %v", round, cp)
			}
		}

		require.Equal(t, next, rec.PacketID)
		require.Equal(t, byte(0), rec.ReasonCode)
		require.Equal(t, fmt.Sprint(round), string(deliv.Payload))
		require.Equal(t, byte(2), deliv.QoS)
		require.NotEqual(t, next, deliv.PacketID, "round %d: the delivery took the reserved packet ID", round)

		// The publisher's flow.
		writePacket(t, conn, &packets.Pubrel{PacketID: next, Properties: &packets.Properties{}})

		cp := readLog(t, conn)
		require.Equal(t, packets.PUBCOMP, cp.Type)
		require.Equal(t, next, cp.Content.(*packets.Pubcomp).PacketID)
		require.Equal(t, byte(0), cp.Content.(*packets.Pubcomp).ReasonCode, "round %d", round)

		// The delivery's flow.
		writePacket(t, conn, &packets.Pubrec{PacketID: deliv.PacketID, Properties: &packets.Properties{}})

		cp = readLog(t, conn)
		require.Equal(t, packets.PUBREL, cp.Type)
		require.Equal(t, byte(0), cp.Content.(*packets.Pubrel).ReasonCode, "round %d", round)

		writePacket(t, conn, &packets.Pubcomp{PacketID: deliv.PacketID, Properties: &packets.Properties{}})

		next = deliv.PacketID + 1
	}

	eventually(t, "nothing left in flight", func() bool {
		cl, ok := a.server.Clients.Get(nsID)

		return ok && cl.State.Inflight.Len() == 0
	})
}

// A pipe abandoned before its QoS 2 commit leaves no PUBREC behind: not in
// the offline session when the connection drops, and not in the session a
// takeover inherits while the old connection is still open. The resend then
// gets a successful PUBREC (a leftover reservation would make mochi answer
// 0x91), once the message is committed.
func TestQoS2AbandonedLeavesNoPubrec(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	gate := newAppendGate()
	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": {Publish: []string{"#"}, Subscribe: []string{"#"}}}},
		func(b *Broker) { b.cluster.holdAppend = gate.hold })
	a := c.nodes[0]
	nsID := namespacedID(t1.ID.String(), "ab")

	expiry := uint32(3600)
	connect := &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: "ab", CleanStart: false, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{SessionExpiryInterval: &expiry},
	}

	inflight := func() int {
		cl, ok := a.server.Clients.Get(nsID)
		if !ok {
			return -1
		}

		return cl.State.Inflight.Len()
	}

	// Dropped connection. (In cluster mode the offline session is evicted
	// from mochi, so nothing at all is left.)
	conn := rawConnect(t, c.addr(0), connect)

	gate.close()

	writePacket(t, conn, &packets.Publish{Topic: "ab/x", QoS: 2, PacketID: 3, Payload: []byte("x"), Properties: &packets.Properties{}})
	eventually(t, "the reservation", func() bool { return inflight() == 1 })

	require.NoError(t, conn.Close())
	eventually(t, "the reservation to go", func() bool { return inflight() <= 0 })

	// Takeover while the old connection is open.
	old := rawConnect(t, c.addr(0), connect)
	writePacket(t, old, &packets.Publish{Topic: "ab/y", QoS: 2, PacketID: 4, Payload: []byte("y"), Properties: &packets.Properties{}})
	eventually(t, "the second reservation", func() bool { return inflight() == 1 })

	conn = rawConnect(t, c.addr(0), connect)
	require.Zero(t, inflight(), "the takeover inherited an uncommitted PUBREC")

	gate.open()

	writePacket(t, conn, &packets.Publish{Topic: "ab/y", QoS: 2, PacketID: 4, Duplicate: true, Payload: []byte("y"), Properties: &packets.Properties{}})

	rec := readLog(t, conn)
	require.Equal(t, packets.PUBREC, rec.Type)
	require.Equal(t, uint16(4), rec.Content.(*packets.Pubrec).PacketID)
	require.Equal(t, byte(0), rec.Content.(*packets.Pubrec).ReasonCode)

	writePacket(t, conn, &packets.Pubrel{PacketID: 4, Properties: &packets.Properties{}})
	require.Equal(t, packets.PUBCOMP, readLog(t, conn).Type)
	eventually(t, "nothing in flight", func() bool { return inflight() == 0 })
}

// appendGate holds the appends of client publishes back while closed, the
// way an unreachable log would, while the session store stays reachable.
// While failing it refuses them at once instead, the way a log whose stream
// has no leader does ("no response from stream").
type appendGate struct {
	mu      sync.Mutex
	ch      chan struct{} // closed while the gate is open
	failing atomic.Bool
	refused atomic.Int64
}

// errLogDown is what a failing appendGate answers: the log cannot be
// reached, as the real "no response from stream" is.
var errLogDown = fmt.Errorf("test: no response from stream: %w", cluster.ErrUnavailable)

func newAppendGate() *appendGate {
	g := &appendGate{ch: make(chan struct{})}
	close(g.ch)

	return g
}

// hold is clusterNode.holdAppend.
func (g *appendGate) hold(ctx context.Context) error {
	if g.failing.Load() {
		g.refused.Add(1)

		return errLogDown
	}

	g.mu.Lock()
	ch := g.ch
	g.mu.Unlock()

	select {
	case <-ch:
	case <-ctx.Done():
	}

	return nil
}

func (g *appendGate) close() {
	g.mu.Lock()
	defer g.mu.Unlock()

	select {
	case <-g.ch:
		g.ch = make(chan struct{})
	default:
	}
}

func (g *appendGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()

	select {
	case <-g.ch:
	default:
		close(g.ch)
	}
}

// Ruling R23 for refusals: in cluster mode the publish ACL is decided in
// OnPublish, so a refused 5.0 publish's PUBACK or PUBREC 0x87 waits behind
// the acks of earlier publishes. QoS 0 refusals are dropped and a 3.1.1
// client sending a refused QoS 1 is disconnected, as without a cluster.
func TestACLRefusalAckedInOrder(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	rules := acl.Rules{Publish: []string{"ok/#"}, Subscribe: []string{"#"}}

	var proxy *chaos.Proxy

	c := startCellVia(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": rules}}, nil,
		func(_ int, url string) string {
			p, err := chaos.NewProxy(strings.TrimPrefix(url, "nats://"))
			require.NoError(t, err)
			t.Cleanup(p.Close)

			proxy = p

			return "nats://" + p.Addr()
		})

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sub, "#", 1, false)

	conn, _ := rawConnectAck(t, c.addr(0), "pub")

	proxy.SetDelay(chaos.Both, 500*time.Millisecond)
	proxy.SetMode(chaos.Both, chaos.Delay)

	writePacket(t, conn, &packets.Publish{Topic: "ok/1", QoS: 1, PacketID: 1, Payload: []byte("ok"), Properties: &packets.Properties{}})
	writePacket(t, conn, &packets.Publish{Topic: "no/1", QoS: 1, PacketID: 2, Payload: []byte("no"), Properties: &packets.Properties{}})
	writePacket(t, conn, &packets.Publish{Topic: "no/2", QoS: 2, PacketID: 3, Payload: []byte("no"), Properties: &packets.Properties{}})
	writePacket(t, conn, &packets.Publish{Topic: "no/0", QoS: 0, Payload: []byte("no"), Properties: &packets.Properties{}})

	cp := readLog(t, conn)
	require.Equal(t, packets.PUBACK, cp.Type)
	require.Equal(t, uint16(1), cp.Content.(*packets.Puback).PacketID)
	require.Equal(t, byte(0), cp.Content.(*packets.Puback).ReasonCode)

	cp = readLog(t, conn)
	require.Equal(t, packets.PUBACK, cp.Type)
	require.Equal(t, uint16(2), cp.Content.(*packets.Puback).PacketID)
	require.Equal(t, byte(0x87), cp.Content.(*packets.Puback).ReasonCode)

	cp = readLog(t, conn)
	require.Equal(t, packets.PUBREC, cp.Type)
	require.Equal(t, uint16(3), cp.Content.(*packets.Pubrec).PacketID)
	require.Equal(t, byte(0x87), cp.Content.(*packets.Pubrec).ReasonCode)

	proxy.SetMode(chaos.Both, chaos.Pass)

	writePacket(t, conn, &packets.Publish{Topic: "ok/end", QoS: 1, PacketID: 4, Payload: []byte("end"), Properties: &packets.Properties{}})
	require.Equal(t, packets.PUBACK, readLog(t, conn).Type)

	require.Equal(t, map[string]int{"ok": 1}, drainToEnd(t, sub))

	v3, err := connect311(t, c.addr(0), "v3", "u1", nil)
	require.NoError(t, err)

	tok := v3.Publish("no/3", 1, false, "no")
	tok.WaitTimeout(logWait)
	eventually(t, "the 3.1.1 client to be disconnected", func() bool { return !v3.IsConnectionOpen() })

	noMoreMsgs(t, sub)
}

// A reconnect that comes while the old connection is ending never inherits
// a PUBREC for a message that is not committed, whichever way the old
// connection's state leaves the ID: its own OnDisconnect (held right after
// the state is removed) or a sweep of closed connections at the tenant's
// connection limit (held before OnDisconnect runs at all). The registry
// closes the pipe before the state stops owning the ID, so mochi's clone of
// the offline session's inflight map has no reservation in it.
func TestQoS2NoPubrecInDisconnectWindow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		removed bool // hold after the state is removed, else before
		limits  tenant.Limits
	}{
		{name: "after remove", removed: true},
		{name: "swept at the connection limit", removed: false, limits: tenant.Limits{MaxConnections: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) { disconnectWindow(t, tc.removed, tc.limits) })
	}
}

func disconnectWindow(t *testing.T, holdRemoved bool, limits tenant.Limits) {
	t1 := newTenant(limits)
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	nsID := namespacedID(t1.ID.String(), "win")

	var (
		armed   atomic.Bool
		reached = make(chan struct{})
		release = make(chan struct{})
		gate    = newAppendGate()
	)

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			b.cluster.holdAppend = gate.hold
			b.disconnectHold = func(cl *mqtt.Client, removed bool) {
				if removed == holdRemoved && cl.ID == nsID && armed.CompareAndSwap(true, false) {
					close(reached)
					<-release
				}
			}
		})
	a := c.nodes[0]

	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	expiry := uint32(3600)
	connect := &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: "win", CleanStart: false, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{SessionExpiryInterval: &expiry},
	}

	inflight := func() int {
		cl, ok := a.server.Clients.Get(nsID)
		if !ok {
			return -1
		}

		return cl.State.Inflight.Len()
	}

	conn := rawConnect(t, c.addr(0), connect)

	gate.close()

	writePacket(t, conn, &packets.Publish{Topic: "w/x", QoS: 2, PacketID: 3, Payload: []byte("x"), Properties: &packets.Properties{}})
	eventually(t, "the reservation", func() bool { return inflight() == 1 })

	armed.Store(true)
	require.NoError(t, conn.Close())

	select {
	case <-reached:
	case <-time.After(logWait):
		t.Fatal("the old connection never reached the held window")
	}

	// The old connection's OnDisconnect is held; the reconnect inherits the
	// offline session from mochi now.
	conn = rawConnect(t, c.addr(0), connect)
	require.Zero(t, inflight(), "the reconnect inherited an uncommitted PUBREC")

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))

	if cp, err := packets.ReadPacket(conn); err == nil {
		t.Fatalf("the reconnect was sent %v", cp)
	}

	close(release)
	gate.open()

	writePacket(t, conn, &packets.Publish{Topic: "w/x", QoS: 2, PacketID: 3, Duplicate: true, Payload: []byte("x"), Properties: &packets.Properties{}})

	rec := readLog(t, conn)
	require.Equal(t, packets.PUBREC, rec.Type)
	require.Equal(t, byte(0), rec.Content.(*packets.Pubrec).ReasonCode)

	writePacket(t, conn, &packets.Pubrel{PacketID: 3, Properties: &packets.Properties{}})
	require.Equal(t, packets.PUBCOMP, readLog(t, conn).Type)
}

// CHAOS-1: after a log outage, the acks of the publishes that were held come
// back promptly and in order. Every one of them has a failed attempt behind
// it; the writer used to retry them one at a time with a 100 ms backoff each
// (about ten acks a second, 15 s for these 150), behind a head whose backoff
// had grown towards 5 s. Now the first retry that commits sends every queued
// publish again at once, so they commit together.
func TestAcksPromptAfterLogReturns(t *testing.T) {
	const n = 150

	t1 := newTenant(tenant.Limits{})
	gate := newAppendGate()
	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": {Publish: []string{"#"}, Subscribe: []string{"#"}}}},
		func(b *Broker) { b.cluster.holdAppend = gate.hold })
	a := c.nodes[0]

	conn, _ := rawConnectAck(t, c.addr(0), "crawl")

	gate.failing.Store(true)

	// Odd packet IDs QoS 1, even QoS 2: a QoS 2 retry keeps its message ID.
	qosOf := func(id uint16) byte { return byte(2 - id%2) }

	for id := uint16(1); id <= n; id++ {
		writePacket(t, conn, &packets.Publish{
			Topic: "crawl/x", QoS: qosOf(id), PacketID: id, Payload: []byte(strconv.Itoa(int(id))), Properties: &packets.Properties{},
		})
	}

	// Long enough for every publish to fail its first attempt and for the
	// head's backoff to reach its cap.
	require.Eventually(t, func() bool { return gate.refused.Load() >= n+4 }, logWait, 10*time.Millisecond)
	time.Sleep(2 * time.Second)

	require.Zero(t, a.cluster.acks.Load(), "acked while the log refused every append")

	gate.failing.Store(false)
	healed := time.Now()

	for id := uint16(1); id <= n; id++ {
		cp := readLog(t, conn)

		if qosOf(id) == 1 {
			require.Equal(t, packets.PUBACK, cp.Type, "packet %d", id)
			require.Equal(t, id, cp.Content.(*packets.Puback).PacketID, "acks out of order")
			require.Equal(t, byte(0), cp.Content.(*packets.Puback).ReasonCode)
		} else {
			require.Equal(t, packets.PUBREC, cp.Type, "packet %d", id)
			require.Equal(t, id, cp.Content.(*packets.Pubrec).PacketID, "acks out of order")
			require.Equal(t, byte(0), cp.Content.(*packets.Pubrec).ReasonCode)
		}
	}

	took := time.Since(healed)
	t.Logf("%d acks %s after the log came back", n, took.Round(time.Millisecond))

	// The head waits out at most the capped backoff (1 s), then everything
	// commits in one pipelined pass.
	require.Less(t, took, 3*time.Second, "acks crawled after the log came back")
	require.Equal(t, uint64(n), a.cluster.appends.Load())

	// Once each in the log, in order, whatever the retries.
	var stored []string

	require.NoError(t, a.cluster.Log.Replay(t.Context(), t1.ID.String(), 1, 1<<62, func(m cluster.LogMessage) error {
		stored = append(stored, string(m.Payload))

		return nil
	}))

	want := make([]string, n)
	for i := range want {
		want[i] = strconv.Itoa(i + 1)
	}

	require.Equal(t, want, stored)
}

// readAcksInOrder reads n PUBACKs for packet IDs 1..n, in that order, all
// successful.
func readAcksInOrder(t *testing.T, conn net.Conn, n int) {
	t.Helper()

	for id := uint16(1); id <= uint16(n); id++ {
		cp := readLog(t, conn)
		require.Equal(t, packets.PUBACK, cp.Type, "packet %d", id)
		require.Equal(t, id, cp.Content.(*packets.Puback).PacketID, "acks out of order")
		require.Equal(t, byte(0), cp.Content.(*packets.Puback).ReasonCode)
	}
}

// storedOnce checks the tenant's log holds the payloads 1..n once each, in
// order. The read is retried for logWait: after a server loss the meta layer
// may still be electing, and a consumer cannot be made until it is done.
func storedOnce(t *testing.T, b *Broker, tenantID string, n int) {
	t.Helper()

	var (
		stored  []string
		lastErr error
	)

	read := func() bool {
		stored = stored[:0]

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		lastErr = b.cluster.Log.Replay(ctx, tenantID, 1, 1<<62, func(m cluster.LogMessage) error {
			stored = append(stored, string(m.Payload))

			return nil
		})

		return lastErr == nil
	}

	require.Eventually(t, read, logWait, 200*time.Millisecond, "reading the log back: %v", lastErr)

	want := make([]string, n)
	for i := range want {
		want[i] = strconv.Itoa(i + 1)
	}

	require.Equal(t, want, stored)
}

// A log that commits slower than appendLate is waited out, not flooded
// (review of CHAOS-1). With every commit taking about 1.2 s (the node's
// NATS link delayed 600 ms each way), the head's first wait runs late and
// it is sent once more; the pipe's late bound doubles, so the publishes
// behind it are waited for, and none of them is sent again. A bound per
// publish, with every late head resending the queue, made this a crawl of
// one ack per 2.3 s and n(n+1)/2 + n attempts.
func TestSlowLogWaitedOutNotFlooded(t *testing.T) {
	const n = 10

	c, proxy := startProxiedCell(t, 1, nil)
	a := c.nodes[0]

	conn, _ := rawConnectAck(t, c.addr(0), "slow")

	proxy.SetDelay(chaos.Both, 600*time.Millisecond)
	proxy.SetMode(chaos.Both, chaos.Delay)

	base := a.cluster.appendAttempts.Load()
	sent := time.Now()

	for id := uint16(1); id <= n; id++ {
		writePacket(t, conn, &packets.Publish{Topic: "slow/x", QoS: 1, PacketID: id, Payload: []byte(strconv.Itoa(int(id))), Properties: &packets.Properties{}})
	}

	readAcksInOrder(t, conn, n)

	took := time.Since(sent)
	attempts := a.cluster.appendAttempts.Load() - base

	t.Logf("%d acks %s after the publishes, %d append attempts", n, took.Round(time.Millisecond), attempts)

	require.Less(t, took, 6*time.Second, "acks crawled on a slow log")
	require.LessOrEqual(t, attempts, uint64(n+3), "a slow log was flooded with resends")

	proxy.SetMode(chaos.Both, chaos.Pass)
	storedOnce(t, a, c.t1.ID.String(), n)
}

// A late ack is taken as lost and the publish sent again, deterministically:
// the server-to-client half of the node's NATS link is held, so every append
// commits but no ack comes back. The head runs late and is resent; once the
// link heals every ack arrives, the head commits and the rest follow, with
// nothing behind the head resent (their answers came, in order) and each
// message stored once.
func TestLateAckResentOnceAndAcked(t *testing.T) {
	const n = 20

	c, proxy := startProxiedCell(t, 1, nil)
	a := c.nodes[0]

	conn, _ := rawConnectAck(t, c.addr(0), "late")

	proxy.SetMode(chaos.ServerToClient, chaos.Blackhole)

	base := a.cluster.appendAttempts.Load()

	for id := uint16(1); id <= n; id++ {
		writePacket(t, conn, &packets.Publish{Topic: "late/x", QoS: 1, PacketID: id, Payload: []byte(strconv.Itoa(int(id))), Properties: &packets.Properties{}})
	}

	time.Sleep(2500 * time.Millisecond)

	require.Zero(t, a.cluster.acks.Load(), "acked with no ack from the log")
	require.Greater(t, a.cluster.appendAttempts.Load()-base, uint64(n), "the late head was not sent again")

	proxy.SetMode(chaos.ServerToClient, chaos.Pass)
	healed := time.Now()

	readAcksInOrder(t, conn, n)

	took := time.Since(healed)
	attempts := a.cluster.appendAttempts.Load() - base

	t.Logf("%d acks %s after the heal, %d append attempts", n, took.Round(time.Millisecond), attempts)

	require.Less(t, took, 4*time.Second)
	require.LessOrEqual(t, attempts, uint64(n+4), "publishes behind the head were resent")

	storedOnce(t, a, c.t1.ID.String(), n)
}

// A stream leader that dies takes the acks of the appends it had with it, for
// real: the server leading the log is cut off through its route proxies while
// a client's publishes are in flight to it, so what was on the routes is lost
// when they time out, and the survivors sit out a raft election. The client
// then sends nothing more. Once the log has a leader again the head is sent
// again and commits promptly, and every publish behind it that is still
// unanswered goes again at once: all of them are acked within a couple of
// seconds of the new leader, in order, and stored once. (Resending only an
// unanswered publish with a later answer behind it left these waiting a
// late bound each, about a second per lost ack.)
func TestLostAcksResentWhenLogReturns(t *testing.T) {
	const n = 20

	nats := clustertest.StartProxied(t)
	home := nats.Servers[0].ClientURL()

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	c := buildCell(t, nats, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil,
		func(int, string) string { return home }, false)
	a := c.nodes[0]

	js := c.conns[0].JS()
	stream := a.cluster.Config.LogStream()

	leaderOf := func() string {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()

		st, err := js.Stream(ctx, stream)
		if err != nil {
			return ""
		}

		info, err := st.Info(ctx)
		if err != nil || info.Cluster == nil {
			return ""
		}

		return info.Cluster.Leader
	}

	// The leader must be another server than the node's, so the node stays
	// connected and only the log goes away.
	var leader string

	require.Eventually(t, func() bool {
		leader = leaderOf()
		if leader == "" {
			return false
		}

		if leader != nats.Servers[0].Name() {
			return true
		}

		_, _ = c.conns[0].NC().Request("$JS.API.STREAM.LEADER.STEPDOWN."+stream, nil, 2*time.Second)

		return false
	}, logWait, 500*time.Millisecond)

	victim := slices.IndexFunc(nats.Servers, func(s *server.Server) bool { return s.Name() == leader })
	require.GreaterOrEqual(t, victim, 0)

	conn, _ := rawConnectAck(t, c.addr(0), "lost")
	base := a.cluster.appendAttempts.Load()

	nats.Isolate(victim)

	for id := uint16(1); id <= n; id++ {
		writePacket(t, conn, &packets.Publish{Topic: "lost/x", QoS: 1, PacketID: id, Payload: []byte(strconv.Itoa(int(id))), Properties: &packets.Properties{}})
	}

	var elected time.Time

	require.Eventually(t, func() bool {
		if l := leaderOf(); l != "" && l != leader {
			elected = time.Now()

			return true
		}

		return false
	}, logWait, 50*time.Millisecond)

	readAcksInOrder(t, conn, n)

	took := time.Since(elected)
	attempts := a.cluster.appendAttempts.Load() - base

	t.Logf("leader %s cut off; %d acks %s after the new leader showed, %d append attempts", leader, n, took.Round(time.Millisecond), attempts)

	require.Less(t, took, 3*time.Second, "lost acks were waited out one by one")
	storedOnce(t, a, t1.ID.String(), n)
}

// A client that reuses a QoS 2 packet ID for a new message after PUBCOMP,
// while the record still holds the old message ID (the PUBREL's clear has
// not been written), and resends the new message with DUP on another node
// gets it stored: the old message ID is reused only for the same message,
// so JetStream does not discard the new one as a duplicate (B-C1).
func TestQoS2ReusedPacketIDNotDedupedAsStale(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	c := startClusterWith(t, 2, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			if b.cluster != nil {
				b.cluster.flushInterval = time.Hour
			}
		})

	sub, _, err := connect5(t, c.addr(1), "watcher", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sub, "q/#", 2, false)

	sess := newRawSession()
	p := dialRaw(t, c.addr(0), "qp", sess, false)
	require.Equal(t, byte(0), p.ack(t).ReasonCode)

	p.send(&packets.Publish{PacketID: 1, QoS: 2, Topic: "q/x", Payload: []byte("m1"), Properties: &packets.Properties{}})
	require.True(t, p.alive())
	require.Equal(t, "m1", recvLog(t, sub).Payload)
	p.send(&packets.Pubrel{PacketID: 1, Properties: &packets.Properties{}})
	require.True(t, p.alive())

	// The node loses the cell (as a fence does): no write gets through, so
	// the record keeps packet ID 1's entry for m1.
	c.nodes[0].cluster.fenced.Store(true)
	t.Cleanup(func() { c.nodes[0].cluster.fenced.Store(false) })

	// The client reuses packet ID 1 for a new message, which cannot commit.
	p.send(&packets.Publish{PacketID: 1, QoS: 2, Topic: "q/x", Payload: []byte("N"), Properties: &packets.Properties{}})
	require.True(t, p.alive())
	time.Sleep(300 * time.Millisecond)
	_ = p.conn.Close()

	// It resumes on the other node and resends N with DUP.
	p2 := dialRaw(t, c.addr(1), "qp", sess, false)
	require.Equal(t, byte(0), p2.ack(t).ReasonCode)
	p2.send(&packets.Publish{PacketID: 1, QoS: 2, Duplicate: true, Topic: "q/x", Payload: []byte("N"), Properties: &packets.Properties{}})
	require.True(t, p2.alive())

	m := recvLog(t, sub)
	require.Equal(t, "N", m.Payload, "N was acked with PUBREC but never stored")
}

// oversizedConnect is a persistent 5.0 CONNECT of clientID with a Maximum
// Packet Size of 300 bytes.
func oversizedConnect(clientID string) *paho.Connect {
	expiry, maxPkt := uint32(3600), uint32(300)

	return &paho.Connect{
		ClientID: clientID, Username: "u1", UsernameFlag: true, Password: []byte("secret"), PasswordFlag: true,
		CleanStart: false, KeepAlive: 30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry, MaximumPacketSize: &maxPkt},
	}
}

// recvPayloads collects the payloads c receives until want have arrived or
// logWait passes.
func recvPayloads(t *testing.T, c *client5, want int) []string {
	t.Helper()

	var got []string

	deadline := time.After(logWait)

	for len(got) < want {
		select {
		case m := <-c.msgs:
			got = append(got, m.Payload)
		case <-deadline:
			return got
		}
	}

	return got
}

// A message over a client's Maximum Packet Size that a resumed session's
// replay reaches is discarded as if delivered [MQTT-3.1.2-25]: the replay
// goes on to the next message and the session goes live (B-C2).
func TestOversizedMessageSkippedByReplay(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	s, _, err := dial5(t, c.addr(0), oversizedConnect("small"))
	require.NoError(t, err)
	subscribeQoS(t, s, "big/#", 1, false)
	require.NoError(t, s.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitOffline(t, c.nodes[1], tid, "small")

	pub, _, err := connect5(t, c.addr(1), "big-pub", "u1", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, pub, "big/x", "a", 1))
	require.Equal(t, byte(0), publishQ(t, pub, "big/x", strings.Repeat("Z", 1000), 1))
	require.Equal(t, byte(0), publishQ(t, pub, "big/x", "b", 1))

	s2, ca, err := dial5(t, c.addr(1), oversizedConnect("small"))
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)
	require.Equal(t, []string{"a", "b"}, recvPayloads(t, s2, 2))

	require.Equal(t, byte(0), publishQ(t, pub, "big/x", "live", 1))
	require.Equal(t, []string{"live"}, recvPayloads(t, s2, 1), "the session never went live")
}

// A live delivery over a client's Maximum Packet Size is discarded as if
// delivered: it leaves the inflight map and the session's deliveries, so the
// session's cursor moves past it (B-C2).
func TestOversizedMessageDoesNotPinCursor(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	s, _, err := dial5(t, c.addr(0), oversizedConnect("small2"))
	require.NoError(t, err)
	subscribeQoS(t, s, "big/#", 1, false)

	pub, _, err := connect5(t, c.addr(1), "big-pub2", "u1", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, pub, "big/x", "a", 1))
	require.Equal(t, byte(0), publishQ(t, pub, "big/x", strings.Repeat("Z", 1000), 1))

	for range 5 {
		require.Equal(t, byte(0), publishQ(t, pub, "big/x", "b", 1))
	}

	require.Equal(t, []string{"a", "b", "b", "b", "b", "b"}, recvPayloads(t, s, 6))

	tt := c.nodes[0].cluster.trackIfAny(tid)
	require.NotNil(t, tt)

	eventually(t, "the cursor passes the oversized message", func() bool {
		rec, err := sessionKey(t, c.nodes[0], tid, "small2")

		return err == nil && len(rec.Pending) == 0 && rec.Cursor >= tt.livePos.Load()
	})
}
