package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
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

	conn, ack := rawConnectAck(t, c.addr(0), "rm", "u1")
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
// back instead of disconnected, and every publish is acked in the end.
func TestReceiveMaximumBackpressure311(t *testing.T) {
	c, proxy := startProxiedCell(t, 1, func(b *Broker) { b.receiveMaximum = 2 })
	a := c.nodes[0]

	v3, err := connect311(t, c.addr(0), "bp", "u1", nil)
	require.NoError(t, err)

	msgs := subscribe311(t, v3, "bp/#")

	proxy.SetDelay(chaos.Both, 300*time.Millisecond)
	proxy.SetMode(chaos.Both, chaos.Delay)

	toks := make([]interface{ WaitTimeout(time.Duration) bool }, 0, 6)
	for i := range 6 {
		toks = append(toks, v3.Publish("bp/x", 1, false, fmt.Sprint(i)))
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

	conn, _ := rawConnectAck(t, c.addr(0), "order", "u1")
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
// reaches t2 alone; one under $SYS is dropped.
func TestLogDeliveryTenantIsolation(t *testing.T) {
	c := startCluster(t, 2)
	b := c.nodes[1]

	subs := map[string]*client5{}
	pubs := map[string]*client5{}

	for _, user := range []string{"u1", "u2"} {
		s, _, err := connect5(t, c.addr(1), "dev", user, true)
		require.NoError(t, err)
		subscribeQoS(t, s, "#", 1, false)

		p, _, err := connect5(t, c.addr(0), "dev", user, true)
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
		require.NoError(t, a.inject(tid, "i/x", []byte(fmt.Sprint(i)), qos, cluster.Props{}, ""))
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

// The cluster's QoS 1 and 2 path leaves the rate limit as it was: a 5.0 QoS
// 1 publish over the tenant's budget gets PUBACK 0x97, through the pipe.
func TestQoS1QuotaExceededInOrder(t *testing.T) {
	t1 := newTenant(tenant.Limits{MessagesPerSecond: 0.1, Burst: 1})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)

	conn, _ := rawConnectAck(t, c.addr(0), "quota", "u1")

	for id := uint16(1); id <= 2; id++ {
		writePacket(t, conn, &packets.Publish{Topic: "z", QoS: 1, PacketID: id, Payload: []byte("x"), Properties: &packets.Properties{}})
	}

	codes := map[uint16]byte{}
	order := make([]uint16, 0, 2)

	for range 2 {
		cp := readLog(t, conn)
		require.Equal(t, packets.PUBACK, cp.Type)

		pa := cp.Content.(*packets.Puback)
		codes[pa.PacketID] = pa.ReasonCode
		order = append(order, pa.PacketID)
	}

	require.Equal(t, []uint16{1, 2}, order)
	require.Equal(t, map[uint16]byte{1: 0, 2: 0x97}, codes)
}

// rawConnectAck is rawConnect5 that also returns the CONNACK.
func rawConnectAck(t *testing.T, addr, clientID, user string) (net.Conn, *packets.Connack) {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	writePacket(t, conn, &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: true, KeepAlive: 30,
		Username: user, UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
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
