package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"
)

// collect counts the payloads c receives under prefix until want of them
// arrived (duplicates included), then listens for settle more.
func collectShared(t *testing.T, want int, prefix string, cs ...*client5) []map[string]int {
	t.Helper()

	out := make([]map[string]int, len(cs))
	for i := range out {
		out[i] = map[string]int{}
	}

	total := 0
	deadline := time.After(logWait)

	for total < want {
		got := false

		for i, c := range cs {
			select {
			case m := <-c.msgs:
				if strings.HasPrefix(m.Payload, prefix) {
					out[i][m.Payload]++
					total++
				}

				got = true
			default:
			}
		}

		if got {
			continue
		}

		select {
		case <-deadline:
			t.Fatalf("got %d of %d messages: %v", total, want, out)
		case <-time.After(5 * time.Millisecond):
		}
	}

	time.Sleep(settle)

	for i, c := range cs {
	drain:
		for {
			select {
			case m := <-c.msgs:
				if strings.HasPrefix(m.Payload, prefix) {
					out[i][m.Payload]++
				}
			default:
				break drain
			}
		}
	}

	return out
}

// merge adds the counts of every map.
func merge(ms ...map[string]int) map[string]int {
	out := map[string]int{}

	for _, m := range ms {
		for k, v := range m {
			out[k] += v
		}
	}

	return out
}

func requireEachOnce(t *testing.T, got map[string]int, prefix string, n int) {
	t.Helper()

	for i := range n {
		p := fmt.Sprintf("%s%d", prefix, i)
		require.Equal(t, 1, got[p], "message %s", p)
	}

	require.Len(t, got, n)
}

// Members of one group on nodes A and B, the publisher on A: each of 100
// QoS 1 messages reaches exactly one of them, and both get some. Neither
// node's log reader nor mochi's own shared selection delivers to them
// (ruling R35), or every message would come two or three times.
func TestSharedGroupOneDeliveryAcrossNodes(t *testing.T) {
	c := startCluster(t, 2)

	ma, _, err := connect5(t, c.addr(0), "ma", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, ma, "$share/g/s/#", 1, false)

	mb, _, err := connect5(t, c.addr(1), "mb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, mb, "$share/g/s/#", 1, false)

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	for i := range 100 {
		require.Equal(t, byte(0), publishQ(t, pub, fmt.Sprintf("s/%d", i), fmt.Sprintf("m%d", i), 1))
	}

	got := collectShared(t, 100, "m", ma, mb)
	requireEachOnce(t, merge(got...), "m", 100)
	require.NotEmpty(t, got[0], "A's member got nothing")
	require.NotEmpty(t, got[1], "B's member got nothing")

	t.Logf("a %d, b %d", len(got[0]), len(got[1]))

	tenant := c.t1.ID.String()

	eventually(t, "every shared delivery finished", func() bool {
		return c.nodes[0].cluster.pendingShares(namespacedID(tenant, "ma")) == 0 &&
			c.nodes[1].cluster.pendingShares(namespacedID(tenant, "mb")) == 0
	})

	require.Equal(t, uint64(100), c.nodes[0].cluster.sharedDone.Load()+c.nodes[1].cluster.sharedDone.Load())
}

// sharedRaw is a 5.0 member on a raw connection that never acks: it records
// the payloads of the PUBLISH packets it reads until its connection closes.
type sharedRaw struct {
	conn net.Conn
	mu   sync.Mutex
	got  []string
	done chan struct{}
}

func rawMember(t *testing.T, addr, clientID, filter string) *sharedRaw {
	t.Helper()

	conn, _ := rawConnectAck(t, addr, clientID)
	writePacket(t, conn, &packets.Subscribe{PacketID: 1, Subscriptions: []packets.SubOptions{{Topic: filter, QoS: 1}}, Properties: &packets.Properties{}})

	cp := readLog(t, conn)
	require.Equal(t, packets.SUBACK, cp.Type)
	require.Equal(t, []byte{1}, cp.Content.(*packets.Suback).Reasons)

	r := &sharedRaw{conn: conn, done: make(chan struct{})}

	go func() {
		defer close(r.done)

		for {
			_ = conn.SetReadDeadline(time.Now().Add(time.Minute))

			cp, err := packets.ReadPacket(conn)
			if err != nil {
				return
			}

			if p, ok := cp.Content.(*packets.Publish); ok {
				r.mu.Lock()
				r.got = append(r.got, string(p.Payload))
				r.mu.Unlock()
			}
		}
	}()

	return r
}

func (r *sharedRaw) payloads() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.got...)
}

// Members on A (acks) and B (a raw client that never acks). B's member is
// killed mid-stream: what it held unacked is redelivered to A at once (its
// session's end gives it back, no ack wait), and the rest goes to A. A gets
// every message exactly once.
func TestSharedMemberLoss(t *testing.T) {
	c := startCluster(t, 2)

	ma, _, err := connect5(t, c.addr(0), "ma", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, ma, "$share/g/l/#", 1, false)

	mb := rawMember(t, c.addr(1), "mb", "$share/g/l/#")

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	for i := range 60 {
		require.Equal(t, byte(0), publishQ(t, pub, fmt.Sprintf("l/%d", i), fmt.Sprintf("m%d", i), 1))
	}

	eventually(t, "B's member holds some", func() bool { return len(mb.payloads()) > 0 })

	require.NoError(t, mb.conn.Close())
	<-mb.done

	held := mb.payloads()
	start := time.Now()

	for i := 60; i < 100; i++ {
		require.Equal(t, byte(0), publishQ(t, pub, fmt.Sprintf("l/%d", i), fmt.Sprintf("m%d", i), 1))
	}

	got := collectShared(t, 100, "m", ma)[0]
	requireEachOnce(t, got, "m", 100)

	for _, p := range held {
		require.Equal(t, 1, got[p], "held %s", p)
	}

	// Given back by the session's end, well inside the 30 s ack wait.
	require.Less(t, time.Since(start), 10*time.Second)
	require.Positive(t, c.nodes[1].cluster.sharedLost.Load())

	t.Logf("B held %d unacked", len(held))
}

// QoS 0 goes through the group's queue group. Published on a node with no
// member, each of 100 messages reaches one member of A and B, both get some.
// A group whose only member is on the publishing node gets the node's own
// publish (ruling R5: the queue connection has no NoEcho).
func TestSharedQoS0QueueGroup(t *testing.T) {
	c := startCluster(t, 3)

	ma, _, err := connect5(t, c.addr(0), "ma", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, ma, "$share/g/q/#", 1, false)

	mb, _, err := connect5(t, c.addr(1), "mb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, mb, "$share/g/q/#", 1, false)

	pub, _, err := connect5(t, c.addr(2), "pub", "u1", true)
	require.NoError(t, err)

	// The queue interest of both nodes reaches the publisher's server: warm
	// up until each member got one.
	seenA, seenB := false, false
	deadline := time.Now().Add(logWait)

	for !seenA || !seenB {
		require.False(t, time.Now().After(deadline), "warm-up never reached both members")
		publish0(t, pub, "q/warm", "warm", nil)

		select {
		case <-ma.msgs:
			seenA = true
		case <-mb.msgs:
			seenB = true
		case <-time.After(20 * time.Millisecond):
		}
	}

	time.Sleep(settle)

	for _, m := range []*client5{ma, mb} {
	drain:
		for {
			select {
			case <-m.msgs:
			default:
				break drain
			}
		}
	}

	for i := range 100 {
		publish0(t, pub, fmt.Sprintf("q/%d", i), fmt.Sprintf("z%d", i), nil)
	}

	got := collectShared(t, 100, "z", ma, mb)
	requireEachOnce(t, merge(got...), "z", 100)
	require.NotEmpty(t, got[0], "A's member got nothing")
	require.NotEmpty(t, got[1], "B's member got nothing")

	t.Run("only member on the publishing node", func(t *testing.T) {
		solo, _, err := connect5(t, c.addr(0), "solo", "u1", true)
		require.NoError(t, err)
		subscribeQoS(t, solo, "$share/solo/own/#", 0, false)

		local, _, err := connect5(t, c.addr(0), "local", "u1", true)
		require.NoError(t, err)

		publish0(t, local, "own/x", "mine", nil)
		require.Equal(t, "mine", recvMsg(t, solo).Payload)
		noMoreMsgs(t, solo)
	})
}

// A member that also holds an ordinary subscription matching the topic gets
// a copy for each [MQTT 4.8.2]: one from its own subscription, one as the
// group's pick. Never a third (mochi's shared selection on the publishing
// node's fan-out) and never one for the shared subscription through the log
// reader.
func TestSharedAndOrdinarySubscription(t *testing.T) {
	c := startCluster(t, 2)

	m, _, err := connect5(t, c.addr(0), "m", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, m, "d/#", 1, false)
	subscribeQoS(t, m, "$share/g/d/#", 1, false)

	pubA, _, err := connect5(t, c.addr(0), "puba", "u1", true)
	require.NoError(t, err)

	pubB, _, err := connect5(t, c.addr(1), "pubb", "u1", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, pubA, "d/a", "pa", 1))
	require.Equal(t, byte(0), publishQ(t, pubB, "d/b", "pb", 1))

	got := collectShared(t, 4, "p", m)[0]
	require.Equal(t, map[string]int{"pa": 2, "pb": 2}, got)

	// The local QoS 0 publish: the ordinary copy from mochi's fan-out, the
	// group's from the queue group.
	publish0(t, pubA, "d/c", "pc", nil)
	require.Equal(t, map[string]int{"pc": 2}, collectShared(t, 2, "pc", m)[0])
}

// Two tenants with the same group and filter are two groups.
func TestSharedTenantIsolation(t *testing.T) {
	c := startCluster(t, 2)

	m1, _, err := connect5(t, c.addr(0), "m", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, m1, "$share/g/i/#", 1, false)

	m2, _, err := connect5(t, c.addr(1), "m", "u2", true)
	require.NoError(t, err)
	subscribeQoS(t, m2, "$share/g/i/#", 1, false)

	p1, _, err := connect5(t, c.addr(1), "p", "u1", true)
	require.NoError(t, err)

	p2, _, err := connect5(t, c.addr(0), "p", "u2", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, p1, "i/x", "one", 1))
	require.Equal(t, byte(0), publishQ(t, p2, "i/x", "two", 1))

	require.Equal(t, "one", recvLog(t, m1).Payload)
	require.Equal(t, "two", recvLog(t, m2).Payload)

	// QoS 0 from each member's own node.
	p1a, _, err := connect5(t, c.addr(0), "pa", "u1", true)
	require.NoError(t, err)

	p2b, _, err := connect5(t, c.addr(1), "pb", "u2", true)
	require.NoError(t, err)

	publish0(t, p2b, "i/y", "two0", nil)
	publish0(t, p1a, "i/y", "one0", nil)

	require.Equal(t, "one0", recvMsg(t, m1).Payload)
	require.Equal(t, "two0", recvMsg(t, m2).Payload)
	noMoreMsgs(t, m1)
	noMoreMsgs(t, m2)
}

// A persistent session's shared subscription outlives its connection: the
// group's messages wait in its consumer, and the session resumed on another
// node rejoins the group there and gets them. Its cursor is not held back by
// them: shared messages are not the session's own deliveries (ruling R35).
func TestSharedPersistentRejoin(t *testing.T) {
	c := startCluster(t, 2)
	tenant := c.t1.ID.String()
	nsID := namespacedID(tenant, "pm")

	m, _, err := connect5(t, c.addr(0), "pm", "u1", false)
	require.NoError(t, err)
	subscribeQoS(t, m, "$share/g/p/#", 1, false)
	require.Equal(t, []string{"$share/g/" + tenant + "/p/#"}, c.nodes[0].cluster.sharedOf(nsID))

	pub, _, err := connect5(t, c.addr(1), "pub", "u1", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, pub, "p/0", "r0", 1))
	require.Equal(t, "r0", recvLog(t, m).Payload)

	require.NoError(t, m.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	eventually(t, "membership ended on A", func() bool { return len(c.nodes[0].cluster.sharedOf(nsID)) == 0 })

	for i := 1; i <= 5; i++ {
		require.Equal(t, byte(0), publishQ(t, pub, fmt.Sprintf("p/%d", i), fmt.Sprintf("r%d", i), 1))
	}

	m2, ca, err := connect5(t, c.addr(1), "pm", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)

	got := collectShared(t, 5, "r", m2)[0]
	require.Equal(t, map[string]int{"r1": 1, "r2": 1, "r3": 1, "r4": 1, "r5": 1}, got)
	require.Equal(t, []string{"$share/g/" + tenant + "/p/#"}, c.nodes[1].cluster.sharedOf(nsID))

	// The cursor follows the reader: nothing of the session's own holds it.
	b := c.nodes[1].cluster

	eventually(t, "cursor at the live position", func() bool {
		tt := b.trackIfAny(tenant)
		if tt == nil {
			return false
		}

		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		defer cancel()

		rec, _, err := b.Sessions.Get(ctx, tenant, "pm")

		return err == nil && tt.livePos.Load() > 0 && rec.Cursor >= tt.livePos.Load()
	})
}

// MQTT 5: No Local on a shared subscription is a protocol error; mochi
// refuses the filter in the SUBACK with 0x82 [MQTT-3.8.3-4], and no group is
// joined.
func TestSharedNoLocalRefused(t *testing.T) {
	c := startCluster(t, 1)

	m, _, err := connect5(t, c.addr(0), "m", "u1", true)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	sa, _ := m.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "$share/g/n/#", QoS: 1, NoLocal: true}}})
	require.NotNil(t, sa)
	require.Equal(t, []byte{0x82}, sa.Reasons)
	require.Empty(t, c.nodes[0].cluster.sharedOf(namespacedID(c.t1.ID.String(), "m")))
}

// UNSUBSCRIBE leaves the group: the member gets nothing more, and with no
// member left the messages wait for the next one.
func TestSharedUnsubscribeLeaves(t *testing.T) {
	c := startCluster(t, 1)
	nsID := namespacedID(c.t1.ID.String(), "m")

	m, _, err := connect5(t, c.addr(0), "m", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, m, "$share/g/u/#", 1, false)

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publishQ(t, pub, "u/a", "before", 1))
	require.Equal(t, "before", recvLog(t, m).Payload)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err = m.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{"$share/g/u/#"}})
	require.NoError(t, err)
	require.Empty(t, c.nodes[0].cluster.sharedOf(nsID))

	require.Equal(t, byte(0), publishQ(t, pub, "u/b", "after", 1))
	noMoreMsgs(t, m)

	next, _, err := connect5(t, c.addr(0), "next", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, next, "$share/g/u/#", 1, false)
	require.Equal(t, "after", recvLog(t, next).Payload)
}

// rawPersist connects a raw 5.0 client of u1 as clientID with a one-hour
// session expiry and, when recvMax is not zero, that Receive Maximum. It
// returns the connection and its CONNACK.
func rawPersist(t *testing.T, addr, clientID string, recvMax uint16) (net.Conn, *packets.Connack) {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	expiry := uint32(3600)
	props := &packets.Properties{SessionExpiryInterval: &expiry}

	if recvMax > 0 {
		props.ReceiveMaximum = &recvMax
	}

	writePacket(t, conn, &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: false, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: props,
	})

	cp := readLog(t, conn)
	require.Equal(t, packets.CONNACK, cp.Type)

	ack := cp.Content.(*packets.Connack)
	require.Equal(t, byte(0), ack.ReasonCode)

	return conn, ack
}

func rawSubscribe(t *testing.T, conn net.Conn, qos byte, filters ...string) {
	t.Helper()

	opts := make([]packets.SubOptions, 0, len(filters))
	for _, f := range filters {
		opts = append(opts, packets.SubOptions{Topic: f, QoS: qos})
	}

	writePacket(t, conn, &packets.Subscribe{PacketID: 1, Subscriptions: opts, Properties: &packets.Properties{}})

	cp := readLog(t, conn)
	require.Equal(t, packets.SUBACK, cp.Type)

	for _, r := range cp.Content.(*packets.Suback).Reasons {
		require.Equal(t, qos, r)
	}
}

// readFor reads one packet from conn, or returns nil when none comes within d.
func readFor(t *testing.T, conn net.Conn, d time.Duration) *packets.ControlPacket {
	t.Helper()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(d)))

	cp, err := packets.ReadPacket(conn)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil
		}

		require.NoError(t, err)
	}

	return cp
}

// A same-node takeover of a persistent session with a shared QoS 1 message
// in flight: mochi clones the inflight map into the new connection and
// clears the old one's, which must not finish the delivery (and ack it to
// the group). The new connection dies before its PUBACK, and the message
// goes to the next member.
func TestSharedTakeoverKeepsInflight(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	m1, _ := rawPersist(t, c.addr(0), "tk", 0)
	rawSubscribe(t, m1, 1, "$share/g/tk/#")

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publishQ(t, pub, "tk/x", "one", 1))

	cp := readLog(t, m1)
	require.Equal(t, packets.PUBLISH, cp.Type)
	require.Equal(t, "one", string(cp.Content.(*packets.Publish).Payload))

	// The same client again, on the same node: handed over, session present,
	// and mochi resends the delivery to it.
	m2, ack := rawPersist(t, c.addr(0), "tk", 0)
	require.True(t, ack.SessionPresent)

	cp = readLog(t, m2)
	require.Equal(t, packets.PUBLISH, cp.Type)
	require.Equal(t, "one", string(cp.Content.(*packets.Publish).Payload))
	require.Equal(t, 1, c.nodes[0].cluster.pendingShares(namespacedID(tid, "tk")))

	// Gone before the PUBACK.
	require.NoError(t, m2.Close())
	waitOffline(t, c.nodes[0], tid, "tk")

	next, _, err := connect5(t, c.addr(1), "next", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, next, "$share/g/tk/#", 1, false)
	require.Equal(t, "one", recvLog(t, next).Payload)
}

// A 5.0 member with a Receive Maximum of 5 holding an ordinary and a shared
// subscription to the same topics resumes with 20 log messages to replay and
// 20 waiting in its group: it never has more than 5 unacknowledged
// deliveries [MQTT-3.3.4-9], and once it acks, it gets all 40.
func TestSharedReceiveMaximum(t *testing.T) {
	const (
		recvMax = 5
		total   = 20
	)

	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	m, _ := rawPersist(t, c.addr(0), "rm", recvMax)
	rawSubscribe(t, m, 1, "rm/#", "$share/g/rm/#")
	require.NoError(t, m.Close())
	waitOffline(t, c.nodes[0], tid, "rm")

	pub, _, err := connect5(t, c.addr(1), "pub", "u1", true)
	require.NoError(t, err)

	for i := range total {
		require.Equal(t, byte(0), publishQ(t, pub, "rm/x", fmt.Sprintf("m%d", i), 1))
	}

	m, ack := rawPersist(t, c.addr(0), "rm", recvMax)
	require.True(t, ack.SessionPresent)

	got := map[string]int{}
	var unacked []uint16

	// Nothing acked: the window fills and stays full.
	for {
		cp := readFor(t, m, 1500*time.Millisecond)
		if cp == nil {
			break
		}

		if p, ok := cp.Content.(*packets.Publish); ok {
			require.False(t, p.Duplicate)

			unacked = append(unacked, p.PacketID)
			got[string(p.Payload)]++
			require.LessOrEqual(t, len(unacked), recvMax, "unacknowledged deliveries over the Receive Maximum")
		}
	}

	require.Len(t, unacked, recvMax)

	// Ack as they come: everything arrives, once from each subscription.
	for _, id := range unacked {
		writePacket(t, m, &packets.Puback{PacketID: id, Properties: &packets.Properties{}})
	}

	deadline := time.Now().Add(logWait)

	for len(got) < total || sum(got) < 2*total {
		require.False(t, time.Now().After(deadline), "got %v", got)

		cp := readFor(t, m, time.Second)
		if cp == nil {
			continue
		}

		if p, ok := cp.Content.(*packets.Publish); ok {
			got[string(p.Payload)]++
			writePacket(t, m, &packets.Puback{PacketID: p.PacketID, Properties: &packets.Properties{}})
		}
	}

	for i := range total {
		require.Equal(t, 2, got[fmt.Sprintf("m%d", i)], "m%d", i)
	}
}

func sum(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}

	return n
}

// Ruling R37: a member that disconnects between a shared QoS 2 delivery's
// PUBREC and PUBCOMP gets the PUBREL when its session resumes, on another
// node, and the packet ID stays taken until the PUBCOMP, so a new QoS 2
// delivery right after does not reuse it (the client would take it for the
// old one and drop it). The first message is not sent again.
func TestSharedPubrelResumed(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	m, _ := rawPersist(t, c.addr(0), "q2", 0)
	rawSubscribe(t, m, 2, "$share/g/q2/#")

	pub, _, err := connect5(t, c.addr(1), "pub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publishQ(t, pub, "q2/a", "first", 2))

	cp := readLog(t, m)
	require.Equal(t, packets.PUBLISH, cp.Type)

	first := cp.Content.(*packets.Publish)
	require.Equal(t, "first", string(first.Payload))

	writePacket(t, m, &packets.Pubrec{PacketID: first.PacketID, Properties: &packets.Properties{}})

	cp = readLog(t, m)
	require.Equal(t, packets.PUBREL, cp.Type)
	require.Equal(t, first.PacketID, cp.Content.(*packets.Pubrel).PacketID)

	// Gone without the PUBCOMP.
	require.NoError(t, m.Close())
	waitOffline(t, c.nodes[0], tid, "q2")

	m, ack := rawPersist(t, c.addr(1), "q2", 0)
	require.True(t, ack.SessionPresent)

	cp = readLog(t, m)
	require.Equal(t, packets.PUBREL, cp.Type, "the PUBREL is sent again on resume")
	require.Equal(t, first.PacketID, cp.Content.(*packets.Pubrel).PacketID)

	require.Equal(t, byte(0), publishQ(t, pub, "q2/b", "second", 2))

	cp = readLog(t, m)
	require.Equal(t, packets.PUBLISH, cp.Type)

	second := cp.Content.(*packets.Publish)
	require.Equal(t, "second", string(second.Payload))
	require.NotEqual(t, first.PacketID, second.PacketID, "the new delivery reused the packet ID still awaiting PUBCOMP")

	writePacket(t, m, &packets.Pubcomp{PacketID: first.PacketID, Properties: &packets.Properties{}})
	writePacket(t, m, &packets.Pubrec{PacketID: second.PacketID, Properties: &packets.Properties{}})

	cp = readLog(t, m)
	require.Equal(t, packets.PUBREL, cp.Type)
	writePacket(t, m, &packets.Pubcomp{PacketID: second.PacketID, Properties: &packets.Properties{}})

	// Nothing more: "first" is not sent again.
	require.Nil(t, readFor(t, m, settle))

	eventually(t, "the record free of shared QoS 2 state", func() bool {
		rec, err := sessionKey(t, c.nodes[1], tid, "q2")

		return err == nil && len(rec.QoS2Out) == 0
	})
}
