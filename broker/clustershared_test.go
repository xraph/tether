package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/tenant"
)

// collectShared counts the payloads c receives under prefix until want of them
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

// A shared QoS 1 write that fails on the network while mochi still has the
// connection open (its reader has not seen the peer go yet) loses the
// message with the connection: the member's connection is dropped and the
// message goes to another member. It used to be taken for a packet that can
// never be sent, finished and acked, so no member got it. This was
// TestSharedMemberLoss failing now and then with one message missing: the
// one being written as the member's peer closed.
func TestSharedWriteFailureLosesConnection(t *testing.T) {
	var (
		armed  atomic.Bool
		target atomic.Value // the namespaced client ID whose write breaks
		broke  = make(chan error, 1)
		node   *clusterNode
	)

	c, tid := sharedSeamCell(t, func(n *clusterNode) {
		node = n
		n.shareBeforeSend = func(nsID string) {
			if id, _ := target.Load().(string); id != nsID || !armed.CompareAndSwap(true, false) {
				return
			}

			// Shut down only the server's sending half: the write fails as
			// on a peer that is gone, and mochi's reader, still blocked,
			// keeps the client open.
			_, cl, _, ok := n.shareClient(nsID)
			if !ok {
				broke <- errors.New("no live connection")

				return
			}

			hc, ok := cl.Net.Conn.(interface{ CloseWrite() error })
			if !ok {
				broke <- fmt.Errorf("%T cannot half-close", cl.Net.Conn)

				return
			}

			broke <- hc.CloseWrite()
		}
	})

	ma, _, err := connect5(t, c.addr(0), "ma", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, ma, "$share/g/w/#", 1, false)

	rawMember(t, c.addr(0), "mb", "$share/g/w/#")

	mbID := namespacedID(tid, "mb")
	target.Store(mbID)
	armed.Store(true)

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	const total = 10

	for i := range total {
		require.Equal(t, byte(0), publishQ(t, pub, fmt.Sprintf("w/%d", i), fmt.Sprintf("w%d", i), 1))
	}

	select {
	case err := <-broke:
		require.NoError(t, err)
	case <-time.After(logWait):
		t.Fatal("no shared message was written to mb")
	}

	got := collectShared(t, total, "w", ma)[0]
	requireEachOnce(t, got, "w", total)

	eventually(t, "mb's connection dropped", func() bool {
		_, _, _, ok := node.shareClient(mbID)

		return !ok
	})
	require.Positive(t, node.sharedLost.Load())
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

// Ruling R39 [MQTT-4.8.2-4]: a shared QoS 2 delivery whose connection breaks
// before its PUBREC is the session's. Another member that joins meanwhile
// never gets it, and the session resumed on another node gets it again with
// DUP under the same packet ID, once.
func TestSharedQoS2PinnedToSession(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	m, _ := rawPersist(t, c.addr(0), "p2", 0)
	rawSubscribe(t, m, 2, "$share/g/p2/#")

	pub, _, err := connect5(t, c.addr(1), "pub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publishQ(t, pub, "p2/a", "pinned", 2))

	cp := readLog(t, m)
	require.Equal(t, packets.PUBLISH, cp.Type)

	first := cp.Content.(*packets.Publish)
	require.Equal(t, "pinned", string(first.Payload))

	// Gone before the PUBREC.
	require.NoError(t, m.Close())
	waitOffline(t, c.nodes[0], tid, "p2")

	other, _, err := connect5(t, c.addr(1), "other", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, other, "$share/g/p2/#", 2, false)

	m, ack := rawPersist(t, c.addr(1), "p2", 0)
	require.True(t, ack.SessionPresent)

	cp = readLog(t, m)
	require.Equal(t, packets.PUBLISH, cp.Type)

	again := cp.Content.(*packets.Publish)
	require.Equal(t, "pinned", string(again.Payload))
	require.Equal(t, first.PacketID, again.PacketID)
	require.True(t, again.Duplicate)
	require.Equal(t, byte(2), again.QoS)

	writePacket(t, m, &packets.Pubrec{PacketID: again.PacketID, Properties: &packets.Properties{}})

	cp = readLog(t, m)
	require.Equal(t, packets.PUBREL, cp.Type)
	writePacket(t, m, &packets.Pubcomp{PacketID: again.PacketID, Properties: &packets.Properties{}})

	require.Nil(t, readFor(t, m, settle), "sent again after its flow completed")
	noMoreMsgs(t, other)

	eventually(t, "the record free of it", func() bool {
		rec, err := sessionKey(t, c.nodes[1], tid, "p2")

		return err == nil && len(rec.QoS2Out) == 0
	})
}

// Ruling R39 [MQTT-4.8.2-5]: when the session of a shared QoS 2 delivery
// ends before the delivery completes (here a clean start), the message goes
// to no other member and not to the new session either.
func TestSharedQoS2DroppedWithSession(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	m, _ := rawPersist(t, c.addr(0), "d2", 0)
	rawSubscribe(t, m, 2, "$share/g/d2/#")

	pub, _, err := connect5(t, c.addr(1), "pub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publishQ(t, pub, "d2/a", "pinned", 2))

	cp := readLog(t, m)
	require.Equal(t, packets.PUBLISH, cp.Type)
	require.NoError(t, m.Close())
	waitOffline(t, c.nodes[0], tid, "d2")

	other, _, err := connect5(t, c.addr(1), "other", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, other, "$share/g/d2/#", 2, false)

	fresh, ack, err := connect5(t, c.addr(0), "d2", "u1", true)
	require.NoError(t, err)
	require.False(t, ack.SessionPresent)

	// Sentinel: a later message reaches the group (the other member), so
	// the pinned one had its chance to go anywhere.
	require.Equal(t, byte(0), publishQ(t, pub, "d2/b", "later", 2))
	require.Equal(t, "later", recvLog(t, other).Payload)
	noMoreMsgs(t, other)
	noMoreMsgs(t, fresh)
}

// gateSeam holds a seam when armed: reached is signalled once and the seam
// waits for release.
type gateSeam struct {
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func newGateSeam() *gateSeam {
	return &gateSeam{reached: make(chan struct{}, 1), release: make(chan struct{})}
}

func (g *gateSeam) hold() {
	if !g.armed.CompareAndSwap(true, false) {
		return
	}

	g.reached <- struct{}{}
	<-g.release
}

func (g *gateSeam) wait(t *testing.T) {
	t.Helper()

	select {
	case <-g.reached:
	case <-time.After(logWait):
		t.Fatal("seam not reached")
	}
}

// sharedSeamCell is a one-node cell of tenant t1 (user u1, everything
// granted) whose broker runs before on its cluster node.
func sharedSeamCell(t *testing.T, before func(n *clusterNode)) (*testCell, string) {
	t.Helper()

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}

	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) { before(b.cluster) })

	return c, t1.ID.String()
}

// Fix round 3, I1: a same-node takeover lands while the writer holds the old
// connection for a shared QoS 2 delivery (seam between choosing the
// connection and the pin). The delivery follows the session to the new
// connection instead of being set on the old one after mochi cloned it.
func TestSharedTakeoverDuringSend(t *testing.T) {
	seam := newGateSeam()
	c, _ := sharedSeamCell(t, func(n *clusterNode) { n.shareBeforeSend = func(string) { seam.hold() } })

	m1, _ := rawPersist(t, c.addr(0), "ts", 0)
	rawSubscribe(t, m1, 2, "$share/g/ts/#")

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	seam.armed.Store(true)
	require.Equal(t, byte(0), publishQ(t, pub, "ts/x", "held", 2))
	seam.wait(t)

	m2, ack := rawPersist(t, c.addr(0), "ts", 0)
	require.True(t, ack.SessionPresent)

	close(seam.release)

	cp := readFor(t, m2, 10*time.Second)
	require.NotNil(t, cp, "the delivery never reached the connection that took the session over")
	require.Equal(t, packets.PUBLISH, cp.Type)

	p := cp.Content.(*packets.Publish)
	require.Equal(t, "held", string(p.Payload))

	writePacket(t, m2, &packets.Pubrec{PacketID: p.PacketID, Properties: &packets.Properties{}})

	cp = readLog(t, m2)
	require.Equal(t, packets.PUBREL, cp.Type)
	writePacket(t, m2, &packets.Pubcomp{PacketID: p.PacketID, Properties: &packets.Properties{}})

	for {
		cp := readFor(t, m2, settle)
		if cp == nil {
			break
		}

		// A copy mochi resent from the clone is the same packet: DUP, same ID.
		q, ok := cp.Content.(*packets.Publish)
		require.True(t, ok, "unexpected %v", cp)
		require.Equal(t, p.PacketID, q.PacketID)
		require.True(t, q.Duplicate)
	}
}

// Fix round 3, I2: a shared QoS 2 pin racing the session's end. Pinned after
// the session's last record was written, it is not durable, so it goes back
// to the group (another member gets it). Pinned before, the last record holds
// it, so no other member gets it and the resumed session does.
func TestSharedPinRacesSessionEnd(t *testing.T) {
	t.Run("pin after the last record", func(t *testing.T) {
		seam := newGateSeam()
		c, tid := sharedSeamCell(t, func(n *clusterNode) { n.shareBeforeSend = func(string) { seam.hold() } })

		m, _ := rawPersist(t, c.addr(0), "pr", 0)
		rawSubscribe(t, m, 2, "$share/g/pr/#")

		pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
		require.NoError(t, err)

		seam.armed.Store(true)
		require.Equal(t, byte(0), publishQ(t, pub, "pr/x", "late", 2))
		seam.wait(t)

		require.NoError(t, m.Close())
		waitOffline(t, c.nodes[0], tid, "pr")

		other, _, err := connect5(t, c.addr(0), "other", "u1", true)
		require.NoError(t, err)
		subscribeQoS(t, other, "$share/g/pr/#", 2, false)

		close(seam.release)

		require.Equal(t, "late", recvLog(t, other).Payload)

		rec, err := sessionKey(t, c.nodes[0], tid, "pr")
		require.NoError(t, err)
		require.Empty(t, rec.QoS2Out)
	})

	t.Run("pin in the last record", func(t *testing.T) {
		seam := newGateSeam()
		c, tid := sharedSeamCell(t, func(n *clusterNode) { n.sharePinned = seam.hold })

		m, _ := rawPersist(t, c.addr(0), "pi", 0)
		rawSubscribe(t, m, 2, "$share/g/pi/#")

		pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
		require.NoError(t, err)

		seam.armed.Store(true)
		require.Equal(t, byte(0), publishQ(t, pub, "pi/x", "kept", 2))
		seam.wait(t)

		require.NoError(t, m.Close())
		waitOffline(t, c.nodes[0], tid, "pi")

		other, _, err := connect5(t, c.addr(0), "other", "u1", true)
		require.NoError(t, err)
		subscribeQoS(t, other, "$share/g/pi/#", 2, false)

		close(seam.release)
		noMoreMsgs(t, other)

		m2, ack := rawPersist(t, c.addr(0), "pi", 0)
		require.True(t, ack.SessionPresent)

		cp := readLog(t, m2)
		require.Equal(t, packets.PUBLISH, cp.Type)
		require.Equal(t, "kept", string(cp.Content.(*packets.Publish).Payload))
		noMoreMsgs(t, other)
	})
}

// Ruling R40: a resumed session's pinned shared QoS 2 far below its own QoS 2
// in the log is read on its own, not by a ranged read across everything the
// tenant logged in between.
func TestSharedPinnedFetchedAlone(t *testing.T) {
	const between = 2000

	c := startCluster(t, 1)
	n := c.nodes[0].cluster
	tid := c.t1.ID.String()

	m, _ := rawPersist(t, c.addr(0), "pf", 0)
	rawSubscribe(t, m, 2, "$share/g/pf/#", "own/#")

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	// The shared one, received and left without PUBREC: pinned.
	require.Equal(t, byte(0), publishQ(t, pub, "pf/x", "pinned", 2))

	cp := readLog(t, m)
	require.Equal(t, "pinned", string(cp.Content.(*packets.Publish).Payload))

	// The tenant logs a lot on other topics.
	pending := make([]*cluster.PendingAppend, 0, between)

	for i := range between {
		pa, err := n.Log.AppendAsync(t.Context(), cluster.LogMessage{Tenant: tid, Topic: "noise/x", Payload: []byte(fmt.Sprint(i)), QoS: 1}, "")
		require.NoError(t, err)

		pending = append(pending, pa)
	}

	for _, pa := range pending {
		_, _, err := pa.Wait(t.Context())
		require.NoError(t, err)
	}

	// The session's own QoS 2, high in the log, also left without PUBREC.
	require.Equal(t, byte(0), publishQ(t, pub, "own/x", "own", 2))

	cp = readLog(t, m)
	require.Equal(t, "own", string(cp.Content.(*packets.Publish).Payload))

	require.NoError(t, m.Close())
	waitOffline(t, c.nodes[0], tid, "pf")

	before := n.fetchRead.Load()
	start := time.Now()

	m2, ack := rawPersist(t, c.addr(0), "pf", 0)
	require.True(t, ack.SessionPresent)

	got := map[string]bool{}

	for len(got) < 2 {
		cp := readLog(t, m2)
		if p, ok := cp.Content.(*packets.Publish); ok {
			require.True(t, p.Duplicate)
			got[string(p.Payload)] = true
		}
	}

	require.Equal(t, map[string]bool{"pinned": true, "own": true}, got)
	require.Less(t, n.fetchRead.Load()-before, uint64(10), "the resume read the log between the pinned and its own")
	t.Logf("resumed in %v", time.Since(start))
}

// A shared message over the member's Maximum Packet Size is discarded as if
// delivered, at QoS 1 and 2: the group's message is finished once, nothing
// stays in flight, and the next message arrives (B-C2).
func TestSharedOversizedDiscarded(t *testing.T) {
	c := startCluster(t, 2)
	tenant := c.t1.ID.String()

	m, _, err := dial5(t, c.addr(0), oversizedConnect("small-member"))
	require.NoError(t, err)
	subscribeQoS(t, m, "$share/g/big/#", 2, false)

	pub, _, err := connect5(t, c.addr(1), "big-pub", "u1", true)
	require.NoError(t, err)

	for _, qos := range []byte{1, 2} {
		require.Equal(t, byte(0), publishQ(t, pub, "big/x", "a", qos))
		require.Equal(t, byte(0), publishQ(t, pub, "big/x", strings.Repeat("Z", 1000), qos))
		require.Equal(t, byte(0), publishQ(t, pub, "big/x", "b", qos))
	}

	require.Equal(t, []string{"a", "b", "a", "b"}, recvPayloads(t, m, 4))

	eventually(t, "every shared delivery finished", func() bool {
		return c.nodes[0].cluster.pendingShares(namespacedID(tenant, "small-member")) == 0 &&
			c.nodes[0].cluster.sharedDone.Load() == 6
	})
	require.Equal(t, uint64(2), c.nodes[0].cluster.oversized.Load())
	require.Zero(t, c.nodes[0].cluster.sharedLost.Load())
}

// C-I3 (2): a shared QoS 2 delivery at PUBREL when a same-node takeover
// lands is the session's own: the new connection gets its PUBREL from the
// clone, and the writer that wrote the PUBLISH to the old connection does
// not take the cloned PUBREL entry for another delivery and send the
// message again under a fresh packet ID.
func TestSharedTakeoverAtPubrelNotResent(t *testing.T) {
	seam := newGateSeam()
	c, _ := sharedSeamCell(t, func(n *clusterNode) { n.shareAfterWrite = func(string) { seam.hold() } })

	m1, _ := rawPersist(t, c.addr(0), "tp", 0)
	rawSubscribe(t, m1, 2, "$share/g/tp/#")

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	seam.armed.Store(true)
	require.Equal(t, byte(0), publishQ(t, pub, "tp/x", "once", 2))
	seam.wait(t)

	cp := readLog(t, m1)
	require.Equal(t, packets.PUBLISH, cp.Type)

	id := cp.Content.(*packets.Publish).PacketID
	writePacket(t, m1, &packets.Pubrec{PacketID: id, Properties: &packets.Properties{}})

	cp = readLog(t, m1)
	require.Equal(t, packets.PUBREL, cp.Type)

	m2, ack := rawPersist(t, c.addr(0), "tp", 0)
	require.True(t, ack.SessionPresent)

	close(seam.release)

	cp = readLog(t, m2)
	require.Equal(t, packets.PUBREL, cp.Type)
	require.Equal(t, id, cp.Content.(*packets.Pubrel).PacketID)

	if cp := readFor(t, m2, time.Second); cp != nil {
		t.Fatalf("after the PUBREL the new connection got %v: the message went out twice", cp)
	}
}

// C-I3 (3): a writer that chose the old connection before a same-node
// takeover must not take a packet ID from it and clear the session's books
// under that ID: the new connection may carry another delivery there (here a
// log QoS 2 delivery the new connection got after the takeover), which would
// be dropped from the books and replayed under a fresh ID, a second copy of
// a QoS 2 message. The writer starts again on the new connection.
func TestSharedStaleWriterKeepsBooks(t *testing.T) {
	seam := newGateSeam()
	c, _ := sharedSeamCell(t, func(n *clusterNode) { n.shareBeforeSend = func(string) { seam.hold() } })

	m1, _ := rawPersist(t, c.addr(0), "sw", 0)
	rawSubscribe(t, m1, 2, "$share/g/sw/#", "swl/#")

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	// The first stays in flight at PUBLISH: m1 does not answer it.
	require.Equal(t, byte(0), publishQ(t, pub, "sw/x", "first", 2))

	cp := readLog(t, m1)
	require.Equal(t, packets.PUBLISH, cp.Type)

	seam.armed.Store(true)
	require.Equal(t, byte(0), publishQ(t, pub, "sw/x", "second", 2))
	seam.wait(t)

	m2, ack := rawPersist(t, c.addr(0), "sw", 0)
	require.True(t, ack.SessionPresent)

	// A log delivery on the new connection, while the writer still holds
	// the old one: it takes the new connection's next packet ID.
	require.Equal(t, byte(0), publishQ(t, pub, "swl/x", "log", 2))

	ids := map[string]map[uint16]bool{}

	// Setup, not a bound on the broker: the loop leaves as soon as the log
	// delivery is in.
	deadline := time.Now().Add(scaled(3 * time.Second))
	for time.Now().Before(deadline) {
		cp := readFor(t, m2, 200*time.Millisecond)
		if cp == nil {
			if len(ids["log"]) > 0 {
				break
			}

			continue
		}

		if p, ok := cp.Content.(*packets.Publish); ok {
			if ids[string(p.Payload)] == nil {
				ids[string(p.Payload)] = map[uint16]bool{}
			}

			ids[string(p.Payload)][p.PacketID] = true
		}
	}

	if len(ids["log"]) == 0 && cellState != nil {
		t.Logf("no log delivery on the new connection\n%s", cellState())
	}

	require.Len(t, ids["log"], 1, "the log delivery reached the new connection before the writer went on")

	close(seam.release)

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cp := readFor(t, m2, 300*time.Millisecond)
		if cp == nil {
			continue
		}

		if p, ok := cp.Content.(*packets.Publish); ok {
			if ids[string(p.Payload)] == nil {
				ids[string(p.Payload)] = map[uint16]bool{}
			}

			ids[string(p.Payload)][p.PacketID] = true
		}
	}

	require.Len(t, ids["second"], 1, "the held shared delivery")
	require.Len(t, ids["log"], 1, "the log QoS 2 message went out under two packet IDs")
}
