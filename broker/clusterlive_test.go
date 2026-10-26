package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/internal/chaos"
	"github.com/xraph/tether/tenant"
)

// Liveness timings of the tests: a peer is dead after liveLease + 2 s.
const (
	liveHeartbeat = 100 * time.Millisecond
	liveLease     = time.Second
	liveDead      = liveLease + 2*time.Second
	// liveWait bounds the waits that span a node's death and a will delay.
	liveWait = 20 * time.Second
)

// liveCell is a cell whose nodes run Members, with node 0 reaching NATS
// through proxy.
type liveCell struct {
	*testCell

	members []*cluster.Members
	proxy   *chaos.Proxy
}

// startLiveCell starts nodes brokers on a three-server NATS cluster, each
// with Members (heartbeat 100 ms, lease 1 s) started after the broker, and
// node 0 behind a fault proxy (NoDiscovery keeps it there). Users u1 and u2
// may publish and subscribe anything, as in startCluster.
func startLiveCell(t *testing.T, nodes int) *liveCell {
	t.Helper()

	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}
	ids := map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")}
	cfg := testConfig()

	c := &liveCell{testCell: &testCell{nats: clustertest.StartCluster(t, 3), t1: t1, t2: t2}}
	urls := c.nats.URLs()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveWait)
		defer cancel()

		if c.proxy != nil {
			c.proxy.SetMode(chaos.Both, chaos.Pass)
		}

		for _, b := range c.nodes {
			_ = b.Stop(ctx)
		}

		for _, m := range c.members {
			m.Stop()
		}

		for _, conn := range c.conns {
			conn.Close()
		}

		// Last, so the nodes stop over a working link.
		if c.proxy != nil {
			c.proxy.Close()
		}
	})

	for i := range nodes {
		ccfg := cluster.DefaultConfig()
		ccfg.Enabled = true
		ccfg.NodeID = fmt.Sprintf("n%d", i+1)
		ccfg.Cell = "c1"
		ccfg.Mode = cluster.ModeExternal
		ccfg.URLs = []string{urls[i%len(urls)]}
		ccfg.Replicas = 3
		ccfg.NoDiscovery = true
		ccfg.Heartbeat = liveHeartbeat
		ccfg.Lease = liveLease

		if i == 0 {
			p, err := chaos.NewProxy(strings.TrimPrefix(ccfg.URLs[0], "nats://"))
			require.NoError(t, err)

			c.proxy = p
			ccfg.URLs = []string{"nats://" + p.Addr()}
		}

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		conn, err := cluster.Connect(ctx, ccfg, testLogger())

		if err == nil && i == 0 {
			err = cluster.Provision(ctx, conn, ccfg, cfg.MaxSessionExpiry)
		}

		cancel()
		require.NoError(t, err)

		c.conns = append(c.conns, conn)
		m := cluster.NewMembers(conn, ccfg)

		b, err := New(cfg, Deps{
			Auth:    auth.NewStatic(ids),
			Perms:   fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}},
			Logger:  testLogger().With("node", ccfg.NodeID),
			Cluster: &Cluster{Conn: conn, Config: ccfg, Members: m},
		})
		require.NoError(t, err)

		require.NoError(t, b.Start(t.Context()))
		c.nodes = append(c.nodes, b)

		ctx, cancel = context.WithTimeout(t.Context(), 20*time.Second)
		err = m.Start(ctx)

		cancel()
		require.NoError(t, err)

		c.members = append(c.members, m)
	}

	// Every node sees every other before a test cuts one off.
	eventually(t, "every node sees the cell", func() bool {
		for _, m := range c.members {
			if len(m.Live()) != nodes {
				return false
			}
		}

		return true
	})

	return c
}

// record reads t1's session clientID through node i.
func (c *liveCell) record(t *testing.T, i int, clientID string) (*cluster.SessionRecord, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	rec, _, err := c.nodes[i].cluster.Sessions.Get(ctx, c.t1.ID.String(), clientID)

	return rec, err
}

// cut blackholes node 0's NATS connection and returns when.
func (c *liveCell) cut() time.Time {
	c.proxy.SetMode(chaos.Both, chaos.Blackhole)

	return time.Now()
}

// willSpec is the CONNECT of a 5.0 test client with a will.
type willSpec struct {
	id, topic string
	qos       byte
	expiry    uint32 // session expiry; 0 is a clean session
	delay     uint32 // will delay
}

// rawConnectWill connects a 5.0 client of u1 over a raw TCP connection with
// the will spec describes ("gone <id>" as its payload).
func rawConnectWill(t *testing.T, addr string, spec willSpec) net.Conn {
	t.Helper()

	expiry, delay := spec.expiry, spec.delay

	return rawConnect(t, addr, &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: spec.id, CleanStart: spec.expiry == 0, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties:     &packets.Properties{SessionExpiryInterval: &expiry},
		WillProperties: &packets.Properties{WillDelayInterval: &delay},
		WillFlag:       true, WillTopic: spec.topic, WillQOS: spec.qos, WillMessage: []byte("gone " + spec.id),
	})
}

// connect311Code sends a 3.1.1 CONNECT of u1 and returns the CONNACK's
// return code.
func connect311Code(t *testing.T, addr, clientID string) byte {
	t.Helper()

	conn, code := rawConnect311(t, addr, clientID)
	_ = conn.Close()

	return code
}

// recvLive waits up to liveWait for the next message on c that is not an
// interest probe.
func recvLive(t *testing.T, c *client5) message {
	t.Helper()

	deadline := time.After(liveWait)

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

// eventuallyLive is eventually bounded by liveWait, for waits that span a
// node's death.
func eventuallyLive(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(liveWait)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// Review Focus 2: a node cut off from NATS drops its clients within half the
// lease and refuses new ones; its peers leave its sessions alone until it has
// been silent for the lease plus 2 s.
func TestFencedNodeDropsClients(t *testing.T) {
	c := startLiveCell(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	p, _, err := connect5(t, c.addr(0), "p", "u1", false)
	require.NoError(t, err)

	v3, code := rawConnect311(t, c.addr(0), "v3")
	require.Equal(t, byte(0), code)

	q, _, err := connect5(t, c.addr(1), "q", "u1", true)
	require.NoError(t, err)

	require.NoError(t, a.ClusterHealth(t.Context()))
	require.NoError(t, b.ClusterHealth(t.Context()))

	cut := c.cut()

	require.Equal(t, byte(0x88), disconnectedWith(t, p))
	require.Less(t, time.Since(cut), liveLease/2+time.Second, "clients dropped within lease/2 + slack")
	requireClosed(t, v3)
	require.ErrorIs(t, a.ClusterHealth(t.Context()), cluster.ErrFenced)
	require.GreaterOrEqual(t, a.cluster.fenceDrops.Load(), uint64(2))

	// New CONNECTs are refused: 0x88, and 0x03 for 3.1.1.
	_, ca, err := connect5(t, c.addr(0), "late", "u1", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	require.Equal(t, byte(0x88), ca.ReasonCode)
	require.Equal(t, byte(0x03), connect311Code(t, c.addr(0), "late3"))

	// B leaves p's key with A for the lease plus 2 s (checked up to
	// 500 ms short of it, for A's last heartbeat before the cut).
	for time.Since(cut) < liveDead-500*time.Millisecond {
		rec, rerr := c.record(t, 1, "p")
		require.NoError(t, rerr)
		require.Equal(t, "n1", rec.Owner, "taken over %s after the cut", time.Since(cut))

		time.Sleep(50 * time.Millisecond)
	}

	// Then it settles it: offline, for its client to take anywhere.
	eventuallyLive(t, "p's key reclaimed", func() bool {
		rec, rerr := c.record(t, 1, "p")

		return rerr == nil && rec.Owner == "" && !rec.DisconnectedAt.IsZero()
	})
	require.GreaterOrEqual(t, time.Since(cut), liveDead-500*time.Millisecond)

	// B's own client was never touched.
	requireConnected(t, q)
	require.NoError(t, b.ClusterHealth(t.Context()))

	// Healed, A unfences and serves again.
	c.proxy.SetMode(chaos.Both, chaos.Pass)
	eventuallyLive(t, "A healthy again", func() bool { return a.ClusterHealth(t.Context()) == nil })

	_, _, err = connect5(t, c.addr(0), "back", "u1", true)
	require.NoError(t, err)
}

// rawConnect311 sends a 3.1.1 CONNECT of u1 and returns the open connection
// and the CONNACK's return code.
func rawConnect311(t *testing.T, addr, clientID string) (net.Conn, byte) {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	var body bytes.Buffer

	str := func(s string) {
		_ = binary.Write(&body, binary.BigEndian, uint16(len(s)))
		body.WriteString(s)
	}

	str("MQTT")
	body.Write([]byte{4, 0xC2, 0, 30})
	str(clientID)
	str("u1")
	str("s")

	_, err = conn.Write(append([]byte{0x10, byte(body.Len())}, body.Bytes()...))
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(waitTimeout)))

	ack := make([]byte, 4)
	_, err = io.ReadFull(conn, ack)
	require.NoError(t, err)
	require.Equal(t, []byte{0x20, 2}, ack[:2], "a CONNACK")

	return conn, ack[3]
}

// The wills of a node that died are published once, by the lowest live node
// only, after the lease plus 2 s; a will the node published itself before it
// died is not published again.
func TestDeadNodeWillsPublished(t *testing.T) {
	c := startLiveCell(t, 3)
	a, b, n3 := c.nodes[0], c.nodes[1], c.nodes[2]

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	sc, _, err := connect5(t, c.addr(2), "sc", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sc, "will/#", 1, false)

	// The leader sends QoS 0 wills to the other nodes through the Router.
	pb, _, err := connect5(t, c.addr(1), "pb", "u1", true)
	require.NoError(t, err)
	awaitInterest(t, pb, sc)

	// y dies while A is up: A publishes its will to the cell at once.
	y := rawConnectWill(t, c.addr(0), willSpec{id: "y", topic: "will/y", qos: 1, expiry: 3600})
	require.NoError(t, y.Close())
	require.Equal(t, "will/y", recvMsg(t, sb).Topic)
	require.Equal(t, "will/y", recvMsg(t, sc).Topic)

	eventually(t, "y's record without its will", func() bool {
		rec, rerr := c.record(t, 1, "y")

		return rerr == nil && rec.Owner == "" && rec.Will == nil
	})

	x0 := rawConnectWill(t, c.addr(0), willSpec{id: "x0", topic: "will/x0", qos: 0})
	x1 := rawConnectWill(t, c.addr(0), willSpec{id: "x1", topic: "will/x1", qos: 1, expiry: 3600})

	cut := c.cut()

	// The fence drops them and holds their wills back.
	requireClosed(t, x0)
	requireClosed(t, x1)

	for _, sub := range []*client5{sb, sc} {
		got := map[string]time.Duration{}

		for len(got) < 2 {
			m := recvLive(t, sub)
			got[m.Topic] = time.Since(cut)
			require.Equal(t, "gone "+strings.TrimPrefix(m.Topic, "will/"), m.Payload)
		}

		require.Contains(t, got, "will/x0")
		require.Contains(t, got, "will/x1")

		for topic, d := range got {
			require.GreaterOrEqual(t, d, liveDead-500*time.Millisecond, "%s published %s after the cut", topic, d)
		}
	}

	noMoreMsgs(t, sb)
	noMoreMsgs(t, sc)

	require.Equal(t, uint64(2), b.cluster.willsSent.Load(), "the leader publishes each will once")
	require.Zero(t, n3.cluster.willsSent.Load(), "only the leader publishes")

	// x0 was a clean session: its key is gone. x1 waits offline, no will.
	_, err = c.record(t, 1, "x0")
	require.ErrorIs(t, err, tether.ErrNotFound)

	rec, err := c.record(t, 1, "x1")
	require.NoError(t, err)
	require.Empty(t, rec.Owner)
	require.Nil(t, rec.Will)

	// Ruling R31: a fenced node writes nothing at Stop, so it stops quickly.
	ctx, cancel := context.WithTimeout(t.Context(), liveWait)
	defer cancel()

	start := time.Now()
	require.NoError(t, a.Stop(ctx))
	require.Less(t, time.Since(start), 3*time.Second)
}

// An MQTT 5 will delay runs from the moment the cell found the session gone,
// and a reconnect anywhere before it ends cancels the will.
func TestDeadNodeWillDelay(t *testing.T) {
	c := startLiveCell(t, 2)
	b := c.nodes[1]

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 3600, delay: 3})
	z := rawConnectWill(t, c.addr(0), willSpec{id: "z", topic: "will/z", qos: 1, expiry: 3600, delay: 3})

	cut := c.cut()

	requireClosed(t, x)
	requireClosed(t, z)

	// B takes both keys over and holds their wills.
	var taken time.Time

	eventuallyLive(t, "B holds z's will", func() bool {
		rec, rerr := c.record(t, 1, "z")
		taken = time.Now()

		return rerr == nil && rec.Owner == "n2" && rec.Will != nil && !rec.DisconnectedAt.IsZero()
	})
	require.ElementsMatch(t, []string{namespacedID(c.t1.ID.String(), "x"), namespacedID(c.t1.ID.String(), "z")}, b.cluster.pendingWills())

	// z comes back, on B, before its delay ends: session present, no will.
	zb, ca, err := connect5(t, c.addr(1), "z", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)

	m := recvLive(t, sb)
	require.Equal(t, "will/x", m.Topic)
	require.GreaterOrEqual(t, time.Since(taken), 2500*time.Millisecond, "the delay runs from the takeover")
	require.GreaterOrEqual(t, time.Since(cut), liveDead+3*time.Second-500*time.Millisecond)

	noMoreMsgs(t, sb)
	eventually(t, "z's will cancelled", func() bool { return b.cluster.willsCancelled.Load() >= 1 })
	require.Empty(t, b.cluster.pendingWills())
	requireConnected(t, zb)

	rec, err := c.record(t, 1, "x")
	require.NoError(t, err)
	require.Empty(t, rec.Owner)
	require.Nil(t, rec.Will)
}

// After a node dies its persistent session is offline in the cell: the
// client reconnects to another node with session present and gets what was
// published for it meanwhile, from its cursor.
func TestDeadNodeSessionTakenFresh(t *testing.T) {
	c := startLiveCell(t, 2)

	p, _, err := connect5(t, c.addr(0), "p", "u1", false)
	require.NoError(t, err)
	subscribeQoS(t, p, "q/#", 1, false)

	pub, _, err := connect5(t, c.addr(1), "pub", "u1", true)
	require.NoError(t, err)

	rec, err := c.record(t, 1, "p")
	require.NoError(t, err)

	before := rec.Cursor

	require.Equal(t, byte(0), publishQ(t, pub, "q/0", "m0", 1))
	require.Equal(t, "m0", recvMsg(t, p).Payload)

	// m0's ack is in the record before the cut, so it is not sent again.
	eventually(t, "p's cursor past m0", func() bool {
		rec, rerr := c.record(t, 1, "p")

		return rerr == nil && rec.Cursor > before
	})

	c.cut()
	require.Equal(t, byte(0x88), disconnectedWith(t, p))

	for i := 1; i <= 3; i++ {
		require.Equal(t, byte(0), publishQ(t, pub, fmt.Sprintf("q/%d", i), fmt.Sprintf("m%d", i), 1))
	}

	eventuallyLive(t, "p's key reclaimed", func() bool {
		rec, rerr := c.record(t, 1, "p")

		return rerr == nil && rec.Owner == ""
	})

	p2, ca, err := connect5(t, c.addr(1), "p", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)

	for i := 1; i <= 3; i++ {
		require.Equal(t, fmt.Sprintf("m%d", i), recvMsg(t, p2).Payload)
	}

	noMoreMsgs(t, p2)
}

// Ruling R30: the will of a client of a live node reaches the whole cell
// (QoS 0 through the Router, QoS 1 through the log) and is delivered once
// on its own node; the record no longer carries it; a delayed will is held
// in the record and published when its delay ends, unless the client comes
// back anywhere first; a session that ends at once publishes at once.
func TestLocalWillsReachCell(t *testing.T) {
	c := startLiveCell(t, 2)
	a := c.nodes[0]

	sa, _, err := connect5(t, c.addr(0), "sa", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sa, "will/#", 1, false)

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	pa, _, err := connect5(t, c.addr(0), "pa", "u1", true)
	require.NoError(t, err)
	awaitInterest(t, pa, sb)

	for _, spec := range []willSpec{
		{id: "w0", topic: "will/w0", qos: 0},
		{id: "w1", topic: "will/w1", qos: 1, expiry: 3600},
		{id: "w2", topic: "will/w2", qos: 2, expiry: 3600},
		{id: "f", topic: "will/f", qos: 1, delay: 30}, // clean: the session ends now, so does the delay
	} {
		conn := rawConnectWill(t, c.addr(0), spec)
		require.NoError(t, conn.Close())

		for _, sub := range []*client5{sa, sb} {
			m := recvMsg(t, sub)
			require.Equal(t, spec.topic, m.Topic)
			require.Equal(t, "gone "+spec.id, m.Payload)
		}
	}

	noMoreMsgs(t, sa)
	noMoreMsgs(t, sb)

	// The records carry no will once it is out.
	_, err = c.record(t, 0, "w0")
	require.ErrorIs(t, err, tether.ErrNotFound)

	for _, id := range []string{"w1", "w2"} {
		eventually(t, id+"'s record without its will", func() bool {
			rec, rerr := c.record(t, 0, id)

			return rerr == nil && rec.Owner == "" && rec.Will == nil
		})
	}

	// A delayed will: held in the record with A as its owner, then
	// published from A once its delay ends.
	d := rawConnectWill(t, c.addr(0), willSpec{id: "d", topic: "will/d", qos: 1, expiry: 3600, delay: 2})
	e := rawConnectWill(t, c.addr(0), willSpec{id: "e", topic: "will/e", qos: 1, expiry: 3600, delay: 2})

	gone := time.Now()

	require.NoError(t, d.Close())
	require.NoError(t, e.Close())

	eventually(t, "d's will held in its record", func() bool {
		rec, rerr := c.record(t, 1, "d")

		return rerr == nil && rec.Owner == "n1" && rec.Will != nil && !rec.DisconnectedAt.IsZero()
	})
	eventually(t, "e's will held in its record", func() bool {
		rec, rerr := c.record(t, 1, "e")

		return rerr == nil && rec.Owner == "n1" && rec.Will != nil
	})

	// e comes back, on the other node: its will is cancelled.
	_, ca, err := connect5(t, c.addr(1), "e", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)

	m := recvLive(t, sb)
	require.Equal(t, "will/d", m.Topic)
	require.GreaterOrEqual(t, time.Since(gone), 2*time.Second)
	require.Equal(t, "will/d", recvMsg(t, sa).Topic)

	noMoreMsgs(t, sa)
	noMoreMsgs(t, sb)
	eventually(t, "e's will cancelled", func() bool { return a.cluster.willsCancelled.Load() >= 1 })
	require.Empty(t, a.cluster.pendingWills())

	rec, err := c.record(t, 1, "d")
	require.NoError(t, err)
	require.Empty(t, rec.Owner)
	require.Nil(t, rec.Will)
}

// A node that is fenced only briefly is never declared dead: once unfenced it
// settles the keys it dropped meanwhile itself, and publishes their wills
// once.
func TestUnfenceReclaimsDroppedSessions(t *testing.T) {
	c := startLiveCell(t, 2)
	a, b := c.nodes[0], c.nodes[1]

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 3600})

	cut := c.cut()

	requireClosed(t, x)
	require.Less(t, time.Since(cut), liveDead-time.Second)
	c.proxy.SetMode(chaos.Both, chaos.Pass)

	m := recvLive(t, sb)
	require.Equal(t, "will/x", m.Topic)
	noMoreMsgs(t, sb)

	require.Equal(t, uint64(1), a.cluster.willsSent.Load())
	require.Zero(t, b.cluster.willsSent.Load(), "B never saw A dead")
	require.Equal(t, []string{"n1", "n2"}, c.members[1].Live())

	rec, err := c.record(t, 1, "x")
	require.NoError(t, err)
	require.Empty(t, rec.Owner)
	require.Nil(t, rec.Will)
}

// Without a cluster the broker is always healthy as far as the cell goes.
func TestClusterHealthAlone(t *testing.T) {
	b := startBroker(t, nil, nil)
	require.NoError(t, b.ClusterHealth(t.Context()))
	require.False(t, errors.Is(b.ClusterHealth(t.Context()), cluster.ErrFenced))
}
