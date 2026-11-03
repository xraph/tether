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
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	mqtt "github.com/mochi-mqtt/server/v2"
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
)

// liveWait bounds the waits that span a node's death and a will delay.
var liveWait = scaled(20 * time.Second)

// liveCell is a cell whose nodes run Members, with the first nodes reaching
// NATS through proxies (proxy is node 0's).
type liveCell struct {
	*testCell

	members []*cluster.Members
	proxies []*chaos.Proxy
	proxy   *chaos.Proxy
}

// startLiveCell starts nodes brokers on a three-server NATS cluster, each
// with Members (heartbeat 100 ms, lease 1 s) started after the broker, and
// node 0 behind a fault proxy (NoDiscovery keeps it there). Users u1 and u2
// may publish and subscribe anything, as in startCluster.
func startLiveCell(t *testing.T, nodes int) *liveCell {
	t.Helper()

	return startLiveCellWith(t, nodes, 1, nil)
}

// startLiveCellWith is startLiveCell with nodes 0 to proxied-1 behind fault
// proxies and before run on each broker before it starts.
func startLiveCellWith(t *testing.T, nodes, proxied int, before func(*Broker)) *liveCell {
	t.Helper()

	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}
	ids := map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")}
	cfg := testConfig()

	c := &liveCell{testCell: &testCell{nats: clustertest.StartCluster(t, 3), t1: t1, t2: t2}}
	c.trackState(t)

	urls := c.nats.URLs()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), liveWait)
		defer cancel()

		for _, p := range c.proxies {
			p.SetMode(chaos.Both, chaos.Pass)
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
		for _, p := range c.proxies {
			p.Close()
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

		if i < proxied {
			p, err := chaos.NewProxy(strings.TrimPrefix(ccfg.URLs[0], "nats://"))
			require.NoError(t, err)

			c.proxies = append(c.proxies, p)
			c.proxy = c.proxies[0]
			ccfg.URLs = []string{"nats://" + p.Addr()}
		}

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		conn, err := cluster.Connect(ctx, ccfg, testLogger())

		if err == nil && i == 0 {
			err = cluster.Provision(ctx, conn, ccfg, cfg.MaxSessionExpiry)
		}

		cancel()
		require.NoError(t, err)

		if i == 0 {
			c.nats.WaitInterest(t, cellSubjects(ccfg)...)
		}

		c.conns = append(c.conns, conn)
		m := cluster.NewMembers(conn, ccfg)

		b, err := New(cfg, Deps{
			Auth:    auth.NewStatic(ids),
			Perms:   fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}},
			Logger:  testLogger().With("node", ccfg.NodeID),
			Cluster: &Cluster{Conn: conn, Config: ccfg, Members: m},
		})
		require.NoError(t, err)

		if before != nil {
			before(b)
		}

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
	user      []packets.User
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
		WillProperties: &packets.Properties{WillDelayInterval: &delay, User: spec.user},
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

	// y dies while A is up: A publishes its will to the cell at once. It is
	// QoS 1, so it reaches B and C through the log.
	y := rawConnectWill(t, c.addr(0), willSpec{id: "y", topic: "will/y", qos: 1, expiry: 3600})
	require.NoError(t, y.Close())
	require.Equal(t, "will/y", recvLog(t, sb).Topic)
	require.Equal(t, "will/y", recvLog(t, sc).Topic)

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
	// B writes a key it takes over before it arms the will's timer, and it
	// sweeps x and z one after the other, so z's record can show before
	// either timer is in.
	held := []string{namespacedID(c.t1.ID.String(), "x"), namespacedID(c.t1.ID.String(), "z")}

	eventually(t, "B holds both wills", func() bool { return len(b.cluster.pendingWills()) == len(held) })
	require.ElementsMatch(t, held, b.cluster.pendingWills())

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

// awaitWills reads sub until a message has arrived on every topic of want,
// failing on a second copy of one or on any other topic, and returns when
// each arrived.
func awaitWills(t *testing.T, sub *client5, want ...string) map[string]time.Time {
	t.Helper()

	got := make(map[string]time.Time, len(want))

	for len(got) < len(want) {
		m := recvLive(t, sub)
		require.Contains(t, want, m.Topic)
		require.NotContains(t, got, m.Topic, "a will published twice")

		got[m.Topic] = time.Now()
	}

	return got
}

// MQTT 5 publishes a will when its delay ends or when its session does,
// whichever comes first: a will delay at or past the session expiry is cut
// short by the expiry, and the will goes out then, once, from the node that
// held it. The key that holds the will outlives that moment, so the settle
// still finds it. A reconnect before the expiry still cancels it.
func TestDelayedWillAtSessionExpiry(t *testing.T) {
	c := startLiveCell(t, 2)
	a := c.nodes[0]

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 2, delay: 5})
	y := rawConnectWill(t, c.addr(0), willSpec{id: "y", topic: "will/y", qos: 1, expiry: 2, delay: 2})
	r := rawConnectWill(t, c.addr(0), willSpec{id: "r", topic: "will/r", qos: 1, expiry: 2, delay: 5})

	gone := time.Now()

	require.NoError(t, x.Close())
	require.NoError(t, y.Close())
	require.NoError(t, r.Close())

	eventually(t, "r's will held in its record", func() bool {
		rec, rerr := c.record(t, 1, "r")

		return rerr == nil && rec.Owner == "n1" && rec.Will != nil && !rec.DisconnectedAt.IsZero()
	})

	// r comes back on the other node well before its session expires
	// (a slow CONNECT at load must still land inside it): session present,
	// and its will is cancelled.
	time.Sleep(time.Until(gone.Add(500 * time.Millisecond)))

	rb, ca, err := connect5(t, c.addr(1), "r", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)

	got := awaitWills(t, sb, "will/x", "will/y")

	for topic, at := range got {
		since := at.Sub(gone)
		require.GreaterOrEqual(t, since, 1800*time.Millisecond, topic+" before its session expired")
		require.Less(t, since, 4500*time.Millisecond, topic+" well after its session expired")
	}

	// Past x's and r's full will delay: nothing more.
	time.Sleep(time.Until(gone.Add(5500 * time.Millisecond)))
	noMoreMsgs(t, sb)
	requireConnected(t, rb)

	require.Equal(t, uint64(2), a.cluster.willsSent.Load())
	require.GreaterOrEqual(t, a.cluster.willsCancelled.Load(), uint64(1))
	require.Empty(t, a.cluster.pendingWills())

	for _, id := range []string{"x", "y"} {
		eventually(t, id+"'s session gone", func() bool {
			_, rerr := c.record(t, 1, id)

			return errors.Is(rerr, tether.ErrNotFound)
		})
	}
}

// The same rule when the node holding the delayed will dies: the key outlives
// the session's expiry until the dead node's leader settles it, and the
// leader publishes the will once. x had disconnected and its node held its
// will; z was still connected, so the leader holds its will (ruling R32) and
// publishes it when z's session expires.
func TestDeadNodeDelayedWillAtSessionExpiry(t *testing.T) {
	c := startLiveCell(t, 2)
	b := c.nodes[1]

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 2, delay: 5})
	z := rawConnectWill(t, c.addr(0), willSpec{id: "z", topic: "will/z", qos: 1, expiry: 2, delay: 30})

	require.NoError(t, x.Close())

	eventually(t, "x's will held in its record", func() bool {
		rec, rerr := c.record(t, 1, "x")

		return rerr == nil && rec.Owner == "n1" && rec.Will != nil && !rec.DisconnectedAt.IsZero()
	})

	cut := c.cut()

	requireClosed(t, z)

	// x's session expired while its node was going dead; B settles it once
	// it sees the node dead.
	m := recvLive(t, sb)
	require.Equal(t, "will/x", m.Topic)
	require.GreaterOrEqual(t, time.Since(cut), liveDead-500*time.Millisecond)

	// z's session was taken over connected, so its expiry runs from the
	// takeover.
	m = recvLive(t, sb)
	require.Equal(t, "will/z", m.Topic)
	require.GreaterOrEqual(t, time.Since(cut), liveDead+1500*time.Millisecond)

	noMoreMsgs(t, sb)
	require.Equal(t, uint64(2), b.cluster.willsSent.Load())
	require.Empty(t, b.cluster.pendingWills())

	for _, id := range []string{"x", "z"} {
		eventually(t, id+"'s session gone", func() bool {
			_, rerr := c.record(t, 1, id)

			return errors.Is(rerr, tether.ErrNotFound)
		})
	}
}

// holdWill stops the timer of the delayed will node holds for clientID, as
// a holder that is slow to fire, and returns it for the test to fire.
func holdWill(t *testing.T, node *Broker, tenantID, clientID string) *pendingWill {
	t.Helper()

	n := node.cluster
	nsID := namespacedID(tenantID, clientID)

	n.willsMu.Lock()
	defer n.willsMu.Unlock()

	pw := n.wills[nsID]
	require.NotNil(t, pw, "no delayed will held for "+clientID)
	pw.timer.Stop()

	return pw
}

// A key that holds a delayed will outlives its session's MQTT expiry while
// the holder is slow, fenced or dead (willKeyTTL). A CONNECT after the
// expiry starts a fresh session (session present 0, nothing of the old one
// carried) and publishes the will, which the session's end owed; the
// holder's timer then finds the new epoch and cancels. One inside the
// expiry resumes the session and cancels the will.
func TestReconnectAfterExpiryStartsFresh(t *testing.T) {
	c := startLiveCell(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	tid := c.t1.ID.String()

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 5, delay: 10})
	r := rawConnectWill(t, c.addr(0), willSpec{id: "r", topic: "will/r", qos: 1, expiry: 5, delay: 10})

	for _, conn := range []net.Conn{x, r} {
		_, err = (&packets.Subscribe{
			PacketID: 1, Subscriptions: []packets.SubOptions{{Topic: "old/#", QoS: 1}}, Properties: &packets.Properties{},
		}).WriteTo(conn)
		require.NoError(t, err)
		require.Equal(t, packets.SUBACK, readPacket(t, conn).Type)
	}

	for _, id := range []string{"x", "r"} {
		eventually(t, id+"'s subscription in its record", func() bool {
			rec, rerr := c.record(t, 1, id)

			return rerr == nil && len(rec.Subscriptions) == 1
		})
	}

	gone := time.Now()

	require.NoError(t, x.Close())
	require.NoError(t, r.Close())

	for _, id := range []string{"x", "r"} {
		eventually(t, id+"'s will held in its record", func() bool {
			rec, rerr := c.record(t, 1, id)

			return rerr == nil && rec.Owner == "n1" && rec.Will != nil && !rec.DisconnectedAt.IsZero()
		})
	}

	// The expiry leaves the holder's timer seconds to run, so it is still
	// pending when holdWill stops it, even on a loaded machine.
	px := holdWill(t, a, tid, "x")
	pr := holdWill(t, a, tid, "r")

	// r inside its expiry: session present, its will cancelled.
	time.Sleep(time.Until(gone.Add(500 * time.Millisecond)))

	rb, ca, err := connect5(t, c.addr(1), "r", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent, "r came back inside its expiry")

	// x a second after its expiry (5s), before the holder fired.
	time.Sleep(time.Until(gone.Add(6 * time.Second)))

	xb, ca, err := connect5(t, c.addr(1), "x", "u1", false)
	require.NoError(t, err)
	require.False(t, ca.SessionPresent, "x's session had expired")

	rec, err := c.record(t, 1, "x")
	require.NoError(t, err)
	require.Empty(t, rec.Subscriptions, "x's old subscription carried")

	m := recvLive(t, sb)
	require.Equal(t, "will/x", m.Topic)

	// The slow holder fires at last: both keys moved, both cancelled.
	cancelled := a.cluster.willsCancelled.Load()

	a.fireWill(px)
	a.fireWill(pr)

	require.Equal(t, cancelled+2, a.cluster.willsCancelled.Load())
	noMoreMsgs(t, sb)
	requireConnected(t, rb)
	requireConnected(t, xb)

	require.Zero(t, a.cluster.willsSent.Load())
	require.Equal(t, uint64(1), b.cluster.willsSent.Load())
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

// Fix round 1 (finding 1): n1 dies while n2, the lowest live node in n3's
// view, is about to die itself. n3 is not n1's leader when n1 is declared
// dead, and n2 is fenced by then, so nobody acts on n1's death; once n2 is
// declared dead n3 leads for both and its sweep settles both nodes' keys.
func TestCorrelatedDeathsSwept(t *testing.T) {
	c := startLiveCellWith(t, 3, 2, nil)
	n3 := c.nodes[2]

	s3, _, err := connect5(t, c.addr(2), "s3", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, s3, "will/#", 1, false)

	x1 := rawConnectWill(t, c.addr(0), willSpec{id: "x1", topic: "will/x1", qos: 1, expiry: 3600})
	x2 := rawConnectWill(t, c.addr(1), willSpec{id: "x2", topic: "will/x2", qos: 1, expiry: 3600})

	cut := c.cut()

	requireClosed(t, x1)

	// n2 goes 1.5 s later: fenced (at about 2 s) before n3 declares n1 dead
	// (at about 3 s), declared dead itself at about 4.5 s. A fixed schedule
	// is the point of the test.
	time.Sleep(time.Until(cut.Add(1500 * time.Millisecond)))
	c.proxies[1].SetMode(chaos.Both, chaos.Blackhole)
	requireClosed(t, x2)

	got := map[string]time.Duration{}

	for len(got) < 2 {
		m := recvLive(t, s3)
		got[m.Topic] = time.Since(cut)
	}

	require.Contains(t, got, "will/x1")
	require.Contains(t, got, "will/x2")
	require.GreaterOrEqual(t, got["will/x1"], liveDead+1500*time.Millisecond-500*time.Millisecond,
		"n1's will waits for n2's death, when n3 becomes its leader")
	noMoreMsgs(t, s3)

	require.Equal(t, uint64(2), n3.cluster.willsSent.Load())
	require.ElementsMatch(t, []string{"n1", "n2"}, c.members[2].Dead())

	for _, id := range []string{"x1", "x2"} {
		rec, err := c.record(t, 2, id)
		require.NoError(t, err)
		require.Empty(t, rec.Owner)
		require.Nil(t, rec.Will)
	}

	// Cleared: later ticks read nothing more.
	sweeps := n3.cluster.sweeps.Load()

	time.Sleep(2 * liveLease)
	require.Equal(t, sweeps, n3.cluster.sweeps.Load(), "a cleared dead node is not swept again")
}

// Fix round 1 (finding 2): once the CAS that clears a will from its key is
// won, the reclaiming node holds the only copy, so it keeps trying to
// publish it, here for longer than the 5 s a local will gets.
func TestReclaimedWillRetried(t *testing.T) {
	var failUntil atomic.Int64

	c := startLiveCellWith(t, 2, 1, func(b *Broker) {
		if b.cluster.Config.NodeID == "n2" {
			b.cluster.willFault = func() error {
				if time.Now().UnixNano() < failUntil.Load() {
					return errors.New("injected will fault")
				}

				return nil
			}
		}
	})

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 3600})

	cut := c.cut()
	until := cut.Add(liveDead + 7*time.Second)
	failUntil.Store(until.UnixNano())

	requireClosed(t, x)

	// The key is settled (its will cleared) while every publish fails.
	eventuallyLive(t, "x's key settled", func() bool {
		rec, rerr := c.record(t, 1, "x")

		return rerr == nil && rec.Owner == "" && rec.Will == nil
	})
	require.True(t, time.Now().Before(until))

	m := recvLive(t, sb)
	require.Equal(t, "will/x", m.Topic)
	require.False(t, time.Now().Before(until), "published only once the fault is gone")
	noMoreMsgs(t, sb)
	require.Equal(t, uint64(1), c.nodes[1].cluster.willsSent.Load())
}

// Fix round 1 (finding 3): a client that disconnects normally while its node
// is cut from NATS leaves the CONNECT's will in its key (the final write is
// skipped once fenced). After the unfence the node settles the key itself,
// and publishes no will: the client never owed one.
func TestCleanDisconnectDuringOutageNoWill(t *testing.T) {
	c := startLiveCellWith(t, 2, 1, func(b *Broker) {
		if b.cluster.Config.NodeID != "n1" {
			return
		}

		b.disconnectHold = func(cl *mqtt.Client, removed bool) {
			if !removed || !strings.HasSuffix(cl.ID, "/c") {
				return
			}

			// The session ends once the node is fenced.
			deadline := time.Now().Add(liveWait)
			for !b.cluster.fenced.Load() && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
		}
	})
	a := c.nodes[0]

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	conn := rawConnectWill(t, c.addr(0), willSpec{id: "c", topic: "will/c", qos: 1, expiry: 3600})

	c.cut()
	writePacket(t, conn, &packets.Disconnect{ReasonCode: 0, Properties: &packets.Properties{}})
	requireClosed(t, conn)

	nsID := namespacedID(c.t1.ID.String(), "c")

	eventually(t, "c's end noted as owing no will", func() bool {
		a.cluster.willsMu.Lock()
		defer a.cluster.willsMu.Unlock()

		_, ok := a.cluster.noWill[nsID]

		return ok && a.cluster.fenced.Load()
	})

	c.proxy.SetMode(chaos.Both, chaos.Pass)

	eventuallyLive(t, "c's key settled", func() bool {
		rec, rerr := c.record(t, 1, "c")

		return rerr == nil && rec.Owner == "" && rec.Will == nil
	})

	time.Sleep(time.Second)
	noMoreMsgs(t, sb)
	require.Zero(t, a.cluster.willsSent.Load())
	require.Zero(t, c.nodes[1].cluster.willsSent.Load())
	require.Equal(t, []string{"n1", "n2"}, c.members[1].Live(), "n2 never saw n1 dead")
}

// heldFault returns a willFault that fails every send while hold is set and
// counts the attempts.
func heldFault(hold *atomic.Bool, attempts *atomic.Int64) func() error {
	return func() error {
		attempts.Add(1)

		if hold.Load() {
			return errors.New("injected will fault")
		}

		return nil
	}
}

// Fix round 2: a will whose publish keeps failing after its key was
// settled does not hold the session's lock, so its client can connect to the
// node meanwhile.
func TestHeldPublishDoesNotBlockConnect(t *testing.T) {
	var (
		hold     atomic.Bool
		attempts atomic.Int64
	)

	hold.Store(true)

	c := startLiveCellWith(t, 2, 1, func(b *Broker) {
		if b.cluster.Config.NodeID == "n1" {
			b.cluster.willFault = heldFault(&hold, &attempts)
		}
	})

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 3600})

	c.cut()
	requireClosed(t, x)
	c.proxy.SetMode(chaos.Both, chaos.Pass)

	// After the unfence n1 settles x's key and keeps failing to send its
	// will.
	eventuallyLive(t, "x's key settled with its publish failing", func() bool {
		rec, rerr := c.record(t, 1, "x")

		return rerr == nil && rec.Owner == "" && rec.Will == nil && attempts.Load() > 1
	})

	start := time.Now()
	x2, ca, err := connect5(t, c.addr(0), "x", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)
	require.Less(t, time.Since(start), 3*time.Second)

	hold.Store(false)

	require.Equal(t, "will/x", recvLive(t, sb).Topic)
	noMoreMsgs(t, sb)
	requireConnected(t, x2)
	require.Equal(t, []string{"n1", "n2"}, c.members[1].Live(), "n2 never saw n1 dead")
}

// Fix round 2: a reclaimed will stuck in its publish does not stall the
// sweep: a node that dies later still has its keys settled.
func TestStuckPublishDoesNotStallSweep(t *testing.T) {
	var (
		hold     atomic.Bool
		attempts atomic.Int64
	)

	hold.Store(true)

	c := startLiveCellWith(t, 3, 3, func(b *Broker) {
		if b.cluster.Config.NodeID == "n2" {
			b.cluster.willFault = heldFault(&hold, &attempts)
		}
	})

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	x1 := rawConnectWill(t, c.addr(0), willSpec{id: "x1", topic: "will/x1", qos: 1, expiry: 3600})
	y3 := rawConnectWill(t, c.addr(2), willSpec{id: "y3", topic: "will/y3", qos: 1, expiry: 3600})

	c.proxies[0].SetMode(chaos.Both, chaos.Blackhole)
	requireClosed(t, x1)

	eventuallyLive(t, "x1's key settled with its publish stuck", func() bool {
		rec, rerr := c.record(t, 1, "x1")

		return rerr == nil && rec.Owner == "" && attempts.Load() > 0
	})

	c.proxies[2].SetMode(chaos.Both, chaos.Blackhole)
	requireClosed(t, y3)

	eventuallyLive(t, "y3's key settled while x1's publish is stuck", func() bool {
		rec, rerr := c.record(t, 1, "y3")

		return rerr == nil && rec.Owner == "" && rec.Will == nil
	})
	require.True(t, hold.Load())

	hold.Store(false)

	got := map[string]bool{}
	for len(got) < 2 {
		got[recvLive(t, sb).Topic] = true
	}

	require.Equal(t, map[string]bool{"will/x1": true, "will/y3": true}, got)
	noMoreMsgs(t, sb)
}

// Fix round 2: a will NATS can never carry (its user properties take it
// over max_payload) is dropped at once and logged, on the local path and on
// the reclaim path, instead of being retried until Stop.
func TestOversizedWillDropped(t *testing.T) {
	var (
		hold     atomic.Bool
		attempts atomic.Int64
	)

	c := startLiveCellWith(t, 2, 1, func(b *Broker) {
		// fitPacketSize lowers the packet size below what this will
		// needs, so such a will is refused at CONNECT; it is raised back
		// to reach the drop path behind it.
		b.cfg.MaxPacketSize = 1 << 20

		if b.cluster.Config.NodeID == "n2" {
			b.cluster.willFault = heldFault(&hold, &attempts)
		}
	})
	b := c.nodes[1]

	// About 800 KB of user properties: under the 1 MiB MQTT packet limit,
	// over NATS's 1 MiB max_payload once base64 encoded into a header.
	big := make([]packets.User, 16)
	for i := range big {
		big[i] = packets.User{Key: fmt.Sprintf("k%d", i), Value: strings.Repeat("v", 50_000)}
	}

	sb, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, sb, "will/#", 1, false)

	// Local path: n2 drops it and clears the record.
	y := rawConnectWill(t, c.addr(1), willSpec{id: "y", topic: "will/y", qos: 1, expiry: 3600, user: big})
	require.NoError(t, y.Close())

	eventually(t, "y's will dropped", func() bool { return b.cluster.willsDropped.Load() == 1 })
	eventually(t, "y's record cleared", func() bool {
		rec, rerr := c.record(t, 1, "y")

		return rerr == nil && rec.Owner == "" && rec.Will == nil
	})

	// Reclaim path: one attempt, then given up.
	x := rawConnectWill(t, c.addr(0), willSpec{id: "x", topic: "will/x", qos: 1, expiry: 3600, user: big})

	c.cut()
	requireClosed(t, x)

	eventuallyLive(t, "x's will dropped", func() bool { return b.cluster.willsDropped.Load() == 2 })

	sent := attempts.Load()
	require.Equal(t, int64(2), sent, "one attempt per will")

	time.Sleep(2 * time.Second)
	require.Equal(t, sent, attempts.Load(), "no retry of a will that can never go out")
	noMoreMsgs(t, sb)
	require.Zero(t, b.cluster.willsSent.Load())

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

// Ruling R13 at CONNECT: in a cell a will topic over the cell's 1024 byte
// cap is refused with 0x90, even when the broker's MaxTopicLength allows it
// (M11).
func TestClusterWillTopicCapped(t *testing.T) {
	cfg := testConfig()
	cfg.MaxTopicLength = 4096

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	c := startClusterWith(t, 1, cfg, map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)

	conn, err := dial(t, c.addr(0))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	cp := &packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: "long-will", CleanStart: true, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{}, WillProperties: &packets.Properties{},
		WillFlag: true, WillTopic: strings.Repeat("w", 2000), WillMessage: []byte("gone"),
	}
	_, err = cp.WriteTo(conn)
	require.NoError(t, err)

	ack := readPacket(t, conn)
	require.Equal(t, packets.CONNACK, ack.Type)
	require.Equal(t, byte(0x90), ack.Content.(*packets.Connack).ReasonCode)
}
