package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// sessionKey reads tenantID's session record of clientID from node b's view
// of the cell.
func sessionKey(t *testing.T, b *Broker, tenantID, clientID string) (*cluster.SessionRecord, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	rec, _, err := b.cluster.Sessions.Get(ctx, tenantID, clientID)

	return rec, err
}

// waitOffline waits until the session's record says no node serves it: the
// last write of its connection's end is in.
func waitOffline(t *testing.T, b *Broker, tenantID, clientID string) {
	t.Helper()

	eventually(t, clientID+" offline in the cell", func() bool {
		rec, err := sessionKey(t, b, tenantID, clientID)

		return err == nil && rec.Owner == ""
	})
}

// A persistent QoS 1 session made on A, offline while B publishes 20
// messages, resumes on B: session present, and all 20 arrive, in order.
func TestPersistentSessionMovesNodes(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	s, ca, err := connect5(t, c.addr(0), "mover", "u1", false)
	require.NoError(t, err)
	require.False(t, ca.SessionPresent)
	subscribeQoS(t, s, "mv/#", 1, false)
	require.NoError(t, s.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitOffline(t, c.nodes[1], tid, "mover")

	pub, _, err := connect5(t, c.addr(1), "mv-pub", "u1", true)
	require.NoError(t, err)

	for i := range 20 {
		require.Equal(t, byte(0), publishQ(t, pub, "mv/x", strconv.Itoa(i), 1))
	}

	s2, ca, err := connect5(t, c.addr(1), "mover", "u1", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent, "the session did not come from the cell")

	for i := range 20 {
		require.Equal(t, strconv.Itoa(i), recvLog(t, s2).Payload)
	}

	// Live after the replay, through the resumed subscription.
	require.Equal(t, byte(0), publishQ(t, pub, "mv/x", "live", 1))
	require.Equal(t, "live", recvLog(t, s2).Payload)

	rec, err := sessionKey(t, c.nodes[0], tid, "mover")
	require.NoError(t, err)
	require.Equal(t, "n2", rec.Owner)
}

// A session that resumes while its tenant is publishing gets, per topic,
// every message it missed in log order and before any newer live message:
// no gap, nothing out of order, on the node it left and on another one.
func TestReplayHandsOverToLiveInOrder(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	for _, node := range []int{0, 1} {
		t.Run(fmt.Sprintf("resume on n%d", node+1), func(t *testing.T) {
			id := fmt.Sprintf("ord%d", node)
			name := fmt.Sprintf("ord%d/x", node)

			s, _, err := connect5(t, c.addr(0), id, "u1", false)
			require.NoError(t, err)
			subscribeQoS(t, s, fmt.Sprintf("ord%d/#", node), 1, false)
			require.NoError(t, s.Disconnect(&paho.Disconnect{ReasonCode: 0}))
			waitOffline(t, c.nodes[0], tid, id)

			pub, _, err := connect5(t, c.addr(1), id+"-pub", "u1", true)
			require.NoError(t, err)

			const total = 300

			published := make(chan int, total)

			go func() {
				defer close(published)

				for i := range total {
					ctx, cancel := context.WithTimeout(context.Background(), logWait)
					_, perr := pub.Publish(ctx, &paho.Publish{Topic: name, QoS: 1, Payload: []byte(strconv.Itoa(i))})

					cancel()

					if perr != nil {
						return
					}

					published <- i
				}
			}()

			// Resume in the middle of the stream.
			for range 100 {
				<-published
			}

			s2, ca, err := connect5(t, c.addr(node), id, "u1", false)
			require.NoError(t, err)
			require.True(t, ca.SessionPresent)

			next := 0

			for next < total {
				m := recvLog(t, s2)

				got, err := strconv.Atoi(m.Payload)
				require.NoError(t, err)

				// A QoS 1 duplicate of an earlier message is allowed; a
				// message out of order or one past a gap is not.
				require.LessOrEqual(t, got, next, "message %d came before %d", got, next)

				if got == next {
					next++
				}
			}

			for range published { //nolint:revive // drain
			}

			require.NotZero(t, c.nodes[node].cluster.replayed.Load(), "nothing was replayed")
			require.NotZero(t, c.nodes[node].cluster.replaysDone.Load(), "no replay went live")
		})
	}
}

// rawSession is the shared state of one MQTT client across its connections:
// the QoS 2 packet IDs it has received and not yet been released from
// (method B of MQTT's exactly-once delivery), and how often each payload was
// handed to the application.
type rawSession struct {
	mu        sync.Mutex
	held      map[uint16]string
	delivered map[string]int
	dups      int
}

func newRawSession() *rawSession {
	return &rawSession{held: map[uint16]string{}, delivered: map[string]int{}}
}

func (s *rawSession) count(payload string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.delivered[payload]
}

// rawConn is one connection of a rawSession: it answers QoS 1 and 2
// deliveries as a conforming client does (unless noAck), and records its
// CONNACK and any DISCONNECT.
type rawConn struct {
	conn    net.Conn
	sess    *rawSession
	noAck   bool
	wmu     sync.Mutex
	connack chan *packets.Connack
	pong    chan struct{}
	gotDisc atomic.Int32 // 0 none, else reason+1
	done    chan struct{}
	pubs    atomic.Int64
}

// dialRaw connects to addr as clientID (user u1, persistent, a one-hour
// expiry) and starts reading. It does not wait for the CONNACK.
func dialRaw(t *testing.T, addr, clientID string, sess *rawSession, noAck bool) *rawConn {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)

	rc := &rawConn{
		conn: conn, sess: sess, noAck: noAck,
		connack: make(chan *packets.Connack, 1), pong: make(chan struct{}, 8), done: make(chan struct{}),
	}

	t.Cleanup(func() {
		_ = conn.Close()
		<-rc.done
	})

	go rc.read()

	expiry := uint32(3600)
	rc.send(&packets.Connect{
		ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: false, KeepAlive: 30,
		Username: "u1", UsernameFlag: true, Password: []byte("s"), PasswordFlag: true,
		Properties: &packets.Properties{SessionExpiryInterval: &expiry},
	})

	return rc
}

func (rc *rawConn) read() {
	defer close(rc.done)

	for {
		cp, err := packets.ReadPacket(rc.conn)
		if err != nil {
			return
		}

		switch p := cp.Content.(type) {
		case *packets.Connack:
			rc.connack <- p
		case *packets.Pingresp:
			rc.pong <- struct{}{}
		case *packets.Disconnect:
			rc.gotDisc.Store(int32(p.ReasonCode) + 1)
		case *packets.Publish:
			rc.pubs.Add(1)
			rc.onPublish(p)
		case *packets.Pubrel:
			if os.Getenv("TETHER_TEST_RAWLOG") != "" {
				fmt.Fprintf(os.Stderr, "RAW %p pubrel id=%d\n", rc, p.PacketID)
			}

			rc.sess.mu.Lock()
			delete(rc.sess.held, p.PacketID)
			rc.sess.mu.Unlock()
			rc.send(&packets.Pubcomp{PacketID: p.PacketID, Properties: &packets.Properties{}})
		}
	}
}

func (rc *rawConn) onPublish(p *packets.Publish) {
	s := rc.sess
	payload := string(p.Payload)

	s.mu.Lock()

	if os.Getenv("TETHER_TEST_RAWLOG") != "" {
		fmt.Fprintf(os.Stderr, "RAW %p pub id=%d qos=%d dup=%v payload=%s\n", rc, p.PacketID, p.QoS, p.Duplicate, payload)
	}

	if p.QoS == 2 {
		if _, held := s.held[p.PacketID]; held {
			s.dups++ // a resend of one it has: not delivered again
		} else {
			s.held[p.PacketID] = payload
			s.delivered[payload]++
		}
	} else {
		s.delivered[payload]++
	}

	s.mu.Unlock()

	if rc.noAck {
		return
	}

	switch p.QoS {
	case 1:
		rc.send(&packets.Puback{PacketID: p.PacketID, Properties: &packets.Properties{}})
	case 2:
		rc.send(&packets.Pubrec{PacketID: p.PacketID, Properties: &packets.Properties{}})
	}
}

// send writes one packet, ignoring a closed connection.
func (rc *rawConn) send(p io.WriterTo) {
	rc.wmu.Lock()
	defer rc.wmu.Unlock()

	_, _ = p.WriteTo(rc.conn)
}

// ack waits for the CONNACK.
func (rc *rawConn) ack(t *testing.T) *packets.Connack {
	t.Helper()

	select {
	case ca := <-rc.connack:
		return ca
	case <-rc.done:
		// A refused CONNECT's connection closes right after its CONNACK.
		select {
		case ca := <-rc.connack:
			return ca
		default:
		}

		t.Fatal("connection closed before its CONNACK")
	case <-time.After(logWait):
		t.Fatal("no CONNACK")
	}

	return nil
}

// alive reports whether the connection still answers a PINGREQ.
func (rc *rawConn) alive() bool {
	rc.send(&packets.Pingreq{})

	select {
	case <-rc.pong:
		return true
	case <-rc.done:
		return false
	case <-time.After(waitTimeout):
		return false
	}
}

// disconnectedWith returns the reason of the DISCONNECT the server sent, and
// false when it sent none.
func (rc *rawConn) disconnectedWith() (byte, bool) {
	v := rc.gotDisc.Load()
	if v == 0 {
		return 0, false
	}

	return byte(v - 1), true
}

// leave sends DISCONNECT (normal, session kept) and waits for the server to
// close the connection.
func (rc *rawConn) leave(t *testing.T) {
	t.Helper()

	rc.send(&packets.Disconnect{ReasonCode: 0, Properties: &packets.Properties{}})

	select {
	case <-rc.done:
	case <-time.After(waitTimeout):
		_ = rc.conn.Close()
	}
}

// subscribe subscribes at qos and waits for the SUBACK by pinging after it.
func (rc *rawConn) subscribe(t *testing.T, filter string, qos byte) {
	t.Helper()

	rc.send(&packets.Subscribe{PacketID: 60000, Subscriptions: []packets.SubOptions{{Topic: filter, QoS: qos}}, Properties: &packets.Properties{}})
	require.True(t, rc.alive())
}

// Review Focus 1: the same client ID connecting to two nodes at once, 50
// times. Each round exactly one connection keeps the session and the other
// is disconnected with 0x8E (or, when it lost the key's race twice, refused
// with 0x88). QoS 2 messages published to the session all along are handed
// to the client exactly once, across every connection and takeover: the
// client keeps its QoS 2 state across connections, as MQTT's exactly-once
// asks of it, and the broker never sends a message it already sent under a
// new packet ID.
func TestConcurrentConnectSameClientID(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()
	sess := newRawSession()

	first := dialRaw(t, c.addr(0), "twin", sess, false)
	require.Equal(t, byte(0), first.ack(t).ReasonCode)
	first.subscribe(t, "race/#", 2)
	first.leave(t)
	waitOffline(t, c.nodes[0], tid, "twin")

	pub, _, err := connect5(t, c.addr(0), "race-pub", "u1", true)
	require.NoError(t, err)

	var (
		published []string
		pubMu     sync.Mutex
		stopPub   = make(chan struct{})
		pubDone   = make(chan struct{})
	)

	go func() {
		defer close(pubDone)

		for i := 0; ; i++ {
			select {
			case <-stopPub:
				return
			case <-time.After(5 * time.Millisecond):
			}

			payload := fmt.Sprintf("m%d", i)

			ctx, cancel := context.WithTimeout(context.Background(), logWait)
			_, err := pub.Publish(ctx, &paho.Publish{Topic: "race/x", QoS: 2, Payload: []byte(payload)})

			cancel()

			if err != nil {
				return
			}

			pubMu.Lock()
			published = append(published, payload)
			pubMu.Unlock()
		}
	}()

	const rounds = 50

	var taken, refused int

	for round := range rounds {
		var (
			conns [2]*rawConn
			acks  [2]*packets.Connack
			wg    sync.WaitGroup
		)

		for i := range 2 {
			wg.Go(func() {
				conns[i] = dialRaw(t, c.addr(i), "twin", sess, false)
			})
		}

		wg.Wait()

		for i := range 2 {
			acks[i] = conns[i].ack(t)
		}

		// The loser hears about it: refused at CONNECT, or disconnected.
		var loser, winner int

		eventually(t, fmt.Sprintf("round %d: a loser", round), func() bool {
			for i := range 2 {
				_, disc := conns[i].disconnectedWith()
				if acks[i].ReasonCode != 0 || disc {
					loser, winner = i, 1-i

					return true
				}
			}

			return false
		})

		require.True(t, conns[winner].alive(), "round %d: no connection survived", round)
		require.Zero(t, acks[winner].ReasonCode, "round %d", round)

		_, winnerDisc := conns[winner].disconnectedWith()
		require.False(t, winnerDisc, "round %d: both connections were disconnected", round)

		if acks[loser].ReasonCode != 0 {
			require.Equal(t, byte(0x88), acks[loser].ReasonCode, "round %d", round)

			refused++
		} else {
			code, _ := conns[loser].disconnectedWith()
			require.Equal(t, byte(0x8E), code, "round %d", round)

			taken++
		}

		rec, err := sessionKey(t, c.nodes[0], tid, "twin")
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("n%d", winner+1), rec.Owner, "round %d: the key names the loser", round)

		conns[winner].leave(t)
		waitOffline(t, c.nodes[0], tid, "twin")
	}

	close(stopPub)
	<-pubDone

	pubMu.Lock()
	all := append([]string(nil), published...)
	pubMu.Unlock()

	require.Equal(t, byte(0), publishQ(t, pub, "race/x", "end", 2))

	// One more connection collects what the rounds left over.
	last := dialRaw(t, c.addr(0), "twin", sess, false)
	require.Equal(t, byte(0), last.ack(t).ReasonCode)
	eventually(t, "the end marker", func() bool { return sess.count("end") > 0 })

	sess.mu.Lock()
	defer sess.mu.Unlock()

	for _, p := range all {
		require.Equal(t, 1, sess.delivered[p], "message %s delivered %d times", p, sess.delivered[p])
	}

	t.Logf("%d rounds: %d taken over (0x8E), %d refused (0x88); %d messages, %d QoS 2 resends recognised",
		rounds, taken, refused, len(all), sess.dups)
}

// Review Focus 4: a persistent subscriber that stops acknowledging while 500
// QoS 1 messages arrive, more than its window holds, then reconnects on
// another node, gets every one of them at least once. Its cursor never moved
// past a message it had not acknowledged or that mochi could not queue.
func TestSlowSubscriberCursorNeverSkips(t *testing.T) {
	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	cfg := testConfig()
	cfg.MaxInflight = 100

	c := startClusterWith(t, 2, cfg, map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)
	tid := t1.ID.String()

	sess := newRawSession()
	slow := dialRaw(t, c.addr(0), "slow", sess, true)
	require.Equal(t, byte(0), slow.ack(t).ReasonCode)
	slow.subscribe(t, "flood/#", 1)

	pub, _, err := connect5(t, c.addr(1), "flood-pub", "u1", true)
	require.NoError(t, err)

	const total = 500

	for i := range total {
		require.Equal(t, byte(0), publishQ(t, pub, "flood/x", strconv.Itoa(i), 1))
	}

	// The window is full; mochi dropped the rest for this connection.
	eventually(t, "the first 100 deliveries", func() bool { return slow.pubs.Load() >= 100 })
	require.Less(t, slow.pubs.Load(), int64(total))

	// The connection dies without a DISCONNECT.
	_ = slow.conn.Close()
	waitOffline(t, c.nodes[1], tid, "slow")

	rec, err := sessionKey(t, c.nodes[1], tid, "slow")
	require.NoError(t, err)
	t.Logf("cursor after the drop: %d", rec.Cursor)

	sess2 := newRawSession()
	again := dialRaw(t, c.addr(1), "slow", sess2, false)
	ca := again.ack(t)
	require.Equal(t, byte(0), ca.ReasonCode)
	require.True(t, ca.SessionPresent)

	eventually(t, "every message", func() bool {
		for i := range total {
			if sess2.count(strconv.Itoa(i)) == 0 {
				return false
			}
		}

		return true
	})
}

// A clean session takes its key for ownership and deletes it when it ends.
func TestCleanSessionKeyDeleted(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	cl, _, err := connect5(t, c.addr(0), "clean", "u1", true)
	require.NoError(t, err)

	rec, err := sessionKey(t, c.nodes[1], tid, "clean")
	require.NoError(t, err)
	require.Equal(t, "n1", rec.Owner)
	require.Zero(t, rec.ExpirySeconds)

	require.NoError(t, cl.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	eventually(t, "the key to go", func() bool {
		_, err := sessionKey(t, c.nodes[1], tid, "clean")

		return errors.Is(err, tether.ErrNotFound)
	})
}

// A persistent session's key expires with the session: TTL 2 s after its
// connection ends, and a later CONNECT finds no session.
func TestSessionExpiresInKV(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	cl, _ := connect5Expiry(t, c.addr(0), "short", "u1", 2)
	subscribeQoS(t, cl, "s/#", 1, false)
	require.NoError(t, cl.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	rec, err := sessionKey(t, c.nodes[1], tid, "short")
	if err == nil {
		require.Equal(t, uint32(2), rec.ExpirySeconds)
	}

	eventually(t, "the key to expire", func() bool {
		_, err := sessionKey(t, c.nodes[1], tid, "short")

		return errors.Is(err, tether.ErrNotFound)
	})

	_, ca := connect5Expiry(t, c.addr(1), "short", "u1", 2)
	require.False(t, ca.SessionPresent)
}

// A 3.1.1 persistent session ends in the cell by its key's TTL, the
// tenant's expiry cap (2 s here), where without a cluster the broker's own
// sweep ends it.
func TestSessionExpiresInKV311(t *testing.T) {
	t1 := newTenant(tenant.Limits{MaxSessionExpiry: 2 * time.Second})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	c := startClusterWith(t, 2, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)
	tid := t1.ID.String()

	v3 := connect311Persistent(t, c.addr(0), "v3", "u1")
	_ = subscribe311(t, v3, "v/#")
	v3.Disconnect(0)
	waitOffline(t, c.nodes[1], tid, "v3")

	rec, err := sessionKey(t, c.nodes[1], tid, "v3")
	if err == nil {
		require.Equal(t, uint32(2), rec.ExpirySeconds)
		require.Equal(t, []cluster.SubRecord{{Filter: "v/#", QoS: 1}}, rec.Subscriptions)
	}

	eventually(t, "the key to expire", func() bool {
		_, err := sessionKey(t, c.nodes[1], tid, "v3")

		return errors.Is(err, tether.ErrNotFound)
	})
}

// Spike 1's checks on the eviction of a persistent session after DISCONNECT:
// nothing of it is left in mochi (clients, topic index, counters, inflight)
// or in the node's QoS 0 routes, and no hook event a storage hook acts on
// fired. On reconnect it comes back from the cell with its unacknowledged
// message first.
func TestEvictionLeavesNoTopicEntries(t *testing.T) {
	c := startCluster(t, 1)
	a := c.nodes[0]
	tid := c.t1.ID.String()
	nsID := namespacedID(tid, "ev")

	sess := newRawSession()
	rc := dialRaw(t, c.addr(0), "ev", sess, true)
	require.Equal(t, byte(0), rc.ack(t).ReasonCode)
	rc.subscribe(t, "a/#", 1)
	require.Equal(t, []string{"a/#"}, a.cluster.routed(nsID))

	pub, _, err := connect5(t, c.addr(0), "ev-pub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publishQ(t, pub, "a/unacked", "x", 1))
	eventually(t, "the delivery", func() bool { return rc.pubs.Load() == 1 })

	evictions := a.cluster.evictions.Load()

	rc.leave(t)
	eventually(t, "the eviction", func() bool { return a.cluster.evictions.Load() > evictions })

	_, ok := a.server.Clients.Get(nsID)
	require.False(t, ok, "the session is still in mochi's clients")
	require.NotContains(t, a.server.Topics.Subscribers(topic.Namespace(tid, "a/b")).Subscriptions, nsID)
	require.Zero(t, atomic.LoadInt64(&a.server.Info.Subscriptions))
	eventually(t, "no inflight", func() bool { return atomic.LoadInt64(&a.server.Info.Inflight) == 0 })
	require.Empty(t, a.cluster.routed(nsID))
	require.Zero(t, a.cluster.routeErrors.Load())

	waitOffline(t, a, tid, "ev")

	rec, err := sessionKey(t, a, tid, "ev")
	require.NoError(t, err)
	require.Equal(t, []cluster.SubRecord{{Filter: "a/#", QoS: 1}}, rec.Subscriptions)

	// Back from the cell: the unacknowledged message first, then live.
	sess2 := newRawSession()
	rc2 := dialRaw(t, c.addr(0), "ev", sess2, false)
	ca := rc2.ack(t)
	require.True(t, ca.SessionPresent)
	eventually(t, "the redelivery", func() bool { return sess2.count("x") == 1 })

	require.Equal(t, byte(0), publishQ(t, pub, "a/b", "y", 1))
	eventually(t, "the live message", func() bool { return sess2.count("y") == 1 })
	require.Equal(t, []string{"a/#"}, a.cluster.routed(nsID))
}

// Ruling R7: DeleteSession deletes a session offline in the cell from any
// node and one this node serves; one another node serves is refused with an
// error naming it.
func TestDeleteSessionClusterWide(t *testing.T) {
	c := startCluster(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	tid := c.t1.ID.String()

	t.Run("offline, from another node", func(t *testing.T) {
		s, _, err := connect5(t, c.addr(0), "off", "u1", false)
		require.NoError(t, err)
		subscribeQoS(t, s, "d/#", 1, false)
		require.NoError(t, s.Disconnect(&paho.Disconnect{ReasonCode: 0}))
		waitOffline(t, b, tid, "off")

		require.NoError(t, b.DeleteSession(tid, "off"))

		_, err = sessionKey(t, a, tid, "off")
		require.ErrorIs(t, err, tether.ErrNotFound)

		_, ca, err := connect5(t, c.addr(0), "off", "u1", false)
		require.NoError(t, err)
		require.False(t, ca.SessionPresent)
	})

	t.Run("served here", func(t *testing.T) {
		s, _, err := connect5(t, c.addr(0), "here", "u1", false)
		require.NoError(t, err)

		require.NoError(t, a.DeleteSession(tid, "here"))

		select {
		case d := <-s.disconnects:
			require.Equal(t, byte(0x98), d.ReasonCode)
		case <-time.After(waitTimeout):
			t.Fatal("no DISCONNECT")
		}

		_, err = sessionKey(t, b, tid, "here")
		require.ErrorIs(t, err, tether.ErrNotFound)
	})

	t.Run("served by another node", func(t *testing.T) {
		_, _, err := connect5(t, c.addr(0), "far", "u1", false)
		require.NoError(t, err)

		err = b.DeleteSession(tid, "far")
		require.ErrorIs(t, err, tether.ErrConflict)
		require.Contains(t, err.Error(), `"n1"`)

		rec, err := sessionKey(t, b, tid, "far")
		require.NoError(t, err)
		require.Equal(t, "n1", rec.Owner)
	})

	t.Run("unknown", func(t *testing.T) {
		require.ErrorIs(t, b.DeleteSession(tid, "nobody"), tether.ErrNotFound)
	})
}

// Clients lists the tenant's sessions in the whole cell.
func TestClientsClusterWide(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	_, _, err := connect5(t, c.addr(0), "on-a", "u1", true)
	require.NoError(t, err)

	off, _, err := connect5(t, c.addr(0), "off-a", "u1", false)
	require.NoError(t, err)
	subscribeQoS(t, off, "z/#", 1, false)
	require.NoError(t, off.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitOffline(t, c.nodes[1], tid, "off-a")

	_, _, err = connect5(t, c.addr(1), "on-b", "u1", true)
	require.NoError(t, err)

	got := c.nodes[1].Clients(tid)
	require.Len(t, got, 3)

	byID := map[string]ClientInfo{}
	for _, ci := range got {
		byID[ci.ClientID] = ci
	}

	require.True(t, byID["on-a"].Connected)
	require.True(t, byID["on-b"].Connected)
	require.NotEmpty(t, byID["on-b"].Remote, "this node's own connection is described from its state")
	require.False(t, byID["off-a"].Connected)
	require.Equal(t, []string{"z/#"}, byID["off-a"].Subscriptions)
	require.Equal(t, uint32(3600), byID["off-a"].SessionExpiry)
}

// Ruling R13: a client ID the cell cannot carry is refused at CONNECT with
// 0x85, before the session store is touched.
func TestClientIDTooLongRefused(t *testing.T) {
	c := startCluster(t, 1)

	_, ca, err := connect5(t, c.addr(0), strings.Repeat("x", 257), "u1", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	require.Equal(t, byte(0x85), ca.ReasonCode)
}
