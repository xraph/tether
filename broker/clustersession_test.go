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
			eventually(t, "the replay to go live", func() bool { return c.nodes[node].cluster.replaysDone.Load() > 0 })
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
	// onDeliver, when set before connecting, sees every payload handed to
	// the application, in arrival order.
	onDeliver func(payload string)
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
// deliveries as a conforming client does (or holds the answers back while
// noAck is set, until releaseAcks), and records its CONNACK and any
// DISCONNECT. With connackOnly it stops reading after the CONNACK, a client
// whose socket fills up.
type rawConn struct {
	conn        net.Conn
	sess        *rawSession
	noAck       atomic.Bool
	connackOnly bool
	heldMu      sync.Mutex
	heldAcks    []io.WriterTo
	wmu         sync.Mutex
	connack     chan *packets.Connack
	pong        chan struct{}
	gotDisc     atomic.Int32 // 0 none, else reason+1
	done        chan struct{}
	pubs        atomic.Int64
}

// dialRaw connects to addr as clientID (user u1, persistent, a one-hour
// expiry) and starts reading. It does not wait for the CONNACK.
func dialRaw(t *testing.T, addr, clientID string, sess *rawSession, noAck bool) *rawConn {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)

	return startRaw(t, conn, clientID, sess, noAck, false)
}

// dialRawStalled is dialRaw for a client that reads its CONNACK and then
// nothing more.
func dialRawStalled(t *testing.T, addr, clientID string, sess *rawSession) *rawConn {
	t.Helper()

	conn, err := dial(t, addr)
	require.NoError(t, err)

	return startRaw(t, conn, clientID, sess, true, true)
}

func startRaw(t *testing.T, conn net.Conn, clientID string, sess *rawSession, noAck, connackOnly bool) *rawConn {
	t.Helper()

	rc := &rawConn{
		conn: conn, sess: sess, connackOnly: connackOnly,
		connack: make(chan *packets.Connack, 1), pong: make(chan struct{}, 8), done: make(chan struct{}),
	}
	rc.noAck.Store(noAck)

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

			if rc.connackOnly {
				return
			}
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

	if s.onDeliver != nil {
		s.onDeliver(payload)
	}

	var ack io.WriterTo

	switch p.QoS {
	case 1:
		ack = &packets.Puback{PacketID: p.PacketID, Properties: &packets.Properties{}}
	case 2:
		ack = &packets.Pubrec{PacketID: p.PacketID, Properties: &packets.Properties{}}
	default:
		return
	}

	rc.heldMu.Lock()
	if rc.noAck.Load() {
		rc.heldAcks = append(rc.heldAcks, ack)
		rc.heldMu.Unlock()

		return
	}
	rc.heldMu.Unlock()

	rc.send(ack)
}

// releaseAcks sends the acks held back so far and answers from now on.
func (rc *rawConn) releaseAcks() {
	rc.heldMu.Lock()
	acks := rc.heldAcks
	rc.heldAcks = nil
	rc.noAck.Store(false)
	rc.heldMu.Unlock()

	for _, a := range acks {
		rc.send(a)
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
// is disconnected with 0x8E (ruling R27: a CONNECT is not refused because the
// owner's own writes moved the key's revision). QoS 2 messages published to the session all along are handed
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

		require.Zero(t, acks[loser].ReasonCode, "round %d: the loser was refused", round)

		code, _ := conns[loser].disconnectedWith()
		require.Equal(t, byte(0x8E), code, "round %d", round)

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
	// What the rounds left over is replayed first, which takes a while on a
	// busy machine.
	deadline := time.Now().Add(logWait)
	for sess.count("end") == 0 {
		require.True(t, time.Now().Before(deadline), "no end marker")
		time.Sleep(20 * time.Millisecond)
	}

	sess.mu.Lock()
	defer sess.mu.Unlock()

	for _, p := range all {
		require.Equal(t, 1, sess.delivered[p], "message %s delivered %d times", p, sess.delivered[p])
	}

	t.Logf("%d rounds; %d messages, %d QoS 2 resends recognised, %d acquire retries on n1, %d on n2",
		rounds, len(all), sess.dups, c.nodes[0].cluster.casRetries.Load(), c.nodes[1].cluster.casRetries.Load())
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

// windowCell is a cell whose clients get an inflight window of max messages.
func windowCell(t *testing.T, nodes int, maxInflight uint16) (*testCell, string) {
	t.Helper()

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	cfg := testConfig()
	cfg.MaxInflight = maxInflight

	c := startClusterWith(t, nodes, cfg, map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, nil)

	return c, t1.ID.String()
}

// waitLive waits until the session is live again with no pin and its cursor
// has reached at least seq.
func waitLive(t *testing.T, b *Broker, nsID string, seq uint64) {
	t.Helper()

	eventually(t, "the session live, unpinned, past "+strconv.FormatUint(seq, 10), func() bool {
		s := b.cluster.sessionByID(nsID)
		if s == nil {
			return false
		}

		lp := s.tt.livePos.Load()

		s.mu.Lock()
		defer s.mu.Unlock()

		return s.mode == modeLive && !s.pinned && s.cursorLocked(lp) >= seq
	})
}

// A replay that fills the client's inflight map to the brim and then goes
// live: the next live message does not fit, mochi drops it without a hook,
// and the session must notice (noteFull from the replay), pin, and replay it
// once the window drains. Without that the cursor would move past it.
func TestReplayFilledWindowThenLiveNotSkipped(t *testing.T) {
	const window = 20

	c, tid := windowCell(t, 1, window)
	a := c.nodes[0]
	nsID := namespacedID(tid, "brim")

	sess := newRawSession()
	rc := dialRaw(t, c.addr(0), "brim", sess, false)
	require.Equal(t, byte(0), rc.ack(t).ReasonCode)
	rc.subscribe(t, "brim/#", 1)
	rc.leave(t)
	waitOffline(t, a, tid, "brim")

	pub, _, err := connect5(t, c.addr(0), "brim-pub", "u1", true)
	require.NoError(t, err)

	for i := range window {
		require.Equal(t, byte(0), publishQ(t, pub, "brim/x", strconv.Itoa(i), 1))
	}

	// Back, acks held: the replay fills the window exactly, then goes live.
	rc2 := dialRaw(t, c.addr(0), "brim", sess, true)
	require.True(t, rc2.ack(t).SessionPresent)
	eventually(t, "the replay to fill the window and go live", func() bool {
		s := a.cluster.sessionByID(nsID)
		if s == nil {
			return false
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		return rc2.pubs.Load() == window && s.mode == modeLive
	})

	repins := a.cluster.repins.Load()

	require.Equal(t, byte(0), publishQ(t, pub, "brim/x", "over", 1))
	eventually(t, "the session to go back to replaying", func() bool { return a.cluster.repins.Load() > repins })

	rc2.releaseAcks()
	eventually(t, "the message that did not fit", func() bool { return sess.count("over") == 1 })
	waitLive(t, a, nsID, a.cluster.track(tid).livePos.Load())
}

// A live subscriber whose window fills drops what does not fit; the pin that
// holds its cursor clears once the window drains: the session replays what
// it missed from the pin, goes live again, and its cursor moves on.
func TestPinClearsAfterWindowDrains(t *testing.T) {
	const window = 20

	c, tid := windowCell(t, 2, window)
	a := c.nodes[0]
	nsID := namespacedID(tid, "pin")

	sess := newRawSession()
	rc := dialRaw(t, c.addr(0), "pin", sess, true)
	require.Equal(t, byte(0), rc.ack(t).ReasonCode)
	rc.subscribe(t, "pin/#", 1)

	pub, _, err := connect5(t, c.addr(1), "pin-pub", "u1", true)
	require.NoError(t, err)

	const total = 60

	for i := range total {
		require.Equal(t, byte(0), publishQ(t, pub, "pin/x", strconv.Itoa(i), 1))
	}

	eventually(t, "a repin", func() bool { return a.cluster.repins.Load() > 0 })
	require.Less(t, rc.pubs.Load(), int64(total), "nothing was dropped")

	rc.releaseAcks()

	eventually(t, "every message", func() bool {
		for i := range total {
			if sess.count(strconv.Itoa(i)) == 0 {
				return false
			}
		}

		return true
	})

	waitLive(t, a, nsID, a.cluster.track(tid).livePos.Load())

	// And the record follows.
	eventually(t, "the record's cursor", func() bool {
		rec, err := sessionKey(t, a, tid, "pin")

		return err == nil && rec.Cursor >= a.cluster.track(tid).livePos.Load()
	})

	// Live again: a new message arrives through the index.
	require.Equal(t, byte(0), publishQ(t, pub, "pin/x", "after", 1))
	eventually(t, "the live message", func() bool { return sess.count("after") == 1 })
}

// A takeover by another node that lands while this node's CONNECT holds the
// key but has not registered the session yet (it is loading it) is not lost:
// the check waits for the CONNECT, and the connection is disconnected 0x8E.
func TestTakeoverDuringLoadNotLost(t *testing.T) {
	gate := make(chan struct{})
	reached := make(chan struct{})

	var armed atomic.Bool

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	c := startClusterWith(t, 2, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			if b.cluster.Config.NodeID == "n1" {
				b.cluster.afterAcquire = func() {
					if armed.CompareAndSwap(true, false) {
						close(reached)
						<-gate
					}
				}
			}
		})
	a, b := c.nodes[0], c.nodes[1]
	tid := t1.ID.String()

	armed.Store(true)

	type result struct {
		c   *client5
		err error
	}

	done := make(chan result, 1)

	go func() {
		cl, _, err := connect5(t, c.addr(0), "mid", "u1", false)
		done <- result{cl, err}
	}()

	select {
	case <-reached:
	case <-time.After(logWait):
		t.Fatal("the CONNECT never got its key")
	}

	// Another node takes the key while n1 is between acquire and register.
	events := a.cluster.ownedEvents.Load()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	_, _, _, err := b.cluster.Sessions.Acquire(ctx, tid, "mid", "n2")

	cancel()
	require.NoError(t, err)
	eventually(t, "n1's watch to report it", func() bool { return a.cluster.ownedEvents.Load() > events })

	close(gate)

	r := <-done
	require.NoError(t, r.err)

	select {
	case d := <-r.c.disconnects:
		require.Equal(t, byte(0x8E), d.ReasonCode)
	case <-time.After(logWait):
		t.Fatal("the connection kept a session another node owns")
	}
}

// A client that stops reading its socket while its session replays holds up
// its own replay only: another session of the same tenant, replaying on the
// same node from a later position (so it joins the same replay consumer),
// finishes promptly.
func TestStalledReplayDoesNotHoldOthers(t *testing.T) {
	c := startCluster(t, 1)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	slowSess, fastSess := newRawSession(), newRawSession()

	slow := dialRaw(t, c.addr(0), "stall", slowSess, false)
	require.Equal(t, byte(0), slow.ack(t).ReasonCode)
	slow.subscribe(t, "big/#", 1)
	slow.leave(t)
	waitOffline(t, a, tid, "stall")

	pub, _, err := connect5(t, c.addr(0), "big-pub", "u1", true)
	require.NoError(t, err)

	big := strings.Repeat("x", 48*1024)

	for i := range 400 {
		require.Equal(t, byte(0), publishQ(t, pub, "big/x", strconv.Itoa(i)+big, 1))
	}

	fast := dialRaw(t, c.addr(0), "quick", fastSess, false)
	require.Equal(t, byte(0), fast.ack(t).ReasonCode)
	fast.subscribe(t, "big/#", 1)
	fast.leave(t)
	waitOffline(t, a, tid, "quick")

	for i := range 50 {
		require.Equal(t, byte(0), publishQ(t, pub, "big/y", strconv.Itoa(i), 1))
	}

	// The stalled client's replay starts first and fills its socket.
	stalled := dialRawStalled(t, c.addr(0), "stall", slowSess)
	require.True(t, stalled.ack(t).SessionPresent)

	replayed := a.cluster.replayed.Load()
	eventually(t, "the stalled replay to stop moving", func() bool {
		now := a.cluster.replayed.Load()
		moved := now != replayed
		replayed = now

		if moved {
			time.Sleep(200 * time.Millisecond)
		}

		return !moved && now > 0
	})

	fast2 := dialRaw(t, c.addr(0), "quick", fastSess, false)
	require.True(t, fast2.ack(t).SessionPresent)

	eventually(t, "the other session's replay", func() bool {
		for i := range 50 {
			if fastSess.count(strconv.Itoa(i)) == 0 {
				return false
			}
		}

		return true
	})
}

// Ruling R28: a resume that fails after its key was acquired gives the key
// back: offline, without the refused CONNECT's will, and with the session's
// expiry as its TTL.
func TestFailedResumeGivesKeyBack(t *testing.T) {
	var fail atomic.Bool

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			b.cluster.loadHook = func() error {
				if fail.Load() {
					return errors.New("injected load failure")
				}

				return nil
			}
		})
	a := c.nodes[0]
	tid := t1.ID.String()

	s, _ := connect5Expiry(t, c.addr(0), "back", "u1", 2)
	subscribeQoS(t, s, "b/#", 1, false)
	require.NoError(t, s.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitOffline(t, a, tid, "back")

	fail.Store(true)

	expiry := uint32(2)
	_, ca, err := dial5(t, c.addr(0), &paho.Connect{
		ClientID: "back", Username: "u1", UsernameFlag: true, Password: []byte("secret"), PasswordFlag: true,
		KeepAlive: 30, Properties: &paho.ConnectProperties{SessionExpiryInterval: &expiry},
		WillMessage: &paho.WillMessage{Topic: "b/will", Payload: []byte("bye")},
	})
	require.Error(t, err)
	require.Equal(t, byte(0x88), ca.ReasonCode)

	rec, err := sessionKey(t, a, tid, "back")
	require.NoError(t, err)
	require.Empty(t, rec.Owner)
	require.Nil(t, rec.Will)
	require.Equal(t, []cluster.SubRecord{{Filter: "b/#", QoS: 1}}, rec.Subscriptions)
	require.Nil(t, a.cluster.sessionByID(namespacedID(tid, "back")))

	eventually(t, "the key to expire", func() bool {
		_, err := sessionKey(t, a, tid, "back")

		return errors.Is(err, tether.ErrNotFound)
	})
}

// An idle session taken by another node is noticed by the owned-keys watch
// alone (no write of this node fails, since it makes none) and disconnected
// with 0x8E. The watch runs for the broker's lifetime, not just its start.
func TestIdleSessionTakenOverByWatch(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()

	for _, clean := range []bool{true, false} {
		id := fmt.Sprintf("idle-%v", clean)

		cl, _, err := connect5(t, c.addr(0), id, "u1", clean)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
		_, _, _, err = c.nodes[1].cluster.Sessions.Acquire(ctx, tid, id, "n2")

		cancel()
		require.NoError(t, err)

		select {
		case d := <-cl.disconnects:
			require.Equal(t, byte(0x8E), d.ReasonCode, id)
		case <-time.After(logWait):
			t.Fatalf("%s: still served on n1 after n2 took it", id)
		}
	}
}

// A reconnect to the same node while the session's replay is stuck on a
// connection that stopped reading: the stuck writer is abandoned, what it
// had queued goes back to the replay, and the new connection gets every
// message, in order (mochi resends what the old one had in flight, ordered
// by sequence at the CONNACK, then the replay goes on from there).
func TestHandoverDuringStalledReplay(t *testing.T) {
	c := startCluster(t, 1)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	sess := newRawSession()

	first := dialRaw(t, c.addr(0), "hand", sess, false)
	require.Equal(t, byte(0), first.ack(t).ReasonCode)
	first.subscribe(t, "hb/#", 1)
	first.leave(t)
	waitOffline(t, a, tid, "hand")

	pub, _, err := connect5(t, c.addr(0), "hand-pub", "u1", true)
	require.NoError(t, err)

	const total = 300

	big := strings.Repeat("x", 48*1024)

	for i := range total {
		require.Equal(t, byte(0), publishQ(t, pub, "hb/x", strconv.Itoa(i)+":"+big, 1))
	}

	stalled := dialRawStalled(t, c.addr(0), "hand", newRawSession())
	require.True(t, stalled.ack(t).SessionPresent)

	replayed := a.cluster.replayed.Load()
	eventually(t, "the stalled replay to stop moving", func() bool {
		now := a.cluster.replayed.Load()
		moved := now != replayed
		replayed = now

		if moved {
			time.Sleep(200 * time.Millisecond)
		}

		return !moved && now > 0
	})

	// Same client ID, same node, the old connection still open.
	got := newRawSession()
	order := make(chan int, 4*total)
	got.onDeliver = func(payload string) {
		i, _, _ := strings.Cut(payload, ":")
		n, err := strconv.Atoi(i)
		if err == nil {
			order <- n
		}
	}

	again := dialRaw(t, c.addr(0), "hand", got, false)
	require.True(t, again.ack(t).SessionPresent)

	next := 0
	deadline := time.After(logWait)

	for next < total {
		select {
		case n := <-order:
			require.LessOrEqual(t, n, next, "message %d came before %d", n, next)

			if n == next {
				next++
			}
		case <-deadline:
			t.Fatalf("stuck at message %d of %d", next, total)
		}
	}
}

// An inbound QoS 2 reservation that fills the client's inflight map between
// the reader's full check and the injection of the next message: mochi drops
// that message without a hook. The reservation pins the cursor at the live
// position read before it and sends the session back to replaying, so the
// message arrives once the window drains.
func TestReservationFillsWindowBeforeInject(t *testing.T) {
	const window = 3

	t1 := newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
	cfg := testConfig()
	cfg.MaxInflight = window

	var (
		armed   atomic.Bool
		reached = make(chan struct{})
		release = make(chan struct{})
		gate    = newAppendGate()
	)

	c := startClusterWith(t, 1, cfg, map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": all}}, func(b *Broker) {
			b.cluster.holdAppend = gate.hold
			b.cluster.beforeInject = func(uint64) {
				if armed.CompareAndSwap(true, false) {
					close(reached)
					<-release
				}
			}
		})
	a := c.nodes[0]
	nsID := namespacedID(t1.ID.String(), "rs")

	sess := newRawSession()
	rc := dialRaw(t, c.addr(0), "rs", sess, true)
	require.Equal(t, byte(0), rc.ack(t).ReasonCode)
	rc.subscribe(t, "w/#", 1)

	pub, _, err := connect5(t, c.addr(0), "rs-pub", "u1", true)
	require.NoError(t, err)

	for i := range window - 1 {
		require.Equal(t, byte(0), publishQ(t, pub, "w/x", strconv.Itoa(i), 1))
	}

	inflight := func() int {
		cl, ok := a.server.Clients.Get(nsID)
		if !ok {
			return -1
		}

		return cl.State.Inflight.Len()
	}

	eventually(t, "the first deliveries", func() bool { return inflight() == window-1 })

	// The next message stops in the reader after its full check.
	armed.Store(true)
	require.Equal(t, byte(0), publishQ(t, pub, "w/x", "late", 1))
	<-reached

	// The client's own QoS 2 publish reserves the last slot meanwhile.
	gate.close()
	rc.send(&packets.Publish{Topic: "other/x", QoS: 2, PacketID: 9, Payload: []byte("q2"), Properties: &packets.Properties{}})
	eventually(t, "the reservation", func() bool { return inflight() == window })

	close(release) // "late" does not fit and is dropped by mochi

	rc.releaseAcks()
	gate.open()

	eventually(t, "the message that did not fit", func() bool { return sess.count("late") == 1 })
}

// A replay start for a connection that no longer serves the session (a repin
// or a waiting flip that read the client before a same-node handover) does
// nothing: it neither stops the current connection's replay nor touches the
// index. Here the stale call is made directly, after the new connection's
// replay has started, while that replay waits for its window.
func TestStaleStartReplayLeavesCurrentReplay(t *testing.T) {
	const window = 10

	c, tid := windowCell(t, 1, window)
	a := c.nodes[0]
	nsID := namespacedID(tid, "stale")

	sess := newRawSession()
	first := dialRaw(t, c.addr(0), "stale", sess, false)
	require.Equal(t, byte(0), first.ack(t).ReasonCode)
	first.subscribe(t, "st/#", 1)
	first.leave(t)
	waitOffline(t, a, tid, "stale")

	pub, _, err := connect5(t, c.addr(0), "stale-pub", "u1", true)
	require.NoError(t, err)

	const total = 30

	for i := range total {
		require.Equal(t, byte(0), publishQ(t, pub, "st/x", strconv.Itoa(i), 1))
	}

	// The old connection's replay fills its window and waits.
	old := dialRaw(t, c.addr(0), "stale", newRawSession(), true)
	require.True(t, old.ack(t).SessionPresent)
	eventually(t, "the old window full", func() bool { return old.pubs.Load() == window })

	oldCl, ok := a.server.Clients.Get(nsID)
	require.True(t, ok)

	// Handed over on the same node; the new connection's replay waits for
	// its window too (acks held).
	got := newRawSession()
	again := dialRaw(t, c.addr(0), "stale", got, true)
	require.True(t, again.ack(t).SessionPresent)
	eventually(t, "the new connection's replay", func() bool {
		s := a.cluster.sessionByID(nsID)
		if s == nil {
			return false
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		return s.replayCancel != nil && again.pubs.Load() >= window
	})

	s := a.cluster.sessionByID(nsID)
	a.startReplay(s, oldCl) // stale

	again.releaseAcks()

	eventually(t, "every message on the new connection", func() bool {
		for i := range total {
			if got.count(strconv.Itoa(i)) == 0 {
				return false
			}
		}

		return true
	})
}

// A session write that loses its CAS to this node's own earlier write (one
// whose wait ran out and that landed later, as during a JetStream election)
// is not a takeover: the node adopts the revision and writes again, and the
// client stays connected (A-I4). The landed write is simulated by writing the
// record as it is, under the same owner and epoch, behind the node's back.
func TestOwnLateWriteNotTakeover(t *testing.T) {
	c := startCluster(t, 2)
	tid := c.t1.ID.String()
	a := c.nodes[0]

	cl, _, err := connect5(t, c.addr(0), "late-write", "u1", false)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	rec, rev, err := a.cluster.Sessions.Get(ctx, tid, "late-write")
	require.NoError(t, err)
	require.Equal(t, a.cluster.Config.NodeID, rec.Owner)

	_, err = a.cluster.Sessions.Update(ctx, tid, "late-write", rev, rec)
	require.NoError(t, err)

	// A subscription changes the record, so the flusher writes it.
	subscribeQoS(t, cl, "late/#", 1, false)

	eventually(t, "the subscription in the record", func() bool {
		got, err := sessionKey(t, a, tid, "late-write")

		return err == nil && len(got.Subscriptions) == 1
	})

	select {
	case d := <-cl.disconnects:
		t.Fatalf("disconnected with %#x: the node took its own write for a takeover", d.ReasonCode)
	case <-time.After(500 * time.Millisecond):
	}

	require.Positive(t, a.cluster.ownWrites.Load())
	require.Zero(t, a.cluster.takeovers.Load())
}
