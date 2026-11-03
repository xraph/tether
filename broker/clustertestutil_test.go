package broker

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/tenant"
)

// testCell is a cell of brokers on one three-server NATS cluster.
type testCell struct {
	nats  *clustertest.Cluster
	nodes []*Broker
	conns []*cluster.Conn
	// ctls are the nodes' control buses, set only by startControlCell.
	ctls []*cluster.Control

	// t1 and t2 are two tenants. The users "u1" (of t1) and "u2" (of t2)
	// may publish and subscribe anything, "$app/#" included.
	t1, t2 *tenant.Tenant
}

// startCluster starts a three-server NATS cluster and nodes brokers on it,
// node i connected to server i%3, with static auth for the users u1 (tenant
// t1) and u2 (tenant t2).
func startCluster(t *testing.T, nodes int) *testCell {
	t.Helper()

	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}

	c := startClusterWith(t, nodes, testConfig(),
		map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")},
		fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}}, nil)
	c.t1, c.t2 = t1, t2

	return c
}

// startClusterWith is startCluster with a broker config, identities, a
// PermLoader and an optional hook on each broker before it starts.
//
// Each node gets its own cluster.Connect, with NoDiscovery so it stays on
// its server, and the cell is provisioned once. One cleanup, registered
// after the NATS cluster's, stops every broker and then closes every
// connection; the servers go after that.
func startClusterWith(t *testing.T, nodes int, cfg Config, ids map[string]*auth.Identity, perms PermLoader, before func(*Broker)) *testCell {
	t.Helper()

	return startCellVia(t, nodes, cfg, ids, perms, before, nil)
}

// startCellVia is startClusterWith where via, when not nil, may replace the
// NATS URL node i connects to (a fault proxy in front of it).
func startCellVia(t *testing.T, nodes int, cfg Config, ids map[string]*auth.Identity, perms PermLoader, before func(*Broker),
	via func(i int, url string) string,
) *testCell {
	t.Helper()

	return buildCell(t, nil, nodes, cfg, ids, perms, before, via, false)
}

// startControlCell is startCluster with a control bus on every node, wired
// the way a host wires it: the bus is made before the broker (whose New
// subscribes to it) and started before the broker's Start.
func startControlCell(t *testing.T, nodes int) *testCell {
	t.Helper()

	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}

	c := buildCell(t, nil, nodes, testConfig(),
		map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")},
		fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}}, nil, nil, true)
	c.t1, c.t2 = t1, t2

	return c
}

// buildCell is startCellVia, with a control bus per node when control is set,
// on nats when it is not nil (clustertest.StartProxied, say).
func buildCell(t *testing.T, nats *clustertest.Cluster, nodes int, cfg Config, ids map[string]*auth.Identity, perms PermLoader, before func(*Broker),
	via func(i int, url string) string, control bool,
) *testCell {
	t.Helper()

	if nats == nil {
		nats = clustertest.StartCluster(t, 3)
	}

	c := &testCell{nats: nats}
	c.trackState(t)

	urls := c.nats.URLs()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		for _, b := range c.nodes {
			_ = b.Stop(ctx)
		}

		for _, ctl := range c.ctls {
			ctl.Stop()
		}

		for _, conn := range c.conns {
			conn.Close()
		}
	})

	for i := range nodes {
		ccfg := cluster.DefaultConfig()
		ccfg.Enabled = true
		ccfg.NodeID = fmt.Sprintf("n%d", i+1)
		ccfg.Cell = "c1"
		ccfg.Mode = cluster.ModeExternal
		ccfg.URLs = []string{urls[i%len(urls)]}

		if via != nil {
			ccfg.URLs = []string{via(i, ccfg.URLs[0])}
		}
		ccfg.Replicas = 3
		ccfg.NoDiscovery = true

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

		cl := &Cluster{Conn: conn, Config: ccfg}
		if control {
			cl.Control = cluster.NewControl(conn, ccfg)
			c.ctls = append(c.ctls, cl.Control)
		}

		b, err := New(cfg, Deps{
			Auth:    auth.NewStatic(ids),
			Perms:   perms,
			Logger:  testLogger().With("node", ccfg.NodeID),
			Cluster: cl,
		})
		require.NoError(t, err)

		if before != nil {
			before(b)
		}

		if cl.Control != nil {
			require.NoError(t, cl.Control.Start(t.Context()))
		}

		require.NoError(t, b.Start(t.Context()))

		c.nodes = append(c.nodes, b)
	}

	return c
}

// cellSubjects is a subject of each stream and bucket Provision makes for
// the cell (the log's "d.>", the control bus's "ctl.>" and the three
// buckets), for clustertest's WaitInterest. A cell is ready for its nodes
// once every server routes a write to all of them: until then the first
// session write, heartbeat or append through a server the interest has not
// reached gets no responders, and a CONNECT is refused.
func cellSubjects(cfg cluster.Config) []string {
	return []string{
		"d.x", "ctl.x",
		"$KV." + cfg.SessionsBucket() + ".x", "$KV." + cfg.RetainedBucket() + ".x", "$KV." + cfg.MembersBucket() + ".x",
	}
}

// addr is node i's MQTT address.
func (c *testCell) addr(i int) string { return c.nodes[i].Addr().String() }

// probePrefix is the topic prefix of interest probes; recvMsg skips them.
const probePrefix = "probe/"

var probeSeq atomic.Int64

// awaitInterest waits until pub's node routes to sub's node everything sub
// subscribed to so far. sub subscribes to a fresh probe topic, and pub
// publishes QoS 0 probes on it until one arrives. Subscription interest
// travels between NATS servers in the order it was made, so once the probe's
// interest has reached pub's node, every earlier subscription of sub's node
// has too. QoS 0 sent before that point can be lost, which is why it polls.
// Both clients must be of the same tenant.
func awaitInterest(t *testing.T, pub, sub *client5) {
	t.Helper()

	probe := fmt.Sprintf("%s%d", probePrefix, probeSeq.Add(1))
	require.Equal(t, []byte{1}, subscribe5(t, sub, probe))

	deadline := time.Now().Add(10 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)

	defer tick.Stop()

	for {
		publish0(t, pub, probe, "probe", nil)

		select {
		case m := <-sub.msgs:
			if m.Topic == probe {
				return
			}

			if strings.HasPrefix(m.Topic, probePrefix) {
				// A late probe of an earlier wait.
				continue
			}

			t.Fatalf("message %q on %q before the interest probe", m.Payload, m.Topic)
		case <-tick.C:
		}

		if time.Now().After(deadline) {
			t.Fatal("interest did not reach the publisher's node")
		}
	}
}

// publish0 publishes at QoS 0 with optional publish properties.
func publish0(t *testing.T, c *client5, name, payload string, props *paho.PublishProperties) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err := c.Publish(ctx, &paho.Publish{Topic: name, QoS: 0, Payload: []byte(payload), Properties: props})
	require.NoError(t, err)
}

// recvMsg waits for the next message on c that is not an interest probe.
func recvMsg(t *testing.T, c *client5) message {
	t.Helper()

	deadline := time.After(waitTimeout)

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

// settle is how long a test listens before it calls a message not
// delivered, after a sentinel proved the publisher's earlier messages left.
const settle = 300 * time.Millisecond

// noMoreMsgs fails when c receives anything but probes within settle.
func noMoreMsgs(t *testing.T, c *client5) {
	t.Helper()

	deadline := time.After(settle)

	for {
		select {
		case m := <-c.msgs:
			if !strings.HasPrefix(m.Topic, probePrefix) {
				t.Fatalf("unexpected message %q on %q", m.Payload, m.Topic)
			}
		case <-deadline:
			return
		}
	}
}

// eventually polls cond until it holds or waitTimeout runs out.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	eventuallyWithin(t, waitTimeout, what, cond)
}

// eventuallyWithin is eventually bounded by d. A timeout inside a test with
// a running cell also prints the cell's delivery state (cellState).
func eventuallyWithin(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(d)

	for !cond() {
		if time.Now().After(deadline) {
			if cellState != nil {
				t.Fatalf("timed out waiting for %s\n%s", what, cellState())
			}

			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// cellState, while a test's cell runs, describes where its deliveries
// stand: every node's log readers and every session it serves. The tests of
// this package run one at a time, so one variable is enough.
var cellState func() string

// trackState makes the cell's state what a timed-out wait prints, until the
// test ends.
func (c *testCell) trackState(t *testing.T) {
	cellState = c.state
	t.Cleanup(func() { cellState = nil })
}

// state describes, per node, the log readers (tenant, position, live
// position, last message) and the sessions (mode 0 live, 1 replay, 2
// waiting; position; flip floor; queued replay packets; pinned; lost or
// ended), with the node's delivery counters.
func (c *testCell) state() string {
	var b strings.Builder

	for i, node := range c.nodes {
		n := node.cluster
		if n == nil {
			continue
		}

		fmt.Fprintf(&b, "node %d: log delivered %d, appends %d, replays done %d, replay retries %d, repins %d\n",
			i, n.logDelivered.Load(), n.appends.Load(), n.replaysDone.Load(), n.replayRetries.Load(), n.repins.Load())

		type readerState struct {
			tenant  string
			pos     uint64
			stopped bool
		}

		n.readersMu.Lock()
		readers := make([]readerState, 0, len(n.readers))

		for tenantID, e := range n.readers {
			rs := readerState{tenant: tenantID, stopped: e.stopped}
			if e.r != nil {
				rs.pos = e.r.Position()
			}

			readers = append(readers, rs)
		}
		n.readersMu.Unlock()

		for _, rs := range readers {
			live, last := uint64(0), uint64(0)

			if tt := n.trackIfAny(rs.tenant); tt != nil {
				live, last = tt.livePos.Load(), tt.lastMsg.Load()
			}

			fmt.Fprintf(&b, "  reader %s: position %d, live %d, last message %d, stopped %v\n", rs.tenant, rs.pos, live, last, rs.stopped)
		}

		type sessState struct {
			s           *session
			lost, ended bool
		}

		n.sessMu.Lock()
		sessions := make([]sessState, 0, len(n.sess))

		for _, s := range n.sess {
			sessions = append(sessions, sessState{s: s, lost: s.lost, ended: s.ended})
		}
		n.sessMu.Unlock()

		for _, ss := range sessions {
			s := ss.s

			s.mu.Lock()
			fmt.Fprintf(&b, "  session %s: mode %d, pos %d, flip floor %d, queued %d, pinned %v, replay seen %d, lost %v, ended %v\n",
				s.nsID, s.mode, s.pos, s.flipFloor, len(s.queued), s.pinned, s.replaySeen, ss.lost, ss.ended)
			s.mu.Unlock()
		}
	}

	return b.String()
}
