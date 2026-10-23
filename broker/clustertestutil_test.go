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

	c := &testCell{nats: clustertest.StartCluster(t, 3)}
	urls := c.nats.URLs()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		for _, b := range c.nodes {
			_ = b.Stop(ctx)
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

		c.conns = append(c.conns, conn)

		b, err := New(cfg, Deps{
			Auth:    auth.NewStatic(ids),
			Perms:   perms,
			Logger:  testLogger().With("node", ccfg.NodeID),
			Cluster: &Cluster{Conn: conn, Config: ccfg},
		})
		require.NoError(t, err)

		if before != nil {
			before(b)
		}

		require.NoError(t, b.Start(t.Context()))

		c.nodes = append(c.nodes, b)
	}

	return c
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

	deadline := time.Now().Add(waitTimeout)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
