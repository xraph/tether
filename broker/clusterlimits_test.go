package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/tenant"
)

// startLimitCell is startLoadCell where t1 carries lim.
func startLimitCell(t *testing.T, nodes int, interval time.Duration, lim tenant.Limits, more func(*Broker)) *testCell {
	t.Helper()

	t1, t2 := newTenant(lim), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}

	c := startClusterWith(t, nodes, testConfig(),
		map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")},
		fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}}, withLoad(interval, more))
	c.t1, c.t2 = t1, t2

	return c
}

// setShareInterval lowers the share loop's interval for brokers started after
// the call, and puts it back when the test ends.
func setShareInterval(t *testing.T, d time.Duration) {
	t.Helper()

	old := shareInterval
	shareInterval = d

	t.Cleanup(func() { shareInterval = old })
}

// A tenant's MaxConnections counts the cell: connections on other nodes spend
// the same limit.
func TestCellConnectionLimit(t *testing.T) {
	c := startLimitCell(t, 2, loadTestInterval, tenant.Limits{MaxConnections: 3}, nil)
	a, b := c.nodes[0], c.nodes[1]
	t1 := c.t1.ID.String()

	for _, id := range []string{"a1", "a2"} {
		_, _, err := connect5(t, c.addr(0), id, "u1", true)
		require.NoError(t, err)
	}

	eventually(t, "B sees the two connections on A", func() bool { return b.RemoteConnections(t1) == 2 })

	_, _, err := connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err, "the third connection of the cell")

	_, ca, err := connect5(t, c.addr(1), "b2", "u1", true)
	require.Error(t, err, "the fourth is over the cell's limit")
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode, "quota exceeded")

	// A client ID that already has a connection on B takes its slot over, at
	// the limit.
	_, _, err = connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err, "a takeover needs no second slot")

	_, ca, err = connect5(t, c.addr(1), "b3", "u1", true)
	require.Error(t, err, "the takeover left the count where it was")
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)

	// Another tenant has its own limit.
	_, _, err = connect5(t, c.addr(1), "other", "u2", true)
	require.NoError(t, err)

	// A's connections are counted against B's too: the cell is at three.
	eventually(t, "A sees B's connection", func() bool { return a.RemoteConnections(t1) == 1 })

	_, ca, err = connect5(t, c.addr(0), "a3", "u1", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)
}

// A device whose connection is still live on A (its TCP died silently) and
// that reconnects to B at the cell's limit replaces its connection: the
// session record says A runs it, so B takes a slot over the limit and the
// session takeover disconnects the old one.
func TestCellConnectionLimitCrossNodeTakeover(t *testing.T) {
	c := startLimitCell(t, 2, loadTestInterval, tenant.Limits{MaxConnections: 3}, nil)
	b := c.nodes[1]
	t1 := c.t1.ID.String()

	oldA1, _, err := connect5(t, c.addr(0), "a1", "u1", true)
	require.NoError(t, err)

	_, _, err = connect5(t, c.addr(0), "a2", "u1", true)
	require.NoError(t, err)

	_, _, err = connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	eventually(t, "B sees A's two connections", func() bool { return b.RemoteConnections(t1) == 2 })

	_, ca, err := connect5(t, c.addr(1), "fresh", "u1", true)
	require.Error(t, err, "a new client ID is over the cell's limit")
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)

	_, _, err = connect5(t, c.addr(1), "a1", "u1", true)
	require.NoError(t, err, "a1 moving to B is a takeover, not a new connection")

	select {
	case d := <-oldA1.disconnects:
		assert.Equal(t, byte(0x8E), d.ReasonCode, "the old a1 on A is taken over")
	case <-time.After(waitTimeout):
		t.Fatal("the old a1 on A was not disconnected")
	}

	// Once A's slot is back and has been reported, the cell is at three
	// again and a fresh ID is refused.
	eventually(t, "B sees A's one connection", func() bool { return b.RemoteConnections(t1) == 1 })
	eventually(t, "B holds b1 and the moved a1", func() bool { return b.CellUsage(t1).Connections == 3 })

	_, ca, err = connect5(t, c.addr(1), "fresh2", "u1", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)
}

// Each node's share of a tenant's rate follows where its connections are.
func TestRateShareRebalances(t *testing.T) {
	setShareInterval(t, 200*time.Millisecond)

	c := startLimitCell(t, 2, loadTestInterval, tenant.Limits{MessagesPerSecond: 100}, nil)
	a, b := c.nodes[0], c.nodes[1]
	t1 := c.t1.ID.String()

	near := func(l interface{ Share(string) float64 }, want float64) func() bool {
		return func() bool {
			got := l.Share(t1)

			return got > want-0.01 && got < want+0.01
		}
	}

	for _, id := range []string{"a1", "a2", "a3"} {
		_, _, err := connect5(t, c.addr(0), id, "u1", true)
		require.NoError(t, err)
	}

	// B has no connection of the tenant, so it gets an even slice of the
	// cell, for the HTTP publish endpoint.
	eventually(t, "B holds half the budget", near(b.deps.Limits, 0.5))
	eventually(t, "A holds all of it", near(a.deps.Limits, 1))

	_, _, err := connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	eventually(t, "A holds three quarters", near(a.deps.Limits, 0.75))
	eventually(t, "B holds one quarter", near(b.deps.Limits, 0.25))
}

// A peer that stops reporting leaves the view within three of this node's
// intervals, whatever TTL its key has in the bucket.
func TestPeerLeavesViewWhenStale(t *testing.T) {
	c := startLoadCell(t, 1, loadTestInterval, nil)
	a := c.nodes[0]
	t1 := c.t1.ID.String()

	cfg := a.cluster.Config
	cfg.NodeID = "n9"
	cfg.LoadInterval = 30 * time.Second // The key outlives the test.
	fake := cluster.NewLoad(c.conns[0], cfg)

	now := time.Now()
	require.NoError(t, fake.Report(t.Context(), &cluster.NodeLoad{
		StartedAt: now,
		At:        now,
		Tenants:   map[string]cluster.TenantLoad{t1: {Connections: 2}},
	}))

	eventually(t, "A holds the report", func() bool {
		_, ok := nodeRow(a.Nodes(), "n9")

		return ok && a.RemoteConnections(t1) == 2
	})

	reported := time.Now()
	bound := 3*loadTestInterval + time.Second

	eventuallyWithin(t, bound+time.Second, "the silent node leaves A's view", func() bool {
		_, ok := nodeRow(a.Nodes(), "n9")

		return !ok && a.RemoteConnections(t1) == 0
	})
	assert.Less(t, time.Since(reported), bound, "gone within 3 x interval + 1 s")
	assert.Zero(t, a.CellUsage(t1).Connections)
}
