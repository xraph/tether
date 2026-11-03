package broker

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/tenant"
)

// loadTestInterval is the LoadInterval of the load tests' cells.
const loadTestInterval = 200 * time.Millisecond

// withLoad gives a cell node a load reporter at interval, the way a host
// passes Cluster.Load. It runs before Start.
func withLoad(interval time.Duration, more func(*Broker)) func(*Broker) {
	return func(b *Broker) {
		b.cluster.Config.LoadInterval = interval
		b.cluster.Load = cluster.NewLoad(b.cluster.Conn, b.cluster.Config)

		if more != nil {
			more(b)
		}
	}
}

// startLoadCell is startCluster with a load reporter on every node.
func startLoadCell(t *testing.T, nodes int, interval time.Duration, more func(*Broker)) *testCell {
	t.Helper()

	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	all := acl.Rules{Publish: []string{"#", "$app/#"}, Subscribe: []string{"#", "$app/#"}}

	c := startClusterWith(t, nodes, testConfig(),
		map[string]*auth.Identity{"u1": ident(t1, "u1"), "u2": ident(t2, "u2")},
		fakePerms{rules: map[string]acl.Rules{"u1": all, "u2": all}}, withLoad(interval, more))
	c.t1, c.t2 = t1, t2

	return c
}

// nodeRow returns the row of nodes for nodeID.
func nodeRow(nodes []NodeUsage, nodeID string) (NodeUsage, bool) {
	for _, n := range nodes {
		if n.NodeID == nodeID {
			return n, true
		}
	}

	return NodeUsage{}, false
}

func TestCellUsageSumsNodes(t *testing.T) {
	c := startLoadCell(t, 2, loadTestInterval, nil)
	a, b := c.nodes[0], c.nodes[1]
	t1 := c.t1.ID.String()

	for _, id := range []string{"a1", "a2"} {
		_, _, err := connect5(t, c.addr(0), id, "u1", true)
		require.NoError(t, err)
	}

	_, _, err := connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	eventually(t, "both nodes count three connections of t1", func() bool {
		return a.CellUsage(t1).Connections == 3 && b.CellUsage(t1).Connections == 3
	})

	assert.Equal(t, 1, a.RemoteConnections(t1))
	assert.Equal(t, 2, b.RemoteConnections(t1))
	assert.Equal(t, t1, a.CellUsage(t1).Tenant)
	assert.Zero(t, a.RemoteConnections(c.t2.ID.String()))
	assert.Zero(t, a.CellUsage(c.t2.ID.String()).Connections)
}

func TestNodesListsLiveNodes(t *testing.T) {
	c := startLoadCell(t, 2, loadTestInterval, nil)
	a, b := c.nodes[0], c.nodes[1]

	eventually(t, "both nodes list both nodes", func() bool {
		return len(a.Nodes()) == 2 && len(b.Nodes()) == 2
	})

	rows := a.Nodes()
	require.Equal(t, []string{"n1", "n2"}, []string{rows[0].NodeID, rows[1].NodeID}, "sorted by node ID")
	assert.True(t, rows[0].Self)
	assert.False(t, rows[1].Self)
	assert.False(t, rows[1].StartedAt.IsZero())
	assert.False(t, rows[1].At.IsZero())

	other, ok := nodeRow(b.Nodes(), "n1")
	require.True(t, ok)
	assert.False(t, other.Self)

	self, ok := nodeRow(b.Nodes(), "n2")
	require.True(t, ok)
	assert.True(t, self.Self)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	require.NoError(t, b.Stop(ctx))

	stopped := time.Now()
	bound := 3*loadTestInterval + time.Second

	eventuallyWithin(t, bound+time.Second, "the stopped node leaves A's list", func() bool {
		_, ok := nodeRow(a.Nodes(), "n2")

		return !ok
	})
	assert.Less(t, time.Since(stopped), bound, "gone within 3 x interval + 1 s")
}

func TestRatesFromReports(t *testing.T) {
	c := startLoadCell(t, 2, loadTestInterval, nil)
	b := c.nodes[1]
	t1 := c.t1.ID.String()

	pub, _, err := connect5(t, c.addr(0), "pub", "u1", true)
	require.NoError(t, err)

	const (
		rate = 50
		span = 3 * time.Second
	)

	// Paced by schedule, so a late tick is caught up and the total over the
	// span stays rate x span.
	done := make(chan struct{})

	go func() {
		defer close(done)

		start := time.Now()

		for i := range int(span.Seconds() * rate) {
			time.Sleep(time.Until(start.Add(time.Duration(i) * time.Second / rate)))

			ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
			_, _ = pub.Publish(ctx, &paho.Publish{Topic: "load/x", QoS: 0, Payload: []byte("x")})

			cancel()
		}
	}()

	// Sample from the second half of the span, when every report pair B
	// holds is inside it.
	time.Sleep(span / 2)

	var samples []float64

	for {
		samples = append(samples, b.TenantRates(t1).MessagesInPerSec)

		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			continue
		}

		break
	}

	slices.Sort(samples)
	median := samples[len(samples)/2]
	t.Logf("B's rate samples for t1: %v", samples)

	assert.GreaterOrEqual(t, median, 30.0)
	assert.LessOrEqual(t, median, 70.0)
}

func TestRateAfterRestartIsZero(t *testing.T) {
	// A longer interval here: the fake node's rows must outlive the checks
	// between its reports.
	c := startLoadCell(t, 1, time.Second, nil)
	a := c.nodes[0]
	t1 := c.t1.ID.String()

	cfg := a.cluster.Config
	cfg.NodeID = "n9"
	cfg.LoadInterval = 2 * time.Second
	fake := cluster.NewLoad(c.conns[0], cfg)

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	firstStart, secondStart := base, base.Add(30*time.Minute)

	report := func(started time.Time, at time.Duration, in uint64) {
		t.Helper()

		r := &cluster.NodeLoad{
			StartedAt: started,
			At:        started.Add(at),
			Tenants:   map[string]cluster.TenantLoad{t1: {Connections: 1, MessagesIn: in, BytesIn: in * 10}},
		}
		require.NoError(t, fake.Report(t.Context(), r))

		eventually(t, "A holds the report", func() bool {
			row, ok := nodeRow(a.Nodes(), "n9")

			return ok && row.At.Equal(r.At)
		})
	}

	ratesOf := func() Rates {
		row, ok := nodeRow(a.Nodes(), "n9")
		require.True(t, ok)

		return row.Rates
	}

	report(firstStart, time.Minute, 100)
	assert.Zero(t, ratesOf(), "one report has no rate")

	report(firstStart, time.Minute+time.Second, 200)
	assert.InDelta(t, 100.0, ratesOf().MessagesInPerSec, 0.001, "the pair before the restart")
	assert.InDelta(t, 1000.0, ratesOf().BytesInPerSec, 0.001)

	// The node restarted: its counters began again from zero.
	report(secondStart, time.Second, 10)
	assert.Zero(t, ratesOf(), "a changed StartedAt gives a zero rate, not a negative or huge one")
	assert.Zero(t, a.TenantRates(t1))

	// A counter that went down under the same StartedAt is no rate either.
	report(secondStart, 2*time.Second, 5)
	assert.Zero(t, ratesOf().MessagesInPerSec)

	report(secondStart, 3*time.Second, 55)
	assert.InDelta(t, 50.0, ratesOf().MessagesInPerSec, 0.001, "a pair after the restart has its rate")
	assert.InDelta(t, 50.0, a.TenantRates(t1).MessagesInPerSec, 0.001)
}

func TestSysConnectedCellWide(t *testing.T) {
	c := startLoadCell(t, 2, loadTestInterval, func(b *Broker) { b.sysInterval = 100 * time.Millisecond })

	sub, _, err := connect5(t, c.addr(0), "sub", "u1", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "$SYS/clients/connected"))

	_, _, err = connect5(t, c.addr(0), "a2", "u1", true)
	require.NoError(t, err)

	_, _, err = connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	deadline := time.After(waitTimeout)

	var seen []string

	for {
		select {
		case m := <-sub.msgs:
			if strings.HasPrefix(m.Topic, probePrefix) {
				continue
			}

			require.Equal(t, "$SYS/clients/connected", m.Topic)

			if m.Payload == "3" {
				return
			}

			seen = append(seen, m.Payload)
		case <-deadline:
			t.Fatalf("no $SYS/clients/connected of 3, saw %v", seen)
		}
	}
}

func TestSingleNodeNodesAndRates(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	b := startBrokerWith(t, testConfig(),
		map[string]*auth.Identity{"u": ident(tn, "u")},
		map[string]acl.Rules{"u": {Publish: []string{"#"}, Subscribe: []string{"#"}}},
		func(b *Broker) { b.loadInterval = 100 * time.Millisecond })

	pub, _, err := connect5(t, b.Addr().String(), "pub", "u", true)
	require.NoError(t, err)

	eventually(t, "a rate under traffic", func() bool {
		publish0(t, pub, "x", "x", nil)

		rows := b.Nodes()

		return len(rows) == 1 && rows[0].Rates.MessagesInPerSec > 0
	})

	rows := b.Nodes()
	require.Len(t, rows, 1)
	assert.Empty(t, rows[0].NodeID)
	assert.True(t, rows[0].Self)
	assert.Equal(t, 1, rows[0].Connections)
	assert.Empty(t, rows[0].Totals.Tenant)
	assert.Positive(t, rows[0].Totals.MessagesIn)
	assert.Equal(t, b.StartedAt(), rows[0].StartedAt)

	// Without a cluster the cell is this node.
	assert.Equal(t, 1, b.CellUsage(tn.ID.String()).Connections)
	assert.Zero(t, b.RemoteConnections(tn.ID.String()))
	assert.Positive(t, b.TenantRates(tn.ID.String()).MessagesInPerSec)
}

// rawLoad watches the cell's load bucket and keeps the latest raw report of
// each node, as it went over NATS.
type rawLoad struct {
	mu   sync.Mutex
	last map[string]*cluster.NodeLoad
}

func watchRawLoad(t *testing.T, c *testCell) *rawLoad {
	t.Helper()

	cfg := c.nodes[0].cluster.Config
	cfg.NodeID = "raw"

	w := &rawLoad{last: make(map[string]*cluster.NodeLoad)}

	stop, err := cluster.NewLoad(c.conns[0], cfg).Watch(t.Context(), func(id string, r *cluster.NodeLoad, _ bool) {
		w.mu.Lock()
		defer w.mu.Unlock()

		w.last[id] = r
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	return w
}

// tenant returns node's latest raw entry for tenantID, and whether there is
// a report at all and the tenant is in it.
func (w *rawLoad) tenant(node, tenantID string) (cluster.TenantLoad, bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	r := w.last[node]
	if r == nil {
		return cluster.TenantLoad{}, false, false
	}

	tl, ok := r.Tenants[tenantID]

	return tl, true, ok
}

// A tenant with no connection whose counters stood still since the node's
// last report is left out of the next one, so the report stays small. Its
// peers keep its last counters (CellUsage), count no connection for it and
// read no rate from it, and when it comes back the rate is its new traffic.
func TestLoadReportLeavesIdleTenantsOut(t *testing.T) {
	c := startLoadCell(t, 2, loadTestInterval, nil)
	b := c.nodes[1]
	t1 := c.t1.ID.String()
	raw := watchRawLoad(t, c)

	pub, _, err := connect5(t, c.addr(0), "a1", "u1", true)
	require.NoError(t, err)

	for range 3 {
		publish0(t, pub, "x", "x", nil)
	}

	eventually(t, "A reports the three messages", func() bool {
		tl, _, ok := raw.tenant("n1", t1)

		return ok && tl.MessagesIn == 3
	})
	require.NoError(t, pub.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	eventually(t, "A leaves the idle tenant out", func() bool {
		_, reported, ok := raw.tenant("n1", t1)

		return reported && !ok
	})

	assert.Equal(t, uint64(3), b.CellUsage(t1).MessagesIn, "B keeps A's counters for it")
	assert.Zero(t, b.CellUsage(t1).Connections)
	assert.Zero(t, b.RemoteConnections(t1))
	assert.Zero(t, b.TenantRates(t1).MessagesInPerSec)

	pub, _, err = connect5(t, c.addr(0), "a2", "u1", true)
	require.NoError(t, err)
	publish0(t, pub, "x", "x", nil)

	eventually(t, "B sees the fourth message", func() bool { return b.CellUsage(t1).MessagesIn == 4 })

	// One message over one interval, not the four since A started.
	assert.Less(t, b.TenantRates(t1).MessagesInPerSec, 3/loadTestInterval.Seconds())
	assert.Equal(t, 1, b.RemoteConnections(t1))
}
