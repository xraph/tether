package broker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/tenant"
)

// requireKicked waits for the server's DISCONNECT on c and checks it is
// 0x98 administrative action.
func requireKicked(t *testing.T, c *client5) {
	t.Helper()

	select {
	case d := <-c.disconnects:
		require.Equal(t, byte(0x98), d.ReasonCode)
	case <-time.After(waitTimeout):
		t.Fatal("no DISCONNECT")
	}
}

// tenantOf is the tenant snapshot node b keeps for its live connection with
// the namespaced client ID nsID, nil when there is none.
func tenantOf(b *Broker, nsID string) *tenant.Tenant {
	for _, st := range b.state.match(func(st *clientState) bool { return st.cl.ID == nsID }) {
		return st.tenant.Load()
	}

	return nil
}

// A tenant change made on one node reaches the connections of every node:
// new limits replace the snapshot everywhere, and disabling the tenant
// disconnects its clients on every node, the publisher's included. Another
// tenant's clients stay.
func TestTenantDisableAcrossNodes(t *testing.T) {
	c := startControlCell(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	tid := c.t1.ID.String()

	onA, _, err := connect5(t, c.addr(0), "a1", "u1", true)
	require.NoError(t, err)

	onB, _, err := connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	other, _, err := connect5(t, c.addr(1), "b2", "u2", true)
	require.NoError(t, err)

	limited := *c.t1
	limited.Limits.MaxConnections = 7
	a.UpdateTenant(&limited)

	eventually(t, "the new limits on B", func() bool {
		tn := tenantOf(b, namespacedID(tid, "b1"))

		return tn != nil && tn.Limits.MaxConnections == 7
	})

	disabled := limited
	disabled.Enabled = false
	a.UpdateTenant(&disabled)

	requireKicked(t, onB)
	requireKicked(t, onA)

	for i, n := range c.nodes {
		require.Equal(t, uint64(2), n.cluster.tenantApplied.Load(), "node %d applied both changes once", i)
	}

	requireConnected(t, other)
}

// Ruling R41: when the change cannot be published, the node that made it
// applies it to its own connections, and the other nodes keep theirs.
func TestTenantUpdateFallsBackWhenBusFails(t *testing.T) {
	c := startControlCell(t, 2)
	a := c.nodes[0]

	onA, _, err := connect5(t, c.addr(0), "a1", "u1", true)
	require.NoError(t, err)

	onB, _, err := connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	a.cluster.publishFault = func(string) error { return errors.New("bus down") }

	disabled := *c.t1
	disabled.Enabled = false
	a.UpdateTenant(&disabled)

	requireKicked(t, onA)
	require.Equal(t, uint64(1), a.cluster.tenantApplied.Load(), "applied here, once")
	require.Zero(t, c.nodes[1].cluster.tenantApplied.Load(), "nothing reached B")
	requireConnected(t, onB)
}

// Ruling R7 completed: Kick and DeleteSession reach a session another node
// serves through the control bus, and the owner acts. The caller gets nil
// once the cell shows the owner let the session go.
func TestKickRemoteSession(t *testing.T) {
	c := startControlCell(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	tid := c.t1.ID.String()

	t.Run("kick", func(t *testing.T) {
		s, _, err := connect5(t, c.addr(0), "far", "u1", false)
		require.NoError(t, err)

		require.NoError(t, b.Kick(tid, "far"))
		requireKicked(t, s)

		rec, err := sessionKey(t, b, tid, "far")
		require.NoError(t, err)
		require.Empty(t, rec.Owner, "the persistent session stays, offline")

		require.ErrorIs(t, b.Kick(tid, "far"), tether.ErrNotFound, "an offline session has no connection to kick")
		require.Equal(t, uint64(1), a.cluster.kickEvents.Load())
		require.Zero(t, b.cluster.kickEvents.Load(), "only the owner handles the kick")
	})

	t.Run("delete", func(t *testing.T) {
		s, _, err := connect5(t, c.addr(0), "gone", "u1", false)
		require.NoError(t, err)
		subscribeQoS(t, s, "d/#", 1, false)

		require.NoError(t, b.DeleteSession(tid, "gone"))
		requireKicked(t, s)

		_, err = sessionKey(t, b, tid, "gone")
		require.ErrorIs(t, err, tether.ErrNotFound)

		_, ca, err := connect5(t, c.addr(0), "gone", "u1", false)
		require.NoError(t, err)
		require.False(t, ca.SessionPresent, "the session was deleted, not just disconnected")
		require.Equal(t, uint64(1), a.cluster.deleteEvents.Load())
	})

	t.Run("unknown", func(t *testing.T) {
		require.ErrorIs(t, b.Kick(tid, "nobody"), tether.ErrNotFound)
		require.ErrorIs(t, b.DeleteSession(tid, "nobody"), tether.ErrNotFound)
	})
}

// Every node, the publisher included, applies each control event published
// while it runs exactly once. Three nodes publish ten tenant changes each at
// once; then one more change disables the tenant of a client on every node.
// The bus delivers in stream order, so when a node's client is disconnected
// the node has handled everything before the marker, and a repeat would
// have pushed its count over.
func TestControlEventOnce(t *testing.T) {
	const perNode = 10

	c := startControlCell(t, 3)

	clients := make([]*client5, len(c.nodes))
	for i := range c.nodes {
		cl, _, err := connect5(t, c.addr(i), "m"+string(rune('0'+i)), "u1", true)
		require.NoError(t, err)

		clients[i] = cl
	}

	var wg sync.WaitGroup

	for _, n := range c.nodes {
		wg.Go(func() {
			for range perNode {
				n.UpdateTenant(newTenant(tenant.Limits{}))
			}
		})
	}

	wg.Wait()

	marker := *c.t1
	marker.Enabled = false
	c.nodes[0].UpdateTenant(&marker)

	want := uint64(len(c.nodes)*perNode + 1)

	for i, n := range c.nodes {
		requireKicked(t, clients[i])
		require.Equal(t, want, n.cluster.tenantApplied.Load(), "node %d", i)
	}
}
