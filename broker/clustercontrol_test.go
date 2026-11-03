package broker

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
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

// Ruling R42: deleting a session whose delayed will is still held publishes
// the will at once, as MQTT 5 does at the end of the session, both on the
// node that holds it and through the bus from another node (C-I1).
func TestDeleteSessionPublishesHeldWill(t *testing.T) {
	c := startControlCell(t, 2)
	tid := c.t1.ID.String()

	watcher, _, err := connect5(t, c.addr(1), "will-watch", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, watcher, "wills/#", 1, false)

	for i, from := range []int{0, 1} {
		id := fmt.Sprintf("held%d", i)

		conn := rawConnectWill(t, c.addr(0), willSpec{id: id, topic: "wills/" + id, qos: 1, expiry: 3600, delay: 60})
		_ = conn.Close() // abnormal: the will is owed, after its delay

		eventually(t, id+"'s will held", func() bool {
			rec, err := sessionKey(t, c.nodes[0], tid, id)

			return err == nil && rec.Will != nil && !rec.DisconnectedAt.IsZero()
		})

		require.NoError(t, c.nodes[from].DeleteSession(tid, id))

		m := recvLive(t, watcher)
		require.Equal(t, "wills/"+id, m.Topic)
		require.Equal(t, "gone "+id, m.Payload)
		require.Empty(t, c.nodes[0].cluster.pendingWills(), "the delay timer still runs")
	}
}

// Deleting the session of a connected client publishes its will once: the
// delete's disconnect fires it (at once, whatever its delay), and the
// record, which still holds the CONNECT's will, does not send it a second
// time (R42), on the node that serves it or through the bus.
func TestDeleteSessionOfConnectedClientWillOnce(t *testing.T) {
	c := startControlCell(t, 2)

	watcher, _, err := connect5(t, c.addr(1), "will-watch", "u1", true)
	require.NoError(t, err)
	subscribeQoS(t, watcher, "wills/#", 1, false)

	tid := c.t1.ID.String()

	for i, tc := range []struct {
		from  int
		delay uint32
	}{{0, 0}, {1, 0}, {0, 60}, {1, 60}} {
		id := fmt.Sprintf("live%d", i)

		conn := rawConnectWill(t, c.addr(0), willSpec{id: id, topic: "wills/" + id, qos: 1, expiry: 3600, delay: tc.delay})
		t.Cleanup(func() { _ = conn.Close() })

		require.NoError(t, c.nodes[tc.from].DeleteSession(tid, id))

		m := recvLive(t, watcher)
		require.Equal(t, "wills/"+id, m.Topic)
		require.Equal(t, "gone "+id, m.Payload)

		deadline := time.After(time.Second)

	listen:
		for {
			select {
			case m := <-watcher.msgs:
				if !strings.HasPrefix(m.Topic, probePrefix) {
					t.Fatalf("unexpected message %q on %q (delete from node %d, delay %d)", m.Payload, m.Topic, tc.from, tc.delay)
				}
			case <-deadline:
				break listen
			}
		}
	}
}

// Kicking, from another node, a client whose will has a delay succeeds: the
// owner keeps the record with DisconnectedAt set while the will waits, which
// counts as the connection gone, and Clients no longer lists it as connected
// (C-I2).
func TestKickRemoteWithDelayedWill(t *testing.T) {
	c := startControlCell(t, 2)
	tid := c.t1.ID.String()

	conn := rawConnectWill(t, c.addr(0), willSpec{id: "kd", topic: "wills/kd", qos: 1, expiry: 3600, delay: 60})
	t.Cleanup(func() { _ = conn.Close() })

	start := time.Now()
	require.NoError(t, c.nodes[1].Kick(tid, "kd"))
	require.Less(t, time.Since(start), remoteAdminWait, "the kick waited out its bound")

	rec, err := sessionKey(t, c.nodes[1], tid, "kd")
	require.NoError(t, err)
	require.False(t, rec.DisconnectedAt.IsZero())

	for _, ci := range c.nodes[1].Clients(tid) {
		if ci.ClientID == "kd" {
			require.False(t, ci.Connected, "a session waiting out its will delay is not connected")
		}
	}
}

// Ruling R51: a tenant change whose publish fails (a control stream
// election, say) is applied on this node at once and sent again until the
// bus takes it, so the other nodes apply it too once the bus is back (D-I1).
// Kick and DeleteSession of a session another node serves send their event
// again within their wait.
func TestControlRepublishedAfterFailure(t *testing.T) {
	c := startControlCell(t, 2)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	var fails atomic.Int32

	a.cluster.publishFault = func(string) error {
		if fails.Add(-1) >= 0 {
			return errors.New("control stream has no leader")
		}

		return nil
	}

	far, _, err := connect5(t, c.addr(1), "far", "u1", false)
	require.NoError(t, err)

	fails.Store(2)
	require.NoError(t, a.Kick(tid, "far"), "the kick's event went out on a later attempt")
	requireKicked(t, far)

	onA, _, err := connect5(t, c.addr(0), "a1", "u1", true)
	require.NoError(t, err)

	onB, _, err := connect5(t, c.addr(1), "b1", "u1", true)
	require.NoError(t, err)

	fails.Store(3)

	disabled := *c.t1
	disabled.Enabled = false
	a.UpdateTenant(&disabled)

	requireKicked(t, onA)
	requireKicked(t, onB)
	require.Positive(t, a.cluster.republished.Load())
}

// A tenant change whose publish failed and is sent again late does not
// overwrite a newer change that landed meanwhile (D-I1 follow-up): node A's
// disable fails to publish, node B then enables the tenant again with a
// newer snapshot, and when A's republish lands it is older than what every
// node holds, so B's client is not disconnected and keeps the newer limits.
// A newer change on the node itself stops the older one's republish.
func TestStaleTenantRepublishIgnored(t *testing.T) {
	c := startControlCell(t, 2)
	a, b := c.nodes[0], c.nodes[1]
	tid := c.t1.ID.String()

	var failing atomic.Bool

	a.cluster.publishFault = func(kind string) error {
		if kind == cluster.KindTenantUpdated && failing.Load() {
			return errors.New("control stream has no leader")
		}

		return nil
	}

	onA, _, err := connect5(t, c.addr(0), "sa", "u1", true)
	require.NoError(t, err)

	onB, _, err := connect5(t, c.addr(1), "sb", "u1", true)
	require.NoError(t, err)

	base := time.Now().UTC().Truncate(time.Microsecond)

	failing.Store(true)

	disabled := *c.t1
	disabled.Enabled = false
	disabled.UpdatedAt = base
	a.UpdateTenant(&disabled)
	requireKicked(t, onA) // applied on A at once (R41)

	enabled := *c.t1
	enabled.Limits.MaxConnections = 7
	enabled.UpdatedAt = base.Add(time.Second)
	b.UpdateTenant(&enabled)

	eventually(t, "B's change on B's client", func() bool {
		tn := tenantOf(b, tid+"/sb")

		return tn != nil && tn.Limits.MaxConnections == 7
	})

	applied := b.cluster.tenantApplied.Load()
	republished := a.cluster.republished.Load()

	failing.Store(false)

	eventually(t, "A's disable sent again", func() bool { return a.cluster.republished.Load() > republished })
	eventually(t, "B to handle it", func() bool { return b.cluster.tenantApplied.Load() > applied })

	select {
	case d := <-onB.disconnects:
		t.Fatalf("B's client disconnected with %#x by a tenant change older than the one it holds", d.ReasonCode)
	case <-time.After(500 * time.Millisecond):
	}

	tn := tenantOf(b, tid+"/sb")
	require.NotNil(t, tn)
	require.True(t, tn.Enabled)
	require.Equal(t, 7, tn.Limits.MaxConnections, "the older snapshot replaced the newer one")

	// On one node: two changes whose publishes fail; only the newer is sent
	// again.
	failing.Store(true)

	older := enabled
	older.Limits.MaxConnections = 8
	older.UpdatedAt = base.Add(2 * time.Second)
	a.UpdateTenant(&older)

	newer := enabled
	newer.Limits.MaxConnections = 9
	newer.UpdatedAt = base.Add(3 * time.Second)
	a.UpdateTenant(&newer)

	republished = a.cluster.republished.Load()

	failing.Store(false)

	eventually(t, "B to get A's newer change", func() bool {
		tn := tenantOf(b, tid+"/sb")

		return tn != nil && tn.Limits.MaxConnections == 9
	})

	time.Sleep(time.Second) // past the older one's next attempt, had it still run
	require.Equal(t, republished+1, a.cluster.republished.Load(), "the older change was still sent again")
	require.Equal(t, 9, tenantOf(b, tid+"/sb").Limits.MaxConnections)
}

// A noWill entry of a session that DeleteSession removed does not outlive
// it: a session created later under the same client ID restarts at epoch 1,
// and the stale entry would bar that session's held will (R42). The entry is
// also dropped when the delete finds no key at all.
func TestDeleteSessionClearsNoWill(t *testing.T) {
	c := startControlCell(t, 1)
	a := c.nodes[0]
	tid := c.t1.ID.String()

	held := func(nsID string) bool {
		a.cluster.willsMu.Lock()
		defer a.cluster.willsMu.Unlock()

		_, ok := a.cluster.noWill[nsID]

		return ok
	}

	cl, _, err := connect5(t, c.addr(0), "nw", "u1", false)
	require.NoError(t, err)
	require.NoError(t, cl.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	var rec *cluster.SessionRecord

	eventually(t, "nw's offline record", func() bool {
		var rerr error

		rec, rerr = sessionKey(t, a, tid, "nw")

		return rerr == nil && rec.Owner == ""
	})

	nsID := namespacedID(tid, "nw")

	a.cluster.markNoWill(nsID, rec.Epoch)
	require.True(t, a.cluster.willBarred(nsID, rec.Epoch))

	require.NoError(t, a.DeleteSession(tid, "nw"))
	require.False(t, held(nsID), "the deleted session's noWill entry is still held")
	require.False(t, a.cluster.willBarred(nsID, 1), "a later session at epoch 1 would be barred")

	ghost := namespacedID(tid, "ghost")

	a.cluster.markNoWill(ghost, 4)
	require.Error(t, a.DeleteSession(tid, "ghost"))
	require.False(t, held(ghost), "a delete that found no key left its noWill entry")
}

// Two changes of a tenant whose publishes both fail can reach
// republishTenant in either order; the republish of the newer snapshot
// stays.
func TestOlderTenantRepublishKeepsNewer(t *testing.T) {
	c := startControlCell(t, 1)
	n := c.nodes[0].cluster
	tid := c.t1.ID.String()

	// The bus is down, so no republish ends before the test does.
	n.publishFault = func(string) error { return errors.New("control stream has no leader") }

	base := time.Now().UTC().Truncate(time.Microsecond)

	mk := func(at time.Time) tenantEvent {
		snap := *c.t1
		snap.UpdatedAt = at

		return tenantEvent{TenantUpdated: cluster.TenantUpdated{TenantID: tid}, Tenant: &snap}
	}

	running := func() *tenantRepublish {
		n.tenantRepubMu.Lock()
		defer n.tenantRepubMu.Unlock()

		return n.tenantRepub[tid]
	}

	n.republishTenant(tid, mk(base.Add(time.Second)))

	newer := running()
	require.NotNil(t, newer)

	n.republishTenant(tid, mk(base))
	require.Same(t, newer, running(), "the older snapshot replaced the newer one's republish")

	n.republishTenant(tid, mk(base.Add(2*time.Second)))
	require.NotSame(t, newer, running(), "a newer snapshot did not replace the older one's republish")
}
