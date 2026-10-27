package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/tenant"
)

// The broker's side of the cell's control bus (Cluster.Control): tenant
// changes reach every node, and a kick or delete of a session another node
// serves is done by that node. Key and role changes go through the bus in
// package events (the reactor), which drives the admin reactions in admin.go.
//
// Every handler runs on the bus's one goroutine and must not block it, so
// disconnects and deletes run on goroutines of their own.

const (
	// controlWait bounds a publish to the control bus.
	controlWait = 5 * time.Second

	// remoteAdminWait bounds how long Kick and DeleteSession wait for the
	// node that serves a session to act on their event, and remoteAdminPoll
	// is how often they read the session's record meanwhile.
	remoteAdminWait = 5 * time.Second
	remoteAdminPoll = 50 * time.Millisecond
)

// tenantEvent is the tenant.updated event the broker publishes:
// cluster.TenantUpdated plus the tenant as it was stored, so every node
// applies the same snapshot without a store of its own. An event without the
// tenant (some other publisher's) changes nothing here.
type tenantEvent struct {
	cluster.TenantUpdated

	Tenant *tenant.Tenant `json:"tenant,omitempty"`
}

// sessionEvent is a session.kick or session.delete event: the fields of
// cluster.SessionKick and cluster.SessionDelete plus the node that serves
// the session and the session's epoch when the event was made, so only that
// node acts, and only on that connection of the client ID.
type sessionEvent struct {
	Tenant   string `json:"tenant"`
	ClientID string `json:"client_id"`
	Node     string `json:"node,omitempty"`
	Epoch    uint64 `json:"epoch,omitempty"`
}

// controlNode is the control bus state of a clusterNode.
type controlNode struct {
	// publishFault, when a test sets it before Start, runs before every
	// control publish and fails it with its error.
	publishFault func(kind string) error

	// republished counts control events sent again after a failed publish
	// and taken by the bus (republish).
	republished atomic.Uint64

	// Counters for tests: tenant changes this node applied (from the bus or,
	// when a publish failed, its own), and kicks and deletes addressed to
	// this node that it handled.
	tenantApplied, kickEvents, deleteEvents atomic.Uint64
}

// subscribeControl registers the broker's handlers on the control bus, when
// there is one. Called once, from New: the bus wants its handlers before its
// Start.
func (b *Broker) subscribeControl() error {
	ctl := b.cluster.Control
	if ctl == nil {
		return nil
	}

	subs := []struct {
		kind string
		fn   func([]byte)
	}{
		{cluster.KindTenantUpdated, b.onTenantUpdated},
		{cluster.KindSessionKick, b.onSessionKick},
		{cluster.KindSessionDelete, b.onSessionDelete},
	}

	for _, s := range subs {
		if err := ctl.Subscribe(s.kind, s.fn); err != nil {
			return fmt.Errorf("tether/broker: control bus: %w", err)
		}
	}

	return nil
}

// publishControl publishes v as an event of kind, bounded by controlWait and
// by the broker's lifetime.
func (n *clusterNode) publishControl(kind string, v any) error {
	ctx, cancel := context.WithTimeout(n.ctx, controlWait)
	defer cancel()

	return n.publishControlCtx(ctx, kind, v)
}

// publishControlCtx is publishControl under ctx.
func (n *clusterNode) publishControlCtx(ctx context.Context, kind string, v any) error {
	if n.publishFault != nil {
		if err := n.publishFault(kind); err != nil {
			return err
		}
	}

	return n.Control.Publish(ctx, kind, v)
}

// publishTenant sends a tenant change to the cell, and reports whether it
// did: false without a control bus, and when the publish failed (logged at
// Warn), and then the caller applies it on this node (ruling R41). A failed
// publish is also sent again in the background until the bus takes it, the
// broker stops, or an hour has passed (ruling R51, cluster.Republish), so
// the other nodes get the change once the bus is back.
func (b *Broker) publishTenant(snap *tenant.Tenant) bool {
	n := b.cluster
	if n == nil || n.Control == nil || n.ctx.Err() != nil {
		// No bus, or the broker has stopped: nothing here to tell the
		// cell with, and applying it locally does nothing either.
		return false
	}

	tenantID := snap.ID.String()
	ev := tenantEvent{TenantUpdated: cluster.TenantUpdated{TenantID: tenantID}, Tenant: snap}

	if err := n.publishControl(cluster.KindTenantUpdated, ev); err != nil {
		n.log.Warn("tether: could not send a tenant change to the cell, applying it on this node and sending it again in the background",
			slog.String("tenant", tenantID), slog.Any("error", err))

		if !errors.Is(err, tether.ErrInvalidInput) {
			go n.republish(cluster.KindTenantUpdated, ev)
		}

		return false
	}

	return true
}

// republish sends a control event whose publish failed again until the bus
// takes it, the broker stops, or an hour has passed (cluster.Republish).
func (n *clusterNode) republish(kind string, v any) {
	err := cluster.Republish(n.ctx.Done(), func(ctx context.Context) error { return n.publishControlCtx(ctx, kind, v) })
	if err != nil {
		n.log.Warn("tether: gave up sending a control event to the cell", slog.String("kind", kind), slog.Any("error", err))

		return
	}

	n.republished.Add(1)
	n.log.Info("tether: control event sent to the cell after the bus came back", slog.String("kind", kind))
}

// publishSession publishes a session.kick or session.delete event for an
// admin call that waits for its outcome: a publish that fails (a control
// stream election, say) is sent again until the bus takes it or
// remoteAdminWait passes.
func (n *clusterNode) publishSession(kind string, ev sessionEvent) error {
	err := n.publishControl(kind, ev)
	if err == nil || errors.Is(err, tether.ErrInvalidInput) {
		return err
	}

	ctx, cancel := context.WithTimeout(n.ctx, remoteAdminWait)
	defer cancel()

	return cluster.Republish(ctx.Done(), func(actx context.Context) error {
		return n.publishControlCtx(actx, kind, ev)
	})
}

// onTenantUpdated applies a tenant.updated event on this node: the snapshot
// goes to the tenant's live connections at once, and a disabled tenant's
// connections are disconnected in the background.
func (b *Broker) onTenantUpdated(data []byte) {
	n := b.cluster

	var ev tenantEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		n.log.Warn("tether: undecodable tenant.updated event", slog.Any("error", err))

		return
	}

	if ev.Tenant == nil || ev.Tenant.ID.IsNil() || ev.Tenant.ID.String() != ev.TenantID {
		n.log.Debug("tether: tenant.updated event without the tenant, nothing to apply", slog.String("tenant", ev.TenantID))

		return
	}

	b.applyTenant(ev.Tenant, true)
}

// decodeSession decodes a session event and reports whether it is for this
// node and names a session the cell can carry.
func (n *clusterNode) decodeSession(kind string, data []byte) (sessionEvent, string, bool) {
	var ev sessionEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		n.log.Warn("tether: undecodable control event", slog.String("kind", kind), slog.Any("error", err))

		return ev, "", false
	}

	if ev.Node != n.Config.NodeID {
		return ev, "", false
	}

	nsID, err := sessionID(ev.Tenant, ev.ClientID)
	if err == nil {
		err = checkClientID(ev.ClientID)
	}

	if err != nil {
		n.log.Warn("tether: control event for a session the cell cannot carry", slog.String("kind", kind), slog.Any("error", err))

		return ev, "", false
	}

	return ev, nsID, true
}

// onSessionKick is a session.kick event: the node it names disconnects the
// client of the session, if the session still runs here at the event's
// epoch. Every other node ignores it.
func (b *Broker) onSessionKick(data []byte) {
	n := b.cluster

	ev, nsID, ok := n.decodeSession(cluster.KindSessionKick, data)
	if !ok {
		return
	}

	n.kickEvents.Add(1)

	srv := b.running()
	if srv == nil {
		return
	}

	s := n.sessionByID(nsID)
	if s == nil || (ev.Epoch != 0 && s.epoch.Load() != ev.Epoch) {
		n.log.Debug("tether: kick for a session no longer here", slog.String("client", nsID))

		return
	}

	cl := n.client(s)
	if cl == nil || cl.Closed() {
		return
	}

	b.log.Info("tether: kicking client for another node", slog.String("client", nsID), slog.String("tenant", ev.Tenant))

	go disconnect(srv, b.log, cl, packets.ErrAdministrativeAction)
}

// onSessionDelete is a session.delete event: the node it names deletes the
// session as DeleteSession would for one it serves, in the background. A
// session of another epoch here, or owned by another node by now, is left
// alone. Every other node ignores the event.
func (b *Broker) onSessionDelete(data []byte) {
	n := b.cluster

	ev, nsID, ok := n.decodeSession(cluster.KindSessionDelete, data)
	if !ok {
		return
	}

	n.deleteEvents.Add(1)

	srv := b.running()
	if srv == nil {
		return
	}

	n.goLive(func() {
		err := b.deleteClusterSession(srv, ev.Tenant, ev.ClientID, nsID, ev.Epoch, true)

		switch {
		case err == nil:
			b.log.Info("tether: session deleted for another node", slog.String("client", nsID), slog.String("tenant", ev.Tenant))
		case errors.Is(err, tether.ErrNotFound):
		default:
			b.log.Warn("tether: could not delete a session for another node", slog.String("client", nsID), slog.Any("error", err))
		}
	})
}

// kickRemote is Kick for a client ID with no open connection here: when the
// cell says another node serves it, that node is asked to kick it.
func (b *Broker) kickRemote(tenantID, clientID string) error {
	n := b.cluster

	if err := checkClientID(clientID); err != nil {
		return fmt.Errorf("tether/broker: %w", err)
	}

	ctx, cancel := context.WithTimeout(n.ctx, sessionWriteWait)
	rec, _, err := n.Sessions.Get(ctx, tenantID, clientID)

	cancel()

	switch {
	case errors.Is(err, tether.ErrNotFound):
		return notConnected(clientID)
	case err != nil:
		return fmt.Errorf("tether/broker: kick %q: %w", clientID, err)
	case rec.Owner == "" || rec.Owner == n.Config.NodeID || !rec.DisconnectedAt.IsZero():
		// Offline, a session this node has no connection for, or a dead
		// node's session a leader holds for its will: nothing to kick.
		return notConnected(clientID)
	}

	ev := sessionEvent{Tenant: tenantID, ClientID: clientID, Node: rec.Owner, Epoch: rec.Epoch}
	if err := n.publishSession(cluster.KindSessionKick, ev); err != nil {
		return fmt.Errorf("tether/broker: kick %q on node %q: %w", clientID, rec.Owner, err)
	}

	b.log.Info("tether: asked another node to kick a client", slog.String("client", clientID), slog.String("tenant", tenantID),
		slog.String("node", rec.Owner))

	return b.awaitRemote("kick", tenantID, clientID, rec.Owner, rec.Epoch)
}

// deleteRemote asks the node that owns a session to delete it, and waits for
// the outcome.
func (b *Broker) deleteRemote(tenantID, clientID, owner string, epoch uint64) error {
	n := b.cluster

	ev := sessionEvent{Tenant: tenantID, ClientID: clientID, Node: owner, Epoch: epoch}
	if err := n.publishSession(cluster.KindSessionDelete, ev); err != nil {
		return fmt.Errorf("tether/broker: delete session %q on node %q: %w", clientID, owner, err)
	}

	b.log.Info("tether: asked another node to delete a session", slog.String("client", clientID), slog.String("tenant", tenantID),
		slog.String("node", owner))

	return b.awaitRemote("delete", tenantID, clientID, owner, epoch)
}

// awaitRemote reads the session's record until owner has let it go at epoch,
// for at most remoteAdminWait.
//
// For a kick, any record that is not owner's at epoch any more, or that is
// marked disconnected (the owner keeps a session whose delayed will waits),
// means the connection is gone: nil. For a delete: no record is nil; a record of
// another epoch is a client that connected again (tether.ErrConflict); an
// offline record of the same epoch (the owner let it go without deleting it)
// is deleted here at the revision read, as for any offline session, and read
// again if that delete loses a race. When the wait runs out it is
// tether.ErrConflict naming the node.
func (b *Broker) awaitRemote(op, tenantID, clientID, owner string, epoch uint64) error {
	n := b.cluster
	deadline := time.Now().Add(remoteAdminWait)

	for {
		ctx, cancel := context.WithTimeout(n.ctx, sessionWriteWait)
		rec, rev, err := n.Sessions.Get(ctx, tenantID, clientID)

		cancel()

		switch {
		case errors.Is(err, tether.ErrNotFound):
			return nil
		case err != nil:
			return fmt.Errorf("tether/broker: %s %q: %w", op, clientID, err)
		case op == "kick" && rec.Epoch == epoch && !rec.DisconnectedAt.IsZero():
			// The connection is gone; the owner keeps the record while
			// a delayed will waits out its delay (finish).
			return nil
		case rec.Owner == owner && rec.Epoch == epoch:
			// Not yet.
		case op == "kick":
			return nil
		case rec.Epoch != epoch:
			return deleteConflict(clientID, "the client connected again")
		case rec.Owner == "":
			// The owner may be deleting the same offline record right now:
			// a lost race means read again, and a gone record is done.
			err := b.deleteOffline(tenantID, clientID, rev)
			if !errors.Is(err, cluster.ErrLostCAS) {
				return err
			}
		default:
			// Still this epoch, held by another node (a dead node's will).
			return deleteConflict(clientID, fmt.Sprintf("owned by node %q", rec.Owner))
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("tether/broker: %s %q: %w: node %q did not act within %s", op, clientID, tether.ErrConflict,
				owner, remoteAdminWait)
		}

		select {
		case <-n.ctx.Done():
			return fmt.Errorf("tether/broker: %s %q: %w: broker stopping", op, clientID, tether.ErrConflict)
		case <-time.After(remoteAdminPoll):
		}
	}
}

// deleteOffline deletes an offline session's record at rev. A lost race is
// cluster.ErrLostCAS, wrapped.
func (b *Broker) deleteOffline(tenantID, clientID string, rev uint64) error {
	n := b.cluster

	ctx, cancel := context.WithTimeout(n.ctx, sessionWriteWait)
	defer cancel()

	if err := n.Sessions.Delete(ctx, tenantID, clientID, rev); err != nil {
		return fmt.Errorf("tether/broker: delete session %q: %w", clientID, err)
	}

	return nil
}
