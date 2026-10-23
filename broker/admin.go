package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/subject"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// Admin reactions. The host calls these when a key, a role or a tenant
// changes (package events wires keysmith and warden to them), so live
// connections lose what they lost at once instead of at their next CONNECT.
//
// Each one first bumps Broker.changes, then scans the registry, so a CONNECT
// running at the same time either is in the scan or sees the bump and
// authorizes again (hook.OnConnectAuthenticate). The scan holds the registry
// lock only to copy the matching states; permission loads and disconnects
// run after it is released. Work runs in the caller's goroutine, which for
// keysmith and warden plugin hooks is the admin call that made the change,
// not an MQTT client's.

// DisconnectPrincipal disconnects every live connection authenticated as
// principalID with code, and returns how many it disconnected. A 5.0 client
// gets a DISCONNECT with code first. Principal IDs are matched exactly and
// must be unique across tenants, which keysmith key IDs (TypeIDs) are.
//
// Before Start and after Stop it does nothing and returns 0.
func (b *Broker) DisconnectPrincipal(principalID string, code packets.Code) int {
	if principalID == "" {
		return 0
	}

	b.changes.Add(1)

	srv := b.running()
	if srv == nil {
		return 0
	}

	n := 0

	for _, st := range b.state.match(func(st *clientState) bool { return st.ident.Load().PrincipalID == principalID }) {
		if st.cl.Closed() {
			continue
		}

		b.log.Info("tether: disconnecting principal", slog.String("client", st.cl.ID), slog.String("tenant", st.tenantID),
			slog.String("principal", principalID), slog.Int("code", int(code.Code)))
		disconnect(srv, b.log, st.cl, code)
		n++
	}

	return n
}

// ExpirePrincipal makes every live connection authenticated as principalID
// that connected before connectedBefore end at at, unless its credentials
// expire sooner already: the connection's Identity.ExpiresAt becomes the
// earlier of the two and its expiry timer is set again. When it fires the
// client is disconnected with 0xA0 (maximum connect time), as for any
// expired credentials. It returns how many connections it shortened.
//
// It is the rotation of a key with a grace window: connections made before
// the rotation used the previous secret, which stops working at the window's
// end. A connection made with that secret during the window already expires
// with it (keysmith reports the grace end when it validates). A CONNECT in
// flight when this runs authorizes again (Broker.changes), so it is not
// missed.
//
// Before Start and after Stop it does nothing and returns 0.
func (b *Broker) ExpirePrincipal(principalID string, connectedBefore, at time.Time) int {
	if principalID == "" {
		return 0
	}

	b.changes.Add(1)

	srv := b.running()
	if srv == nil {
		return 0
	}

	n := 0

	for _, st := range b.state.match(func(st *clientState) bool { return st.ident.Load().PrincipalID == principalID }) {
		if st.cl.Closed() || !st.connectedAt.Before(connectedBefore) {
			continue
		}

		if b.shorten(srv, st, at) {
			n++
		}
	}

	return n
}

// shorten sets st's credentials to expire at at, unless they expire sooner,
// and sets its expiry timer again. It holds reloadMu, like every writer of
// the identity.
func (b *Broker) shorten(srv *mqtt.Server, st *clientState, at time.Time) bool {
	st.reloadMu.Lock()
	defer st.reloadMu.Unlock()

	ident := *st.ident.Load()
	if !ident.ExpiresAt.IsZero() && !at.Before(ident.ExpiresAt) {
		return false
	}

	ident.ExpiresAt = at
	st.ident.Store(&ident)
	st.setExpiry(time.AfterFunc(time.Until(at), func() { b.expireConnection(srv, st) }))

	b.log.Info("tether: connection ends with its credentials' grace window", slog.String("client", st.cl.ID),
		slog.String("tenant", st.tenantID), slog.Time("at", at))

	return true
}

// expireConnection disconnects a connection whose credentials expired with
// 0xA0 (maximum connect time), unless it is already gone.
func (b *Broker) expireConnection(srv *mqtt.Server, st *clientState) {
	if b.state.get(st.cl) != st || st.cl.Closed() {
		return
	}

	b.log.Info("tether: credentials expired, disconnecting", slog.String("client", st.cl.ID), slog.String("tenant", st.tenantID))
	disconnect(srv, b.log, st.cl, packets.ErrMaxConnectTime)
}

// ReloadRole reloads the grants of every live connection whose current
// grants came from roleID, directly or through role inheritance (the role
// IDs its PermLoader returned). See ReloadPrincipal for what a reload does.
func (b *Broker) ReloadRole(ctx context.Context, roleID string) {
	if roleID == "" {
		return
	}

	b.reloadMatching(ctx, func(st *clientState) bool { return st.hasRole(roleID) })
}

// ReloadPrincipal reloads the grants of every live connection authenticated
// as principalID (matched exactly, like DisconnectPrincipal).
//
// A reload calls PermLoader.Load again with the connection's identity and
// current tenant snapshot, compiles the rules with the connection's client
// ID and username, and swaps the grants in: every later PUBLISH, SUBSCRIBE
// and delivery is checked against them. Then every subscription of the
// connection that the new grants no longer allow is removed, from mochi's
// topic index, the client's session and the storage hook, the same way an
// UNSUBSCRIBE would (no UNSUBACK is sent, the client asked for nothing).
// A will whose topic is no longer granted is withheld when it fires
// (hook.OnWill).
//
// When Load fails with an error that says the principal may no longer
// connect (wrapping auth.ErrBadCredentials, auth.ErrNotAuthorized,
// tether.ErrNotFound or tether.ErrInvalidInput), the connection is
// disconnected with 0x87 not authorized. Any other error is an outage: the
// connection keeps its previous grants and the error is logged.
func (b *Broker) ReloadPrincipal(ctx context.Context, principalID string) {
	if principalID == "" {
		return
	}

	b.reloadMatching(ctx, func(st *clientState) bool { return st.ident.Load().PrincipalID == principalID })
}

// UpdateTenant replaces the tenant snapshot of every live connection of
// t.ID, so they use t's limits from their next message on. If t is
// disabled, those connections are disconnected with 0x98 administrative
// action instead. The broker keeps its own copy of t.
//
// Call it after the tenant's change is stored and the authenticator's tenant
// cache is cleared (service.Tenants.Update does both), so a connection that
// is authorizing at the same time reads the new tenant when it authorizes
// again.
func (b *Broker) UpdateTenant(t *tenant.Tenant) {
	if t == nil || t.ID.IsNil() {
		return
	}

	snap := *t
	tenantID := snap.ID.String()

	b.changes.Add(1)

	srv := b.running()

	for _, st := range b.state.match(func(st *clientState) bool { return st.tenantID == tenantID }) {
		st.tenant.Store(&snap)

		if snap.Enabled || srv == nil || st.cl.Closed() {
			continue
		}

		b.log.Info("tether: tenant disabled, disconnecting", slog.String("client", st.cl.ID), slog.String("tenant", tenantID))
		disconnect(srv, b.log, st.cl, packets.ErrAdministrativeAction)
	}
}

// running returns the mochi server while the broker is started.
func (b *Broker) running() *mqtt.Server {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.server
}

// reloadMatching bumps changes and reloads every live connection keep
// selects, one after another.
func (b *Broker) reloadMatching(ctx context.Context, keep func(*clientState) bool) {
	b.changes.Add(1)

	srv := b.running()
	if srv == nil {
		return
	}

	for _, st := range b.state.match(keep) {
		b.reload(ctx, srv, st)
	}
}

// Reload retry defaults.
const (
	defaultRetryBase   = time.Second
	defaultRetryMax    = 30 * time.Second
	defaultRetryWindow = 2 * time.Minute
)

// reloadOutcome is what one reload attempt did.
type reloadOutcome int

const (
	reloadDone        reloadOutcome = iota // new grants are in
	reloadGone                             // the connection is closed, or was disconnected as refused
	reloadUnavailable                      // Load failed on an outage; the old grants stay
)

// reload loads and swaps in fresh grants for one connection and prunes its
// subscriptions to them. reloadMu is held across the load, so two reloads of
// one connection apply in the order they loaded.
//
// A load that fails on an outage starts a retry loop for the connection
// (retryReload) unless one is pending already; any other outcome ends a
// pending one, since the grants it would load are in now (or the connection
// is gone).
func (b *Broker) reload(ctx context.Context, srv *mqtt.Server, st *clientState) {
	st.reloadMu.Lock()
	defer st.reloadMu.Unlock()

	if b.reloadLocked(ctx, srv, st) == reloadUnavailable {
		b.startRetry(srv, st)

		return
	}

	st.stopRetry()
}

// reloadLocked is one reload attempt. The caller holds st.reloadMu.
func (b *Broker) reloadLocked(ctx context.Context, srv *mqtt.Server, st *clientState) reloadOutcome {
	if st.cl.Closed() {
		return reloadGone
	}

	log := b.log.With(slog.String("client", st.cl.ID), slog.String("tenant", st.tenantID))

	rules, roleIDs, err := b.deps.Perms.Load(ctx, st.identity())
	if err != nil {
		if principalRefused(err) {
			log.Info("tether: principal no longer authorized, disconnecting", slog.Any("error", err))
			disconnect(srv, b.log, st.cl, packets.ErrNotAuthorized)

			return reloadGone
		}

		log.Warn("tether: reloading permissions failed, keeping the previous grants for now", slog.Any("error", err))

		return reloadUnavailable
	}

	g := b.compile(rules, roleIDs, st.rawID, st.username, log)
	st.grants.Store(g)

	b.pruneSubscriptions(srv, st, g.acl)

	return reloadDone
}

// startRetry starts a reload retry loop for st unless one is pending or the
// broker is stopping.
func (b *Broker) startRetry(srv *mqtt.Server, st *clientState) {
	st.retryMu.Lock()
	defer st.retryMu.Unlock()

	if st.retryStop != nil {
		return
	}

	b.retryMu.Lock()
	defer b.retryMu.Unlock()

	if b.retryClosed {
		return
	}

	stop := make(chan struct{})
	st.retryStop = stop

	b.retries.Add(1)
	b.pendingRetries.Add(1)

	go b.retryReload(srv, st, stop)
}

// retryReload retries a reload that failed on an outage: after retryBase,
// then doubling up to retryMax, until it succeeds or retryWindow has passed
// since the first failure. If it is still failing then, the connection is
// disconnected with 0x88 server unavailable, so it authorizes again when it
// reconnects instead of keeping grants that may have been revoked.
//
// It ends early when stop is closed (a later reload succeeded, or the
// connection's state was dropped) or the broker stops.
func (b *Broker) retryReload(srv *mqtt.Server, st *clientState, stop chan struct{}) {
	defer b.retries.Done()
	defer b.pendingRetries.Add(-1)
	defer st.clearRetry(stop)

	// Load gets a context that ends when the broker stops.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-b.retryQuit:
			cancel()
		case <-ctx.Done():
		}
	}()

	deadline := time.Now().Add(b.retryWindow)
	delay := b.retryBase

	for {
		timer := time.NewTimer(min(delay, max(time.Until(deadline), 0)))

		select {
		case <-timer.C:
		case <-stop:
			timer.Stop()

			return
		case <-b.retryQuit:
			timer.Stop()

			return
		}

		if b.attemptRetry(ctx, srv, st, stop) != reloadUnavailable {
			return
		}

		if !time.Now().Before(deadline) {
			b.log.Warn("tether: permissions still unavailable, disconnecting so the client authorizes again",
				slog.String("client", st.cl.ID), slog.String("tenant", st.tenantID))
			disconnect(srv, b.log, st.cl, packets.ErrServerUnavailable)

			return
		}

		delay = min(delay*2, b.retryMax)
	}
}

// attemptRetry runs one retry under reloadMu, unless stop was closed while
// it waited for the lock.
func (b *Broker) attemptRetry(ctx context.Context, srv *mqtt.Server, st *clientState, stop chan struct{}) reloadOutcome {
	st.reloadMu.Lock()
	defer st.reloadMu.Unlock()

	select {
	case <-stop:
		return reloadGone
	default:
	}

	attemptCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	return b.reloadLocked(attemptCtx, srv, st)
}

// stopRetries ends every reload retry loop and waits for them. No new one
// starts afterwards.
func (b *Broker) stopRetries() {
	b.retryMu.Lock()
	if !b.retryClosed {
		b.retryClosed = true
		close(b.retryQuit)
	}
	b.retryMu.Unlock()

	b.retries.Wait()
}

// principalRefused reports whether a PermLoader error means the principal
// may no longer connect, as opposed to the grants being unavailable.
func principalRefused(err error) bool {
	return errors.Is(err, auth.ErrBadCredentials) ||
		errors.Is(err, auth.ErrNotAuthorized) ||
		errors.Is(err, tether.ErrNotFound) ||
		errors.Is(err, tether.ErrInvalidInput)
}

// compile compiles a connection's grants and logs the ones acl.Compile
// dropped.
func (b *Broker) compile(rules acl.Rules, roleIDs []string, clientID, username string, log *slog.Logger) *grants {
	set := acl.Compile(rules, acl.Vars{ClientID: clientID, Username: username})
	if dropped := set.Dropped(); len(dropped) > 0 {
		log.Warn("tether: grants dropped for this connection", slog.Any("grants", dropped))
	}

	return &grants{acl: set, roleIDs: slices.Clone(roleIDs)}
}

// pruneSession prunes a session a connection has just inherited to the
// connection's current grants: its subscriptions, and the PUBLISH packets
// queued for it while it was offline, which mochi would otherwise resend
// right after the CONNACK. An offline session receives anything in its
// tenant, so a grant revoked while the device was
// away is enforced here.
func (b *Broker) pruneSession(srv *mqtt.Server, st *clientState) {
	set := st.acl()
	b.pruneSubscriptions(srv, st, set)

	cl := st.cl

	for _, pk := range cl.State.Inflight.GetAll(false) {
		if pk.FixedHeader.Type != packets.Publish || b.canRead(st.tenantID, set, pk.TopicName) {
			continue
		}

		// What processPuback does for an acknowledged message, with the
		// storage hook's drop in place of completion.
		if cl.State.Inflight.Delete(pk.PacketID) {
			cl.State.Inflight.IncreaseSendQuota()
			atomic.AddInt64(&srv.Info.Inflight, -1)

			if b.storage != nil {
				b.storage.OnQosDropped(cl, pk)
			}
		}
	}
}

// pruneSubscriptions removes every subscription of st's client that set no
// longer allows, as processUnsubscribe would, without an UNSUBACK.
func (b *Broker) pruneSubscriptions(srv *mqtt.Server, st *clientState, set *acl.Set) {
	n := b.unsubscribe(srv, st.cl, func(filter string) bool { return !b.canRead(st.tenantID, set, filter) })
	if n > 0 {
		b.log.Info("tether: subscriptions removed, no longer granted", slog.String("client", st.cl.ID), slog.Int("count", n))
	}
}

// unsubscribe removes every subscription of cl that drop selects from
// mochi's topic index, the client's session and the storage hook, and
// returns how many it removed.
func (b *Broker) unsubscribe(srv *mqtt.Server, cl *mqtt.Client, drop func(filter string) bool) int {
	var removed []packets.Subscription

	for filter, sub := range cl.State.Subscriptions.GetAll() {
		if !drop(filter) {
			continue
		}

		if srv.Topics.Unsubscribe(filter, cl.ID) {
			atomic.AddInt64(&srv.Info.Subscriptions, -1)
		}

		cl.State.Subscriptions.Delete(filter)

		removed = append(removed, sub)
	}

	if len(removed) > 0 && b.storage != nil {
		b.storage.OnUnsubscribed(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe}, Filters: removed})
	}

	if len(removed) > 0 && b.cluster != nil {
		b.releaseRoutes(cl, removed)
	}

	return len(removed)
}

// releaseRoutes drops the Router references of subscriptions unsubscribe
// removed from cl's session.
func (b *Broker) releaseRoutes(cl *mqtt.Client, removed []packets.Subscription) {
	tenantID, ok := tenantOfID(cl.ID)
	if !ok {
		return
	}

	rels := make([]string, 0, len(removed))

	for _, sub := range removed {
		if rel, ok := routedFilter(tenantID, sub.Filter); ok {
			rels = append(rels, rel)
		}
	}

	b.cluster.release(cl.ID, tenantID, rels)
}

// Admin operations. The host's admin API calls these for one tenant, after
// it has checked that the caller may (the broker checks no permissions
// here). Each takes a Tether tenant ID and client IDs and topics as the
// tenant's devices see them, namespaces them itself, and never lists or
// touches another tenant's clients, sessions or retained messages.
//
// A tenant ID that is not a canonical tenant TypeID, an empty client ID or a
// topic that fails topic.Valid is tether.ErrInvalidInput; the two listings
// return nothing instead. None of them holds the registry lock while it
// calls into mochi.

// ClientInfo describes one of a tenant's sessions on this node.
type ClientInfo struct {
	// ClientID is the client ID the client connected with.
	ClientID string `json:"client_id"`

	// Username is the CONNECT username (a keysmith key ID for devices).
	Username string `json:"username,omitempty"`

	// DeviceID is the Tether device the connection authenticated as. Empty
	// for an offline session.
	DeviceID string `json:"device_id,omitempty"`

	// Remote is the client's network address as mochi recorded it. Empty
	// for a session restored from storage.
	Remote string `json:"remote,omitempty"`

	// Connected is true for an open connection and false for the kept
	// session of a persistent client that is disconnected.
	Connected bool `json:"connected"`

	// ProtocolVersion is 4 for MQTT 3.1.1 and 5 for MQTT 5.0.
	ProtocolVersion byte `json:"protocol_version"`

	// Subscriptions are the session's topic filters, sorted.
	Subscriptions []string `json:"subscriptions"`

	// ConnectedAt is when the open connection authenticated. Zero for an
	// offline session.
	ConnectedAt time.Time `json:"connected_at,omitzero"`

	// SessionExpiry is how many seconds the session is kept after its
	// connection ends; 0 means it ends with the connection.
	SessionExpiry uint32 `json:"session_expiry"`
}

// RetainedInfo describes one retained message.
type RetainedInfo struct {
	// Topic is the message's topic as the tenant's devices see it.
	Topic string `json:"topic"`

	// Size is the payload's length in bytes.
	Size int `json:"size"`

	// QoS is the QoS the message was published with.
	QoS byte `json:"qos"`
}

// Clients lists tenantID's sessions on this node, sorted by client ID: open
// connections and the sessions mochi keeps for disconnected persistent
// clients. Mochi's inline client is never listed. Username, DeviceID and
// ConnectedAt of an open connection come from its authentication; for an
// offline session only what mochi kept is known. Before Start, after Stop
// and for a malformed tenant ID it returns nil.
func (b *Broker) Clients(tenantID string) []ClientInfo {
	if !validTenant(tenantID) {
		return nil
	}

	srv := b.running()
	if srv == nil {
		return nil
	}

	states := make(map[*mqtt.Client]*clientState)
	for _, st := range b.state.match(func(st *clientState) bool { return st.tenantID == tenantID }) {
		states[st.cl] = st
	}

	maxExpiry := b.cfg.sessionExpirySeconds()
	prefix := tenantID + "/"

	var out []ClientInfo

	for nsID, cl := range srv.Clients.GetAll() {
		if cl.Net.Inline {
			continue
		}

		raw, ok := strings.CutPrefix(nsID, prefix)
		if !ok || raw == "" {
			continue
		}

		out = append(out, clientInfo(tenantID, raw, cl, states[cl], maxExpiry))
	}

	slices.SortFunc(out, func(x, y ClientInfo) int { return strings.Compare(x.ClientID, y.ClientID) })

	return out
}

// clientInfo describes cl, whose state is st (nil once the connection has
// ended). Mochi changes a client's properties while its connection is open,
// so they are read only from a closed client.
func clientInfo(tenantID, raw string, cl *mqtt.Client, st *clientState, maxExpiry uint32) ClientInfo {
	info := ClientInfo{
		ClientID:        raw,
		Remote:          cl.Net.Remote,
		Connected:       !cl.Closed(),
		ProtocolVersion: cl.Properties.ProtocolVersion,
		Subscriptions:   filtersOf(tenantID, cl),
	}

	switch {
	case st != nil:
		info.Username = st.username
		info.DeviceID = st.ident.Load().DeviceID
		info.ConnectedAt = st.connectedAt
		info.SessionExpiry = st.sessionExpiry
	case !info.Connected:
		info.Username = string(cl.Properties.Username)
		info.SessionExpiry = sessionExpiry(cl, maxExpiry)
	}

	return info
}

// filtersOf returns cl's subscriptions without the tenant, sorted.
func filtersOf(tenantID string, cl *mqtt.Client) []string {
	subs := cl.State.Subscriptions.GetAll()
	out := make([]string, 0, len(subs))

	for filter := range subs {
		if rel, ok := topic.Strip(tenantID, filter); ok {
			out = append(out, rel)
		}
	}

	slices.Sort(out)

	return out
}

// Kick disconnects tenantID's open connection with clientID, with 0x98
// administrative action (a 5.0 client gets a DISCONNECT first), within about
// a second whatever the client does. A persistent session stays, offline;
// use DeleteSession to remove it. It returns tether.ErrNotFound when no
// connection with that client ID is open, an offline session included.
func (b *Broker) Kick(tenantID, clientID string) error {
	nsID, err := sessionID(tenantID, clientID)
	if err != nil {
		return err
	}

	srv := b.running()
	if srv == nil {
		return notConnected(clientID)
	}

	cl, ok := srv.Clients.Get(nsID)
	if !ok || cl.Closed() {
		return notConnected(clientID)
	}

	b.log.Info("tether: kicking client", slog.String("client", nsID), slog.String("tenant", tenantID))
	disconnect(srv, b.log, cl, packets.ErrAdministrativeAction)

	return nil
}

// sessionEndWait bounds how long DeleteSession waits for mochi to finish
// with a connection.
const sessionEndWait = 2 * time.Second

// DeleteSession removes tenantID's session with clientID, open or offline.
// An open connection is disconnected first (as Kick does) and DeleteSession
// waits until mochi and its storage hook are done with it. Then the
// session's subscriptions are removed (topic index, session, storage), its
// queued messages dropped (mochi's ClearInflights, which tells storage),
// and the session deleted from mochi's clients and from storage
// (OnClientExpired), so it does not come back after a restart. A client
// that connects again later starts a new session.
//
// While it runs, a CONNECT with that client ID is refused with 0x88 server
// unavailable (3.1.1: 0x03), so no connection can take the session over
// halfway through the delete. A connection for the client ID that
// registered before the delete started makes it fail with
// tether.ErrConflict instead, leaving the session alone; so does a
// connection that is still open after sessionEndWait (2 s). A closed connection
// mochi never finished (it skips OnDisconnect when writing the CONNACK
// fails) is deleted once the wait runs out.
//
// It returns tether.ErrNotFound when there is no such session. A delayed
// will (5.0 Will Delay Interval) already queued by mochi is still sent.
func (b *Broker) DeleteSession(tenantID, clientID string) error {
	nsID, err := sessionID(tenantID, clientID)
	if err != nil {
		return err
	}

	srv := b.running()
	if srv == nil {
		return noSession(clientID)
	}

	cl, ok := srv.Clients.Get(nsID)
	if !ok {
		return noSession(clientID)
	}

	st, ok := b.state.beginDelete(nsID, cl)
	if !ok {
		return deleteConflict(clientID, "another connection or delete for the client ID is in progress")
	}
	defer b.state.endDelete(nsID)

	log := b.log.With(slog.String("client", nsID), slog.String("tenant", tenantID))

	if !cl.Closed() {
		log.Info("tether: disconnecting client to delete its session")
		disconnect(srv, b.log, cl, packets.ErrAdministrativeAction)
	}

	// The storage hook writes the session's record in its OnDisconnect;
	// ended is closed after that (lastHook).
	if st != nil && !waitEnded(st, b.endWait) {
		if !cl.Closed() {
			return deleteConflict(clientID, "the connection is still open")
		}

		log.Warn("tether: connection never finished closing, deleting its session anyway")
	}

	if b.beforePurge != nil {
		b.beforePurge()
	}

	cur, ok := srv.Clients.Get(nsID)
	if !ok {
		// A session that ended with its connection: mochi removed it.
		return nil
	}

	if cur != cl {
		return deleteConflict(clientID, "the client connected again")
	}

	b.purgeSession(srv, cl)
	log.Info("tether: session deleted")

	return nil
}

// waitEnded waits up to d for mochi to finish with st's connection.
func waitEnded(st *clientState, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-st.ended:
		return true
	case <-timer.C:
		return false
	}
}

func deleteConflict(clientID, why string) error {
	return fmt.Errorf("tether/broker: delete session %q: %w: %s", clientID, tether.ErrConflict, why)
}

// purgeSession deletes the session of cl, whose connection has ended: its
// subscriptions, its queued messages, mochi's record and the storage
// hook's. Mochi's own expiry (clearExpiredClients) deletes the record only.
func (b *Broker) purgeSession(srv *mqtt.Server, cl *mqtt.Client) {
	b.unsubscribe(srv, cl, func(string) bool { return true })

	// Fires OnQosDropped on every hook, so storage forgets them too.
	cl.ClearInflights()

	if cur, ok := srv.Clients.Get(cl.ID); ok && cur == cl {
		srv.Clients.Delete(cl.ID)
	}

	if b.storage != nil {
		b.storage.OnClientExpired(cl)
	}

	if b.cluster != nil {
		b.cluster.dropQoS2(cl.ID)
	}
}

// Retained lists tenantID's retained messages whose topics match filter,
// sorted by topic. As in MQTT, a filter starting with a wildcard matches no
// topic starting with "$" (so "#" does not list the tenant's $SYS).
// Shared filters ("$share/...") match nothing. Before Start, after Stop and
// for a malformed tenant ID or filter it returns nil.
func (b *Broker) Retained(tenantID, filter string) []RetainedInfo {
	if !validTenant(tenantID) || topic.Valid(filter, true, b.cfg.MaxTopicLength) != nil || withoutShare(filter) != filter {
		return nil
	}

	srv := b.running()
	if srv == nil {
		return nil
	}

	var out []RetainedInfo

	for _, pk := range srv.Topics.Messages(topic.Namespace(tenantID, filter)) {
		name, ok := topic.Strip(tenantID, pk.TopicName)
		if !ok || !topic.Match(filter, name) {
			continue
		}

		out = append(out, RetainedInfo{Topic: name, Size: len(pk.Payload), QoS: pk.FixedHeader.Qos})
	}

	slices.SortFunc(out, func(x, y RetainedInfo) int { return strings.Compare(x.Topic, y.Topic) })

	return out
}

// DeleteRetained deletes tenantID's retained message on topicName by
// publishing an empty retained message there through mochi's inline client,
// which also deletes it from storage. As with any MQTT clearing, the
// topic's current subscribers receive that empty message. Topics under
// "$SYS" (in any casing) are refused like a client's publish would be.
// It returns tether.ErrNotFound when nothing is retained on the topic.
func (b *Broker) DeleteRetained(tenantID, topicName string) error {
	ns, err := b.namespaceName(tenantID, topicName)
	if err != nil {
		return err
	}

	srv := b.running()
	if srv == nil {
		return noRetained(topicName)
	}

	if _, ok := srv.Topics.Retained.Get(ns); !ok {
		return noRetained(topicName)
	}

	if err := srv.Publish(ns, nil, true, 0); err != nil {
		return fmt.Errorf("tether/broker: delete retained %q: %w", topicName, err)
	}

	return nil
}

// Publish publishes payload on tenantID's topicName through mochi's inline
// client, as the host rather than a device: no ACL check, no rate limit and
// no message counted in (the deliveries count as messages out). Callers
// check the caller's permissions and limits first.
//
// The topic must be a valid topic name. As for a client's PUBLISH, names
// under "$SYS" and "$share" are refused in any casing; any other "$" name is
// a topic of the tenant like the rest. qos is 0 or 1. Mochi treats an inline
// publish as received at QoS 0 and delivers it to each subscriber at the
// lower of qos and the subscription's QoS. With retain, the message is
// retained (an empty payload clears the retained message instead, as in
// MQTT). The payload is copied. Before Start or after Stop it fails with
// tether.ErrConflict.
func (b *Broker) Publish(ctx context.Context, tenantID, topicName string, payload []byte, qos byte, retain bool) error {
	return b.publish(ctx, tenantID, topicName, payload, qos, retain, false)
}

// DevicePublish is Publish for a message a device sent some other way than
// MQTT (the HTTP publish endpoint): it also counts as a message in for the
// tenant, in Stats, the tenant's $SYS and Metrics.MessageIn, as the device's
// MQTT PUBLISH would. The caller has checked the device's grants and spent
// its rate budget; a refused publish counts nothing.
func (b *Broker) DevicePublish(ctx context.Context, tenantID, topicName string, payload []byte, qos byte, retain bool) error {
	return b.publish(ctx, tenantID, topicName, payload, qos, retain, true)
}

func (b *Broker) publish(ctx context.Context, tenantID, topicName string, payload []byte, qos byte, retain, countIn bool) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tether/broker: publish: %w", err)
	}

	ns, err := b.namespaceName(tenantID, topicName)
	if err != nil {
		return err
	}

	if qos > 1 {
		return invalid(fmt.Sprintf("publish qos %d, want 0 or 1", qos))
	}

	if b.cluster != nil {
		// The cell carries topics of at most 1024 bytes (ruling R13).
		if err := subject.CheckTopic(topicName); err != nil {
			return fmt.Errorf("tether/broker: topic %q: %w", topicName, err)
		}
	}

	srv := b.running()
	if srv == nil {
		return fmt.Errorf("tether/broker: publish: %w: broker not running", tether.ErrConflict)
	}

	switch {
	case b.cluster != nil && qos > 0:
		// Through the log, like a device's QoS 1 publish (ruling R21):
		// every node delivers it from its reader, this one included.
		if err := b.appendHost(ctx, srv, tenantID, topicName, ns, slices.Clone(payload), qos, retain); err != nil {
			return fmt.Errorf("tether/broker: publish %q: %w", topicName, err)
		}
	default:
		if err := srv.Publish(ns, slices.Clone(payload), retain, qos); err != nil {
			return fmt.Errorf("tether/broker: publish %q: %w", topicName, err)
		}
	}

	if countIn {
		b.countersFor(tenantID).in.Add(1)
		b.deps.Metrics.MessageIn()
	}

	return nil
}

// namespaceName validates tenantID and the topic name and returns the name
// namespaced. Names under "$SYS", in any casing, are refused, as they are
// for a client's PUBLISH.
func (b *Broker) namespaceName(tenantID, name string) (string, error) {
	if !validTenant(tenantID) {
		return "", invalidTenant(tenantID)
	}

	if err := topic.Valid(name, false, b.cfg.MaxTopicLength); err != nil {
		return "", fmt.Errorf("tether/broker: topic %q: %w", name, err)
	}

	if len(name) >= len(sysPrefix) && strings.EqualFold(name[:len(sysPrefix)], sysPrefix) {
		return "", invalid(fmt.Sprintf("topic %q is under $SYS", name))
	}

	return topic.Namespace(tenantID, name), nil
}

// sessionID validates tenantID and clientID and returns the namespaced
// client ID mochi knows the session by.
func sessionID(tenantID, clientID string) (string, error) {
	if !validTenant(tenantID) {
		return "", invalidTenant(tenantID)
	}

	if clientID == "" || strings.IndexByte(clientID, 0) >= 0 {
		return "", invalid(fmt.Sprintf("client ID %q", clientID))
	}

	return namespacedID(tenantID, clientID), nil
}

// validTenant reports whether tenantID is a tenant TypeID in its canonical
// form, the only form the broker namespaces with. Anything else could reach
// past the tenant's prefix (a "/" in it, for one).
func validTenant(tenantID string) bool {
	tid, err := id.ParseTenantID(tenantID)

	return err == nil && tid.String() == tenantID
}

func invalidTenant(tenantID string) error {
	return invalid(fmt.Sprintf("tenant ID %q", tenantID))
}

func notConnected(clientID string) error {
	return fmt.Errorf("tether/broker: client %q: %w: not connected", clientID, tether.ErrNotFound)
}

func noSession(clientID string) error {
	return fmt.Errorf("tether/broker: session %q: %w", clientID, tether.ErrNotFound)
}

func noRetained(topicName string) error {
	return fmt.Errorf("tether/broker: retained message on %q: %w", topicName, tether.ErrNotFound)
}
