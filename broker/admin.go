package broker

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
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

// Reload retry defaults (controller ruling R14).
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

// retryReload retries a reload that failed on an outage (controller ruling
// R14): after retryBase, then doubling up to retryMax, until it succeeds or
// retryWindow has passed since the first failure. If it is still failing
// then, the connection is disconnected with 0x88 server unavailable, so it
// authorizes again when it reconnects instead of keeping grants that may
// have been revoked.
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
// tenant (controller ruling R8), so a grant revoked while the device was
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
	cl := st.cl

	var removed []packets.Subscription

	for filter, sub := range cl.State.Subscriptions.GetAll() {
		if b.canRead(st.tenantID, set, filter) {
			continue
		}

		if srv.Topics.Unsubscribe(filter, cl.ID) {
			atomic.AddInt64(&srv.Info.Subscriptions, -1)
		}

		cl.State.Subscriptions.Delete(filter)

		removed = append(removed, sub)
	}

	if len(removed) == 0 {
		return
	}

	b.log.Info("tether: subscriptions removed, no longer granted", slog.String("client", cl.ID), slog.Int("count", len(removed)))

	if b.storage != nil {
		b.storage.OnUnsubscribed(cl, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Unsubscribe}, Filters: removed})
	}
}
