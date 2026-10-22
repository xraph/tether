package broker

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/limits"
	"github.com/xraph/tether/tenant"
)

// clientState is what the broker knows about one live connection. It is
// created in OnConnectAuthenticate and dropped in OnDisconnect.
//
// The fields that change while the connection is open (the identity, the
// tenant snapshot and the grants) are atomic pointers to values that are
// never modified, so the hot path reads them without a lock. reloadMu
// serialises the writers that load grants (a reload, or a CONNECT that has to
// authorize again), so an older load can never overwrite a newer one.
type clientState struct {
	cl       *mqtt.Client
	tenantID string
	rawID    string
	username string

	ident  atomic.Pointer[auth.Identity]
	tenant atomic.Pointer[tenant.Tenant]
	grants atomic.Pointer[grants]

	reloadMu sync.Mutex

	// expiry disconnects the client when its credentials expire. Nil when
	// they do not, or once stopped.
	expiry atomic.Pointer[time.Timer]

	// retryStop, under retryMu, is closed to end this connection's pending
	// reload retry (Broker.retryReload). Nil when none is pending.
	retryMu   sync.Mutex
	retryStop chan struct{}

	limiter  *limits.Limiter
	counters *tenantCounters
	release  sync.Once

	// connectedAt is when the connection authenticated. sessionExpiry is
	// how long, in seconds, mochi keeps the session after the connection
	// ends, as the CONNECT asked for it and capped by the broker. Both are
	// set before the state is registered and never change.
	connectedAt   time.Time
	sessionExpiry uint32

	// gone is closed once the state is dropped from the registry, which
	// for a connection that ends normally happens after every mochi hook,
	// storage included, has run its OnDisconnect (lastHook).
	gone     chan struct{}
	goneOnce sync.Once
}

// grants is one compiled set of topic grants and the roles it came from.
type grants struct {
	acl     *acl.Set
	roleIDs []string
}

// newClientState returns the state of a connection authorized as ident in
// tn with the compiled grants g.
func newClientState(cl *mqtt.Client, rawID, username string, ident *auth.Identity, tn *tenant.Tenant, g *grants,
	limiter *limits.Limiter, counters *tenantCounters,
) *clientState {
	st := &clientState{
		cl:       cl,
		tenantID: tn.ID.String(),
		rawID:    rawID,
		username: username,
		limiter:  limiter,
		counters: counters,

		connectedAt: time.Now(),
		gone:        make(chan struct{}),
	}
	st.ident.Store(ident)
	st.tenant.Store(tn)
	st.grants.Store(g)

	return st
}

// acl returns the connection's current grants.
func (st *clientState) acl() *acl.Set {
	return st.grants.Load().acl
}

// identity returns the connection's identity with its current tenant
// snapshot, as a copy the caller may hand to a PermLoader.
func (st *clientState) identity() *auth.Identity {
	ident := *st.ident.Load()
	ident.Tenant = st.tenant.Load()

	return &ident
}

// hasRole reports whether roleID is one of the roles the current grants
// came from.
func (st *clientState) hasRole(roleID string) bool {
	return slices.Contains(st.grants.Load().roleIDs, roleID)
}

// setExpiry installs the timer that ends the connection when its
// credentials expire, stopping any previous one.
func (st *clientState) setExpiry(t *time.Timer) {
	if old := st.expiry.Swap(t); old != nil {
		old.Stop()
	}
}

// stopExpiry stops the expiry timer, if any. Safe to call more than once.
func (st *clientState) stopExpiry() {
	st.setExpiry(nil)
}

// stopRetry ends a pending reload retry, if any. Safe to call more than once.
func (st *clientState) stopRetry() {
	st.retryMu.Lock()
	defer st.retryMu.Unlock()

	if st.retryStop != nil {
		close(st.retryStop)
		st.retryStop = nil
	}
}

// clearRetry forgets stop as the pending retry, if it still is: the retry
// loop that owns it has ended.
func (st *clientState) clearRetry(stop chan struct{}) {
	st.retryMu.Lock()
	defer st.retryMu.Unlock()

	if st.retryStop == stop {
		st.retryStop = nil
	}
}

// stopTimers ends the expiry timer and any pending reload retry, for a
// connection whose state is being dropped.
func (st *clientState) stopTimers() {
	st.stopExpiry()
	st.stopRetry()
}

// dropped stops the state's timers and closes gone. Every path that drops
// a state from the registry calls it. Safe to call more than once.
func (st *clientState) dropped() {
	st.stopTimers()
	st.goneOnce.Do(func() { close(st.gone) })
}

// releaseSlot gives the tenant's connection slot back. It is safe to call more
// than once and from more than one goroutine.
func (st *clientState) releaseSlot() {
	st.release.Do(func() { st.limiter.ReleaseConnection(st.tenantID) })
}

// transferSlot hands this connection's slot to a connection taking over its
// client ID. It reports false when the slot was already released; after a
// transfer, releaseSlot does nothing.
func (st *clientState) transferSlot() bool {
	transferred := false
	st.release.Do(func() { transferred = true })

	return transferred
}

// registry indexes connection state.
//
// byClient is the authority: every hook that gets a *mqtt.Client looks its
// state up by pointer, because a namespaced client ID is shared by the old and
// the new connection during a session takeover. byID points at the state of
// the connection that currently owns a namespaced client ID.
//
// refusals holds the CONNACK reason a refused connection should get, from
// OnConnectAuthenticate until OnPacketEncode rewrites mochi's default CONNACK.
type registry struct {
	mu       sync.RWMutex
	byClient map[*mqtt.Client]*clientState
	byID     map[string]*clientState
	refusals map[*mqtt.Client]packets.Code
}

func newRegistry() *registry {
	return &registry{
		byClient: make(map[*mqtt.Client]*clientState),
		byID:     make(map[string]*clientState),
		refusals: make(map[*mqtt.Client]packets.Code),
	}
}

func (r *registry) add(st *clientState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byClient[st.cl] = st
}

func (r *registry) get(cl *mqtt.Client) *clientState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.byClient[cl]
}

// bind records st as the owner of its client's (namespaced) ID. A previous
// owner whose connection is already closed but was never disconnected through
// OnDisconnect (mochi skips the hook when writing the CONNACK fails) is
// dropped and its slot released here.
//
// A previous owner is being taken over (or is long gone), so its expiry
// timer is stopped: it must never fire on the session's new connection.
func (r *registry) bind(st *clientState) {
	r.mu.Lock()

	prev := r.byID[st.cl.ID]
	r.byID[st.cl.ID] = st

	stale := prev != nil && prev != st && prev.cl.Closed()
	if stale {
		delete(r.byClient, prev.cl)
	}

	r.mu.Unlock()

	if prev != nil && prev != st {
		prev.stopTimers()
	}

	if stale {
		prev.dropped()
		prev.releaseSlot()
	}
}

// match returns the state of every connection for which keep is true. keep
// runs under the registry's read lock, so it must not call back into the
// registry or block.
func (r *registry) match(keep func(*clientState) bool) []*clientState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var out []*clientState

	for _, st := range r.byClient {
		if keep(st) {
			out = append(out, st)
		}
	}

	return out
}

// remove drops the state of cl (stopping its timers and closing gone) and
// returns it, or nil when there is none (mochi also calls OnDisconnect for
// sessions it restores from storage).
func (r *registry) remove(cl *mqtt.Client) *clientState {
	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.byClient[cl]
	if st == nil {
		return nil
	}

	delete(r.byClient, cl)

	if r.byID[cl.ID] == st {
		delete(r.byID, cl.ID)
	}

	st.dropped()

	return st
}

// sweepClosed drops every state of tenantID whose connection is closed and
// releases its slot. It returns how many it dropped.
//
// A closed connection never uses its slot again. One that is still tearing
// down finds no state in OnDisconnect afterwards, which is fine: the slot is
// released once (releaseSlot is behind a sync.Once). A live connection is
// never closed, so its slot is never touched. The scan is linear in the
// broker's connections and runs only when a tenant is at its limit.
func (r *registry) sweepClosed(tenantID string) int {
	r.mu.Lock()

	var swept []*clientState

	for cl, st := range r.byClient {
		if st.tenantID != tenantID || !cl.Closed() {
			continue
		}

		delete(r.byClient, cl)

		if r.byID[cl.ID] == st {
			delete(r.byID, cl.ID)
		}

		swept = append(swept, st)
	}

	r.mu.Unlock()

	for _, st := range swept {
		st.dropped()
		st.releaseSlot()
	}

	return len(swept)
}

// owner returns the state of the connection that owns the namespaced client
// ID, or nil.
func (r *registry) owner(nsID string) *clientState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.byID[nsID]
}

// hasID reports whether a live connection owns the namespaced client ID.
func (r *registry) hasID(nsID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, ok := r.byID[nsID]

	return ok
}

func (r *registry) refuse(cl *mqtt.Client, code packets.Code) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.refusals[cl] = code
}

func (r *registry) takeRefusal(cl *mqtt.Client) (packets.Code, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	code, ok := r.refusals[cl]
	if ok {
		delete(r.refusals, cl)
	}

	return code, ok
}

// namespacedID is the client ID mochi stores for a tenant's client.
func namespacedID(tenantID, rawID string) string {
	return tenantID + "/" + rawID
}

// tenantOfID returns the tenant prefix of a namespaced client ID. It is false
// for anything a Tether connection could not have produced.
func tenantOfID(nsID string) (string, bool) {
	prefix, _, ok := strings.Cut(nsID, "/")
	if !ok {
		return "", false
	}

	if _, err := id.ParseTenantID(prefix); err != nil {
		return "", false
	}

	return prefix, true
}
