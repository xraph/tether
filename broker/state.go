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
	// slot is who holds the connection's tenant slot: slotHeld (the zero
	// value) until releaseSlot gives it back to the tenant or transferSlot
	// hands it to a takeover. A refused takeover can return a transferred
	// slot (returnSlot).
	slot atomic.Int32

	// metrics hears about the slot's release as a closed connection. Nil
	// for states the hook did not create (tests).
	metrics Metrics

	// connectedAt is when the connection authenticated. sessionExpiry is
	// how long, in seconds, mochi keeps the session after the connection
	// ends, as the CONNECT asked for it and capped by expiryCap, the lower
	// of the broker's cap and the tenant's. expiryShortened records that a
	// 5.0 CONNECT asked for more than the cap, so the CONNACK has to say so.
	// All are set before the state is registered and never change.
	connectedAt     time.Time
	sessionExpiry   uint32
	expiryCap       uint32
	expiryShortened bool

	// ended is closed when mochi has run every hook's OnDisconnect for the
	// connection, storage included (lastHook), or when the connection was
	// refused at CONNECT and never got that far. Nothing else closes it:
	// a sweep or a stale bind drops the state while the connection may
	// still be tearing down. It stays open for a connection mochi never
	// calls OnDisconnect for.
	ended     chan struct{}
	endedOnce sync.Once

	// pipe carries the connection's QoS 1 and 2 publishes to the log
	// (clusterlog.go). Cluster mode only; set before the state is
	// registered and never changed.
	pipe *pubPipe
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
		ended:       make(chan struct{}),
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

// closePipe abandons the connection's pending QoS 1 and 2 publishes and
// removes their uncommitted PUBREC reservations (cluster mode; nil-safe and
// idempotent). The registry calls it before a state stops being the owner of
// its namespaced ID (bind, remove, sweepClosed): once a CONNECT for the ID
// can no longer find this connection, mochi may clone its inflight map into
// the new connection (inheritClientSession), so no reservation of an
// uncommitted message may be left in it by then.
func (st *clientState) closePipe() {
	if st.pipe != nil {
		st.pipe.close()
	}
}

// markEnded closes ended. Safe to call more than once.
func (st *clientState) markEnded() {
	st.endedOnce.Do(func() { close(st.ended) })
}

// releaseSlot gives the tenant's connection slot back. It is safe to call more
// than once and from more than one goroutine.
func (st *clientState) releaseSlot() {
	if st.slot.CompareAndSwap(slotHeld, slotReleased) {
		st.limiter.ReleaseConnection(st.tenantID)

		if st.metrics != nil {
			st.metrics.ConnectionClosed()
		}

		return
	}

	// A transferred slot is the taker's now; this connection is gone, so
	// it can no longer be handed back.
	st.slot.CompareAndSwap(slotTransferred, slotReleased)
}

// transferSlot hands this connection's slot to a connection taking over its
// client ID. It reports false when the slot was already released or handed
// on; after a transfer, releaseSlot does nothing.
func (st *clientState) transferSlot() bool {
	return st.slot.CompareAndSwap(slotHeld, slotTransferred)
}

// returnSlot gives up the slot of a connection that was refused before it
// took any session over. A slot it was handed by from (transferSlot) goes
// back to from, which is still connected and will release it itself; if
// from has gone meanwhile, or the slot was a fresh one, it is released.
func (st *clientState) returnSlot(from *clientState) {
	if from != nil && from.slot.CompareAndSwap(slotTransferred, slotHeld) {
		// from holds it again. This state never held a slot of its own,
		// and never reports one.
		st.slot.Store(slotReleased)

		return
	}

	st.releaseSlot()
}

// Who holds a connection's tenant slot (clientState.slot).
const (
	slotHeld int32 = iota
	slotTransferred
	slotReleased
)

// registry indexes connection state.
//
// byClient is the authority: every hook that gets a *mqtt.Client looks its
// state up by pointer, because a namespaced client ID is shared by the old and
// the new connection during a session takeover. byID points at the state of
// the connection that currently owns a namespaced client ID.
//
// refusals holds the CONNACK reason a refused connection should get, from
// OnConnectAuthenticate until OnPacketEncode rewrites mochi's default CONNACK.
//
// closing holds the states a sweep or a stale bind dropped, whose
// connections may still be tearing down, so lastHook can still mark them
// ended; entries older than closingTTL are forgotten (mochi never called
// OnDisconnect for those). deleting holds the namespaced client IDs
// DeleteSession is deleting: no connection for one may register meanwhile.
type registry struct {
	mu       sync.RWMutex
	byClient map[*mqtt.Client]*clientState
	byID     map[string]*clientState
	refusals map[*mqtt.Client]packets.Code
	closing  map[*mqtt.Client]closingState
	deleting map[string]struct{}
}

// closingState is a dropped state waiting for its connection's OnDisconnect.
type closingState struct {
	st    *clientState
	since time.Time
}

// closingTTL is how long a dropped state waits for its OnDisconnect before
// the registry forgets it. A connection that is tearing down gets there in
// milliseconds; one mochi skipped never does.
const closingTTL = time.Minute

func newRegistry() *registry {
	return &registry{
		byClient: make(map[*mqtt.Client]*clientState),
		byID:     make(map[string]*clientState),
		refusals: make(map[*mqtt.Client]packets.Code),
		closing:  make(map[*mqtt.Client]closingState),
		deleting: make(map[string]struct{}),
	}
}

func (r *registry) add(st *clientState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.byClient[st.cl] = st
}

// addUnlessDeleting registers st unless DeleteSession is deleting the
// session of nsID, the namespaced ID st's connection will have. The check
// and the add are one step under the lock, so DeleteSession sees every
// connection that registered before it started (beginDelete) and refuses
// every later one.
func (r *registry) addUnlessDeleting(st *clientState, nsID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.deleting[nsID]; ok {
		return false
	}

	r.byClient[st.cl] = st

	return true
}

// beginDelete marks nsID as being deleted, for cl, the client mochi holds
// the session under. It returns cl's state (nil when cl has none: an
// offline session, or one restored from storage) and false when another
// delete of nsID is running or another connection for nsID has registered,
// which may be about to take the session over. On true the caller must
// call endDelete.
func (r *registry) beginDelete(nsID string, cl *mqtt.Client) (*clientState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.deleting[nsID]; ok {
		return nil, false
	}

	for other, st := range r.byClient {
		if other != cl && namespacedID(st.tenantID, st.rawID) == nsID {
			return nil, false
		}
	}

	r.deleting[nsID] = struct{}{}

	st := r.byClient[cl]
	if st == nil {
		st = r.closing[cl].st
	}

	return st, true
}

// endDelete lets connections for nsID register again.
func (r *registry) endDelete(nsID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.deleting, nsID)
}

// park keeps a dropped state for lastHook and forgets the ones that have
// waited longer than closingTTL. The caller holds the lock.
func (r *registry) park(st *clientState) {
	now := time.Now()

	for cl, c := range r.closing {
		if now.Sub(c.since) > closingTTL {
			delete(r.closing, cl)
		}
	}

	r.closing[st.cl] = closingState{st: st, since: now}
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

	if prev != nil && prev != st {
		// A takeover: mochi clones prev's inflight map right after the
		// hook that called bind.
		prev.closePipe()
	}

	r.byID[st.cl.ID] = st

	stale := prev != nil && prev != st && prev.cl.Closed()
	if stale {
		delete(r.byClient, prev.cl)
		r.park(prev)
	}

	r.mu.Unlock()

	if prev != nil && prev != st {
		prev.stopTimers()
	}

	if stale {
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

// remove drops the state of cl, stops its timers and marks it ended, for a
// connection mochi is done with. A state a sweep or a stale bind dropped
// earlier is found among the closing ones. It returns the state, or nil
// when there is none (mochi also calls OnDisconnect for sessions it
// restores from storage).
func (r *registry) remove(cl *mqtt.Client) *clientState {
	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.byClient[cl]
	if st == nil {
		c, ok := r.closing[cl]
		if !ok {
			return nil
		}

		delete(r.closing, cl)
		c.st.closePipe()
		c.st.markEnded()

		return c.st
	}

	st.closePipe()
	delete(r.byClient, cl)

	if r.byID[cl.ID] == st {
		delete(r.byID, cl.ID)
	}

	st.stopTimers()
	st.markEnded()

	return st
}

// sweepClosed drops every state of tenantID whose connection is closed and
// releases its slot. It returns how many it dropped.
//
// A closed connection never uses its slot again. One that is still tearing
// down finds its state among the closing ones in OnDisconnect afterwards,
// which marks it ended; the slot is released once (releaseSlot moves it out
// of the held state atomically). A live connection is
// never closed, so its slot is never touched. The scan is linear in the
// broker's connections and runs only when a tenant is at its limit.
func (r *registry) sweepClosed(tenantID string) int {
	r.mu.Lock()

	var swept []*clientState

	for cl, st := range r.byClient {
		if st.tenantID != tenantID || !cl.Closed() {
			continue
		}

		st.closePipe()
		delete(r.byClient, cl)

		if r.byID[cl.ID] == st {
			delete(r.byID, cl.ID)
		}

		r.park(st)
		swept = append(swept, st)
	}

	r.mu.Unlock()

	for _, st := range swept {
		st.stopTimers()
		st.releaseSlot()
	}

	return len(swept)
}

// liveTenants counts the open connections of each tenant.
func (r *registry) liveTenants() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make(map[string]int)

	for cl, st := range r.byClient {
		if !cl.Closed() {
			out[st.tenantID]++
		}
	}

	return out
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
