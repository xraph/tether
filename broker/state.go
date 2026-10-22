package broker

import (
	"strings"
	"sync"

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
type clientState struct {
	cl       *mqtt.Client
	tenantID string
	tenant   *tenant.Tenant
	ident    *auth.Identity
	acl      *acl.Set
	roleIDs  []string
	rawID    string

	limiter  *limits.Limiter
	counters *tenantCounters
	release  sync.Once
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
func (r *registry) bind(st *clientState) {
	r.mu.Lock()

	prev := r.byID[st.cl.ID]
	r.byID[st.cl.ID] = st

	stale := prev != nil && prev != st && prev.cl.Closed()
	if stale {
		delete(r.byClient, prev.cl)
	}

	r.mu.Unlock()

	if stale {
		prev.releaseSlot()
	}
}

// remove drops the state of cl and returns it, or nil when there is none
// (mochi also calls OnDisconnect for sessions it restores from storage).
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
