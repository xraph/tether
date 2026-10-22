package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/mochi-mqtt/server/v2/listeners"
	mochipackets "github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
	"github.com/xraph/tether/topic"
)

// mutablePerms is a PermLoader keyed by username whose answers tests change
// while clients are connected. A username in gate has its next Load wait:
// the loader reports it on started and blocks until gate is closed.
type mutablePerms struct {
	mu      sync.Mutex
	rules   map[string]acl.Rules
	roles   map[string][]string
	errs    map[string]error
	failN   map[string]int
	loads   map[string]int
	gate    map[string]chan struct{}
	started chan string
}

func newMutablePerms(rules map[string]acl.Rules) *mutablePerms {
	return &mutablePerms{
		rules:   rules,
		roles:   map[string][]string{},
		errs:    map[string]error{},
		failN:   map[string]int{},
		loads:   map[string]int{},
		gate:    map[string]chan struct{}{},
		started: make(chan string, 16),
	}
}

func (m *mutablePerms) Load(_ context.Context, ident *auth.Identity) (acl.Rules, []string, error) {
	m.mu.Lock()
	gate := m.gate[ident.Username]
	delete(m.gate, ident.Username)
	m.mu.Unlock()

	if gate != nil {
		m.started <- ident.Username
		<-gate
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.loads[ident.Username]++

	if err := m.errs[ident.Username]; err != nil {
		if m.failN[ident.Username] > 0 {
			m.failN[ident.Username]--

			if m.failN[ident.Username] == 0 {
				delete(m.errs, ident.Username)
			}
		}

		return acl.Rules{}, nil, err
	}

	roles, ok := m.roles[ident.Username]
	if !ok {
		roles = []string{"role_" + ident.Username}
	}

	return m.rules[ident.Username], roles, nil
}

func (m *mutablePerms) set(user string, r acl.Rules) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.rules[user] = r
}

func (m *mutablePerms) fail(user string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errs[user] = err
}

// failTimes makes the next n Loads for user fail with err.
func (m *mutablePerms) failTimes(user string, n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errs[user] = err
	m.failN[user] = n
}

func (m *mutablePerms) setRoles(user string, roles ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.roles[user] = roles
}

// hold makes the next Load for user wait until the returned func is called.
func (m *mutablePerms) hold(user string) (release func()) {
	ch := make(chan struct{})

	m.mu.Lock()
	m.gate[user] = ch
	m.mu.Unlock()

	return func() { close(ch) }
}

func (m *mutablePerms) loadCount(user string) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.loads[user]
}

// switchAuth is an Authenticator over a map of identities that tests can
// change or turn off while clients are connected.
type switchAuth struct {
	mu     sync.Mutex
	ids    map[string]*auth.Identity
	refuse bool
}

func (s *switchAuth) Authenticate(_ context.Context, c auth.Credentials) (*auth.Identity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	id, ok := s.ids[c.Username]
	if !ok || s.refuse {
		return nil, fmt.Errorf("%w: test", auth.ErrBadCredentials)
	}

	cp := *id

	return &cp, nil
}

func (s *switchAuth) setRefuse(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.refuse = v
}

func (s *switchAuth) setExpiry(user string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ids[user].ExpiresAt = at
}

// startBrokerDeps starts a broker on a random loopback port with authn and
// perms.
func startBrokerDeps(t *testing.T, authn auth.Authenticator, perms PermLoader) *Broker {
	t.Helper()

	return startBrokerCfg(t, testConfig(), authn, perms)
}

func startBrokerCfg(t *testing.T, cfg Config, authn auth.Authenticator, perms PermLoader) *Broker {
	t.Helper()

	b, err := New(cfg, Deps{Auth: authn, Perms: perms, Logger: testLogger()})
	require.NoError(t, err)

	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	return b
}

// subscriptions returns the namespaced filters of a session in mochi.
func subscriptions(t *testing.T, b *Broker, nsID string) []string {
	t.Helper()

	cl, ok := b.server.Clients.Get(nsID)
	require.True(t, ok, "no session %s", nsID)

	subs := cl.State.Subscriptions.GetAll()

	out := make([]string, 0, len(subs))
	for f := range subs {
		out = append(out, f)
	}

	return out
}

func disconnectedWith(t *testing.T, c *client5) byte {
	t.Helper()

	select {
	case d := <-c.disconnects:
		return d.ReasonCode
	case <-time.After(waitTimeout):
		t.Fatal("not disconnected")

		return 0
	}
}

func requireConnected(t *testing.T, c *client5) {
	t.Helper()

	select {
	case d := <-c.disconnects:
		t.Fatalf("disconnected with reason %#x", d.ReasonCode)
	default:
	}
}

func TestReloadPrincipalPrunesSubscriptions(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{
		"dev": {Subscribe: []string{"cmd/#", "ok"}},
		"pub": {Publish: []string{"#"}},
	})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev"), "pub": ident(ta, "pub")}), perms)
	addr := b.Addr().String()
	ns := func(s string) string { return topic.Namespace(ta.ID.String(), s) }

	c, _, err := connect5(t, addr, "dev-1", "dev", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, c, "cmd/#", "ok"))

	p, _, err := connect5(t, addr, "pub-1", "pub", true)
	require.NoError(t, err)

	perms.set("dev", acl.Rules{Subscribe: []string{"ok"}})
	b.ReloadPrincipal(t.Context(), "akey_pub") // another principal: nothing changes for dev
	assert.ElementsMatch(t, []string{ns("cmd/#"), ns("ok")}, subscriptions(t, b, ns("dev-1")))

	b.ReloadPrincipal(t.Context(), "akey_dev")

	assert.Equal(t, []string{ns("ok")}, subscriptions(t, b, ns("dev-1")))
	assert.Empty(t, b.server.Topics.Subscribers(ns("cmd/x")).Subscriptions, "gone from mochi's topic index")
	assert.Equal(t, 1, b.Stats(ta.ID.String()).Subscriptions)

	require.Equal(t, byte(0), publish5(t, p, "cmd/x", "secret"))
	require.Equal(t, byte(0), publish5(t, p, "ok", "sentinel"))
	assert.Equal(t, message{Topic: "ok", Payload: "sentinel"}, recv(t, c.msgs))

	assert.Equal(t, []byte{0x87}, subscribe5(t, c, "cmd/#"))
	requireConnected(t, c)
}

func TestReloadRoleOnlyReloadsHolders(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{
		"a": {Subscribe: []string{"x"}},
		"b": {Subscribe: []string{"x"}},
	})
	perms.setRoles("a", "role_child", "role_parent") // role_parent is inherited
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"a": ident(ta, "a"), "b": ident(ta, "b")}), perms)
	addr := b.Addr().String()

	ca, _, err := connect5(t, addr, "a-1", "a", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, ca, "x"))

	cb, _, err := connect5(t, addr, "b-1", "b", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, cb, "x"))

	perms.set("a", acl.Rules{})
	perms.set("b", acl.Rules{})

	b.ReloadRole(t.Context(), "role_parent")

	assert.Equal(t, 2, perms.loadCount("a"), "a holds role_parent through inheritance")
	assert.Equal(t, 1, perms.loadCount("b"), "b holds only role_b")
	assert.Empty(t, subscriptions(t, b, topic.Namespace(ta.ID.String(), "a-1")))
	assert.Len(t, subscriptions(t, b, topic.Namespace(ta.ID.String(), "b-1")), 1)

	perms.setRoles("a", "role_other")
	b.ReloadRole(t.Context(), "role_other")
	assert.Equal(t, 2, perms.loadCount("a"), "the role list is the one from the last load")

	b.ReloadRole(t.Context(), "role_b")
	assert.Equal(t, 2, perms.loadCount("b"))
}

func TestReloadRefusedPrincipalIsDisconnected(t *testing.T) {
	for name, err := range map[string]error{
		"not authorized":  fmt.Errorf("perm: %w", auth.ErrNotAuthorized),
		"bad credentials": fmt.Errorf("perm: %w", auth.ErrBadCredentials),
	} {
		t.Run(name, func(t *testing.T) {
			ta := newTenant(tenant.Limits{})
			perms := newMutablePerms(map[string]acl.Rules{"dev": {Subscribe: []string{"x"}}})
			b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev")}), perms)

			c, _, cerr := connect5(t, b.Addr().String(), "dev-1", "dev", true)
			require.NoError(t, cerr)

			perms.fail("dev", err)
			b.ReloadPrincipal(t.Context(), "akey_dev")

			assert.Equal(t, byte(0x87), disconnectedWith(t, c))
		})
	}
}

func TestReloadOutageKeepsGrants(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{
		"sensor": {Subscribe: []string{"x"}},
		"pub":    {Publish: []string{"x"}},
	})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"sensor": ident(ta, "sensor"), "pub": ident(ta, "pub")}), perms)
	addr := b.Addr().String()

	c, _, err := connect5(t, addr, "sensor-1", "sensor", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, c, "x"))

	p, _, err := connect5(t, addr, "pub-1", "pub", true)
	require.NoError(t, err)

	perms.fail("sensor", errors.New("warden is down"))
	b.ReloadPrincipal(t.Context(), "akey_sensor")

	require.Equal(t, byte(0), publish5(t, p, "x", "still"))
	assert.Equal(t, message{Topic: "x", Payload: "still"}, recv(t, c.msgs))
	requireConnected(t, c)
}

func TestDisconnectPrincipalOnlyThatPrincipal(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	ids := map[string]*auth.Identity{"x": ident(ta, "x"), "y": ident(ta, "y"), "z": ident(tb, "z")}
	b := startBrokerDeps(t, auth.NewStatic(ids), newMutablePerms(map[string]acl.Rules{"y": {Publish: []string{"t"}}, "z": {Publish: []string{"t"}}}))
	addr := b.Addr().String()

	x1, _, err := connect5(t, addr, "same", "x", true)
	require.NoError(t, err)
	x2, _, err := connect5(t, addr, "other", "x", true)
	require.NoError(t, err)
	y, _, err := connect5(t, addr, "y-1", "y", true)
	require.NoError(t, err)
	z, _, err := connect5(t, addr, "same", "z", true) // same raw client ID, tenant B
	require.NoError(t, err)

	assert.Equal(t, 2, b.DisconnectPrincipal("akey_x", mochipackets.ErrAdministrativeAction))
	assert.Equal(t, byte(0x98), disconnectedWith(t, x1))
	assert.Equal(t, byte(0x98), disconnectedWith(t, x2))

	assert.Equal(t, byte(0), publish5(t, y, "t", "alive"))
	assert.Equal(t, byte(0), publish5(t, z, "t", "alive"))
	requireConnected(t, y)
	requireConnected(t, z)

	assert.Equal(t, 0, b.DisconnectPrincipal("akey_nobody", mochipackets.ErrAdministrativeAction))
	assert.Equal(t, 0, b.DisconnectPrincipal("", mochipackets.ErrAdministrativeAction))
}

func TestAdminBeforeStart(t *testing.T) {
	b, err := New(testConfig(), Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}})
	require.NoError(t, err)

	assert.Equal(t, 0, b.DisconnectPrincipal("akey_x", mochipackets.ErrAdministrativeAction))
	b.ReloadRole(t.Context(), "role_x")
	b.ReloadPrincipal(t.Context(), "akey_x")
	b.UpdateTenant(newTenant(tenant.Limits{}))
	b.UpdateTenant(nil)
}

// Controller ruling R8 follow-through. An offline session receives anything
// in its tenant, so a grant revoked while the device is away is enforced
// when the session is inherited: the subscription goes, and so do the
// messages queued for it.
func TestPrunesInheritedSessionOnReconnect(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{
		"dev": {Subscribe: []string{"cmd/#", "ok"}},
		"pub": {Publish: []string{"#"}, Subscribe: []string{"self"}},
	})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev"), "pub": ident(ta, "pub")}), perms)
	addr := b.Addr().String()
	ns := func(s string) string { return topic.Namespace(ta.ID.String(), s) }

	c, _, err := connect5(t, addr, "keeper", "dev", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, c, "cmd/#", "ok"))
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b, ns("keeper"))

	p, _, err := connect5(t, addr, "pub-1", "pub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, p, "self"))

	// Queued for the offline session (R8), then "self" proves the fan-out
	// of cmd/q is over.
	require.Equal(t, byte(0), publish5(t, p, "cmd/q", "queued"))
	require.Equal(t, byte(0), publish5(t, p, "self", "done"))
	require.Equal(t, message{Topic: "self", Payload: "done"}, recv(t, p.msgs))

	cl, ok := b.server.Clients.Get(ns("keeper"))
	require.True(t, ok)
	require.Equal(t, 1, cl.State.Inflight.Len(), "cmd/q is queued for the offline session")

	perms.set("dev", acl.Rules{Subscribe: []string{"ok"}})

	c2, ca, err := connect5(t, addr, "keeper", "dev", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)

	assert.Equal(t, []string{ns("ok")}, subscriptions(t, b, ns("keeper")))
	assert.Empty(t, b.server.Topics.Subscribers(ns("cmd/x")).Subscriptions)

	cl, ok = b.server.Clients.Get(ns("keeper"))
	require.True(t, ok)
	assert.Zero(t, cl.State.Inflight.Len(), "the queued cmd/q was dropped, not resent")

	require.Equal(t, byte(0), publish5(t, p, "cmd/x", "secret"))
	require.Equal(t, byte(0), publish5(t, p, "ok", "sentinel"))
	assert.Equal(t, message{Topic: "ok", Payload: "sentinel"}, recv(t, c2.msgs), "neither cmd/q nor cmd/x arrived")
}

// Pruning reaches mochi's storage hook: after a restart the session comes
// back without the pruned subscription, even with the grant restored. (The
// dropped message is not a storage check: mochi v2.7.9 already deletes a
// session's stored inflight messages on every takeover, through the old
// client's ClearInflights, so it is gone from badger either way.)
func TestPrunedSessionStaysPrunedAfterRestart(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	ids := auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev"), "pub": ident(ta, "pub")})
	perms := newMutablePerms(map[string]acl.Rules{
		"dev": {Subscribe: []string{"cmd/#", "ok"}},
		"pub": {Publish: []string{"#"}, Subscribe: []string{"self"}},
	})
	ns := func(s string) string { return topic.Namespace(ta.ID.String(), s) }

	cfg := testConfig()
	cfg.DataDir = t.TempDir()

	b1, err := New(cfg, Deps{Auth: ids, Perms: perms, Logger: testLogger()})
	require.NoError(t, err)
	require.NoError(t, b1.Start(t.Context()))

	addr := b1.Addr().String()

	c, _, err := connect5(t, addr, "keeper", "dev", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, c, "cmd/#", "ok"))
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b1, ns("keeper"))

	p, _, err := connect5(t, addr, "pub-1", "pub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, p, "self"))
	require.Equal(t, byte(0), publish5(t, p, "cmd/q", "queued"))
	require.Equal(t, byte(0), publish5(t, p, "self", "done"))
	require.Equal(t, message{Topic: "self", Payload: "done"}, recv(t, p.msgs))

	perms.set("dev", acl.Rules{Subscribe: []string{"ok"}})

	c, _, err = connect5(t, addr, "keeper", "dev", false)
	require.NoError(t, err)
	require.Equal(t, []string{ns("ok")}, subscriptions(t, b1, ns("keeper")))
	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b1, ns("keeper"))

	require.NoError(t, b1.Stop(t.Context()))

	perms.set("dev", acl.Rules{Subscribe: []string{"cmd/#", "ok"}})
	b2 := startBrokerCfg(t, cfg, ids, perms)
	addr = b2.Addr().String()

	c, ca, err := connect5(t, addr, "keeper", "dev", false)
	require.NoError(t, err)
	require.True(t, ca.SessionPresent)
	assert.Equal(t, []string{ns("ok")}, subscriptions(t, b2, ns("keeper")), "cmd/# was deleted from storage")

	p, _, err = connect5(t, addr, "pub-1", "pub", true)
	require.NoError(t, err)
	require.Equal(t, byte(0), publish5(t, p, "ok", "sentinel"))
	assert.Equal(t, message{Topic: "ok", Payload: "sentinel"}, recv(t, c.msgs), "cmd/q is not resent")
}

func TestUpdateTenantSwapsLimits(t *testing.T) {
	ta := newTenant(tenant.Limits{MessagesPerSecond: 0.01, Burst: 1})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"a": ident(ta, "a")}),
		newMutablePerms(map[string]acl.Rules{"a": {Publish: []string{"t"}}}))

	c, _, err := connect5(t, b.Addr().String(), "a-1", "a", true)
	require.NoError(t, err)

	require.Equal(t, byte(0), publish5(t, c, "t", "1"))
	require.Equal(t, byte(0x97), publish5(t, c, "t", "2"))

	updated := *ta
	updated.Limits = tenant.Limits{} // no rate limit
	b.UpdateTenant(&updated)
	updated.Limits = tenant.Limits{MessagesPerSecond: 0.01, Burst: 1} // the broker keeps its own copy

	assert.Equal(t, byte(0), publish5(t, c, "t", "3"))
	assert.Equal(t, byte(0), publish5(t, c, "t", "4"))
	requireConnected(t, c)
}

func TestUpdateTenantDisabledDisconnects(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	ids := map[string]*auth.Identity{"a": ident(ta, "a"), "b": ident(tb, "b")}
	b := startBrokerDeps(t, auth.NewStatic(ids), newMutablePerms(map[string]acl.Rules{"b": {Publish: []string{"t"}}}))
	addr := b.Addr().String()

	a1, _, err := connect5(t, addr, "a-1", "a", true)
	require.NoError(t, err)
	a2, _, err := connect5(t, addr, "a-2", "a", true)
	require.NoError(t, err)
	b1, _, err := connect5(t, addr, "a-1", "b", true)
	require.NoError(t, err)

	disabled := *ta
	disabled.Enabled = false
	b.UpdateTenant(&disabled)

	assert.Equal(t, byte(0x98), disconnectedWith(t, a1))
	assert.Equal(t, byte(0x98), disconnectedWith(t, a2))

	assert.Equal(t, byte(0), publish5(t, b1, "t", "alive"))
	requireConnected(t, b1)
}

func TestExpiredCredentialsRefused(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	id := ident(ta, "dev")
	id.ExpiresAt = time.Now().Add(-time.Second)
	b := startBrokerDeps(t, &switchAuth{ids: map[string]*auth.Identity{"dev": id}}, newMutablePerms(nil))

	_, ca, err := connect5(t, b.Addr().String(), "dev-1", "dev", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x86), ca.ReasonCode)
}

func TestExpiryTimerStopsOnDisconnectAndTakeover(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	authn := &switchAuth{ids: map[string]*auth.Identity{"dev": ident(ta, "dev")}}
	authn.setExpiry("dev", time.Now().Add(time.Hour))
	b := startBrokerDeps(t, authn, newMutablePerms(nil))
	addr := b.Addr().String()
	ns := func(s string) string { return topic.Namespace(ta.ID.String(), s) }

	stateOf := func(nsID string) *clientState {
		t.Helper()

		cl, ok := b.server.Clients.Get(nsID)
		require.True(t, ok)

		st := b.state.get(cl)
		require.NotNil(t, st)

		return st
	}

	// Disconnect.
	c1, _, err := connect5(t, addr, "dev-1", "dev", true)
	require.NoError(t, err)

	st1 := stateOf(ns("dev-1"))
	require.NotNil(t, st1.expiry.Load(), "a timer for credentials that expire")

	require.NoError(t, c1.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b, ns("dev-1"))
	assert.Nil(t, st1.expiry.Load(), "stopped on disconnect")

	// Takeover: the old connection's timer must not outlive it.
	old, _, err := connect5(t, addr, "dev-2", "dev", true)
	require.NoError(t, err)

	stOld := stateOf(ns("dev-2"))
	require.NotNil(t, stOld.expiry.Load())

	_, _, err = connect5(t, addr, "dev-2", "dev", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0x8E), disconnectedWith(t, old))
	assert.Nil(t, stOld.expiry.Load(), "stopped on takeover")

	stNew := stateOf(ns("dev-2"))
	assert.NotSame(t, stOld, stNew)
	assert.NotNil(t, stNew.expiry.Load(), "the new connection has its own timer")

	// Credentials with no expiry get no timer.
	authn.setExpiry("dev", time.Time{})

	_, _, err = connect5(t, addr, "dev-3", "dev", true)
	require.NoError(t, err)
	assert.Nil(t, stateOf(ns("dev-3")).expiry.Load())
}

func TestExpiryDisconnects(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	authn := &switchAuth{ids: map[string]*auth.Identity{"dev": ident(ta, "dev")}}
	authn.setExpiry("dev", time.Now().Add(300*time.Millisecond))
	b := startBrokerDeps(t, authn, newMutablePerms(nil))

	c, _, err := connect5(t, b.Addr().String(), "dev-1", "dev", true)
	require.NoError(t, err)

	assert.Equal(t, byte(0xA0), disconnectedWith(t, c))
}

// A will is published without a publish ACL check, so one whose topic lost
// its grant after CONNECT must not go out.
func TestWillWithheldAfterGrantRevoked(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{
		"willer": {Publish: []string{"w/#"}},
		"sub":    {Subscribe: []string{"w/#", "ok"}},
	})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"willer": ident(ta, "willer"), "sub": ident(ta, "sub")}), perms)
	addr := b.Addr().String()
	ns := func(s string) string { return topic.Namespace(ta.ID.String(), s) }

	s, _, err := connect5(t, addr, "sub-1", "sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, s, "w/#", "ok"))

	conn := rawConnect5Will(t, addr, "willer-1", "willer", "w/x")

	perms.set("willer", acl.Rules{})
	b.ReloadPrincipal(t.Context(), "akey_willer")

	require.NoError(t, conn.Close()) // no DISCONNECT: mochi sends the will
	waitStateDropped(t, b, ns("willer-1"))

	require.NoError(t, b.server.Publish(ns("ok"), []byte("sentinel"), false, 0))
	assert.Equal(t, message{Topic: "ok", Payload: "sentinel"}, recv(t, s.msgs), "the will never arrived")
}

// A role change announced while a CONNECT is loading its grants: the reload
// scans the registry before the new connection is in it, so the connection
// must load its grants again.
func TestConnectRacingReloadLoadsAgain(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{"dev": {Subscribe: []string{"cmd/#"}}})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev")}), perms)

	release := perms.hold("dev")

	type result struct {
		c   *client5
		err error
	}

	done := make(chan result, 1)

	go func() {
		c, _, err := connect5(t, b.Addr().String(), "dev-1", "dev", true)
		done <- result{c, err}
	}()

	select {
	case <-perms.started:
	case <-time.After(waitTimeout):
		t.Fatal("the CONNECT never loaded its grants")
	}

	perms.set("dev", acl.Rules{})
	b.ReloadRole(t.Context(), "role_dev")
	release()

	var r result
	select {
	case r = <-done:
	case <-time.After(waitTimeout):
		t.Fatal("connect never finished")
	}

	require.NoError(t, r.err)
	assert.Equal(t, 2, perms.loadCount("dev"))
	assert.Equal(t, []byte{0x87}, subscribe5(t, r.c, "cmd/#"), "the grants are the ones after the change")
}

// A key revoked while a CONNECT is in flight: DisconnectPrincipal finds no
// connection yet, so the CONNECT must authenticate again and be refused.
func TestConnectRacingRevocationIsRefused(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	authn := &switchAuth{ids: map[string]*auth.Identity{"dev": ident(ta, "dev")}}
	perms := newMutablePerms(nil)
	b := startBrokerDeps(t, authn, perms)

	release := perms.hold("dev")

	type result struct {
		ca  *paho.Connack
		err error
	}

	done := make(chan result, 1)

	go func() {
		_, ca, err := connect5(t, b.Addr().String(), "dev-1", "dev", true)
		done <- result{ca, err}
	}()

	select {
	case <-perms.started:
	case <-time.After(waitTimeout):
		t.Fatal("the CONNECT never loaded its grants")
	}

	authn.setRefuse(true) // the key is revoked and evicted
	assert.Equal(t, 0, b.DisconnectPrincipal("akey_dev", mochipackets.ErrAdministrativeAction))
	release()

	var r result
	select {
	case r = <-done:
	case <-time.After(waitTimeout):
		t.Fatal("connect never finished")
	}

	require.Error(t, r.err)
	require.NotNil(t, r.ca)
	assert.Equal(t, byte(0x86), r.ca.ReasonCode)
	assert.False(t, b.state.hasID(topic.Namespace(ta.ID.String(), "dev-1")))
	assert.Equal(t, Stats{}, b.Stats(ta.ID.String()))
}

// stallListener accepts real connections whose writes block, once stall is
// set, until the connection is closed: a client that has stopped reading
// and whose socket buffer is full.
type stallListener struct {
	net.Listener

	stall *atomic.Bool
}

func (l *stallListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &stallConn{Conn: c, stall: l.stall, closed: make(chan struct{})}, nil
}

type stallConn struct {
	net.Conn

	stall  *atomic.Bool
	closed chan struct{}
	once   sync.Once
}

func (c *stallConn) Write(p []byte) (int, error) {
	if c.stall.Load() {
		<-c.closed

		return 0, net.ErrClosed
	}

	return c.Conn.Write(p)
}

func (c *stallConn) Close() error {
	c.once.Do(func() { close(c.closed) })

	return c.Conn.Close()
}

// Fix round 1, item 1: a revoked client that stops reading must not hold up
// its own disconnect, the admin call behind it, or the principal's other
// connections.
func TestDisconnectPrincipalDoesNotWaitForAStalledClient(t *testing.T) {
	ta := newTenant(tenant.Limits{})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"x": ident(ta, "x")}), newMutablePerms(nil))

	raw, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	stall := &atomic.Bool{}
	require.NoError(t, b.server.AddListener(listeners.NewNet("stall", &stallListener{Listener: raw, stall: stall})))
	b.server.Listeners.Serve("stall", b.server.EstablishConnection)

	stuck := rawConnect5(t, raw.Addr().String(), "stuck", "x")
	other, _, err := connect5(t, b.Addr().String(), "other", "x", true)
	require.NoError(t, err)

	stall.Store(true)

	done := make(chan int, 1)
	start := time.Now()

	go func() { done <- b.DisconnectPrincipal("akey_x", mochipackets.ErrAdministrativeAction) }()

	select {
	case n := <-done:
		assert.Equal(t, 2, n)
		assert.Less(t, time.Since(start), disconnectWait+time.Second, "bounded by disconnectWait")
	case <-time.After(waitTimeout):
		t.Fatal("DisconnectPrincipal is stuck behind a client that does not read")
	}

	assert.Equal(t, byte(0x98), disconnectedWith(t, other))
	requireClosed(t, stuck)
	waitStateDropped(t, b, topic.Namespace(ta.ID.String(), "stuck"))
}

// fastRetry shortens the reload retry backoff (ruling R14) for a test.
func fastRetry(b *Broker, base, maxDelay, window time.Duration) {
	b.retryBase, b.retryMax, b.retryWindow = base, maxDelay, window
}

// waitPendingRetries waits until n reload retries are pending.
func waitPendingRetries(t *testing.T, b *Broker, n int64) {
	t.Helper()

	deadline := time.After(waitTimeout)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()

	for b.pendingRetries.Load() != n {
		select {
		case <-deadline:
			t.Fatalf("pending retries: got %d, want %d", b.pendingRetries.Load(), n)
		case <-tick.C:
		}
	}
}

// waitSubscriptions waits until the session nsID holds exactly want.
func waitSubscriptions(t *testing.T, b *Broker, nsID string, want ...string) {
	t.Helper()

	deadline := time.After(waitTimeout)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()

	slices.Sort(want)

	for {
		got := subscriptions(t, b, nsID)
		slices.Sort(got)

		if slices.Equal(got, want) {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("subscriptions: got %v, want %v", got, want)
		case <-tick.C:
		}
	}
}

// retryEnv is a broker with one connected client "dev" subscribed to cmd/#
// and ok.
func retryEnv(t *testing.T) (*Broker, *mutablePerms, *client5, func(string) string) {
	t.Helper()

	ta := newTenant(tenant.Limits{})
	perms := newMutablePerms(map[string]acl.Rules{"dev": {Subscribe: []string{"cmd/#", "ok"}}})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev")}), perms)

	c, _, err := connect5(t, b.Addr().String(), "dev-1", "dev", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 1}, subscribe5(t, c, "cmd/#", "ok"))

	return b, perms, c, func(s string) string { return topic.Namespace(ta.ID.String(), s) }
}

// Ruling R14: a reload that fails on an outage is retried, so the change it
// carried is not lost.
func TestReloadRetriesAfterOutage(t *testing.T) {
	b, perms, c, ns := retryEnv(t)
	fastRetry(b, 10*time.Millisecond, 40*time.Millisecond, time.Minute)

	perms.set("dev", acl.Rules{Subscribe: []string{"ok"}})
	perms.failTimes("dev", 2, errors.New("warden is down"))

	b.ReloadPrincipal(t.Context(), "akey_dev")
	assert.Len(t, subscriptions(t, b, ns("dev-1")), 2, "the first load failed, the old grants stay for now")

	waitSubscriptions(t, b, ns("dev-1"), ns("ok"))
	waitPendingRetries(t, b, 0)
	assert.Equal(t, 4, perms.loadCount("dev"), "CONNECT, the failed reload, one failed retry, one that worked")
	requireConnected(t, c)
}

func TestReloadRetryGivesUpWithServerUnavailable(t *testing.T) {
	b, perms, c, _ := retryEnv(t)
	fastRetry(b, 10*time.Millisecond, 20*time.Millisecond, 100*time.Millisecond)

	perms.fail("dev", errors.New("warden is down"))

	start := time.Now()
	b.ReloadPrincipal(t.Context(), "akey_dev")

	assert.Equal(t, byte(0x88), disconnectedWith(t, c), "server unavailable, so it authorizes again on reconnect")
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond, "not before the window ran out")
	waitPendingRetries(t, b, 0)
}

func TestReloadRetrySupersededBySuccessfulReload(t *testing.T) {
	b, perms, c, ns := retryEnv(t)
	fastRetry(b, time.Hour, time.Hour, 2*time.Hour)

	perms.failTimes("dev", 1, errors.New("warden is down"))
	b.ReloadPrincipal(t.Context(), "akey_dev")
	waitPendingRetries(t, b, 1)

	perms.set("dev", acl.Rules{Subscribe: []string{"ok"}})
	b.ReloadRole(t.Context(), "role_dev")

	waitPendingRetries(t, b, 0)
	assert.Equal(t, []string{ns("ok")}, subscriptions(t, b, ns("dev-1")))
	requireConnected(t, c)
}

func TestReloadRetryStopsWhenClientLeaves(t *testing.T) {
	b, perms, c, ns := retryEnv(t)
	fastRetry(b, time.Hour, time.Hour, 2*time.Hour)

	perms.fail("dev", errors.New("warden is down"))
	b.ReloadPrincipal(t.Context(), "akey_dev")
	waitPendingRetries(t, b, 1)

	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b, ns("dev-1"))
	waitPendingRetries(t, b, 0)
}

func TestStopDuringPendingReloadRetry(t *testing.T) {
	b, perms, _, _ := retryEnv(t)
	fastRetry(b, time.Hour, time.Hour, 2*time.Hour)

	perms.fail("dev", errors.New("warden is down"))
	b.ReloadPrincipal(t.Context(), "akey_dev")
	waitPendingRetries(t, b, 1)

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	start := time.Now()
	require.NoError(t, b.Stop(ctx))
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Equal(t, int64(0), b.pendingRetries.Load(), "Stop waited for the retry to end")
}

// Admin operations (Task 12).

// publishRetained5 publishes a retained message at QoS 1.
func publishRetained5(t *testing.T, c *client5, name, payload string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), waitTimeout)
	defer cancel()

	_, err := c.Publish(ctx, &paho.Publish{Topic: name, QoS: 1, Retain: true, Payload: []byte(payload)})
	require.NoError(t, err)
}

// clientIDs returns the client IDs Clients lists for tenantID.
func clientIDs(b *Broker, tenantID string) []string {
	infos := b.Clients(tenantID)

	out := make([]string, 0, len(infos))
	for _, c := range infos {
		out = append(out, c.ClientID)
	}

	return out
}

func TestClientsListsOnlyTenant(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	aDev := ident(ta, "a-dev")
	aDev.DeviceID = "tdev_a"
	b := startBroker(t,
		map[string]*auth.Identity{"a-dev": aDev, "a-away": ident(ta, "a-away"), "b-dev": ident(tb, "b-dev")},
		map[string]acl.Rules{
			"a-dev":  {Subscribe: []string{"cmd/#"}},
			"a-away": {Subscribe: []string{"away/+"}},
			"b-dev":  {Subscribe: []string{"b/#"}},
		})
	addr := b.Addr().String()

	before := time.Now()

	dev, _, err := connect5(t, addr, "dev", "a-dev", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, dev, "cmd/#"))

	away, _, err := connect5(t, addr, "away", "a-away", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, away, "away/+"))
	require.NoError(t, away.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b, topic.Namespace(ta.ID.String(), "away"))

	// Tenant B uses the same raw client ID.
	bDev, _, err := connect5(t, addr, "dev", "b-dev", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, bDev, "b/#"))

	got := b.Clients(ta.ID.String())
	require.Len(t, got, 2)

	off := got[0]
	assert.Equal(t, "away", off.ClientID)
	assert.False(t, off.Connected, "a kept session of a disconnected client")
	assert.Equal(t, "a-away", off.Username)
	assert.Equal(t, byte(5), off.ProtocolVersion)
	assert.Equal(t, []string{"away/+"}, off.Subscriptions)
	assert.Equal(t, uint32(3600), off.SessionExpiry)
	assert.True(t, off.ConnectedAt.IsZero())
	assert.Empty(t, off.DeviceID)

	live := got[1]
	assert.Equal(t, "dev", live.ClientID)
	assert.True(t, live.Connected)
	assert.Equal(t, "a-dev", live.Username)
	assert.Equal(t, "tdev_a", live.DeviceID)
	assert.NotEmpty(t, live.Remote)
	assert.Equal(t, byte(5), live.ProtocolVersion)
	assert.Equal(t, []string{"cmd/#"}, live.Subscriptions)
	assert.Equal(t, uint32(3600), live.SessionExpiry)
	assert.False(t, live.ConnectedAt.Before(before))
	assert.False(t, live.ConnectedAt.After(time.Now()))

	gotB := b.Clients(tb.ID.String())
	require.Len(t, gotB, 1)
	assert.Equal(t, "dev", gotB[0].ClientID)
	assert.Equal(t, "b-dev", gotB[0].Username)
	assert.True(t, gotB[0].Connected)
	assert.Equal(t, []string{"b/#"}, gotB[0].Subscriptions)
	assert.Zero(t, gotB[0].SessionExpiry, "a clean start keeps no session")

	// Never the inline client, never anything for a malformed tenant ID.
	for _, tid := range []string{"", "inline", "ttnt_nobody", ta.ID.String() + "/dev", "/"} {
		assert.Empty(t, b.Clients(tid), "tenant %q", tid)
	}
}

func TestKick(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a-dev": ident(ta, "a-dev"), "b-dev": ident(tb, "b-dev")},
		map[string]acl.Rules{"b-dev": {Subscribe: []string{"x"}}})
	addr := b.Addr().String()
	tenantA := ta.ID.String()

	a, _, err := connect5(t, addr, "dev", "a-dev", false)
	require.NoError(t, err)

	bDev, _, err := connect5(t, addr, "dev", "b-dev", false)
	require.NoError(t, err)

	require.NoError(t, b.Kick(tenantA, "dev"))
	assert.Equal(t, byte(0x98), disconnectedWith(t, a))
	waitStateDropped(t, b, topic.Namespace(tenantA, "dev"))

	// B's client with the same raw ID was not touched: it still answers.
	require.Equal(t, []byte{1}, subscribe5(t, bDev, "x"))
	requireConnected(t, bDev)

	// The kicked client's session is kept; kicking it again finds nobody.
	infos := b.Clients(tenantA)
	require.Len(t, infos, 1)
	assert.False(t, infos[0].Connected)
	require.ErrorIs(t, b.Kick(tenantA, "dev"), tether.ErrNotFound)
	require.ErrorIs(t, b.Kick(tenantA, "nobody"), tether.ErrNotFound)

	require.ErrorIs(t, b.Kick(tenantA, ""), tether.ErrInvalidInput)
	require.ErrorIs(t, b.Kick("ttnt_nobody", "dev"), tether.ErrInvalidInput)
	require.ErrorIs(t, b.Kick(tenantA+"/x", "dev"), tether.ErrInvalidInput)
}

func TestDeleteSessionSurvivesRestart(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	ids := map[string]*auth.Identity{
		"a-live": ident(ta, "a-live"), "a-away": ident(ta, "a-away"), "a-pub": ident(ta, "a-pub"), "b-live": ident(tb, "b-live"),
	}
	rules := map[string]acl.Rules{
		"a-live": {Subscribe: []string{"t"}},
		"a-away": {Subscribe: []string{"t"}},
		"a-pub":  {Publish: []string{"#"}, Subscribe: []string{"self"}},
		"b-live": {Subscribe: []string{"t"}},
	}
	tenantA, tenantB := ta.ID.String(), tb.ID.String()
	nsA := func(s string) string { return topic.Namespace(tenantA, s) }
	nsB := func(s string) string { return topic.Namespace(tenantB, s) }

	cfg := testConfig()
	cfg.DataDir = t.TempDir()

	b1, err := New(cfg, Deps{Auth: auth.NewStatic(ids), Perms: fakePerms{rules: rules}, Logger: testLogger()})
	require.NoError(t, err)
	require.NoError(t, b1.Start(t.Context()))

	addr := b1.Addr().String()

	live, _, err := connect5(t, addr, "live", "a-live", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, live, "t"))

	away, _, err := connect5(t, addr, "away", "a-away", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, away, "t"))
	require.NoError(t, away.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b1, nsA("away"))

	// Tenant B has a persistent session under the same raw client ID.
	bLive, _, err := connect5(t, addr, "live", "b-live", false)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, bLive, "t"))

	// Queue a message for the offline session.
	pub, _, err := connect5(t, addr, "pub", "a-pub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, pub, "self"))
	require.Equal(t, byte(0), publish5(t, pub, "t", "queued"))
	require.Equal(t, byte(0), publish5(t, pub, "self", "done"))
	require.Equal(t, message{Topic: "self", Payload: "done"}, recv(t, pub.msgs))
	require.Equal(t, message{Topic: "t", Payload: "queued"}, recv(t, live.msgs))

	cl, ok := b1.server.Clients.Get(nsA("away"))
	require.True(t, ok)
	require.Equal(t, 1, cl.State.Inflight.Len(), "queued for the offline session")

	// A connected session is disconnected first, an offline one just goes.
	require.NoError(t, b1.DeleteSession(tenantA, "live"))
	assert.Equal(t, byte(0x98), disconnectedWith(t, live))
	require.NoError(t, b1.DeleteSession(tenantA, "away"))

	for _, raw := range []string{"live", "away"} {
		_, ok := b1.server.Clients.Get(nsA(raw))
		assert.False(t, ok, "%s is gone from mochi", raw)
	}

	assert.Equal(t, []string{"pub"}, clientIDs(b1, tenantA))
	assert.Equal(t, 1, b1.Stats(tenantA).Sessions)

	subs := b1.server.Topics.Subscribers(nsA("t")).Subscriptions
	assert.Empty(t, subs, "tenant A's subscriptions on t are gone")
	assert.Equal(t, []string{nsB("t")}, subscriptions(t, b1, nsB("live")), "tenant B's session is untouched")

	require.ErrorIs(t, b1.DeleteSession(tenantA, "live"), tether.ErrNotFound)
	require.ErrorIs(t, b1.DeleteSession(tenantA, ""), tether.ErrInvalidInput)
	require.ErrorIs(t, b1.DeleteSession("ttnt_nobody", "live"), tether.ErrInvalidInput)

	// A clean session that is connected goes too.
	require.NoError(t, b1.DeleteSession(tenantA, "pub"))
	assert.Equal(t, byte(0x98), disconnectedWith(t, pub))
	assert.Empty(t, b1.Clients(tenantA))

	require.NoError(t, bLive.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	waitStateDropped(t, b1, nsB("live"))
	require.NoError(t, b1.Stop(t.Context()))

	b2 := startBrokerWith(t, cfg, ids, rules, nil)

	assert.Empty(t, b2.Clients(tenantA), "deleted sessions are not restored")
	assert.Equal(t, []string{"live"}, clientIDs(b2, tenantB))

	// Nothing of A's sessions is left in storage.
	stored, err := b2.storage.StoredClients()
	require.NoError(t, err)

	for _, c := range stored {
		assert.NotContains(t, []string{nsA("live"), nsA("away")}, c.ID)
	}

	storedSubs, err := b2.storage.StoredSubscriptions()
	require.NoError(t, err)

	for _, s := range storedSubs {
		assert.NotContains(t, []string{nsA("live"), nsA("away")}, s.Client)
	}

	storedMsgs, err := b2.storage.StoredInflightMessages()
	require.NoError(t, err)

	for _, m := range storedMsgs {
		assert.NotEqual(t, nsA("away"), m.Client, "the queued message is gone from storage")
	}

	addr = b2.Addr().String()

	for _, raw := range []string{"live", "away"} {
		_, ca, cerr := connect5(t, addr, raw, "a-"+raw, false)
		require.NoError(t, cerr)
		assert.False(t, ca.SessionPresent, "%s starts a new session", raw)
	}

	_, ca, err := connect5(t, addr, "live", "b-live", false)
	require.NoError(t, err)
	assert.True(t, ca.SessionPresent, "tenant B's session survived")
}

func TestRetainedListAndDelete(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	ids := map[string]*auth.Identity{"a-pub": ident(ta, "a-pub"), "a-sub": ident(ta, "a-sub"), "b-pub": ident(tb, "b-pub")}
	rules := map[string]acl.Rules{
		"a-pub": {Publish: []string{"#"}},
		"a-sub": {Subscribe: []string{"s/#"}},
		"b-pub": {Publish: []string{"#"}},
	}
	tenantA, tenantB := ta.ID.String(), tb.ID.String()

	cfg := testConfig()
	cfg.DataDir = t.TempDir()

	b1, err := New(cfg, Deps{Auth: auth.NewStatic(ids), Perms: fakePerms{rules: rules}, Logger: testLogger()})
	require.NoError(t, err)
	require.NoError(t, b1.Start(t.Context()))

	addr := b1.Addr().String()

	pa, _, err := connect5(t, addr, "pub", "a-pub", true)
	require.NoError(t, err)
	publishRetained5(t, pa, "s/1", "hello")
	publishRetained5(t, pa, "s/2", "hi!")

	pb, _, err := connect5(t, addr, "pub", "b-pub", true)
	require.NoError(t, err)
	publishRetained5(t, pb, "s/1", "bee")

	// The tenant's own $SYS, retained (the broker never retains its stats,
	// so this one is planted).
	require.NoError(t, b1.server.Publish(topic.Namespace(tenantA, "$SYS/x"), []byte("sys"), true, 0))

	both := []RetainedInfo{{Topic: "s/1", Size: 5, QoS: 1}, {Topic: "s/2", Size: 3, QoS: 1}}
	assert.Equal(t, both, b1.Retained(tenantA, "s/#"))
	assert.Equal(t, both, b1.Retained(tenantA, "#"), "# lists no $SYS topic")
	assert.Equal(t, both[:1], b1.Retained(tenantA, "s/1"))
	assert.Equal(t, []RetainedInfo{{Topic: "$SYS/x", Size: 3, QoS: 0}}, b1.Retained(tenantA, "$SYS/#"))
	assert.Equal(t, []RetainedInfo{{Topic: "s/1", Size: 3, QoS: 1}}, b1.Retained(tenantB, "#"))

	for _, f := range []string{"", "s/#/x", "s+", "$share/g/#"} {
		assert.Empty(t, b1.Retained(tenantA, f), "filter %q", f)
	}

	assert.Empty(t, b1.Retained("ttnt_nobody", "#"))
	assert.Empty(t, b1.Retained("", "#"))

	sub, _, err := connect5(t, addr, "sub", "a-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sub, "s/#"))
	// The retained replay.
	recv(t, sub.msgs)
	recv(t, sub.msgs)

	require.NoError(t, b1.DeleteRetained(tenantA, "s/1"))
	assert.Equal(t, message{Topic: "s/1", Payload: ""}, recv(t, sub.msgs), "clearing reaches subscribers as an empty message")

	assert.Equal(t, both[1:], b1.Retained(tenantA, "#"))
	assert.Equal(t, []RetainedInfo{{Topic: "s/1", Size: 3, QoS: 1}}, b1.Retained(tenantB, "#"), "tenant B's s/1 stays")

	require.ErrorIs(t, b1.DeleteRetained(tenantA, "s/1"), tether.ErrNotFound)

	for _, name := range []string{"", "s/+", "#", "$SYS/x", "$sys/x"} {
		require.ErrorIs(t, b1.DeleteRetained(tenantA, name), tether.ErrInvalidInput, "topic %q", name)
	}

	require.ErrorIs(t, b1.DeleteRetained("ttnt_nobody", "s/2"), tether.ErrInvalidInput)

	require.NoError(t, b1.Stop(t.Context()))

	b2 := startBrokerWith(t, cfg, ids, rules, nil)
	assert.Equal(t, both[1:], b2.Retained(tenantA, "s/#"), "the deletion reached storage")
	assert.Equal(t, []RetainedInfo{{Topic: "s/1", Size: 3, QoS: 1}}, b2.Retained(tenantB, "s/#"))
}

func TestInlinePublishDelivered(t *testing.T) {
	ta, tb := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"a-sub": ident(ta, "a-sub"), "b-sub": ident(tb, "b-sub")},
		map[string]acl.Rules{"a-sub": {Subscribe: []string{"t/#"}}, "b-sub": {Subscribe: []string{"t/#"}}})
	addr := b.Addr().String()
	tenantA, tenantB := ta.ID.String(), tb.ID.String()
	ctx := t.Context()

	sa, _, err := connect5(t, addr, "sub", "a-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sa, "t/#"))

	sb, _, err := connect5(t, addr, "sub", "b-sub", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, sb, "t/#"))

	require.NoError(t, b.Publish(ctx, tenantA, "t/1", []byte("hello"), 1, false))
	assert.Equal(t, message{Topic: "t/1", Payload: "hello"}, recv(t, sa.msgs))

	// Publish fans out before it returns, so B's first message is its own
	// sentinel: A's publish never reached B's subscriber on the same topic.
	require.NoError(t, b.Publish(ctx, tenantB, "t/sentinel", []byte("s"), 0, false))
	assert.Equal(t, message{Topic: "t/sentinel", Payload: "s"}, recv(t, sb.msgs))

	payload := []byte("kept")
	require.NoError(t, b.Publish(ctx, tenantA, "t/kept", payload, 1, true))
	copy(payload, "XXXX")
	assert.Equal(t, message{Topic: "t/kept", Payload: "kept"}, recv(t, sa.msgs))
	assert.Equal(t, []RetainedInfo{{Topic: "t/kept", Size: 4, QoS: 1}}, b.Retained(tenantA, "#"))
	assert.Empty(t, b.Retained(tenantB, "#"))

	cl, ok := b.server.Topics.Retained.Get(topic.Namespace(tenantA, "t/kept"))
	require.True(t, ok)
	assert.Equal(t, "kept", string(cl.Payload), "the caller's buffer is not retained")

	for _, name := range []string{"", "t/+", "#", "$SYS/x", "$foo/x", "$share/g/t"} {
		require.ErrorIs(t, b.Publish(ctx, tenantA, name, nil, 0, false), tether.ErrInvalidInput, "topic %q", name)
	}

	require.ErrorIs(t, b.Publish(ctx, tenantA, "t/1", nil, 2, false), tether.ErrInvalidInput)
	require.ErrorIs(t, b.Publish(ctx, "ttnt_nobody", "t/1", nil, 0, false), tether.ErrInvalidInput)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, b.Publish(cancelled, tenantA, "t/1", nil, 0, false), context.Canceled)
}

func TestAdminOperationsBeforeStart(t *testing.T) {
	b, err := New(testConfig(), Deps{Auth: auth.NewStatic(nil), Perms: fakePerms{}})
	require.NoError(t, err)

	tenantA := newTenant(tenant.Limits{}).ID.String()

	assert.Empty(t, b.Clients(tenantA))
	assert.Empty(t, b.Retained(tenantA, "#"))
	require.ErrorIs(t, b.Kick(tenantA, "dev"), tether.ErrNotFound)
	require.ErrorIs(t, b.DeleteSession(tenantA, "dev"), tether.ErrNotFound)
	require.ErrorIs(t, b.DeleteRetained(tenantA, "t"), tether.ErrNotFound)
	require.ErrorIs(t, b.Publish(t.Context(), tenantA, "t", nil, 0, false), tether.ErrConflict)
}
