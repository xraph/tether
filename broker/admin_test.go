package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	mochipackets "github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	loads   map[string]int
	gate    map[string]chan struct{}
	started chan string
}

func newMutablePerms(rules map[string]acl.Rules) *mutablePerms {
	return &mutablePerms{
		rules:   rules,
		roles:   map[string][]string{},
		errs:    map[string]error{},
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
		"dev": {Subscribe: []string{"x"}},
		"pub": {Publish: []string{"x"}},
	})
	b := startBrokerDeps(t, auth.NewStatic(map[string]*auth.Identity{"dev": ident(ta, "dev"), "pub": ident(ta, "pub")}), perms)
	addr := b.Addr().String()

	c, _, err := connect5(t, addr, "dev-1", "dev", true)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, subscribe5(t, c, "x"))

	p, _, err := connect5(t, addr, "pub-1", "pub", true)
	require.NoError(t, err)

	perms.fail("dev", errors.New("warden is down"))
	b.ReloadPrincipal(t.Context(), "akey_dev")

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
