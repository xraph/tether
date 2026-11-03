package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

// auditSpy is an audit.Emitter that keeps every event.
type auditSpy struct {
	mu  sync.Mutex
	evs []audit.Event
}

func (s *auditSpy) Emit(ev audit.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.evs = append(s.evs, ev)
}

// of returns the events of action, in order.
func (s *auditSpy) of(action string) []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []audit.Event

	for _, ev := range s.evs {
		if ev.Action == action {
			out = append(out, ev)
		}
	}

	return out
}

// await waits for n events of action and returns them.
func (s *auditSpy) await(t *testing.T, action string, n int) []audit.Event {
	t.Helper()

	eventually(t, action, func() bool { return len(s.of(action)) >= n })

	return s.of(action)
}

// withAudit sets spy as the broker's audit emitter, before Start.
func withAudit(spy *auditSpy) func(*Broker) {
	return func(b *Broker) { b.deps.Audit = spy }
}

func TestAuditConnectDisconnect(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	spy := &auditSpy{}
	rules := map[string]acl.Rules{"dev": {Publish: []string{"#"}, Subscribe: []string{"#"}}}
	b := startBrokerWith(t, testConfig(), map[string]*auth.Identity{"dev": ident(tn, "dev")}, rules, withAudit(spy))

	c, _, err := connect5(t, b.Addr().String(), "c1", "dev", true)
	require.NoError(t, err)

	conn := spy.await(t, audit.ActionConnect, 1)[0]
	assert.Equal(t, audit.OutcomeSuccess, conn.Outcome)
	assert.Equal(t, tn.ID.String(), conn.TenantID)
	assert.Equal(t, "c1", conn.ClientID, "the client ID as the client sent it")
	assert.Equal(t, "akey_dev", conn.Subject)
	assert.Equal(t, singleNode, conn.Node)
	assert.Equal(t, 5, conn.Meta["protocol"])
	assert.Equal(t, true, conn.Meta["clean_start"])
	assert.Equal(t, false, conn.Meta["session_present"])
	assert.NotEmpty(t, conn.Meta["remote"])

	// Publishing is never audited.
	require.Equal(t, byte(0), publish5(t, c, "a/b", "x"))

	require.NoError(t, c.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	disc := spy.await(t, audit.ActionDisconnect, 1)[0]
	assert.Equal(t, tn.ID.String(), disc.TenantID)
	assert.Equal(t, "c1", disc.ClientID)
	assert.Equal(t, 0, disc.Meta["reason_code"])
	assert.Contains(t, disc.Meta, "connected_for")

	assert.Empty(t, spy.of(audit.ActionACLDenied))
	assert.Empty(t, spy.of(audit.ActionAuthFailed))
}

func TestAuditAuthFailure(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	off := newTenant(tenant.Limits{})
	off.Enabled = false
	spy := &auditSpy{}
	b := startBrokerWith(t, testConfig(),
		map[string]*auth.Identity{"dev": ident(tn, "dev"), "off": ident(off, "off")}, nil, withAudit(spy))

	_, ca, err := connect5(t, b.Addr().String(), "c1", "nobody", true)
	require.Error(t, err)
	require.NotNil(t, ca)

	ev := spy.await(t, audit.ActionAuthFailed, 1)[0]
	assert.Equal(t, audit.OutcomeFailure, ev.Outcome)
	assert.Equal(t, AuthReasonBadCredentials, ev.Reason)
	assert.Equal(t, int(ca.ReasonCode), ev.Meta["reason_code"])
	assert.Equal(t, "c1", ev.ClientID)
	assert.Equal(t, "nobody", ev.Subject)
	assert.Empty(t, ev.TenantID, "bad credentials name no tenant")

	// A refusal after the credentials resolved names the tenant.
	_, _, err = connect5(t, b.Addr().String(), "c2", "off", true)
	require.Error(t, err)

	evs := spy.await(t, audit.ActionAuthFailed, 2)
	assert.Equal(t, AuthReasonNotAuthorized, evs[1].Reason)
	assert.Equal(t, off.ID.String(), evs[1].TenantID)

	assert.Empty(t, spy.of(audit.ActionConnect))
	assert.Empty(t, spy.of(audit.ActionDisconnect), "a refused CONNECT never connected")
}

func TestAuditACLDeny(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	spy := &auditSpy{}
	rules := map[string]acl.Rules{"dev": {Publish: []string{"up/#"}, Subscribe: []string{"down/#"}}}
	b := startBrokerWith(t, testConfig(), map[string]*auth.Identity{"dev": ident(tn, "dev")}, rules, withAudit(spy))

	c, _, err := connect5(t, b.Addr().String(), "c1", "dev", true)
	require.NoError(t, err)

	assert.Equal(t, byte(0x87), publish5(t, c, "down/x", "no"))

	ev := spy.await(t, audit.ActionACLDenied, 1)[0]
	assert.Equal(t, audit.OutcomeDenied, ev.Outcome)
	assert.Equal(t, tn.ID.String(), ev.TenantID)
	assert.Equal(t, "c1", ev.ClientID)
	assert.Equal(t, ACLOpPublish, ev.Meta[audit.MetaOp])
	assert.Equal(t, "down/x", ev.Meta[audit.MetaTopic], "the topic without the tenant")

	codes := subscribe5(t, c, "down/#", "up/#")
	require.Equal(t, []byte{0x01, 0x87}, codes)

	evs := spy.await(t, audit.ActionACLDenied, 2)
	assert.Equal(t, ACLOpSubscribe, evs[1].Meta[audit.MetaOp])
	assert.Equal(t, "up/#", evs[1].Meta[audit.MetaTopic])

	// A granted publish and subscribe add nothing.
	assert.Equal(t, byte(0), publish5(t, c, "up/x", "yes"))
	time.Sleep(50 * time.Millisecond)
	assert.Len(t, spy.of(audit.ActionACLDenied), 2)
}

func TestAuditTakeover(t *testing.T) {
	t.Run("single node", func(t *testing.T) {
		tn := newTenant(tenant.Limits{})
		spy := &auditSpy{}
		b := startBrokerWith(t, testConfig(), map[string]*auth.Identity{"dev": ident(tn, "dev")}, nil, withAudit(spy))

		_, _, err := connect5(t, b.Addr().String(), "c1", "dev", true)
		require.NoError(t, err)

		_, _, err = connect5(t, b.Addr().String(), "c1", "dev", true)
		require.NoError(t, err)

		ev := spy.await(t, audit.ActionTakeover, 1)[0]
		assert.Equal(t, tn.ID.String(), ev.TenantID)
		assert.Equal(t, "c1", ev.ClientID)
		assert.Equal(t, "akey_dev", ev.Subject)
		assert.Equal(t, "same_node", ev.Meta["by"])

		spy.await(t, audit.ActionConnect, 2)
		spy.await(t, audit.ActionDisconnect, 1)
		assert.Len(t, spy.of(audit.ActionTakeover), 1)
	})

	t.Run("other node", func(t *testing.T) {
		spies := []*auditSpy{{}, {}}
		next := 0
		before := func(b *Broker) {
			b.deps.Audit = spies[next]
			next++
		}

		t1 := newTenant(tenant.Limits{})
		all := acl.Rules{Publish: []string{"#"}, Subscribe: []string{"#"}}
		c := startClusterWith(t, 2, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
			fakePerms{rules: map[string]acl.Rules{"u1": all}}, before)

		old, _, err := connect5(t, c.addr(0), "c1", "u1", false)
		require.NoError(t, err)

		_, _, err = connect5(t, c.addr(1), "c1", "u1", false)
		require.NoError(t, err)

		select {
		case d := <-old.disconnects:
			assert.Equal(t, byte(0x8E), d.ReasonCode)
		case <-time.After(waitTimeout):
			t.Fatal("the old connection was not taken over")
		}

		ev := spies[0].await(t, audit.ActionTakeover, 1)[0]
		assert.Equal(t, t1.ID.String(), ev.TenantID)
		assert.Equal(t, "c1", ev.ClientID)
		assert.Equal(t, "n1", ev.Node, "recorded where the displaced connection was")
		assert.Equal(t, "other_node", ev.Meta["by"])
		assert.Equal(t, "akey_u1", ev.Subject)

		conn := spies[1].await(t, audit.ActionConnect, 1)[0]
		assert.Equal(t, "n2", conn.Node)
		assert.Empty(t, spies[1].of(audit.ActionTakeover), "the new node displaced nobody of its own")

		spies[0].await(t, audit.ActionDisconnect, 1)
		assert.Len(t, spies[0].of(audit.ActionTakeover), 1, "one takeover, one event")
	})
}

// TestAuditClusterPublishDenied: in a cell OnPublish makes the publish
// refusal, and it is audited there.
func TestAuditClusterPublishDenied(t *testing.T) {
	spy := &auditSpy{}
	t1 := newTenant(tenant.Limits{})
	c := startClusterWith(t, 1, testConfig(), map[string]*auth.Identity{"u1": ident(t1, "u1")},
		fakePerms{rules: map[string]acl.Rules{"u1": {Publish: []string{"up/#"}}}}, withAudit(spy))

	cl, _, err := connect5(t, c.addr(0), "c1", "u1", true)
	require.NoError(t, err)

	assert.Equal(t, byte(0x87), publish5(t, cl, "down/x", "no"))

	ev := spy.await(t, audit.ActionACLDenied, 1)[0]
	assert.Equal(t, ACLOpPublish, ev.Meta[audit.MetaOp])
	assert.Equal(t, "down/x", ev.Meta[audit.MetaTopic])
	assert.Equal(t, "n1", ev.Node)
}
