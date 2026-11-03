package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

// usageOf returns tenantID's row of b.Usage, and whether the tenant has one.
func usageOf(b *Broker, tenantID string) (TenantUsage, bool) {
	for _, u := range b.Usage() {
		if u.Tenant == tenantID {
			return u, true
		}
	}

	return TenantUsage{}, false
}

func TestUsageCountsBytes(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"pub": ident(tn, "pub"), "sub": ident(tn, "sub")},
		map[string]acl.Rules{"pub": {Publish: []string{"t"}}, "sub": {Subscribe: []string{"t"}}})
	addr := b.Addr().String()

	sub, err := connect311(t, addr, "sub", "sub", nil)
	require.NoError(t, err)

	msgs := subscribe311(t, sub, "t")

	pub, err := connect311(t, addr, "pub", "pub", nil)
	require.NoError(t, err)

	payload := strings.Repeat("x", 100)
	publish311(t, pub, "t", payload)
	require.Equal(t, payload, recv(t, msgs).Payload)

	u, ok := usageOf(b, tn.ID.String())
	require.True(t, ok)
	assert.Equal(t, uint64(1), u.MessagesIn)
	assert.Equal(t, uint64(1), u.MessagesOut)
	assert.Equal(t, uint64(100), u.BytesIn, "payload only, not the topic")
	assert.Equal(t, uint64(100), u.BytesOut)
	assert.Equal(t, 2, u.Connections)

	st := b.Stats(tn.ID.String())
	assert.Equal(t, uint64(100), st.BytesIn)
	assert.Equal(t, uint64(100), st.BytesOut)
}

func TestUsageConnectionTime(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	b := startBroker(t,
		map[string]*auth.Identity{"c": ident(tn, "c")},
		map[string]acl.Rules{"c": {Publish: []string{"t"}}})
	tid := tn.ID.String()

	start := time.Now()

	c, err := connect311(t, b.Addr().String(), "c", "c", nil)
	require.NoError(t, err)

	u, ok := usageOf(b, tid)
	require.True(t, ok)
	assert.Equal(t, 1, u.Connections)

	time.Sleep(300 * time.Millisecond)
	c.Disconnect(0)

	require.Eventually(t, func() bool {
		cur, _ := usageOf(b, tid)

		return cur.Connections == 0
	}, waitTimeout, 5*time.Millisecond)

	// The connection cannot have lived longer than the test saw it live.
	ceiling := time.Since(start)

	u, _ = usageOf(b, tid)
	assert.GreaterOrEqual(t, u.ConnectionTime, 300*time.Millisecond)
	assert.LessOrEqual(t, u.ConnectionTime, ceiling)

	// Nothing accrues with no connection.
	time.Sleep(50 * time.Millisecond)

	again, _ := usageOf(b, tid)
	assert.Equal(t, u.ConnectionTime, again.ConnectionTime)
}

func TestUsageTakeoverCountsOnce(t *testing.T) {
	// With a limit of one the second CONNECT inherits the first one's slot;
	// without a limit it takes a second slot and the first gives its own back.
	// Either way the tenant has one connection once the takeover settles.
	for name, limits := range map[string]tenant.Limits{
		"slot transferred": {MaxConnections: 1},
		"slot released":    {},
	} {
		t.Run(name, func(t *testing.T) {
			tn := newTenant(limits)
			b := startBroker(t,
				map[string]*auth.Identity{"c": ident(tn, "c")},
				map[string]acl.Rules{"c": {Publish: []string{"t"}}})
			addr := b.Addr().String()
			tid := tn.ID.String()

			_, err := connect311(t, addr, "same", "c", nil)
			require.NoError(t, err)

			_, err = connect311(t, addr, "same", "c", nil)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				cur, _ := usageOf(b, tid)

				return cur.Connections == 1 && b.Stats(tid).Connected == 1
			}, waitTimeout, 5*time.Millisecond)

			before, _ := usageOf(b, tid)
			start := time.Now()

			time.Sleep(300 * time.Millisecond)

			after, _ := usageOf(b, tid)
			ceiling := time.Since(start)

			assert.Equal(t, 1, after.Connections)

			grown := after.ConnectionTime - before.ConnectionTime
			assert.GreaterOrEqual(t, grown, 300*time.Millisecond)
			assert.LessOrEqual(t, grown, ceiling, "one connection, not two")
		})
	}
}

func TestUsageHostPublishCounts(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	b := startBroker(t, map[string]*auth.Identity{}, nil)
	tid := tn.ID.String()

	require.NoError(t, b.DevicePublish(t.Context(), tid, "up/1", []byte("0123456789"), 0, false))

	u, ok := usageOf(b, tid)
	require.True(t, ok)
	assert.Equal(t, uint64(10), u.BytesIn)
	assert.Equal(t, uint64(1), u.MessagesIn)
	assert.Equal(t, 0, u.Connections)

	// A refused publish counts nothing.
	require.Error(t, b.DevicePublish(t.Context(), tid, "$SYS/x", []byte("nope"), 0, false))

	u, _ = usageOf(b, tid)
	assert.Equal(t, uint64(10), u.BytesIn)

	// The host's own publish is not a device's message, as MessagesIn has it.
	require.NoError(t, b.Publish(t.Context(), tid, "up/1", []byte("host"), 0, false))

	u, _ = usageOf(b, tid)
	assert.Equal(t, uint64(10), u.BytesIn)
	assert.Equal(t, uint64(1), u.MessagesIn)
}

func TestUsageTenantIsolation(t *testing.T) {
	t1, t2 := newTenant(tenant.Limits{}), newTenant(tenant.Limits{})
	rules := map[string]acl.Rules{
		"p1": {Publish: []string{"t"}}, "s1": {Subscribe: []string{"t"}},
		"p2": {Publish: []string{"t"}}, "s2": {Subscribe: []string{"t"}},
	}
	b := startBroker(t, map[string]*auth.Identity{
		"p1": ident(t1, "p1"), "s1": ident(t1, "s1"),
		"p2": ident(t2, "p2"), "s2": ident(t2, "s2"),
	}, rules)
	addr := b.Addr().String()

	// The same raw client IDs and the same topic in both tenants.
	s1, err := connect311(t, addr, "sub", "s1", nil)
	require.NoError(t, err)

	m1 := subscribe311(t, s1, "t")

	s2, err := connect311(t, addr, "sub", "s2", nil)
	require.NoError(t, err)

	m2 := subscribe311(t, s2, "t")

	p1, err := connect311(t, addr, "pub", "p1", nil)
	require.NoError(t, err)

	p2, err := connect311(t, addr, "pub", "p2", nil)
	require.NoError(t, err)

	publish311(t, p1, "t", "aaa")
	require.Equal(t, "aaa", recv(t, m1).Payload)

	u1, ok := usageOf(b, t1.ID.String())
	require.True(t, ok)
	assert.Equal(t, uint64(3), u1.BytesIn)
	assert.Equal(t, uint64(3), u1.BytesOut)

	u2, ok := usageOf(b, t2.ID.String())
	require.True(t, ok, "a tenant with connections is listed before it sends anything")
	assert.Equal(t, TenantUsage{Tenant: t2.ID.String(), Connections: 2, ConnectionTime: u2.ConnectionTime}, u2)

	publish311(t, p2, "t", "bbbbb")
	require.Equal(t, "bbbbb", recv(t, m2).Payload)

	u1, _ = usageOf(b, t1.ID.String())
	u2, _ = usageOf(b, t2.ID.String())
	assert.Equal(t, uint64(3), u1.BytesIn)
	assert.Equal(t, uint64(3), u1.BytesOut)
	assert.Equal(t, uint64(5), u2.BytesIn)
	assert.Equal(t, uint64(5), u2.BytesOut)
	assert.Equal(t, 2, u1.Connections)
	assert.Equal(t, 2, u2.Connections)

	// Sorted by tenant.
	all := b.Usage()
	require.Len(t, all, 2)
	assert.Less(t, all[0].Tenant, all[1].Tenant)
}
