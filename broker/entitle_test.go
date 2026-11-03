package broker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	pahov3packets "github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/tenant"
)

// fakeEntitlement answers Allow from a table: a tenant it holds an error for
// is refused with it, every other one is allowed.
type fakeEntitlement struct {
	mu      sync.Mutex
	refused map[string]error
}

func (f *fakeEntitlement) Allow(_ context.Context, tenantID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.refused[tenantID]
}

func (f *fakeEntitlement) set(tenantID string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.refused == nil {
		f.refused = map[string]error{}
	}

	f.refused[tenantID] = err
}

// startEntitled starts a broker with ent as its Entitlement and m as its
// Metrics.
func startEntitled(t *testing.T, ids map[string]*auth.Identity, ent Entitlement, m Metrics) *Broker {
	t.Helper()

	b, err := New(testConfig(), Deps{
		Auth:        auth.NewStatic(ids),
		Perms:       fakePerms{rules: map[string]acl.Rules{"paid": {Publish: []string{"t"}}}},
		Logger:      testLogger(),
		Metrics:     m,
		Entitlement: ent,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	return b
}

func notEntitled(feature, reason string) error {
	return fmt.Errorf("%w: %s: %s", ErrNotEntitled, feature, reason)
}

func TestEntitlementRefusedAtConnect(t *testing.T) {
	lapsed := newTenant(tenant.Limits{})
	unknown := newTenant(tenant.Limits{})
	paid := newTenant(tenant.Limits{})

	ent := &fakeEntitlement{}
	ent.set(lapsed.ID.String(), notEntitled("tether.access", "no active subscription"))
	ent.set(unknown.ID.String(), errors.New("ledger unreachable past fail_open"))

	m := newFakeMetrics()
	b := startEntitled(t, map[string]*auth.Identity{
		"lapsed":  ident(lapsed, "lapsed"),
		"unknown": ident(unknown, "unknown"),
		"paid":    ident(paid, "paid"),
	}, ent, m)
	addr := b.Addr().String()

	// 5.0: quota exceeded.
	_, ca, err := connect5(t, addr, "dev-1", "lapsed", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)

	// 3.1.1 has no quota code: server unavailable, as for the quota limits.
	_, err = connect311(t, addr, "dev-2", "lapsed", nil)
	require.ErrorIs(t, err, pahov3packets.ErrorRefusedServerUnavailable)

	// Any other error is an outage: 0x88.
	_, ca, err = connect5(t, addr, "dev-3", "unknown", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x88), ca.ReasonCode)

	// An entitled tenant connects.
	_, ca, err = connect5(t, addr, "dev-4", "paid", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0), ca.ReasonCode)

	got := m.snapshot()
	assert.Equal(t, map[string]int{AuthReasonNotEntitled: 2, AuthReasonUnavailable: 1}, got.authFails)
	assert.Equal(t, 1, got.opened, "refused CONNECTs take no slot")
}

func TestEntitlementLostDisconnectsLiveClients(t *testing.T) {
	lapsing := newTenant(tenant.Limits{})
	paid := newTenant(tenant.Limits{})

	ent := &fakeEntitlement{}
	b := startEntitled(t, map[string]*auth.Identity{
		"lapsing": ident(lapsing, "lapsing"),
		"paid":    ident(paid, "paid"),
	}, ent, nil)
	addr := b.Addr().String()

	c1, _, err := connect5(t, addr, "dev-1", "lapsing", true)
	require.NoError(t, err)

	c2, _, err := connect5(t, addr, "dev-2", "lapsing", true)
	require.NoError(t, err)

	other, _, err := connect5(t, addr, "dev-3", "paid", true)
	require.NoError(t, err)

	want := []string{lapsing.ID.String(), paid.ID.String()}
	slices.Sort(want)
	require.Equal(t, want, b.LiveTenants())

	// A check loop like metering.Entitlements.Run, every 100 ms.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	disconnected := make(chan int, 4)

	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()

		lost := map[string]bool{}

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}

			for _, id := range b.LiveTenants() {
				if errors.Is(ent.Allow(ctx, id), ErrNotEntitled) && !lost[id] {
					lost[id] = true
					disconnected <- b.DisconnectTenant(id, packets.ErrQuotaExceeded)
				}
			}
		}
	}()

	ent.set(lapsing.ID.String(), notEntitled("tether.access", "subscription canceled"))

	select {
	case n := <-disconnected:
		assert.Equal(t, 2, n)
	case <-time.After(waitTimeout):
		require.FailNow(t, "the tenant was not disconnected")
	}

	for _, c := range []*client5{c1, c2} {
		select {
		case d := <-c.disconnects:
			assert.Equal(t, byte(0x97), d.ReasonCode)
		case <-time.After(waitTimeout):
			require.FailNow(t, "no DISCONNECT 0x97")
		}
	}

	require.Eventually(t, func() bool {
		return slices.Equal(b.LiveTenants(), []string{paid.ID.String()})
	}, waitTimeout, 10*time.Millisecond)

	// The other tenant is untouched.
	select {
	case d := <-other.disconnects:
		require.FailNow(t, "the entitled tenant was disconnected", "reason 0x%02x", d.ReasonCode)
	default:
	}

	assert.Equal(t, byte(0), publish5(t, other, "t", "still here"))

	// And the lapsed tenant cannot come back.
	_, ca, err := connect5(t, addr, "dev-1", "lapsing", true)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x97), ca.ReasonCode)
}

func TestNoEntitlementDepUnchanged(t *testing.T) {
	tn := newTenant(tenant.Limits{})
	m := newFakeMetrics()

	b, err := New(testConfig(), Deps{
		Auth:    auth.NewStatic(map[string]*auth.Identity{"dev": ident(tn, "dev")}),
		Perms:   fakePerms{},
		Logger:  testLogger(),
		Metrics: m,
	})
	require.NoError(t, err)

	// Before Start there is nothing to disconnect or list.
	assert.Equal(t, 0, b.DisconnectTenant(tn.ID.String(), packets.ErrQuotaExceeded))
	assert.Empty(t, b.LiveTenants())

	require.NoError(t, b.Start(t.Context()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		_ = b.Stop(ctx)
	})

	_, ca, err := connect5(t, b.Addr().String(), "dev-1", "dev", true)
	require.NoError(t, err)
	assert.Equal(t, byte(0), ca.ReasonCode)

	_, err = connect311(t, b.Addr().String(), "dev-2", "dev", nil)
	require.NoError(t, err)

	assert.Equal(t, []string{tn.ID.String()}, b.LiveTenants())
	assert.Empty(t, m.snapshot().authFails)
	assert.Equal(t, 0, b.DisconnectTenant("not-a-tenant", packets.ErrQuotaExceeded))
}
