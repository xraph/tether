package extension

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/ledger"
	"github.com/xraph/ledger/meter"
	"github.com/xraph/ledger/plan"
	"github.com/xraph/ledger/store/memory"
	"github.com/xraph/ledger/subscription"
	"github.com/xraph/ledger/types"

	"github.com/xraph/tether"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/metering"
	"github.com/xraph/tether/tenant"
)

type countingGet struct {
	mu      sync.Mutex
	calls   int
	tenants map[id.TenantID]*tenant.Tenant
	err     error
	// onGet runs inside the lookup, before it returns.
	onGet func()
}

func (g *countingGet) get(_ context.Context, tid id.TenantID) (*tenant.Tenant, error) {
	g.mu.Lock()
	g.calls++
	onGet, err := g.onGet, g.err
	t := g.tenants[tid]
	g.mu.Unlock()

	if onGet != nil {
		onGet()
	}

	if err != nil {
		return nil, err
	}

	if t == nil {
		return nil, tether.ErrNotFound
	}

	cp := *t

	return &cp, nil
}

func (g *countingGet) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.calls
}

func newTestTenant(external string) *tenant.Tenant {
	return &tenant.Tenant{ID: id.NewTenantID(), AppID: "app_1", ExternalID: external}
}

func TestTenantCacheHitExpireEvict(t *testing.T) {
	tn := newTestTenant("ext-1")
	g := &countingGet{tenants: map[id.TenantID]*tenant.Tenant{tn.ID: tn}}
	c := newTenantCache(g.get)

	now := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	want := tenantRef{ID: tn.ID.String(), AppID: "app_1", ExternalID: "ext-1"}

	for range 3 {
		got, err := c.lookup(t.Context(), tn.ID.String())
		if err != nil || got != want {
			t.Fatalf("got %+v, %v; want %+v", got, err, want)
		}
	}

	if g.count() != 1 {
		t.Fatalf("store read %d times, want 1", g.count())
	}

	now = now.Add(29 * time.Second)

	if _, err := c.lookup(t.Context(), tn.ID.String()); err != nil || g.count() != 1 {
		t.Fatalf("29 s: err %v, reads %d", err, g.count())
	}

	now = now.Add(2 * time.Second)

	if _, err := c.lookup(t.Context(), tn.ID.String()); err != nil || g.count() != 2 {
		t.Fatalf("after 30 s: err %v, reads %d", err, g.count())
	}

	// A change plus an eviction shows at once.
	g.mu.Lock()
	tn.ExternalID = "ext-2"
	g.mu.Unlock()

	c.evict(tn.ID.String())

	got, err := c.lookup(t.Context(), tn.ID.String())
	if err != nil || got.ExternalID != "ext-2" {
		t.Fatalf("after evict: %+v, %v", got, err)
	}
}

func TestTenantCacheDoesNotCacheErrors(t *testing.T) {
	tn := newTestTenant("ext-1")
	g := &countingGet{tenants: map[id.TenantID]*tenant.Tenant{tn.ID: tn}, err: errors.New("store down")}
	c := newTenantCache(g.get)

	if _, err := c.lookup(t.Context(), tn.ID.String()); err == nil {
		t.Fatal("want the store error")
	}

	g.mu.Lock()
	g.err = nil
	g.mu.Unlock()

	if _, err := c.lookup(t.Context(), tn.ID.String()); err != nil {
		t.Fatalf("a failed lookup was cached: %v", err)
	}
}

func TestTenantCacheRejectsMalformedID(t *testing.T) {
	g := &countingGet{}
	c := newTenantCache(g.get)

	if _, err := c.lookup(t.Context(), "not-an-id"); err == nil || g.count() != 0 {
		t.Fatalf("err %v, reads %d", err, g.count())
	}
}

// A lookup already in flight when its tenant is evicted must not put the
// stale result back.
func TestTenantCacheEvictDuringLookup(t *testing.T) {
	tn := newTestTenant("old")
	g := &countingGet{tenants: map[id.TenantID]*tenant.Tenant{tn.ID: tn}}
	c := newTenantCache(g.get)

	g.onGet = func() {
		g.mu.Lock()
		g.onGet = nil
		g.mu.Unlock()

		c.evict(tn.ID.String())
	}

	if _, err := c.lookup(t.Context(), tn.ID.String()); err != nil {
		t.Fatal(err)
	}

	g.mu.Lock()
	tn.ExternalID = "new"
	g.mu.Unlock()

	got, err := c.lookup(t.Context(), tn.ID.String())
	if err != nil || got.ExternalID != "new" {
		t.Fatalf("got %+v, %v; the stale entry survived the eviction", got, err)
	}
}

func TestMeteringTenantsAdapter(t *testing.T) {
	tn := newTestTenant("ext-1")
	g := &countingGet{tenants: map[id.TenantID]*tenant.Tenant{tn.ID: tn}}

	got, err := meteringTenants{newTenantCache(g.get)}.Get(t.Context(), tn.ID.String())
	if err != nil {
		t.Fatal(err)
	}

	want := metering.Tenant{ID: tn.ID.String(), AppID: "app_1", ExternalID: "ext-1"}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	if _, err := (meteringTenants{newTenantCache(g.get)}).Get(t.Context(), id.NewTenantID().String()); !errors.Is(err, tether.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func startLedger(t *testing.T) (*ledger.Ledger, *memory.Store) {
	t.Helper()

	st := memory.New()
	l := ledger.New(st, ledger.WithLifecycleInterval(0), ledger.WithMeterConfig(100, 10*time.Millisecond))

	if err := l.Start(t.Context()); err != nil {
		t.Fatalf("ledger Start: %v", err)
	}

	return l, st
}

func usage(t *testing.T, st *memory.Store, tenantID, appID string) []*meter.UsageEvent {
	t.Helper()

	evs, err := st.QueryUsage(context.Background(), tenantID, appID, meter.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}

	return evs
}

func TestLedgerSinkMetersUnderExternalIDAndApp(t *testing.T) {
	l, st := startLedger(t)
	s := ledgerSink{l: l}
	acme := metering.Tenant{ID: "ttnt_a", AppID: "app_1", ExternalID: "ext-acme"}
	zeta := metering.Tenant{ID: "ttnt_z", AppID: "app_2", ExternalID: "ext-zeta"}

	if err := s.Meter(t.Context(), acme, "tether.messages.in", 5, "tether:n:1:1:ttnt_a:tether.messages.in"); err != nil {
		t.Fatal(err)
	}

	if err := s.Meter(t.Context(), zeta, "tether.messages.in", 8, "tether:n:1:1:ttnt_z:tether.messages.in"); err != nil {
		t.Fatal(err)
	}

	// A replayed key adds nothing.
	if err := s.Meter(t.Context(), acme, "tether.messages.in", 5, "tether:n:1:1:ttnt_a:tether.messages.in"); err != nil {
		t.Fatal(err)
	}

	if err := l.Stop(); err != nil {
		t.Fatal(err)
	}

	a := usage(t, st, "ext-acme", "app_1")
	if len(a) != 1 || a[0].Quantity != 5 || a[0].FeatureKey != "tether.messages.in" {
		t.Fatalf("acme usage %+v", a)
	}

	z := usage(t, st, "ext-zeta", "app_2")
	if len(z) != 1 || z[0].Quantity != 8 {
		t.Fatalf("zeta usage %+v", z)
	}

	if n := len(usage(t, st, "ttnt_a", "app_1")); n != 0 {
		t.Fatalf("usage landed under the Tether tenant ID: %d events", n)
	}
}

// R12: ledger's ErrInvalidInput comes back as metering.ErrRejected, so the
// Meter drops the delta; the buffer and a stopped ledger do not.
func TestLedgerSinkMapsInvalidInputToRejected(t *testing.T) {
	l, _ := startLedger(t)
	s := ledgerSink{l: l}
	tn := metering.Tenant{ID: "ttnt_a", AppID: "app_1", ExternalID: "ext-acme"}

	err := s.Meter(t.Context(), tn, "tether.messages.in", 1, strings.Repeat("k", 256))
	if !errors.Is(err, metering.ErrRejected) || !errors.Is(err, ledger.ErrInvalidInput) {
		t.Fatalf("got %v, want ErrRejected wrapping ErrInvalidInput", err)
	}

	if stopErr := l.Stop(); stopErr != nil {
		t.Fatal(stopErr)
	}

	err = s.Meter(t.Context(), tn, "tether.messages.in", 1, "k-after-stop")
	if !errors.Is(err, ledger.ErrStoreClosed) || errors.Is(err, metering.ErrRejected) {
		t.Fatalf("got %v, want ErrStoreClosed and not ErrRejected", err)
	}
}

// ledgerChecker asks ledger under the tenant's ExternalID and AppID, passes
// a refusal's reason through, and reports a store outage as an error.
func TestLedgerCheckerUsesExternalIDAndApp(t *testing.T) {
	ctx := t.Context()
	st := memory.New()
	l := ledger.New(st, ledger.WithLifecycleInterval(0))

	p := &plan.Plan{
		Name: "tether", Slug: "tether", Currency: "usd", AppID: "app_1",
		Features: []plan.Feature{{Key: "tether.access", Name: "Tether", Type: plan.FeatureBoolean, Limit: 1}},
		Pricing:  &plan.Pricing{BaseAmount: types.USD(100), BillingPeriod: plan.PeriodMonthly},
	}
	if err := l.CreatePlan(ctx, p); err != nil {
		t.Fatal(err)
	}

	if err := l.ActivatePlan(ctx, p.ID); err != nil {
		t.Fatal(err)
	}

	if err := l.CreateSubscription(ctx, &subscription.Subscription{TenantID: "ext-acme", AppID: "app_1", PlanID: p.ID}); err != nil {
		t.Fatal(err)
	}

	c := ledgerChecker{l: l}

	ok, reason, err := c.Check(ctx, metering.Tenant{ID: "ttnt_a", AppID: "app_1", ExternalID: "ext-acme"}, "tether.access")
	if err != nil || !ok {
		t.Fatalf("subscribed tenant: allowed %v, reason %q, err %v", ok, reason, err)
	}

	// Subscribed under its Tether ID only: ledger does not know it.
	ok, reason, err = c.Check(ctx, metering.Tenant{ID: "ext-acme", AppID: "app_1", ExternalID: "ext-zeta"}, "tether.access")
	if err != nil || ok || reason == "" {
		t.Fatalf("unsubscribed tenant: allowed %v, reason %q, err %v; want a refusal with a reason", ok, reason, err)
	}

	down := errors.New("connection refused")
	out := ledgerChecker{l: ledger.New(&outageStore{Store: memory.New(), err: down}, ledger.WithLifecycleInterval(0))}

	_, _, err = out.Check(ctx, metering.Tenant{ID: "ttnt_a", AppID: "app_1", ExternalID: "ext-acme"}, "tether.access")
	if !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("store outage: got %v, want ErrUnavailable", err)
	}
}

// outageStore fails the subscription lookup, the way a dropped connection
// does.
type outageStore struct {
	*memory.Store
	err error
}

func (o *outageStore) GetActiveSubscription(context.Context, string, string) (*subscription.Subscription, error) {
	return nil, o.err
}
