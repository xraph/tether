package extension

import (
	"context"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store/memory"
)

// The product's tenant cache is evicted on every path that evicts
// service.Tenants: a REST or dashboard Update and Delete on this node, and
// Evict, which a cell's tenant.updated calls on every other node.
func TestProductTenantCacheFollowsServiceEvictions(t *testing.T) {
	ctx := t.Context()
	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	p := &product{}
	tenants := service.NewTenants(st, p.tenantsOption())
	p.bindTenants(tenants)

	tn, err := tenants.Create(ctx, service.CreateTenantInput{AppID: "app_1", ExternalID: "ext-1"})
	if err != nil {
		t.Fatal(err)
	}

	cached := func() bool {
		p.tenants.mu.Lock()
		defer p.tenants.mu.Unlock()

		_, ok := p.tenants.entries[tn.ID.String()]

		return ok
	}

	fill := func() {
		t.Helper()

		if _, err := p.tenants.lookup(ctx, tn.ID.String()); err != nil {
			t.Fatal(err)
		}

		if !cached() {
			t.Fatal("the lookup was not cached")
		}
	}

	fill()

	name := "renamed"
	if _, err := tenants.Update(ctx, tn.ID, service.UpdateTenantInput{Name: &name}); err != nil {
		t.Fatal(err)
	}

	if cached() {
		t.Fatal("Update left the tenant in the product's cache")
	}

	fill()
	tenants.Evict(tn.ID)

	if cached() {
		t.Fatal("Evict (a cell's tenant.updated) left the tenant in the product's cache")
	}

	fill()

	if err := tenants.Delete(ctx, tn.ID); err != nil {
		t.Fatal(err)
	}

	if cached() {
		t.Fatal("Delete left the tenant in the product's cache")
	}
}

// addCounter records Add; nothing else of forge.Counter is called.
type addCounter struct {
	forge.Counter
	n float64
}

func (c *addCounter) Add(d float64) { c.n += d }

// blockingSink holds every write until its context ends.
type blockingSink struct{}

func (blockingSink) Record(ctx context.Context, _ audit.Event, _ audit.Tenant) error {
	<-ctx.Done()

	return ctx.Err()
}

// tether_audit_dropped_total follows the recorder's count by its growth.
func TestAuditDroppedFeedsTheCounter(t *testing.T) {
	cfg := audit.DefaultConfig()
	cfg.Enabled = true
	cfg.QueueSize = 1
	cfg.Writers = 1

	rec := audit.NewRecorder(cfg, blockingSink{}, nil, nil)
	rec.Start()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_ = rec.Stop(ctx)
	})

	c := &addCounter{}
	p := &product{rec: rec, metrics: &productMetrics{auditDropped: c}}

	for range 10 {
		rec.Emit(audit.Event{Action: audit.ActionConnect, Outcome: audit.OutcomeSuccess, TenantID: "t"})
	}

	p.syncDropped()

	first := rec.Dropped()
	if first == 0 || c.n != float64(first) {
		t.Fatalf("counter %v, recorder dropped %d", c.n, first)
	}

	p.syncDropped()

	if c.n != float64(first) {
		t.Fatalf("a sync with nothing new added %v", c.n-float64(first))
	}

	for range 5 {
		rec.Emit(audit.Event{Action: audit.ActionConnect, Outcome: audit.OutcomeSuccess, TenantID: "t"})
	}

	p.syncDropped()

	if c.n != float64(rec.Dropped()) {
		t.Fatalf("counter %v, recorder dropped %d", c.n, rec.Dropped())
	}
}
