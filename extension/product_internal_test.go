package extension

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/tether"
	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/metering"
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

// incCounter counts Inc; nothing else of forge.Counter is called.
type incCounter struct {
	forge.Counter
	n int
}

func (c *incCounter) Inc() { c.n++ }

// goneTenants answers every lookup the way the store answers a deleted
// tenant.
type goneTenants struct{}

func (goneTenants) Get(_ context.Context, tenantID string) (metering.Tenant, error) {
	return metering.Tenant{}, fmt.Errorf("tether/memory: get tenant %s: %w", tenantID, tether.ErrNotFound)
}

// failSink fails every call: the meter must not reach it.
type failSink struct{ calls *int }

func (s failSink) Meter(context.Context, metering.Tenant, string, int64, string) error {
	*s.calls++

	return errors.New("not expected")
}

// usageSource is a started broker with fixed usage.
type usageSource struct{ usage []broker.TenantUsage }

func (s usageSource) Usage() []broker.TenantUsage { return s.usage }
func (usageSource) StartedAt() time.Time          { return time.Unix(1_700_000_000, 0) }

// A deleted tenant with unsent usage is not a metering error: no window
// fails over it and tether_metering_errors_total stays where it was.
func TestMeteringDeletedTenantNotCounted(t *testing.T) {
	errs := &incCounter{}
	m := &productMetrics{meteringErrors: errs}
	calls := 0

	meter := metering.New(metering.DefaultConfig(),
		usageSource{usage: []broker.TenantUsage{{Tenant: "ttnt_gone", MessagesIn: 4, BytesIn: 40}}},
		countingTenants{t: goneTenants{}, m: m}, countingSink{s: failSink{calls: &calls}, m: m}, "n1", slog.New(slog.DiscardHandler))

	for i := range 2 {
		if err := meter.Flush(t.Context()); err != nil {
			t.Fatalf("Flush %d: %v", i+1, err)
		}
	}

	if calls != 0 {
		t.Fatalf("the sink was called %d times", calls)
	}

	if errs.n != 0 {
		t.Fatalf("tether_metering_errors_total went up by %d", errs.n)
	}

	// Another lookup error still counts.
	if _, err := (countingTenants{t: failTenants{}, m: m}).Get(t.Context(), "ttnt_x"); err == nil || errs.n != 1 {
		t.Fatalf("a failed lookup: err %v, counter %d", err, errs.n)
	}
}

// failTenants fails every lookup with an outage.
type failTenants struct{}

func (failTenants) Get(context.Context, string) (metering.Tenant, error) {
	return metering.Tenant{}, errors.New("store down")
}

// failingSink refuses every write.
type failingSink struct{}

func (failingSink) Record(context.Context, audit.Event, audit.Tenant) error {
	return errors.New("chronicle down")
}

// tether_audit_failures_total follows the recorder's sink failures by their
// growth, apart from the dropped count.
func TestAuditFailuresFeedTheCounter(t *testing.T) {
	cfg := audit.DefaultConfig()
	cfg.Enabled = true

	rec := audit.NewRecorder(cfg, failingSink{}, nil, nil)
	rec.Start()

	dropped, failed := &addCounter{}, &addCounter{}
	p := &product{rec: rec, metrics: &productMetrics{auditDropped: dropped, auditFailures: failed}}

	for range 4 {
		rec.Emit(audit.Event{Action: audit.ActionConnect, Outcome: audit.OutcomeSuccess, TenantID: "t"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rec.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	p.syncDropped()
	p.syncFailures()
	p.syncFailures()

	if failed.n != 4 || dropped.n != 0 {
		t.Fatalf("failures counter %v, dropped counter %v; want 4 and 0", failed.n, dropped.n)
	}
}

// recordingSink keeps every event it is given, 50 ms after it is given it,
// so a Stop finds the writers busy.
type recordingSink struct {
	mu  sync.Mutex
	evs []audit.Event
}

func (s *recordingSink) Record(ctx context.Context, ev audit.Event, _ audit.Tenant) error {
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.evs = append(s.evs, ev)

	return nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.evs)
}

// meterSink counts what the meter sends.
type meterSink struct {
	mu  sync.Mutex
	qty int64
}

func (s *meterSink) Meter(_ context.Context, _ metering.Tenant, _ string, q int64, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.qty += q

	return nil
}

// someTenants knows every tenant.
type someTenants struct{}

func (someTenants) Get(_ context.Context, id string) (metering.Tenant, error) {
	return metering.Tenant{ID: id, AppID: "app", ExternalID: "ext"}, nil
}

// runningProduct is a product with a started recorder holding three events
// and a started meter with usage to send.
func runningProduct(t *testing.T) (*product, *recordingSink, *meterSink) {
	t.Helper()

	cfg := audit.DefaultConfig()
	cfg.Enabled = true

	rs := &recordingSink{}
	rec := audit.NewRecorder(cfg, rs, nil, nil)
	rec.Start()

	for range 3 {
		rec.Emit(audit.Event{Action: audit.ActionDisconnect, Outcome: audit.OutcomeSuccess, TenantID: "t"})
	}

	mcfg := metering.DefaultConfig()
	mcfg.Enabled = true
	mcfg.Interval = time.Hour
	mcfg.Features = metering.Features{MessagesIn: "tether.messages.in"}

	ms := &meterSink{}
	meter := metering.New(mcfg, usageSource{usage: []broker.TenantUsage{{Tenant: "ttnt_a", MessagesIn: 7}}},
		someTenants{}, ms, "n1", slog.New(slog.DiscardHandler))
	meter.Start()

	m := &productMetrics{auditDropped: &addCounter{}, auditFailures: &addCounter{}}

	return &product{rec: rec, meter: meter, metrics: m, log: slog.New(slog.DiscardHandler)}, rs, ms
}

// The meter's last window and the recorder's drain each get a bounded slice
// of their own: a Stop whose ctx the broker's Stop used up still sends the
// last window and drains the queue.
func TestStopAfterBrokerOutlivesSpentContext(t *testing.T) {
	p, rs, ms := runningProduct(t)

	spent, cancel := context.WithCancel(context.Background())
	cancel()

	p.stopAfterBroker(spent, forge.NewNoopLogger())

	if n := rs.count(); n != 3 {
		t.Fatalf("the recorder drained %d of 3 events", n)
	}

	if ms.qty != 7 {
		t.Fatalf("the last window sent %d of 7", ms.qty)
	}
}

// A Stop that finds no broker (its Start failed after the product was made)
// still drains the recorder.
func TestStopWithoutBrokerDrains(t *testing.T) {
	p, rs, ms := runningProduct(t)

	e := New()
	e.started = true
	e.product = p

	if err := e.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	if n := rs.count(); n != 3 {
		t.Fatalf("the recorder drained %d of 3 events", n)
	}

	if ms.qty != 7 {
		t.Fatalf("the last window sent %d of 7", ms.qty)
	}

	p.rec.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t"})

	if p.rec.Dropped() != 1 {
		t.Fatal("the recorder still takes events after Stop")
	}
}
