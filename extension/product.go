package extension

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/xraph/chronicle"
	"github.com/xraph/forge"
	"github.com/xraph/ledger"
	"github.com/xraph/vessel"

	"github.com/xraph/tether"
	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/metering"
	"github.com/xraph/tether/service"
)

var (
	// ErrLedgerRequired is returned by Start when metering or entitlements
	// are enabled and no *ledger.Ledger is in the container.
	ErrLedgerRequired = errors.New("tether: metering and entitlements need ledger; " +
		"register ledger's extension (github.com/xraph/ledger/extension), " +
		"or set metering.enabled and entitlements.enabled to false")

	// ErrChronicleRequired is returned by Start when audit is enabled and no
	// chronicle.Emitter is in the container.
	ErrChronicleRequired = errors.New("tether: audit needs chronicle; " +
		"register chronicle's extension (github.com/xraph/chronicle/extension), " +
		"or set audit.enabled to false")
)

// auditDropSync is how often tether_audit_dropped_total catches up with the
// recorder's count. Stop syncs it once more after the recorder has drained.
var auditDropSync = 5 * time.Second

// stopProductWait bounds the recorder's drain when Start fails after it
// started.
const stopProductWait = 5 * time.Second

// product is what Start builds for metering, entitlements and audit. Each
// part is nil when its config is off. One product belongs to one broker run:
// Start builds it, Stop ends it, and the next Start builds a new one, so the
// meter's window keys never restart under one broker start time.
type product struct {
	log *slog.Logger

	// tenants serves the tenant's host pair to the meter, the entitlement
	// checks and the recorder. Every service.Tenants eviction drops it too.
	tenants *tenantCache

	led *ledger.Ledger
	em  chronicle.Emitter

	meter *metering.Meter
	ent   *metering.Entitlements
	rec   *audit.Recorder

	metrics *productMetrics

	// runStop ends Entitlements.Run and the dropped-count sync; wg waits
	// for both.
	runStop context.CancelFunc
	wg      sync.WaitGroup
	// dropped is the recorder's count already added to the counter. Only
	// syncDropped touches it, from one goroutine at a time.
	dropped uint64
}

// validateProduct checks the metering, entitlements and audit configs, before
// Start builds anything (ruling R13).
func (e *Extension) validateProduct() error {
	if err := e.config.Metering.Validate(); err != nil {
		return fmt.Errorf("tether: metering config: %w", err)
	}

	if err := e.config.Entitlements.Validate(); err != nil {
		return fmt.Errorf("tether: entitlements config: %w", err)
	}

	if err := e.config.Audit.Validate(); err != nil {
		return fmt.Errorf("tether: audit config: %w", err)
	}

	return nil
}

// newProduct resolves ledger and chronicle from the container for the parts
// that are on. A part that is off needs nothing, and a ledger or chronicle in
// the container turns nothing on by itself.
func (e *Extension) newProduct(log *slog.Logger) (*product, error) {
	p := &product{log: log}
	c := e.App().Container()

	if e.config.Metering.Enabled || e.config.Entitlements.Enabled {
		led, err := vessel.Inject[*ledger.Ledger](c)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLedgerRequired, err)
		}

		p.led = led
	}

	if e.config.Audit.Enabled {
		em, err := vessel.Inject[chronicle.Emitter](c)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrChronicleRequired, err)
		}

		p.em = em
	}

	p.metrics = newProductMetrics(e.Metrics(), e.config)

	return p, nil
}

// tenantsOption makes the tenant service evict the product's tenant cache
// with its own, and the cache read through the service. Every eviction path
// of service.Tenants (Update, Delete and a cell's tenant.updated through
// Evict) reaches the cache.
func (p *product) tenantsOption() service.TenantsOption {
	return service.WithEvictHook(func(tid id.TenantID) {
		if p.tenants != nil {
			p.tenants.evict(tid.String())
		}
	})
}

// bindTenants builds the cache over the tenant service.
func (p *product) bindTenants(t *service.Tenants) {
	p.tenants = newTenantCache(t.Get)
}

// startAudit builds the recorder and starts it, before broker.New hands it to
// the broker: an event emitted before Start would be lost uncounted.
func (p *product) startAudit(cfg audit.Config) {
	if p.em == nil {
		return
	}

	p.rec = audit.NewRecorder(cfg, chronicleSink{e: p.em}, auditTenants{c: p.tenants}, p.log)
	p.rec.Start()
}

// buildEntitlements builds the CONNECT check the broker's Deps carry.
func (p *product) buildEntitlements(cfg metering.EntitlementConfig) {
	if !cfg.Enabled {
		return
	}

	chk := countingChecker{c: ledgerChecker{l: p.led}, m: p.metrics}
	p.ent = metering.NewEntitlements(cfg, meteringTenants{c: p.tenants}, chk, p.log)
}

// brokerDeps fills the broker's Entitlement and Audit, leaving each nil when
// its part is off (a typed nil would not read as nil).
func (p *product) brokerDeps(d *broker.Deps) {
	if p.ent != nil {
		d.Entitlement = p.ent
	}

	if p.rec != nil {
		d.Audit = p.rec
	}
}

// emitter is the REST API's and the dashboard's audit hook, nil when audit is
// off.
func (p *product) emitter() audit.Emitter {
	if p.rec == nil {
		return nil
	}

	return p.rec
}

// run starts what needs a running broker: the meter (ruling R13: never
// before broker.Start, one per broker run), the entitlement loop over the
// node's live tenants, and the dropped-count sync.
func (p *product) run(cfg Config, b *broker.Broker) {
	ctx, cancel := context.WithCancel(context.Background())
	p.runStop = cancel

	if p.led != nil && cfg.Metering.Enabled {
		sink := countingSink{s: ledgerSink{l: p.led}, m: p.metrics}
		tenants := countingTenants{t: meteringTenants{c: p.tenants}, m: p.metrics}
		p.meter = metering.New(cfg.Metering, b, tenants, sink, b.NodeID(), p.log)
		p.meter.Start()
	}

	if p.ent != nil {
		p.wg.Go(func() {
			p.ent.Run(ctx, b.LiveTenants, func(tenantID string) { b.DisconnectTenant(tenantID, packets.ErrQuotaExceeded) })
		})
	}

	if p.rec != nil {
		p.wg.Go(func() {
			tick := time.NewTicker(auditDropSync)
			defer tick.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					p.syncDropped()
				}
			}
		})
	}
}

// stopChecks ends the entitlement loop and the background refreshes (ruling
// R17), before the broker and ledger stop, and the dropped-count sync.
func (p *product) stopChecks() {
	if p.runStop != nil {
		p.runStop()
	}

	p.wg.Wait()

	if p.ent != nil {
		p.ent.Close()
	}
}

// stopAfterBroker runs the meter's last window, now that the broker's
// counters have stopped, then drains the recorder, which also takes the
// disconnects the broker's Stop emitted. Their errors are logged: neither
// should fail Stop.
func (p *product) stopAfterBroker(ctx context.Context, log forge.Logger) {
	if p.meter != nil {
		if err := p.meter.Stop(ctx); err != nil {
			log.Warn("tether: the last metering window failed", forge.F("error", err.Error()))
		}
	}

	if p.rec != nil {
		if err := p.rec.Stop(ctx); err != nil {
			log.Warn("tether: the audit recorder did not drain", forge.F("error", err.Error()))
		}

		p.syncDropped()
	}
}

// abort undoes a Start that failed after the recorder started. The broker
// never ran, so the meter and the loop were never started.
func (p *product) abort(ctx context.Context) {
	if p.ent != nil {
		p.ent.Close()
	}

	if p.rec != nil {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), stopProductWait)
		if err := p.rec.Stop(sctx); err != nil {
			p.log.Warn("tether: the audit recorder did not drain after a failed start", "error", err)
		}

		cancel()
	}
}

// syncDropped adds what the recorder dropped since the last sync to
// tether_audit_dropped_total.
func (p *product) syncDropped() {
	n := p.rec.Dropped()
	if n > p.dropped {
		p.metrics.auditDropped.Add(float64(n - p.dropped))
		p.dropped = n
	}
}

// Entitlement check results, the values of MetricEntitlementChecks' result
// label.
const (
	checkAllowed = "allowed"
	checkDenied  = "denied"
	checkError   = "error"
)

// productMetrics are the metering, entitlement and audit metrics, each made
// only when its part is on.
type productMetrics struct {
	meteringErrors forge.Counter
	checks         *forge.Vec[forge.Counter]
	auditDropped   forge.Counter
}

func newProductMetrics(m forge.Metrics, cfg Config) *productMetrics {
	pm := &productMetrics{}

	if cfg.Metering.Enabled {
		pm.meteringErrors = m.Counter(MetricMeteringErrors)
	}

	if cfg.Entitlements.Enabled {
		pm.checks = forge.CounterVec(m, MetricEntitlementChecks, "result")
	}

	if cfg.Audit.Enabled {
		pm.auditDropped = m.Counter(MetricAuditDropped)
	}

	return pm
}

// countingSink counts the meter's sink errors, temporary and rejected alike.
type countingSink struct {
	s metering.Sink
	m *productMetrics
}

func (c countingSink) Meter(ctx context.Context, t metering.Tenant, feature string, quantity int64, key string) error {
	err := c.s.Meter(ctx, t, feature, quantity, key)
	if err != nil {
		c.m.meteringErrors.Inc()
	}

	return err
}

// countingTenants counts the meter's failed tenant lookups. A tenant that
// no longer exists is not a failure: the meter drops its usage (and says so
// once), and counting it would put a deleted tenant on the outage signal.
type countingTenants struct {
	t metering.Tenants
	m *productMetrics
}

func (c countingTenants) Get(ctx context.Context, tenantID string) (metering.Tenant, error) {
	t, err := c.t.Get(ctx, tenantID)
	if err != nil && !errors.Is(err, tether.ErrNotFound) {
		c.m.meteringErrors.Inc()
	}

	return t, err
}

// countingChecker counts every feature check that reached ledger by its
// result.
type countingChecker struct {
	c metering.Checker
	m *productMetrics
}

func (c countingChecker) Check(ctx context.Context, t metering.Tenant, feature string) (allowed bool, reason string, err error) {
	allowed, reason, err = c.c.Check(ctx, t, feature)

	switch {
	case err != nil:
		c.m.checks.With(checkError).Inc()
	case allowed:
		c.m.checks.With(checkAllowed).Inc()
	default:
		c.m.checks.With(checkDenied).Inc()
	}

	return allowed, reason, err
}
