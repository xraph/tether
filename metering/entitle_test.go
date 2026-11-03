package metering_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/metering"
)

var (
	zetaT = metering.Tenant{ID: "ttnt_zeta", AppID: "app_1", ExternalID: "ext-zeta"}
	betaT = metering.Tenant{ID: "ttnt_beta", AppID: "app_2", ExternalID: "ext-beta"}
)

// fakeChecker answers per tenant and feature. Everything is allowed unless
// refuse names the pair; fail makes every check fail, and hang makes every
// check wait for its context instead.
type fakeChecker struct {
	mu     sync.Mutex
	calls  map[string]int // by "tenantID feature"
	refuse map[string]string
	fail   error
	hang   bool
}

func newChecker() *fakeChecker {
	return &fakeChecker{calls: map[string]int{}, refuse: map[string]string{}}
}

func (c *fakeChecker) Check(ctx context.Context, t metering.Tenant, feature string) (bool, string, error) {
	k := t.ID + " " + feature

	c.mu.Lock()
	c.calls[k]++
	fail, hang := c.fail, c.hang
	reason, refused := c.refuse[k]
	c.mu.Unlock()

	if hang {
		<-ctx.Done()

		return false, "", ctx.Err()
	}

	if fail != nil {
		return false, "", fail
	}

	return !refused, reason, nil
}

func (c *fakeChecker) set(f func(c *fakeChecker)) {
	c.mu.Lock()
	f(c)
	c.mu.Unlock()
}

// count is the number of checks of tenantID for every feature together.
func (c *fakeChecker) count(tenantID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := 0

	for k, v := range c.calls {
		if strings.HasPrefix(k, tenantID+" ") {
			n += v
		}
	}

	return n
}

func (c *fakeChecker) countOf(tenantID, feature string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.calls[tenantID+" "+feature]
}

func entCfg(interval time.Duration, features ...string) metering.EntitlementConfig {
	cfg := metering.DefaultEntitlementConfig()
	cfg.Enabled = true
	cfg.Interval = interval

	if len(features) > 0 {
		cfg.Features = features
	}

	return cfg
}

func newEnt(cfg metering.EntitlementConfig, chk metering.Checker) *metering.Entitlements {
	return metering.NewEntitlements(cfg, newTenants(acme, zetaT, betaT), chk, quietLogger())
}

// waitTime bounds every waitFor. It is generous: the machine running these
// may be heavily loaded.
const waitTime = 5 * time.Second

// waitFor polls cond every 5 ms until it holds or waitTime passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(waitTime)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

// runEnt starts Run and returns a stop function that waits for it to end.
func runEnt(t *testing.T, e *metering.Entitlements, live func() []string, disconnect func(string)) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		defer close(done)
		e.Run(ctx, live, disconnect)
	}()

	stop := func() {
		cancel()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not end with its context")
		}
	}
	t.Cleanup(stop)

	return stop
}

type disconnects struct {
	mu  sync.Mutex
	ids []string
}

func (d *disconnects) add(id string) {
	d.mu.Lock()
	d.ids = append(d.ids, id)
	d.mu.Unlock()
}

func (d *disconnects) all() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	return slices.Clone(d.ids)
}

func TestDefaultEntitlementConfig(t *testing.T) {
	cfg := metering.DefaultEntitlementConfig()

	if cfg.Enabled || !slices.Equal(cfg.Features, []string{"tether.access"}) || cfg.Interval != time.Minute ||
		cfg.CheckTimeout != 2*time.Second || cfg.FailOpen != 10*time.Minute {
		t.Fatalf("got %+v", cfg)
	}

	cfg.Enabled = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config: %v", err)
	}
}

func TestEntitlementConfigValidate(t *testing.T) {
	bad := map[string]func(c *metering.EntitlementConfig){
		"no features":       func(c *metering.EntitlementConfig) { c.Features = nil },
		"bad key":           func(c *metering.EntitlementConfig) { c.Features = []string{"tether access"} },
		"duplicate key":     func(c *metering.EntitlementConfig) { c.Features = []string{"a", "a"} },
		"zero interval":     func(c *metering.EntitlementConfig) { c.Interval = 0 },
		"zero timeout":      func(c *metering.EntitlementConfig) { c.CheckTimeout = 0 },
		"negative failopen": func(c *metering.EntitlementConfig) { c.FailOpen = -time.Second },
	}

	for name, mutate := range bad {
		cfg := metering.DefaultEntitlementConfig()
		cfg.Enabled = true
		mutate(&cfg)

		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: Validate passed %+v", name, cfg)
		}
	}

	// Off, only the keys are checked.
	off := metering.DefaultEntitlementConfig()
	off.Interval = 0

	if err := off.Validate(); err != nil {
		t.Errorf("disabled config with zero interval: %v", err)
	}
}

func TestEntitlementsDisabledAllowsWithoutChecking(t *testing.T) {
	chk := newChecker()
	chk.refuse[acme.ID+" tether.access"] = "no plan"

	cfg := entCfg(time.Minute)
	cfg.Enabled = false
	e := newEnt(cfg, chk)

	if err := e.Allow(t.Context(), acme.ID); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	if n := chk.count(acme.ID); n != 0 {
		t.Fatalf("%d checks, want none", n)
	}
}

func TestAllowCachesDecision(t *testing.T) {
	chk := newChecker()
	e := newEnt(entCfg(time.Minute, "tether.access", "tether.extra"), chk)

	for range 2 {
		if err := e.Allow(t.Context(), acme.ID); err != nil {
			t.Fatalf("Allow: %v", err)
		}
	}

	if a, x := chk.countOf(acme.ID, "tether.access"), chk.countOf(acme.ID, "tether.extra"); a != 1 || x != 1 {
		t.Fatalf("checks: tether.access %d, tether.extra %d; want one each", a, x)
	}
}

func TestRefusalNamesFeature(t *testing.T) {
	chk := newChecker()
	chk.refuse[acme.ID+" tether.pro"] = "plan lacks it"
	e := newEnt(entCfg(time.Minute, "tether.access", "tether.pro"), chk)

	err := e.Allow(t.Context(), acme.ID)
	if !errors.Is(err, broker.ErrNotEntitled) {
		t.Fatalf("got %v, want ErrNotEntitled", err)
	}

	if msg := err.Error(); !strings.Contains(msg, "tether.pro") || !strings.Contains(msg, "plan lacks it") {
		t.Fatalf("error %q does not name the feature and its reason", msg)
	}

	// The refusal is remembered like a grant.
	if err := e.Allow(t.Context(), acme.ID); !errors.Is(err, broker.ErrNotEntitled) {
		t.Fatalf("second Allow: %v", err)
	}

	if n := chk.countOf(acme.ID, "tether.pro"); n != 1 {
		t.Fatalf("%d checks of tether.pro, want 1", n)
	}
}

// Review Focus 1: during an outage an allowed tenant stays allowed, a refused
// one stays refused, a tenant with no answer yet is let in, and no Allow
// waits longer than CheckTimeout (plus 100 ms of slack).
func TestLedgerOutageKeepsLastAnswer(t *testing.T) {
	chk := newChecker()
	chk.refuse[betaT.ID+" tether.access"] = "no plan"

	cfg := entCfg(50 * time.Millisecond)
	cfg.CheckTimeout = 200 * time.Millisecond
	cfg.FailOpen = time.Hour
	e := newEnt(cfg, chk)

	if err := e.Allow(t.Context(), acme.ID); err != nil {
		t.Fatalf("Allow before the outage: %v", err)
	}

	if err := e.Allow(t.Context(), betaT.ID); !errors.Is(err, broker.ErrNotEntitled) {
		t.Fatalf("beta before the outage: %v", err)
	}

	chk.set(func(c *fakeChecker) { c.hang = true })

	bound := cfg.CheckTimeout + 100*time.Millisecond

	timed := func(tenantID string) error {
		start := time.Now()
		err := e.Allow(t.Context(), tenantID)

		if d := time.Since(start); d > bound {
			t.Fatalf("Allow(%s) took %s, want at most %s", tenantID, d, bound)
		}

		return err
	}

	// Keep asking until a check has run into the outage for each tenant.
	for _, tc := range []struct {
		id      string
		refused bool
	}{{acme.ID, false}, {betaT.ID, true}} {
		before := chk.count(tc.id)

		waitFor(t, "a check during the outage", func() bool {
			err := timed(tc.id)
			if tc.refused != errors.Is(err, broker.ErrNotEntitled) || (!tc.refused && err != nil) {
				t.Fatalf("Allow(%s) during the outage: %v", tc.id, err)
			}

			return chk.count(tc.id) > before
		})
	}

	// A tenant never decided is let in.
	if err := timed(zetaT.ID); err != nil {
		t.Fatalf("unknown tenant during the outage: %v", err)
	}

	if chk.count(zetaT.ID) != 1 {
		t.Fatalf("unknown tenant: %d checks, want 1", chk.count(zetaT.ID))
	}
}

func TestOutagePastFailOpenRefusesNew(t *testing.T) {
	chk := newChecker()

	cfg := entCfg(20 * time.Millisecond)
	cfg.CheckTimeout = 100 * time.Millisecond
	cfg.FailOpen = 300 * time.Millisecond
	e := newEnt(cfg, chk)

	if err := e.Allow(t.Context(), acme.ID); err != nil {
		t.Fatalf("Allow before the outage: %v", err)
	}

	down := errors.New("ledger down")
	chk.set(func(c *fakeChecker) { c.fail = down })
	outage := time.Now()

	var err error

	waitFor(t, "a refusal past FailOpen", func() bool {
		err = e.Allow(t.Context(), acme.ID)

		return err != nil
	})

	if since := time.Since(outage); since < cfg.FailOpen {
		t.Fatalf("refused %s into the outage, before FailOpen (%s)", since, cfg.FailOpen)
	}

	if errors.Is(err, broker.ErrNotEntitled) {
		t.Fatalf("got %v, want an error that is not ErrNotEntitled", err)
	}

	if !errors.Is(err, down) {
		t.Fatalf("got %v, want the checker's error wrapped", err)
	}

	// Once ledger answers again, so does Allow.
	chk.set(func(c *fakeChecker) { c.fail = nil })
	waitFor(t, "Allow to recover", func() bool { return e.Allow(t.Context(), acme.ID) == nil })
}

func TestRunDisconnectsOnLoss(t *testing.T) {
	chk := newChecker()
	e := newEnt(entCfg(100*time.Millisecond), chk)

	for _, id := range []string{acme.ID, zetaT.ID} {
		if err := e.Allow(t.Context(), id); err != nil {
			t.Fatalf("Allow(%s): %v", id, err)
		}
	}

	var got disconnects

	runEnt(t, e, func() []string { return []string{acme.ID, zetaT.ID} }, got.add)

	chk.set(func(c *fakeChecker) { c.refuse[acme.ID+" tether.access"] = "subscription ended" })

	waitFor(t, "acme's disconnect", func() bool { return len(got.all()) > 0 })

	// Three more checks of acme: it stays refused and is not disconnected
	// again, and zeta never is.
	n := chk.count(acme.ID)
	waitFor(t, "more checks", func() bool { return chk.count(acme.ID) >= n+3 })

	if ids := got.all(); !slices.Equal(ids, []string{acme.ID}) {
		t.Fatalf("disconnected %v, want acme once", ids)
	}

	if err := e.Allow(t.Context(), acme.ID); !errors.Is(err, broker.ErrNotEntitled) {
		t.Fatalf("Allow after the loss: %v", err)
	}
}

func TestOutageNeverDisconnects(t *testing.T) {
	chk := newChecker()
	chk.refuse[betaT.ID+" tether.access"] = "no plan"

	cfg := entCfg(50 * time.Millisecond)
	cfg.CheckTimeout = 50 * time.Millisecond
	cfg.FailOpen = 100 * time.Millisecond
	e := newEnt(cfg, chk)

	if err := e.Allow(t.Context(), acme.ID); err != nil {
		t.Fatalf("Allow: %v", err)
	}

	var got disconnects

	runEnt(t, e, func() []string { return []string{acme.ID, zetaT.ID} }, got.add)

	chk.set(func(c *fakeChecker) { c.fail = errors.New("ledger down") })

	// Well past FailOpen, through several checks.
	n := chk.count(acme.ID)
	waitFor(t, "checks during the outage", func() bool { return chk.count(acme.ID) >= n+5 })

	if ids := got.all(); len(ids) != 0 {
		t.Fatalf("an outage disconnected %v", ids)
	}
}

func TestRunEndsWithContext(t *testing.T) {
	chk := newChecker()
	cfg := entCfg(10 * time.Millisecond)
	cfg.CheckTimeout = time.Hour
	chk.hang = true
	e := newEnt(cfg, chk)

	stop := runEnt(t, e, func() []string { return []string{acme.ID} }, func(string) {})

	waitFor(t, "a check in flight", func() bool { return chk.count(acme.ID) > 0 })
	stop()
}
