package metering

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/xraph/tether/broker"
)

// Checker answers whether a tenant's plan includes a feature. allowed false
// with a reason is a refusal; an error means no answer (the billing system is
// unreachable or slow). A soft-limit feature over its limit is allowed. Check
// must return once ctx is done.
type Checker interface {
	Check(ctx context.Context, t Tenant, feature string) (allowed bool, reason string, err error)
}

// EntitlementConfig configures Entitlements.
type EntitlementConfig struct {
	// Enabled turns the checks on. Without it Allow lets every tenant in
	// and Run returns at once.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Features are the feature keys a tenant must be entitled to, every one
	// of them. Default tether.access.
	Features []string `yaml:"features" json:"features"`

	// Interval is how long a decision is used before it is checked again,
	// and how often Run checks the live tenants. Default 60 s.
	Interval time.Duration `yaml:"interval" json:"interval"`

	// CheckTimeout bounds one tenant's checks, all features together.
	// Default 2 s.
	CheckTimeout time.Duration `yaml:"check_timeout" json:"check_timeout"`

	// FailOpen is how long a tenant keeps its last decision while its checks
	// fail. Past it, new CONNECTs are refused as unavailable. Default 10 min.
	FailOpen time.Duration `yaml:"fail_open" json:"fail_open"`
}

// DefaultEntitlementConfig returns the checks off, on feature tether.access,
// every 60 s, with a 2 s check timeout and 10 min of fail-open.
func DefaultEntitlementConfig() EntitlementConfig {
	return EntitlementConfig{
		Features:     []string{"tether.access"},
		Interval:     time.Minute,
		CheckTimeout: 2 * time.Second,
		FailOpen:     10 * time.Minute,
	}
}

// Validate reports a feature key that is longer than 64 bytes, has a byte
// outside [-_.a-zA-Z0-9] or is listed twice. When the checks are enabled it
// also reports an empty feature list and a non-positive interval, check
// timeout or fail-open.
func (c EntitlementConfig) Validate() error {
	seen := make(map[string]bool, len(c.Features))

	for i, f := range c.Features {
		if err := checkFeatureKey(f); err != nil {
			return fmt.Errorf("metering: entitlements.features[%d]: %w", i, err)
		}

		if seen[f] {
			return fmt.Errorf("metering: entitlements.features lists %q twice", f)
		}

		seen[f] = true
	}

	if !c.Enabled {
		return nil
	}

	if len(c.Features) == 0 {
		return errors.New("metering: entitlements.features is empty")
	}

	for _, d := range []struct {
		name string
		v    time.Duration
	}{{"interval", c.Interval}, {"check_timeout", c.CheckTimeout}, {"fail_open", c.FailOpen}} {
		if d.v <= 0 {
			return fmt.Errorf("metering: entitlements.%s must be positive, got %s", d.name, d.v)
		}
	}

	return nil
}

// runParallel is how many tenants Run checks at once.
const runParallel = 8

// Entitlements answers the broker's CONNECT check (broker.Entitlement) from
// decisions it keeps per tenant, and Run re-checks the live tenants every
// Interval, disconnecting a tenant that lost its entitlement.
//
// A decision younger than Interval is used as is. An older one, or none, is
// checked again: every feature, within CheckTimeout, one check per tenant at
// a time (concurrent CONNECTs of the tenant wait for it and share it). A
// failed check (no answer) leaves the last decision in place for FailOpen,
// counted per tenant from its first failed check since its last answer; a
// tenant with no decision yet is let in meanwhile. Past FailOpen new CONNECTs
// are refused with an error that is not broker.ErrNotEntitled (0x88). A
// failed check never disconnects anyone.
type Entitlements struct {
	cfg     EntitlementConfig
	tenants Tenants
	chk     Checker
	log     *slog.Logger

	mu     sync.Mutex
	states map[string]*entState
}

// entState is one tenant's decision. Entitlements.mu guards it.
type entState struct {
	// checked is when the last check ended, answered or not.
	checked time.Time
	// known says a check answered; allowed and refusal are its answer.
	known   bool
	allowed bool
	refusal error
	// outageSince is when checks started failing, zero while they answer;
	// lastErr is the latest failure.
	outageSince time.Time
	lastErr     error
	// warnedOpen and warnedClosed keep the outage warnings to one each.
	warnedOpen, warnedClosed bool
	// acted says Run disconnected the tenant for its current refusal.
	acted bool
	// inflight is closed when the check under way ends; nil when none is.
	inflight chan struct{}
}

var _ broker.Entitlement = (*Entitlements)(nil)

// NewEntitlements returns Entitlements over tenants and chk. A non-positive
// Interval, CheckTimeout or FailOpen takes its default, as does an empty
// feature list. A nil logger uses slog.Default().
func NewEntitlements(cfg EntitlementConfig, tenants Tenants, chk Checker, logger *slog.Logger) *Entitlements {
	def := DefaultEntitlementConfig()

	if len(cfg.Features) == 0 {
		cfg.Features = def.Features
	}

	cfg.Features = slices.Clone(cfg.Features)

	if cfg.Interval <= 0 {
		cfg.Interval = def.Interval
	}

	if cfg.CheckTimeout <= 0 {
		cfg.CheckTimeout = def.CheckTimeout
	}

	if cfg.FailOpen <= 0 {
		cfg.FailOpen = def.FailOpen
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &Entitlements{cfg: cfg, tenants: tenants, chk: chk, log: logger, states: make(map[string]*entState)}
}

// Allow implements broker.Entitlement. It answers from memory when the
// tenant's decision is younger than Interval, and otherwise checks first,
// waiting at most CheckTimeout. Not enabled, it lets every tenant in.
func (e *Entitlements) Allow(ctx context.Context, tenantID string) error {
	if !e.cfg.Enabled {
		return nil
	}

	if err := e.decide(ctx, tenantID, false); err != nil {
		return fmt.Errorf("metering: entitlement of tenant %s: %w", tenantID, err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	return e.answerLocked(tenantID, e.stateLocked(tenantID), time.Now())
}

// Run checks every tenant live returns once per Interval, until ctx ends, and
// calls disconnect for a tenant whose check refuses it, once per refusal: a
// tenant that stays refused is not disconnected again, and one allowed again
// can be disconnected the next time it loses. A check that fails disconnects
// nobody. Not enabled, Run returns at once.
//
// The extension passes the broker's LiveTenants and DisconnectTenant with
// 0x97 quota exceeded.
func (e *Entitlements) Run(ctx context.Context, live func() []string, disconnect func(tenantID string)) {
	if !e.cfg.Enabled {
		return
	}

	tick := time.NewTicker(e.cfg.Interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		e.sweep(ctx, live(), disconnect)
		e.prune(time.Now())
	}
}

// sweep checks tenantIDs, runParallel at a time, and disconnects the ones a
// check refused.
func (e *Entitlements) sweep(ctx context.Context, tenantIDs []string, disconnect func(tenantID string)) {
	sem := make(chan struct{}, runParallel)

	var wg sync.WaitGroup

	for _, tenantID := range tenantIDs {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()

			return
		}

		wg.Go(func() {
			defer func() { <-sem }()

			if e.lost(ctx, tenantID) {
				e.log.Info("metering: tenant is no longer entitled, disconnecting it", "tenant", tenantID)
				disconnect(tenantID)
			}
		})
	}

	wg.Wait()
}

// lost checks tenantID now and reports whether the check answered with a
// refusal Run has not acted on yet. It marks the refusal acted on.
func (e *Entitlements) lost(ctx context.Context, tenantID string) bool {
	start := time.Now()

	if e.decide(ctx, tenantID, true) != nil {
		return false
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	s := e.stateLocked(tenantID)

	// Only a check that ended during this call and answered counts. A
	// failed one leaves outageSince set.
	if !s.known || s.allowed || s.acted || !s.outageSince.IsZero() || s.checked.Before(start) {
		return false
	}

	s.acted = true

	return true
}

// prune drops the decisions nobody asked about for longer than Interval plus
// FailOpen: past that they could not be used even during an outage.
func (e *Entitlements) prune(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for tenantID, s := range e.states {
		if s.inflight == nil && now.Sub(s.checked) > e.cfg.Interval+e.cfg.FailOpen {
			delete(e.states, tenantID)
		}
	}
}

// decide makes tenantID's state current. Without force a decision (or failed
// check) younger than Interval is current already; with force only a check
// that ended while decide waited for it is. A check already in flight is
// waited for rather than repeated. It returns ctx's error when ctx ends
// before the state is current.
func (e *Entitlements) decide(ctx context.Context, tenantID string, force bool) error {
	waited := false

	for {
		e.mu.Lock()
		s := e.stateLocked(tenantID)

		if w := s.inflight; w != nil {
			e.mu.Unlock()

			select {
			case <-w:
				waited = true

				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if (!force || waited) && !s.checked.IsZero() && time.Since(s.checked) < e.cfg.Interval {
			e.mu.Unlock()

			return nil
		}

		done := make(chan struct{})
		s.inflight = done
		e.mu.Unlock()

		e.check(ctx, tenantID, s, done)

		return ctx.Err()
	}
}

// check runs tenantID's checks and records the outcome in s, unless ctx
// ended first (the caller went away, which says nothing about the billing
// system). It ends the in-flight check done.
func (e *Entitlements) check(ctx context.Context, tenantID string, s *entState, done chan struct{}) {
	allowed, refusal, err := e.ask(ctx, tenantID)

	e.mu.Lock()
	defer e.mu.Unlock()

	s.inflight = nil
	close(done)

	if ctx.Err() != nil {
		return
	}

	now := time.Now()
	s.checked = now

	if err != nil {
		if s.outageSince.IsZero() {
			s.outageSince = now
			e.log.Warn("metering: entitlement check failed, keeping the last decision", "tenant", tenantID,
				"fail_open", e.cfg.FailOpen, "error", err)
		}

		s.lastErr = err

		return
	}

	if !s.outageSince.IsZero() {
		e.log.Info("metering: entitlement checks answer again", "tenant", tenantID)
	}

	s.known, s.allowed, s.refusal = true, allowed, refusal
	s.outageSince, s.lastErr = time.Time{}, nil
	s.warnedOpen, s.warnedClosed = false, false

	if allowed {
		s.acted = false
	}
}

// ask checks every feature for tenantID within CheckTimeout. It returns
// allowed when all are, the first refusal as an error wrapping
// broker.ErrNotEntitled, or err when there was no answer. The checks run on
// their own goroutine, so a Checker that ignores its context still cannot
// hold the caller past CheckTimeout.
func (e *Entitlements) ask(ctx context.Context, tenantID string) (allowed bool, refusal, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.CheckTimeout)
	defer cancel()

	type answer struct {
		allowed bool
		refusal error
		err     error
	}

	out := make(chan answer, 1)

	go func() {
		var a answer
		a.allowed, a.refusal, a.err = e.askAll(ctx, tenantID)
		out <- a
	}()

	select {
	case a := <-out:
		return a.allowed, a.refusal, a.err
	case <-ctx.Done():
		return false, nil, fmt.Errorf("metering: entitlement check of tenant %s: %w", tenantID, ctx.Err())
	}
}

func (e *Entitlements) askAll(ctx context.Context, tenantID string) (allowed bool, refusal, err error) {
	t, err := e.tenants.Get(ctx, tenantID)
	if err != nil {
		return false, nil, fmt.Errorf("metering: tenant %s: %w", tenantID, err)
	}

	for _, feature := range e.cfg.Features {
		ok, reason, err := e.chk.Check(ctx, t, feature)
		if err != nil {
			return false, nil, fmt.Errorf("metering: checking %s: %w", feature, err)
		}

		if !ok {
			return false, fmt.Errorf("%w: %s: %s", broker.ErrNotEntitled, feature, reason), nil
		}
	}

	return true, nil, nil
}

// answerLocked is Allow's answer from s at now.
func (e *Entitlements) answerLocked(tenantID string, s *entState, now time.Time) error {
	if s.outageSince.IsZero() {
		if !s.known {
			// decide returned without a check that answered; it cannot,
			// short of ctx ending, which Allow handles first.
			return fmt.Errorf("metering: no entitlement decision for tenant %s", tenantID)
		}

		if s.allowed {
			return nil
		}

		return s.refusal
	}

	if age := now.Sub(s.outageSince); age >= e.cfg.FailOpen {
		if !s.warnedClosed {
			s.warnedClosed = true
			e.log.Warn("metering: entitlement checks failing past fail_open, refusing new connections",
				"tenant", tenantID, "since", s.outageSince, "error", s.lastErr)
		}

		return fmt.Errorf("metering: entitlement of tenant %s unknown for %s: %w", tenantID, age.Round(time.Second), s.lastErr)
	}

	if !s.known {
		if !s.warnedOpen {
			s.warnedOpen = true
			e.log.Warn("metering: letting a tenant in without an entitlement decision, checks are failing",
				"tenant", tenantID, "error", s.lastErr)
		}

		return nil
	}

	if s.allowed {
		return nil
	}

	return s.refusal
}

func (e *Entitlements) stateLocked(tenantID string) *entState {
	s := e.states[tenantID]
	if s == nil {
		s = &entState{}
		e.states[tenantID] = s
	}

	return s
}
