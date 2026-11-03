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
// also reports an empty feature list, a non-positive interval, check timeout
// or fail-open, and a check timeout of MaxCheckTimeout (10 s) or more.
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

	if c.CheckTimeout >= MaxCheckTimeout {
		return fmt.Errorf("metering: entitlements.check_timeout must be under %s (the broker's CONNECT bound), got %s",
			MaxCheckTimeout, c.CheckTimeout)
	}

	return nil
}

// MaxCheckTimeout is the bound EntitlementConfig.Validate holds CheckTimeout
// under: the broker gives a whole CONNECT 10 s, and a check that took all of
// it would leave nothing for the rest.
const MaxCheckTimeout = 10 * time.Second

// runParallel is how many tenants Run checks at once.
const runParallel = 8

// connectChecks is how many CONNECT-time checks (an undecided tenant's
// blocking check and the background refreshes) may run at once on the node
// (ruling R16). Run's checks do not count against it.
const connectChecks = 32

// errCheckBusy is the failure a CONNECT-time check records when the node
// already runs connectChecks of them.
var errCheckBusy = errors.New("metering: too many entitlement checks in flight")

// errNoAnswer is the failure an undecided tenant's CONNECT reports when its
// wait for a check ran out before any check recorded one.
var errNoAnswer = errors.New("metering: no entitlement answer in time")

// Entitlements answers the broker's CONNECT check (broker.Entitlement) from
// decisions it keeps per tenant, and Run re-checks the live tenants every
// Interval, disconnecting a tenant that lost its entitlement.
//
// A tenant with a decision or a failed check younger than Interval plus
// FailOpen is answered from memory at once (ruling R14). When its last check
// is older than Interval, a background refresh starts, one per tenant at a
// time, bounded by CheckTimeout, and the caller does not wait for it. Only a
// tenant with nothing remembered waits, for CheckTimeout at most in all.
//
// A failed check (no answer) leaves a tenant's last decision in place for
// FailOpen from its own first failed check since its last answer. A tenant
// with no decision is let in while the node's outage clock (the first failed
// check of any tenant with no successful check since) is younger than
// FailOpen, and refused with an error that is not broker.ErrNotEntitled
// (0x88) after it (ruling R15). A decided tenant past its own FailOpen is
// refused the same way. A failed check never disconnects anyone.
//
// At most 32 CONNECT-time checks run at once on the node; one that cannot
// start counts as a failed check (ruling R16).
type Entitlements struct {
	cfg     EntitlementConfig
	tenants Tenants
	chk     Checker
	log     *slog.Logger

	// slots holds one token per CONNECT-time check in flight.
	slots chan struct{}

	mu     sync.Mutex
	states map[string]*entState
	// nodeOutageSince is when a check of any tenant first failed with no
	// successful check since; zero while checks answer.
	nodeOutageSince time.Time
}

// entState is one tenant's decision. Entitlements.mu guards it.
type entState struct {
	// checked is when the last check ended, answered or not.
	checked time.Time
	// known says a check answered; allowed and refusal are its answer.
	known   bool
	allowed bool
	refusal error
	// outageSince is when this tenant's checks started failing, zero while
	// they answer; lastErr is the latest failure.
	outageSince time.Time
	lastErr     error
	// warnedOpen and warnedClosed keep the outage warnings to one each.
	warnedOpen, warnedClosed bool
	// acted says Run disconnected the tenant for its current refusal.
	acted bool
	// inflight is closed when the check under way ends; nil when none is.
	inflight chan struct{}
}

// result is what one tenant's checks came to.
type result struct {
	allowed bool
	refusal error
	err     error
}

var _ broker.Entitlement = (*Entitlements)(nil)

// NewEntitlements returns Entitlements over tenants and chk. A non-positive
// Interval, CheckTimeout or FailOpen takes its default, as does an empty
// feature list. A nil logger uses slog.Default(). It panics when tenants or
// chk is nil: that is a wiring mistake, and it shows at construction rather
// than at the first CONNECT.
func NewEntitlements(cfg EntitlementConfig, tenants Tenants, chk Checker, logger *slog.Logger) *Entitlements {
	if tenants == nil || chk == nil {
		panic("metering: NewEntitlements needs non-nil Tenants and Checker")
	}

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

	return &Entitlements{
		cfg: cfg, tenants: tenants, chk: chk, log: logger,
		slots:  make(chan struct{}, connectChecks),
		states: make(map[string]*entState),
	}
}

// Allow implements broker.Entitlement. A tenant with anything remembered is
// answered at once (refreshed in the background when its last check is older
// than Interval); one with nothing remembered waits for a check, CheckTimeout
// at most in all. Not enabled, it lets every tenant in.
func (e *Entitlements) Allow(ctx context.Context, tenantID string) error {
	if !e.cfg.Enabled {
		return nil
	}

	deadline := time.Now().Add(e.cfg.CheckTimeout)

	for {
		e.mu.Lock()
		s := e.stateLocked(tenantID)
		now := time.Now()

		if e.rememberedLocked(s, now) {
			if s.inflight == nil && now.Sub(s.checked) >= e.cfg.Interval {
				e.refreshLocked(tenantID, s)
			}

			err := e.answerLocked(tenantID, s, now)
			e.mu.Unlock()

			return err
		}

		if w := s.inflight; w != nil {
			e.mu.Unlock()

			if err := waitUntil(ctx, w, deadline); err != nil {
				return e.gaveUp(ctx, tenantID)
			}

			continue
		}

		timeout := time.Until(deadline)
		if timeout <= 0 {
			e.mu.Unlock()

			return e.gaveUp(ctx, tenantID)
		}

		done := make(chan struct{})
		s.inflight = done

		if !e.takeSlot() {
			e.recordLocked(tenantID, s, done, result{err: errCheckBusy})
			err := e.answerLocked(tenantID, s, now)
			e.mu.Unlock()

			return err
		}

		e.mu.Unlock()

		r := e.ask(ctx, tenantID, timeout)
		e.releaseSlot()

		e.mu.Lock()

		if ctx.Err() != nil {
			// The caller went away, which says nothing about ledger.
			e.endLocked(s, done)
			e.mu.Unlock()

			return fmt.Errorf("metering: entitlement of tenant %s: %w", tenantID, ctx.Err())
		}

		e.recordLocked(tenantID, s, done, r)
		err := e.answerLocked(tenantID, s, time.Now())
		e.mu.Unlock()

		return err
	}
}

// gaveUp is Allow's answer for a tenant whose wait ran out: ctx's error when
// ctx ended, else the outage rules for what is remembered by now.
func (e *Entitlements) gaveUp(ctx context.Context, tenantID string) error {
	if ctx.Err() != nil {
		return fmt.Errorf("metering: entitlement of tenant %s: %w", tenantID, ctx.Err())
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	return e.answerLocked(tenantID, e.stateLocked(tenantID), time.Now())
}

// waitUntil waits for w to close, and returns an error when ctx ends or the
// deadline passes first.
func waitUntil(ctx context.Context, w <-chan struct{}, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()

	select {
	case <-w:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errNoAnswer
	}
}

// rememberedLocked reports whether s holds a decision or a failed check
// younger than Interval plus FailOpen.
func (e *Entitlements) rememberedLocked(s *entState, now time.Time) bool {
	return (s.known || !s.outageSince.IsZero()) && now.Sub(s.checked) < e.cfg.Interval+e.cfg.FailOpen
}

// refreshLocked starts a background check of tenantID, which nobody waits
// for. Without a free slot it records a failed check instead.
func (e *Entitlements) refreshLocked(tenantID string, s *entState) {
	done := make(chan struct{})
	s.inflight = done

	if !e.takeSlot() {
		e.recordLocked(tenantID, s, done, result{err: errCheckBusy})

		return
	}

	go func() {
		r := e.ask(context.Background(), tenantID, e.cfg.CheckTimeout)
		e.releaseSlot()

		e.mu.Lock()
		e.recordLocked(tenantID, s, done, r)
		e.mu.Unlock()
	}()
}

func (e *Entitlements) takeSlot() bool {
	select {
	case e.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (e *Entitlements) releaseSlot() { <-e.slots }

// Run checks every tenant live returns once per Interval, until ctx ends, and
// calls disconnect for a tenant whose check refuses it, once per refusal: a
// tenant that stays refused is not disconnected again, and one allowed again
// can be disconnected the next time it loses. A check that fails disconnects
// nobody. Run's checks are not limited by the CONNECT-time limit; it checks
// 8 tenants at a time. Not enabled, Run returns at once.
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

	if e.forceCheck(ctx, tenantID) != nil {
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

// forceCheck checks tenantID now for Run. A check already in flight (a
// CONNECT's or a refresh, each bounded by CheckTimeout) is waited for and
// taken as this one. It returns ctx's error when ctx ends first.
func (e *Entitlements) forceCheck(ctx context.Context, tenantID string) error {
	e.mu.Lock()
	s := e.stateLocked(tenantID)

	if w := s.inflight; w != nil {
		e.mu.Unlock()

		select {
		case <-w:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	done := make(chan struct{})
	s.inflight = done
	e.mu.Unlock()

	r := e.ask(ctx, tenantID, e.cfg.CheckTimeout)

	e.mu.Lock()
	defer e.mu.Unlock()

	if ctx.Err() != nil {
		e.endLocked(s, done)

		return ctx.Err()
	}

	e.recordLocked(tenantID, s, done, r)

	return nil
}

// prune drops the states nobody asked about for longer than Interval plus
// FailOpen: past that they are not used even during an outage.
func (e *Entitlements) prune(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for tenantID, s := range e.states {
		if s.inflight == nil && now.Sub(s.checked) > e.cfg.Interval+e.cfg.FailOpen {
			delete(e.states, tenantID)
		}
	}
}

// endLocked ends the in-flight check done of s without recording anything.
func (e *Entitlements) endLocked(s *entState, done chan struct{}) {
	if s.inflight == done {
		s.inflight = nil
	}

	close(done)
}

// recordLocked records r as tenantID's latest check, ends the in-flight check
// done and moves the node's outage clock.
func (e *Entitlements) recordLocked(tenantID string, s *entState, done chan struct{}, r result) {
	e.endLocked(s, done)

	now := time.Now()
	s.checked = now

	if r.err != nil {
		if e.nodeOutageSince.IsZero() {
			e.nodeOutageSince = now
		}

		if s.outageSince.IsZero() {
			s.outageSince = now
			e.log.Warn("metering: entitlement check failed, keeping the last decision", "tenant", tenantID,
				"fail_open", e.cfg.FailOpen, "error", r.err)
		}

		s.lastErr = r.err

		return
	}

	e.nodeOutageSince = time.Time{}

	if !s.outageSince.IsZero() {
		e.log.Info("metering: entitlement checks answer again", "tenant", tenantID)
	}

	s.known, s.allowed, s.refusal = true, r.allowed, r.refusal
	s.outageSince, s.lastErr = time.Time{}, nil
	s.warnedOpen, s.warnedClosed = false, false

	if r.allowed {
		s.acted = false
	}
}

// ask checks every feature for tenantID within timeout. The checks run on
// their own goroutine, so a Checker that ignores its context still cannot
// hold the caller past timeout.
func (e *Entitlements) ask(ctx context.Context, tenantID string, timeout time.Duration) result {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	out := make(chan result, 1)

	go func() { out <- e.askAll(ctx, tenantID) }()

	select {
	case r := <-out:
		return r
	case <-ctx.Done():
		return result{err: fmt.Errorf("metering: entitlement check of tenant %s: %w", tenantID, ctx.Err())}
	}
}

// askAll is allowed when every feature is, the first refusal as an error
// wrapping broker.ErrNotEntitled, or err when there was no answer.
func (e *Entitlements) askAll(ctx context.Context, tenantID string) result {
	t, err := e.tenants.Get(ctx, tenantID)
	if err != nil {
		return result{err: fmt.Errorf("metering: tenant %s: %w", tenantID, err)}
	}

	for _, feature := range e.cfg.Features {
		ok, reason, err := e.chk.Check(ctx, t, feature)
		if err != nil {
			return result{err: fmt.Errorf("metering: checking %s: %w", feature, err)}
		}

		if !ok {
			return result{refusal: fmt.Errorf("%w: %s: %s", broker.ErrNotEntitled, feature, reason)}
		}
	}

	return result{allowed: true}
}

// answerLocked is Allow's answer from s at now.
func (e *Entitlements) answerLocked(tenantID string, s *entState, now time.Time) error {
	cause := s.lastErr
	if cause == nil {
		cause = errNoAnswer
	}

	if s.known {
		if s.outageSince.IsZero() || now.Sub(s.outageSince) < e.cfg.FailOpen {
			if s.allowed {
				return nil
			}

			return s.refusal
		}

		age := now.Sub(s.outageSince)
		e.warnClosedLocked(tenantID, s, age)

		return fmt.Errorf("metering: entitlement of tenant %s unknown for %s: %w", tenantID, age.Round(time.Second), cause)
	}

	// No decision: the node's outage clock decides (ruling R15).
	if !e.nodeOutageSince.IsZero() && now.Sub(e.nodeOutageSince) >= e.cfg.FailOpen {
		age := now.Sub(e.nodeOutageSince)
		e.warnClosedLocked(tenantID, s, age)

		return fmt.Errorf("metering: no entitlement decision for tenant %s, checks failing for %s: %w",
			tenantID, age.Round(time.Second), cause)
	}

	if !s.warnedOpen {
		s.warnedOpen = true
		e.log.Warn("metering: letting a tenant in without an entitlement decision, checks are failing",
			"tenant", tenantID, "error", cause)
	}

	return nil
}

func (e *Entitlements) warnClosedLocked(tenantID string, s *entState, age time.Duration) {
	if s.warnedClosed {
		return
	}

	s.warnedClosed = true
	e.log.Warn("metering: entitlement checks failing past fail_open, refusing new connections",
		"tenant", tenantID, "for", age, "error", s.lastErr)
}

func (e *Entitlements) stateLocked(tenantID string) *entState {
	s := e.states[tenantID]
	if s == nil {
		s = &entState{}
		e.states[tenantID] = s
	}

	return s
}
