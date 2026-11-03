package metering

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/xraph/tether/broker"
)

// Meter sends tenant usage deltas to a Sink, one window per interval.
type Meter struct {
	cfg     Config
	src     Source
	tenants Tenants
	sink    Sink
	nodeID  string
	log     *slog.Logger

	// flushMu serialises windows. Everything below it belongs to the holder.
	flushMu sync.Mutex
	window  uint64
	started int64
	// accepted is the cumulative value the sink last took, per tenant and
	// feature key.
	accepted map[pair]uint64

	// runMu guards the loop's lifecycle.
	runMu  sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

type pair struct{ tenant, feature string }

// New returns a Meter. nodeID names this broker node in the keys it sends:
// the cluster node ID, or "single" without a cluster. The window counts from
// 1 per Meter, and the source's start time keeps a restarted node's keys
// apart from its earlier run's. A nil logger uses slog.Default().
func New(cfg Config, src Source, tenants Tenants, sink Sink, nodeID string, logger *slog.Logger) *Meter {
	if logger == nil {
		logger = slog.Default()
	}

	return &Meter{
		cfg: cfg, src: src, tenants: tenants, sink: sink, nodeID: nodeID, log: logger,
		accepted: make(map[pair]uint64),
	}
}

// Start begins the window loop. It does nothing when metering is not
// enabled or the loop already runs.
func (m *Meter) Start() {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	if !m.cfg.Enabled || m.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})

	go m.loop(ctx, m.done)
}

func (m *Meter) loop(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	tick := time.NewTicker(m.cfg.Interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			wctx, cancel := context.WithTimeout(ctx, m.cfg.Interval)

			if err := m.Flush(wctx); err != nil && ctx.Err() == nil {
				m.log.Warn("metering: window incomplete, retrying next window", "error", err)
			}

			cancel()
		}
	}
}

// Stop ends the loop and sends one last window, bounded by ctx. It does
// nothing when the Meter was never started or is already stopped. Usage a
// crash or a failed last window leaves behind is lost: at most one interval
// per node.
func (m *Meter) Stop(ctx context.Context) error {
	m.runMu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel, m.done = nil, nil
	m.runMu.Unlock()

	if cancel == nil {
		return nil
	}

	cancel()

	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("metering: stopping: %w", ctx.Err())
	}

	return m.Flush(ctx)
}

// Flush runs one window: for each tenant and enabled feature it sends the
// cumulative value minus what the sink last accepted, when that is positive.
// It returns the errors of every delta the sink did not take, joined. Those
// deltas go out in a later window, with the growth since. A delta the sink
// rejected (ErrRejected) is logged and dropped, and is not an error here.
// While the source's StartedAt is zero (the broker is not started) it sends
// nothing and the window is not counted.
func (m *Meter) Flush(ctx context.Context) error {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()

	// Ruling R13: the broker has not started yet. A window now would key
	// its sends with no start time and send the same usage again once
	// StartedAt is set, so it sends nothing and the window does not count.
	started := m.src.StartedAt()
	if started.IsZero() {
		return nil
	}

	m.window++

	if started.UnixNano() != m.started {
		// The broker restarted and its counters began again from zero.
		m.started = started.UnixNano()
		clear(m.accepted)
	}

	var errs []error

	for _, u := range m.src.Usage() {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())

			break
		}

		errs = append(errs, m.flushTenant(ctx, u)...)
	}

	return errors.Join(errs...)
}

// delta is one pending send.
type delta struct {
	feature string
	cum     uint64
	qty     int64
}

func (m *Meter) flushTenant(ctx context.Context, u broker.TenantUsage) []error {
	var pending []delta

	for _, f := range m.cfg.Features.list() {
		if f.key == "" {
			continue
		}

		cum := f.read(u)

		last := m.accepted[pair{u.Tenant, f.key}]
		if cum <= last {
			continue
		}

		pending = append(pending, delta{feature: f.key, cum: cum, qty: clampInt64(cum - last)})
	}

	if len(pending) == 0 {
		return nil
	}

	t, err := m.tenants.Get(ctx, u.Tenant)
	if err != nil {
		return []error{fmt.Errorf("metering: tenant %s: %w", u.Tenant, err)}
	}

	var errs []error

	for _, d := range pending {
		key := fmt.Sprintf("tether:%s:%d:%d:%s:%s", m.nodeID, m.started, m.window, u.Tenant, d.feature)

		err := m.sink.Meter(ctx, t, d.feature, d.qty, key)

		switch {
		case err == nil:
			m.accepted[pair{u.Tenant, d.feature}] = d.cum
		case errors.Is(err, ErrRejected):
			m.log.Error("metering: sink rejected usage, dropping it",
				"tenant", u.Tenant, "feature", d.feature, "quantity", d.qty, "key", key, "error", err)

			m.accepted[pair{u.Tenant, d.feature}] = d.cum
		default:
			errs = append(errs, fmt.Errorf("metering: tenant %s feature %s: %w", u.Tenant, d.feature, err))
		}
	}

	return errs
}

func clampInt64(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(v)
}
