package audit_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/audit"
)

// fakeSink records what it is given. When block is set, Record waits on it
// (or on ctx) before recording.
type fakeSink struct {
	block chan struct{}

	mu   sync.Mutex
	evs  []audit.Event
	tens []audit.Tenant
}

func (s *fakeSink) Record(ctx context.Context, ev audit.Event, t audit.Tenant) error {
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.evs = append(s.evs, ev)
	s.tens = append(s.tens, t)

	return nil
}

func (s *fakeSink) events() []audit.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]audit.Event(nil), s.evs...)
}

func (s *fakeSink) tenants() []audit.Tenant {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]audit.Tenant(nil), s.tens...)
}

// fakeTenants answers every tenant with an app and an external ID derived
// from its ID.
type fakeTenants struct{}

func (fakeTenants) Get(_ context.Context, tenantID string) (audit.Tenant, error) {
	if tenantID == "missing" {
		return audit.Tenant{}, errors.New("no such tenant")
	}

	return audit.Tenant{ID: tenantID, AppID: "app-" + tenantID, ExternalID: "ext-" + tenantID}, nil
}

func enabled() audit.Config {
	cfg := audit.DefaultConfig()
	cfg.Enabled = true

	return cfg
}

func stop(t *testing.T, r *audit.Recorder) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, r.Stop(ctx))
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()

	require.Eventually(t, cond, 10*time.Second, 5*time.Millisecond, what)
}

func TestDefaultConfig(t *testing.T) {
	cfg := audit.DefaultConfig()
	require.False(t, cfg.Enabled)
	require.Equal(t, []string{"connect", "disconnect", "auth_failure", "acl_deny", "takeover", "admin"}, cfg.Events)
	require.Equal(t, 10000, cfg.QueueSize)
	require.Equal(t, 4, cfg.Writers)
	require.Equal(t, time.Minute, cfg.DenyWindow)
	require.NoError(t, cfg.Validate())

	cfg.Events = []string{"connect", "publish"}
	require.Error(t, cfg.Validate())
}

// TestEmitNeverBlocks: a sink that never returns cannot slow Emit down. The
// queue fills and the rest is dropped and counted.
func TestEmitNeverBlocks(t *testing.T) {
	sink := &fakeSink{block: make(chan struct{})}
	cfg := enabled()
	cfg.QueueSize = 100

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	ev := audit.Event{Action: audit.ActionConnect, TenantID: "t1", ClientID: "c1"}
	start := time.Now()

	for range 100_000 {
		r.Emit(ev)
	}

	require.Less(t, time.Since(start), time.Second)
	require.Positive(t, r.Dropped())

	close(sink.block)
	stop(t, r)
}

// TestDenyCollapsed: the same denial within the window is recorded once,
// and once more when the window closes, with the count of the rest.
func TestDenyCollapsed(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.Writers = 1 // the sink sees the queue's order
	cfg.DenyWindow = 200 * time.Millisecond

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()
	t.Cleanup(func() { stop(t, r) })

	// One time for all 50, so a slow machine cannot spread them over two
	// windows.
	at := time.Now()

	for range 50 {
		r.Emit(audit.Event{
			Action: audit.ActionACLDenied, Outcome: audit.OutcomeDenied, TenantID: "t1", ClientID: "c1",
			Meta: map[string]any{audit.MetaOp: "publish", audit.MetaTopic: "a/b"}, At: at,
		})
	}

	eventually(t, "the summary of the window", func() bool { return len(sink.events()) == 2 })

	evs := sink.events()
	require.Equal(t, audit.ActionACLDenied, evs[0].Action)
	require.NotContains(t, evs[0].Meta, audit.MetaSuppressed)
	require.Equal(t, 49, evs[1].Meta[audit.MetaSuppressed])
	require.Equal(t, "a/b", evs[1].Meta[audit.MetaTopic])

	// Another topic is another key, and the window has closed for the first.
	r.Emit(audit.Event{
		Action: audit.ActionACLDenied, TenantID: "t1", ClientID: "c1",
		Meta: map[string]any{audit.MetaOp: "publish", audit.MetaTopic: "a/c"},
	})
	r.Emit(audit.Event{
		Action: audit.ActionACLDenied, TenantID: "t1", ClientID: "c1",
		Meta: map[string]any{audit.MetaOp: "publish", audit.MetaTopic: "a/b"},
	})

	eventually(t, "two fresh denials", func() bool { return len(sink.events()) == 4 })
}

// TestDenyAloneNoSummary: one denial in a window gets no summary.
func TestDenyAloneNoSummary(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.DenyWindow = 50 * time.Millisecond

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	r.Emit(audit.Event{Action: audit.ActionACLDenied, TenantID: "t1", ClientID: "c1",
		Meta: map[string]any{audit.MetaOp: "subscribe", audit.MetaTopic: "#"}})

	eventually(t, "the denial", func() bool { return len(sink.events()) == 1 })
	time.Sleep(200 * time.Millisecond)
	stop(t, r)

	require.Len(t, sink.events(), 1)
}

// TestDenyWindowEndedBeforeSweep: a denial after a window ended but before
// the sweep closed it closes the window itself, so the repeats it counted
// still get their summary.
func TestDenyWindowEndedBeforeSweep(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.DenyWindow = time.Hour
	cfg.Writers = 1 // the sink sees the queue's order

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	deny := func(at time.Time) {
		r.Emit(audit.Event{Action: audit.ActionACLDenied, TenantID: "t1", ClientID: "c1", At: at,
			Meta: map[string]any{audit.MetaOp: "publish", audit.MetaTopic: "x"}})
	}

	start := time.Now()
	deny(start)

	for i := range 10 {
		deny(start.Add(time.Duration(i+1) * time.Minute))
	}

	// The sweep ticks every 15 minutes of wall time, so it has not run.
	deny(start.Add(time.Hour + time.Second))

	eventually(t, "the first denial, the summary and the new one", func() bool { return len(sink.events()) == 3 })

	evs := sink.events()
	require.NotContains(t, evs[0].Meta, audit.MetaSuppressed)
	require.Equal(t, 10, evs[1].Meta[audit.MetaSuppressed])
	require.Equal(t, start.Add(10*time.Minute), evs[1].At, "the summary is the latest repeat")
	require.NotContains(t, evs[2].Meta, audit.MetaSuppressed)

	// The new window counted nothing, so Stop adds no summary.
	stop(t, r)
	require.Len(t, sink.events(), 3)
}

func TestEventsFilter(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.Events = []string{audit.KindAuthFailure, audit.KindAdmin}

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	for _, a := range []string{
		audit.ActionConnect, audit.ActionDisconnect, audit.ActionAuthFailed, audit.ActionACLDenied,
		audit.ActionTakeover, audit.ActionDeviceProvision, audit.ActionTenantDelete,
	} {
		r.Emit(audit.Event{Action: a, TenantID: "t1"})
	}

	stop(t, r)

	evs := sink.events()
	got := make([]string, 0, len(evs))

	for _, ev := range evs {
		got = append(got, ev.Action)
	}

	require.ElementsMatch(t, []string{audit.ActionAuthFailed, audit.ActionDeviceProvision, audit.ActionTenantDelete}, got)
}

func TestDisabledRecordsNothing(t *testing.T) {
	sink := &fakeSink{}
	r := audit.NewRecorder(audit.DefaultConfig(), sink, fakeTenants{}, nil)
	r.Start()

	r.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t1"})
	stop(t, r)

	require.Empty(t, sink.events())
	require.Zero(t, r.Dropped())
}

// TestStopDrains: Stop writes everything queued before it returns.
func TestStopDrains(t *testing.T) {
	sink := &fakeSink{block: make(chan struct{})}
	cfg := enabled()
	cfg.Writers = 2

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	for i := range 500 {
		r.Emit(audit.Event{Action: audit.ActionDisconnect, TenantID: "t1", ClientID: fmt.Sprintf("c%d", i)})
	}

	close(sink.block)
	stop(t, r)

	require.Len(t, sink.events(), 500)
	require.Zero(t, r.Dropped())

	// After Stop nothing is taken.
	r.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t1"})
	require.Len(t, sink.events(), 500)
}

// TestStopDrainsDenySummaries: a window still open at Stop gets its summary.
func TestStopDrainsDenySummaries(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.Writers = 1 // the sink sees the queue's order
	cfg.DenyWindow = time.Hour

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	for range 3 {
		r.Emit(audit.Event{Action: audit.ActionACLDenied, TenantID: "t1", ClientID: "c1",
			Meta: map[string]any{audit.MetaOp: "publish", audit.MetaTopic: "x"}})
	}

	stop(t, r)

	evs := sink.events()
	require.Len(t, evs, 2)
	require.Equal(t, 2, evs[1].Meta[audit.MetaSuppressed])
}

// TestStopBounded: a sink that never returns cannot hold Stop past its ctx.
func TestStopBounded(t *testing.T) {
	sink := &fakeSink{block: make(chan struct{})}
	r := audit.NewRecorder(enabled(), sink, fakeTenants{}, nil)
	r.Start()

	r.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t1"})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, r.Stop(ctx), context.DeadlineExceeded)
	close(sink.block)
}

// TestRecorderTenantPair: the sink gets the event's own tenant, looked up by
// its Tether ID; a lookup that fails still records, with the ID alone.
func TestRecorderTenantPair(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.Writers = 1

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	r.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t1", ClientID: "a"})
	r.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t2", ClientID: "b"})
	r.Emit(audit.Event{Action: audit.ActionTenantDelete, TenantID: "missing"})
	r.Emit(audit.Event{Action: audit.ActionTenantDelete, TenantID: "gone",
		Meta: map[string]any{audit.MetaAppID: "app_g", audit.MetaExternalID: "org_g"}})
	stop(t, r)

	evs, tens := sink.events(), sink.tenants()
	require.Len(t, evs, 4)

	for i, ev := range evs {
		switch ev.TenantID {
		case "missing":
			require.Equal(t, audit.Tenant{ID: "missing"}, tens[i])
		case "gone":
			require.Equal(t, audit.Tenant{ID: "gone", AppID: "app_g", ExternalID: "org_g"}, tens[i],
				"a pair on the event wins over the lookup")
		default:
			require.Equal(t, audit.Tenant{ID: ev.TenantID, AppID: "app-" + ev.TenantID, ExternalID: "ext-" + ev.TenantID}, tens[i])
		}

		require.False(t, ev.At.IsZero(), "Emit stamps the time")
	}
}

func TestKind(t *testing.T) {
	require.Equal(t, audit.KindConnect, audit.Kind(audit.ActionConnect))
	require.Equal(t, audit.KindDisconnect, audit.Kind(audit.ActionDisconnect))
	require.Equal(t, audit.KindAuthFailure, audit.Kind(audit.ActionAuthFailed))
	require.Equal(t, audit.KindACLDeny, audit.Kind(audit.ActionACLDenied))
	require.Equal(t, audit.KindTakeover, audit.Kind(audit.ActionTakeover))
	require.Equal(t, audit.KindAdmin, audit.Kind(audit.ActionRetainedDelete))
	require.Empty(t, audit.Kind("mqtt.publish"))
}

// errSink fails every Record.
type errSink struct{}

func (errSink) Record(context.Context, audit.Event, audit.Tenant) error {
	return errors.New("chronicle down")
}

// TestFailuresCounted: an event the sink refused is counted in Failures, not
// in Dropped.
func TestFailuresCounted(t *testing.T) {
	r := audit.NewRecorder(enabled(), errSink{}, fakeTenants{}, nil)
	r.Start()

	for range 5 {
		r.Emit(audit.Event{Action: audit.ActionConnect, TenantID: "t1"})
	}

	stop(t, r)

	require.Equal(t, uint64(5), r.Failures())
	require.Zero(t, r.Dropped())
}

// authFailure is a refused CONNECT from remote, as the broker emits it.
func authFailure(tenantID, user, reason, remote string, at time.Time) audit.Event {
	return audit.Event{
		Action: audit.ActionAuthFailed, Outcome: audit.OutcomeFailure, TenantID: tenantID,
		ClientID: fmt.Sprintf("c-%d", at.UnixNano()), Subject: user, Reason: reason,
		Meta: map[string]any{"reason_code": 0x86, audit.MetaRemote: remote}, At: at,
	}
}

// TestAuthFailureCollapsed (ruling R23): a credential-spraying client costs
// one event per (tenant, reason, remote host, username) and window, plus one
// with the count of the rest. Client IDs and source ports do not matter.
func TestAuthFailureCollapsed(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.Writers = 1 // the sink sees the queue's order
	cfg.DenyWindow = 200 * time.Millisecond

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()
	t.Cleanup(func() { stop(t, r) })

	at := time.Now()

	for i := range 50 {
		r.Emit(authFailure("", "akey_spray", "bad_credentials", fmt.Sprintf("10.0.0.9:%d", 40000+i), at))
	}

	eventually(t, "the summary of the window", func() bool { return len(sink.events()) == 2 })

	evs := sink.events()
	require.Equal(t, audit.ActionAuthFailed, evs[0].Action)
	require.NotContains(t, evs[0].Meta, audit.MetaSuppressed)
	require.Equal(t, 49, evs[1].Meta[audit.MetaSuppressed])
	require.Equal(t, "akey_spray", evs[1].Subject)

	// Each of tenant, reason, host and username is its own key.
	later := time.Now()
	r.Emit(authFailure("", "akey_spray", "bad_credentials", "10.0.0.10:1", later))
	r.Emit(authFailure("", "akey_other", "bad_credentials", "10.0.0.9:1", later))
	r.Emit(authFailure("", "akey_spray", "rate_limited", "10.0.0.9:1", later))
	r.Emit(authFailure("t1", "akey_spray", "bad_credentials", "10.0.0.9:1", later))
	r.Emit(authFailure("", "akey_spray", "bad_credentials", "[2001:db8::1]:1", later))
	r.Emit(authFailure("", "akey_spray", "bad_credentials", "[2001:db8::1]:2", later)) // a repeat

	eventually(t, "five fresh failures", func() bool { return len(sink.events()) == 7 })
}

// TestAuthFailureWindowEndedBeforeSweep: a failure after a window ended but
// before the sweep closed it closes the window itself, so the repeats it
// counted still get their summary.
func TestAuthFailureWindowEndedBeforeSweep(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.DenyWindow = time.Hour
	cfg.Writers = 1 // the sink sees the queue's order

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	fail := func(at time.Time, port int) {
		r.Emit(authFailure("", "akey_spray", "bad_credentials", fmt.Sprintf("10.0.0.9:%d", port), at))
	}

	start := time.Now()
	fail(start, 1)

	for i := range 10 {
		fail(start.Add(time.Duration(i+1)*time.Minute), i+2)
	}

	// The sweep ticks every 15 minutes of wall time, so it has not run.
	fail(start.Add(time.Hour+time.Second), 99)

	eventually(t, "the first failure, the summary and the new one", func() bool { return len(sink.events()) == 3 })

	evs := sink.events()
	require.NotContains(t, evs[0].Meta, audit.MetaSuppressed)
	require.Equal(t, 10, evs[1].Meta[audit.MetaSuppressed])
	require.Equal(t, start.Add(10*time.Minute), evs[1].At, "the summary is the latest repeat")
	require.NotContains(t, evs[2].Meta, audit.MetaSuppressed)

	// The new window counted nothing, so Stop adds no summary.
	stop(t, r)
	require.Len(t, sink.events(), 3)
}

// TestAuthFailureStopDrainsSummary: one refused CONNECT in a window gets no
// summary, and Stop records the summary of a window still open.
func TestAuthFailureStopDrainsSummary(t *testing.T) {
	sink := &fakeSink{}
	cfg := enabled()
	cfg.DenyWindow = time.Hour

	r := audit.NewRecorder(cfg, sink, fakeTenants{}, nil)
	r.Start()

	at := time.Now()
	r.Emit(authFailure("", "akey_one", "bad_credentials", "10.0.0.1:1", at))

	for i := range 3 {
		r.Emit(authFailure("", "akey_spray", "bad_credentials", fmt.Sprintf("10.0.0.9:%d", i), at))
	}

	stop(t, r)

	evs := sink.events()
	require.Len(t, evs, 3, "one alone, and the spray's first and summary")

	summaries := 0

	for _, ev := range evs {
		if n, ok := ev.Meta[audit.MetaSuppressed]; ok {
			summaries++

			require.Equal(t, 2, n)
			require.Equal(t, "akey_spray", ev.Subject)
		}
	}

	require.Equal(t, 1, summaries)
}
