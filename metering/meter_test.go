package metering_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/metering"
)

type fakeSource struct {
	mu      sync.Mutex
	usage   []broker.TenantUsage
	started time.Time
}

func (s *fakeSource) Usage() []broker.TenantUsage {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]broker.TenantUsage(nil), s.usage...)
}

func (s *fakeSource) StartedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.started
}

func (s *fakeSource) set(u ...broker.TenantUsage) {
	s.mu.Lock()
	s.usage = u
	s.mu.Unlock()
}

func (s *fakeSource) restart(at time.Time, u ...broker.TenantUsage) {
	s.mu.Lock()
	s.started = at
	s.usage = u
	s.mu.Unlock()
}

type fakeTenants struct {
	mu    sync.Mutex
	byID  map[string]metering.Tenant
	gets  map[string]int
	fails map[string]error
}

func newTenants(ts ...metering.Tenant) *fakeTenants {
	f := &fakeTenants{byID: map[string]metering.Tenant{}, gets: map[string]int{}, fails: map[string]error{}}
	for _, t := range ts {
		f.byID[t.ID] = t
	}

	return f
}

func (f *fakeTenants) Get(_ context.Context, id string) (metering.Tenant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.gets[id]++

	if err := f.fails[id]; err != nil {
		return metering.Tenant{}, err
	}

	t, ok := f.byID[id]
	if !ok {
		return metering.Tenant{}, errors.New("no such tenant")
	}

	return t, nil
}

type call struct {
	Tenant  metering.Tenant
	Feature string
	Qty     int64
	Key     string
}

type fakeSink struct {
	mu    sync.Mutex
	calls []call
	// fail decides the error for a call; nil means accept.
	fail func(c call) error
}

func (s *fakeSink) Meter(_ context.Context, t metering.Tenant, feature string, qty int64, key string) error {
	c := call{Tenant: t, Feature: feature, Qty: qty, Key: key}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail != nil {
		if err := s.fail(c); err != nil {
			return err
		}
	}

	s.calls = append(s.calls, c)

	return nil
}

func (s *fakeSink) accepted() []call {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]call(nil), s.calls...)
}

func (s *fakeSink) totalMessagesIn(tenantID string) int64 {
	var n int64

	for _, c := range s.accepted() {
		if c.Tenant.ID == tenantID && c.Feature == "tether.messages.in" {
			n += c.Qty
		}
	}

	return n
}

var (
	t0   = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	acme = metering.Tenant{ID: "ttnt_acme", AppID: "app_1", ExternalID: "ext-acme"}
	zeta = metering.Tenant{ID: "ttnt_zeta", AppID: "app_2", ExternalID: "ext-zeta"}
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)) }

type rig struct {
	src     *fakeSource
	tenants *fakeTenants
	sink    *fakeSink
	meter   *metering.Meter
}

func newRig(t *testing.T, cfg metering.Config, ts ...metering.Tenant) *rig {
	t.Helper()

	r := &rig{src: &fakeSource{started: t0}, tenants: newTenants(ts...), sink: &fakeSink{}}
	r.meter = metering.New(cfg, r.src, r.tenants, r.sink, "node-a", quietLogger())

	return r
}

func flush(t *testing.T, m *metering.Meter) {
	t.Helper()

	if err := m.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}

func onlyMessagesIn() metering.Config {
	cfg := metering.DefaultConfig()
	cfg.Features = metering.Features{MessagesIn: "tether.messages.in"}

	return cfg
}

func TestDefaultConfig(t *testing.T) {
	cfg := metering.DefaultConfig()

	want := metering.Features{
		MessagesIn:        "tether.messages.in",
		MessagesOut:       "tether.messages.out",
		BytesIn:           "tether.bytes.in",
		BytesOut:          "tether.bytes.out",
		ConnectionMinutes: "tether.connection.minutes",
	}

	if cfg.Interval != time.Minute || cfg.Features != want {
		t.Fatalf("got %+v", cfg)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config: %v", err)
	}
}

func TestDeltasPerWindow(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 5})
	flush(t, r.meter)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 12})
	flush(t, r.meter)

	// Nothing grew: nothing is sent.
	flush(t, r.meter)

	got := r.sink.accepted()
	if len(got) != 2 || got[0].Qty != 5 || got[1].Qty != 7 {
		t.Fatalf("got %+v, want quantities 5 then 7", got)
	}

	if got[0].Feature != "tether.messages.in" || got[0].Tenant != acme {
		t.Fatalf("got %+v", got[0])
	}
}

func TestKeyFormat(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 1})
	flush(t, r.meter)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 2})
	flush(t, r.meter)

	got := r.sink.accepted()
	prefix := fmt.Sprintf("tether:node-a:%d:", t0.UnixNano())

	if want := prefix + "1:ttnt_acme:tether.messages.in"; got[0].Key != want {
		t.Fatalf("key %q, want %q", got[0].Key, want)
	}

	if want := prefix + "2:ttnt_acme:tether.messages.in"; got[1].Key != want {
		t.Fatalf("key %q, want %q", got[1].Key, want)
	}
}

func TestEveryFeature(t *testing.T) {
	r := newRig(t, metering.DefaultConfig(), acme)

	r.src.set(broker.TenantUsage{
		Tenant: acme.ID, MessagesIn: 1, MessagesOut: 2, BytesIn: 3, BytesOut: 4, ConnectionTime: 5 * time.Minute,
	})
	flush(t, r.meter)

	want := map[string]int64{
		"tether.messages.in": 1, "tether.messages.out": 2, "tether.bytes.in": 3, "tether.bytes.out": 4,
		"tether.connection.minutes": 5,
	}

	got := map[string]int64{}
	for _, c := range r.sink.accepted() {
		got[c.Feature] = c.Qty
	}

	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: got %d, want %d", k, got[k], v)
		}
	}
}

// Review Focus 2: a failed window loses nothing and counts nothing twice.
func TestRetryAfterSinkErrorNoLossNoDouble(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)

	var window int

	errBusy := errors.New("ledger: meter buffer full")
	r.sink.fail = func(call) error {
		if window == 2 {
			return errBusy
		}

		return nil
	}

	cum := []uint64{5, 12, 20}
	for i, n := range cum {
		window = i + 1

		r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: n})

		err := r.meter.Flush(t.Context())
		if window == 2 && !errors.Is(err, errBusy) {
			t.Fatalf("window 2: got %v, want the sink error", err)
		}

		if window != 2 && err != nil {
			t.Fatalf("window %d: %v", window, err)
		}
	}

	got := r.sink.accepted()
	if len(got) != 2 {
		t.Fatalf("got %+v", got)
	}

	// Window 3 carries windows 2 and 3 together, under window 3's key.
	if got[0].Qty != 5 || got[1].Qty != 15 {
		t.Fatalf("quantities %d, %d; want 5, 15", got[0].Qty, got[1].Qty)
	}

	if !strings.Contains(got[1].Key, ":3:") || got[0].Key == got[1].Key {
		t.Fatalf("keys %q and %q", got[0].Key, got[1].Key)
	}

	if total := r.sink.totalMessagesIn(acme.ID); total != 20 {
		t.Fatalf("sink total %d, source 20", total)
	}
}

// R12: a sink that wraps ErrRejected will never accept the value, so the
// delta is dropped and logged at Error, not retried.
func TestRejectedDeltaDroppedNotRetried(t *testing.T) {
	var logs bytes.Buffer

	r := newRig(t, onlyMessagesIn(), acme)
	r.meter = metering.New(onlyMessagesIn(), r.src, r.tenants, r.sink, "node-a",
		slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))

	reject := true
	r.sink.fail = func(call) error {
		if reject {
			return fmt.Errorf("%w: bad key", metering.ErrRejected)
		}

		return nil
	}

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 5})

	if err := r.meter.Flush(t.Context()); err != nil {
		t.Fatalf("a rejected delta is dropped, not an error: %v", err)
	}

	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("no Error log: %s", logs.String())
	}

	reject = false

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 8})
	flush(t, r.meter)

	got := r.sink.accepted()
	if len(got) != 1 || got[0].Qty != 3 {
		t.Fatalf("got %+v, want only the 3 that grew after the drop", got)
	}
}

func TestKeysUniqueAcrossRestart(t *testing.T) {
	keys := map[string]bool{}

	for _, started := range []time.Time{t0, t0.Add(time.Hour)} {
		r := newRig(t, onlyMessagesIn(), acme)
		r.src.started = started
		r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 3})
		flush(t, r.meter)

		for _, c := range r.sink.accepted() {
			if keys[c.Key] {
				t.Fatalf("key %q repeated across restarts", c.Key)
			}

			keys[c.Key] = true
		}
	}

	if len(keys) != 2 {
		t.Fatalf("got %d keys", len(keys))
	}
}

// A broker that restarts under a running Meter counts from zero again; the
// Meter must not wait for the new counters to pass the old ones.
func TestSourceRestartResetsBaseline(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 100})
	flush(t, r.meter)

	r.src.restart(t0.Add(time.Hour), broker.TenantUsage{Tenant: acme.ID, MessagesIn: 4})
	flush(t, r.meter)

	got := r.sink.accepted()
	if len(got) != 2 || got[1].Qty != 4 {
		t.Fatalf("got %+v", got)
	}

	if got[0].Key == got[1].Key || !strings.Contains(got[1].Key, fmt.Sprint(t0.Add(time.Hour).UnixNano())) {
		t.Fatalf("keys %q, %q", got[0].Key, got[1].Key)
	}
}

func TestConnectionMinutesCarry(t *testing.T) {
	cfg := metering.DefaultConfig()
	cfg.Features = metering.Features{ConnectionMinutes: "tether.connection.minutes"}
	r := newRig(t, cfg, acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, ConnectionTime: 90 * time.Second})
	flush(t, r.meter)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, ConnectionTime: 180 * time.Second})
	flush(t, r.meter)

	// 30 s short of the next whole minute: nothing yet.
	r.src.set(broker.TenantUsage{Tenant: acme.ID, ConnectionTime: 209 * time.Second})
	flush(t, r.meter)

	got := r.sink.accepted()
	if len(got) != 2 || got[0].Qty != 1 || got[1].Qty != 2 {
		t.Fatalf("got %+v, want 1 then 2", got)
	}
}

func TestDisabledFeatureNotSent(t *testing.T) {
	cfg := metering.DefaultConfig()
	cfg.Features.BytesIn = ""
	cfg.Features.BytesOut = ""
	r := newRig(t, cfg, acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 1, MessagesOut: 1, BytesIn: 9, BytesOut: 9})
	flush(t, r.meter)

	for _, c := range r.sink.accepted() {
		if strings.Contains(c.Feature, "bytes") || c.Feature == "" {
			t.Fatalf("sent %+v for a disabled feature", c)
		}
	}

	if n := len(r.sink.accepted()); n != 2 {
		t.Fatalf("got %d calls, want 2", n)
	}
}

// Review Focus 5: each sink call carries its own tenant's pair.
func TestMeterTenantIsolation(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme, zeta)

	r.src.set(
		broker.TenantUsage{Tenant: acme.ID, MessagesIn: 3},
		broker.TenantUsage{Tenant: zeta.ID, MessagesIn: 8},
	)
	flush(t, r.meter)

	got := r.sink.accepted()
	if len(got) != 2 {
		t.Fatalf("got %+v", got)
	}

	for _, c := range got {
		switch c.Tenant.ID {
		case acme.ID:
			if c.Tenant != acme || c.Qty != 3 || !strings.HasSuffix(c.Key, ":ttnt_acme:tether.messages.in") {
				t.Fatalf("acme call %+v", c)
			}
		case zeta.ID:
			if c.Tenant != zeta || c.Qty != 8 || !strings.HasSuffix(c.Key, ":ttnt_zeta:tether.messages.in") {
				t.Fatalf("zeta call %+v", c)
			}
		default:
			t.Fatalf("unexpected tenant %+v", c)
		}
	}
}

// A tenant that cannot be looked up is skipped for the window, and its growth
// goes out with the next one.
func TestTenantLookupFailureRetriesNextWindow(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme, zeta)
	r.tenants.fails[acme.ID] = errors.New("store down")

	r.src.set(
		broker.TenantUsage{Tenant: acme.ID, MessagesIn: 3},
		broker.TenantUsage{Tenant: zeta.ID, MessagesIn: 8},
	)

	if err := r.meter.Flush(t.Context()); err == nil {
		t.Fatal("want the lookup error from Flush")
	}

	if r.sink.totalMessagesIn(zeta.ID) != 8 {
		t.Fatal("one tenant's lookup failure held back another's usage")
	}

	delete(r.tenants.fails, acme.ID)
	flush(t, r.meter)

	if got := r.sink.totalMessagesIn(acme.ID); got != 3 {
		t.Fatalf("acme total %d, want 3", got)
	}
}

func TestIdleTenantNotLooked(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 3})
	flush(t, r.meter)
	flush(t, r.meter)
	flush(t, r.meter)

	if n := r.tenants.gets[acme.ID]; n != 1 {
		t.Fatalf("looked the tenant up %d times, want 1", n)
	}
}

func TestStopFlushes(t *testing.T) {
	cfg := onlyMessagesIn()
	cfg.Enabled = true
	cfg.Interval = time.Hour
	r := newRig(t, cfg, acme)

	r.meter.Start()
	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 9})

	if err := r.meter.Stop(t.Context()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	got := r.sink.accepted()
	if len(got) != 1 || got[0].Qty != 9 {
		t.Fatalf("got %+v, want the final window's 9", got)
	}

	// Stop is idempotent and sends nothing more.
	if err := r.meter.Stop(t.Context()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	if len(r.sink.accepted()) != 1 {
		t.Fatal("second Stop flushed again")
	}
}

func TestTickFlushes(t *testing.T) {
	cfg := onlyMessagesIn()
	cfg.Enabled = true
	cfg.Interval = 10 * time.Millisecond
	r := newRig(t, cfg, acme)

	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 2})
	r.meter.Start()

	t.Cleanup(func() { _ = r.meter.Stop(context.Background()) })

	deadline := time.Now().Add(5 * time.Second)
	for r.sink.totalMessagesIn(acme.ID) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("no window ran")
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func TestStopWithoutStartIsNoop(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)
	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 9})

	if err := r.meter.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	if len(r.sink.accepted()) != 0 {
		t.Fatal("Stop without Start flushed")
	}
}

func TestConfigValidate(t *testing.T) {
	long := strings.Repeat("a", 65)

	cases := []struct {
		name string
		edit func(*metering.Config)
		ok   bool
	}{
		{"default", func(*metering.Config) {}, true},
		{"empty key turns a meter off", func(c *metering.Config) { c.Features.BytesIn = "" }, true},
		{"all meters off", func(c *metering.Config) { c.Features = metering.Features{} }, true},
		{"64 bytes", func(c *metering.Config) { c.Features.MessagesIn = strings.Repeat("a", 64) }, true},
		{"65 bytes", func(c *metering.Config) { c.Features.MessagesIn = long }, false},
		{"space", func(c *metering.Config) { c.Features.MessagesIn = "tether messages" }, false},
		{"colon", func(c *metering.Config) { c.Features.MessagesIn = "tether:messages" }, false},
		{"slash", func(c *metering.Config) { c.Features.BytesOut = "a/b" }, false},
		{"non ascii", func(c *metering.Config) { c.Features.BytesOut = "caf\u00e9" }, false},
		{"NUL", func(c *metering.Config) { c.Features.BytesOut = "a\x00b" }, false},
		{"dash underscore dot", func(c *metering.Config) { c.Features.MessagesOut = "Tether_msgs-out.1" }, true},
		{"two meters one key", func(c *metering.Config) { c.Features.BytesOut = c.Features.BytesIn }, false},
		{"zero interval enabled", func(c *metering.Config) { c.Enabled = true; c.Interval = 0 }, false},
		{"negative interval enabled", func(c *metering.Config) { c.Enabled = true; c.Interval = -time.Second }, false},
		{"zero interval disabled", func(c *metering.Config) { c.Interval = 0 }, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := metering.DefaultConfig()
			tc.edit(&cfg)

			err := cfg.Validate()
			if tc.ok && err != nil {
				t.Fatalf("got %v", err)
			}

			if !tc.ok && err == nil {
				t.Fatal("want an error")
			}
		})
	}
}

func TestStartDisabledDoesNothing(t *testing.T) {
	cfg := onlyMessagesIn()
	cfg.Interval = time.Millisecond
	r := newRig(t, cfg, acme)
	r.src.set(broker.TenantUsage{Tenant: acme.ID, MessagesIn: 9})

	r.meter.Start()
	time.Sleep(30 * time.Millisecond)

	if err := r.meter.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}

	if len(r.sink.accepted()) != 0 {
		t.Fatal("a disabled Meter sent usage")
	}
}

// R13: before the broker has started (StartedAt zero) a window sends nothing
// and advances nothing, so a window racing the start cannot count the same
// usage under two start times.
func TestFlushBeforeSourceStartsSendsNothing(t *testing.T) {
	r := newRig(t, onlyMessagesIn(), acme)
	r.src.restart(time.Time{}, broker.TenantUsage{Tenant: acme.ID, MessagesIn: 5})

	flush(t, r.meter)
	flush(t, r.meter)

	if got := r.sink.accepted(); len(got) != 0 {
		t.Fatalf("sent %+v before the source started", got)
	}

	r.tenants.mu.Lock()
	gets := r.tenants.gets[acme.ID]
	r.tenants.mu.Unlock()

	if gets != 0 {
		t.Fatalf("%d tenant lookups before the source started, want none", gets)
	}

	r.src.restart(t0, broker.TenantUsage{Tenant: acme.ID, MessagesIn: 5})
	flush(t, r.meter)

	got := r.sink.accepted()
	if len(got) != 1 || got[0].Qty != 5 {
		t.Fatalf("got %+v, want 5 once", got)
	}

	// The skipped windows did not count: this is window 1.
	if want := fmt.Sprintf("tether:node-a:%d:1:ttnt_acme:tether.messages.in", t0.UnixNano()); got[0].Key != want {
		t.Fatalf("key %q, want %q", got[0].Key, want)
	}
}
