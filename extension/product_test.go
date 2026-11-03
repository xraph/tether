package extension_test

import (
	"context"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chronicleaudit "github.com/xraph/chronicle/audit"
	chronicleext "github.com/xraph/chronicle/extension"
	"github.com/xraph/forge"
	keysmithext "github.com/xraph/keysmith/extension"
	"github.com/xraph/ledger"
	ledgerext "github.com/xraph/ledger/extension"
	"github.com/xraph/ledger/meter"
	"github.com/xraph/ledger/plan"
	"github.com/xraph/ledger/subscription"
	"github.com/xraph/ledger/types"
	"github.com/xraph/vessel"
	"github.com/xraph/warden"
	wardenext "github.com/xraph/warden/extension"

	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/extension"
	"github.com/xraph/tether/metering"
)

// product is an app running Tether with ledger and chronicle beside it, both
// on the app's grove SQLite database.
type product struct {
	*env
	ledger    *ledger.Ledger
	chronicle *chronicleext.Extension
}

// startProduct boots grove SQLite, keysmith, warden, ledger, chronicle and
// Tether with opts. Ledger flushes its meter buffer every 10 ms and caches an
// entitlement for 1 ms, so a test sees usage and its effect at once.
func startProduct(t *testing.T, opts ...extension.Option) *product {
	t.Helper()

	return startProductOn(t, newApp(t), opts...)
}

// startProductOn is startProduct on an app of the caller's (one with a
// config file).
func startProductOn(t *testing.T, app forge.App, opts ...extension.Option) *product {
	t.Helper()

	ext := extension.New(append([]extension.Option{
		extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
	}, opts...)...)

	led := ledgerext.New(ledgerext.WithDisableRoutes(), ledgerext.WithDisableLifecycle(),
		ledgerext.WithMeterFlushInterval(10*time.Millisecond), ledgerext.WithEntitlementCacheTTL(time.Millisecond))
	chron := chronicleext.New(chronicleext.WithDisableRoutes())

	require.NoError(t, boot(t, app,
		ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), led, chron, ext))

	wd, err := vessel.Inject[*warden.Engine](app.Container())
	require.NoError(t, err)

	srv := httptest.NewServer(app.Router())
	t.Cleanup(srv.Close)

	return &product{
		env:       &env{t: t, base: "/tether", app: app, ext: ext, srv: srv, wd: wd},
		ledger:    led.Engine(),
		chronicle: chron,
	}
}

// subscribe puts testOrg on a plan of testApp with features.
func (p *product) subscribe(features ...plan.Feature) {
	p.t.Helper()

	ctx := p.t.Context()
	pl := &plan.Plan{
		Name: "tether", Slug: "tether", Currency: "usd", AppID: testApp, Features: features,
		Pricing: &plan.Pricing{BaseAmount: types.USD(100), BillingPeriod: plan.PeriodMonthly},
	}
	require.NoError(p.t, p.ledger.CreatePlan(ctx, pl))
	require.NoError(p.t, p.ledger.ActivatePlan(ctx, pl.ID))
	require.NoError(p.t, p.ledger.CreateSubscription(ctx, &subscription.Subscription{
		TenantID: testOrg, AppID: testApp, PlanID: pl.ID,
	}))
}

// used is what ledger counts against feature for testOrg this period.
func (p *product) used(feature string) int64 {
	p.t.Helper()

	res, err := p.ledger.InspectEntitlement(p.t.Context(), testOrg, testApp, feature)
	require.NoError(p.t, err)

	return res.Used
}

var (
	accessFeature = plan.Feature{Key: "tether.access", Name: "Tether", Type: plan.FeatureBoolean, Limit: 1}
	messagesIn    = func(limit int64) plan.Feature {
		return plan.Feature{
			Key: "tether.messages.in", Name: "Messages in", Type: plan.FeatureMetered,
			Limit: limit, Period: plan.PeriodMonthly,
		}
	}
)

func meteringOn() metering.Config {
	cfg := metering.DefaultConfig()
	cfg.Enabled = true
	// Long enough that only the test's flushes send anything.
	cfg.Interval = time.Hour

	return cfg
}

// Review Focus 2: five publishes reach ledger as five, once. The same
// idempotency key sent again adds nothing.
func TestLedgerIdempotentUsage(t *testing.T) {
	p := startProduct(t, extension.WithMetering(meteringOn()))
	p.subscribe(accessFeature, messagesIn(1000))

	tid := p.tenant()
	p.role(tid, "sensor", []string{"up"}, nil)
	d := p.device(tid, "sensor")

	c, err := p.connect(d, "dev-1")
	require.NoError(t, err)

	for range 5 {
		c.publish(t, "up", "x")
	}

	require.NoError(t, extension.FlushMeter(t.Context(), p.ext))
	require.Eventually(t, func() bool { return p.used("tether.messages.in") == 5 }, waitTimeout, 10*time.Millisecond,
		"ledger never counted the five messages")

	evs, err := p.ledger.Store().QueryUsage(t.Context(), testOrg, testApp, meter.QueryOpts{})
	require.NoError(t, err)

	var key string

	for _, ev := range evs {
		if ev.FeatureKey == "tether.messages.in" {
			key = ev.IdempotencyKey
		}
	}

	require.NotEmpty(t, key, "the usage event carries no idempotency key")

	lctx := ledger.WithTenant(t.Context(), testOrg, testApp)
	require.NoError(t, p.ledger.MeterWithKey(lctx, "tether.messages.in", 5, key))

	// A later event on a fresh key shows the buffer has been flushed past
	// the replay.
	require.NoError(t, p.ledger.MeterWithKey(lctx, "tether.probe", 1, "probe-1"))
	require.Eventually(t, func() bool {
		n, aerr := p.ledger.Store().Aggregate(t.Context(), testOrg, testApp, "tether.probe", plan.PeriodMonthly)

		return aerr == nil && n == 1
	}, waitTimeout, 10*time.Millisecond)

	assert.Equal(t, int64(5), p.used("tether.messages.in"), "the replayed key was counted again")
}

// A tenant over a hard metered limit it must be entitled to is disconnected
// with 0x97 at the next check, and a new CONNECT is refused with 0x97.
func TestHardQuotaDisconnects(t *testing.T) {
	ent := metering.DefaultEntitlementConfig()
	ent.Enabled = true
	ent.Features = []string{"tether.access", "tether.messages.in"}
	ent.Interval = 200 * time.Millisecond

	p := startProduct(t, extension.WithMetering(meteringOn()), extension.WithEntitlements(ent))
	p.subscribe(accessFeature, messagesIn(10))

	tid := p.tenant()
	p.role(tid, "sensor", []string{"up"}, nil)
	d := p.device(tid, "sensor")

	c := p.connectDevice5(d.username, d.password, "dev-1")

	for i := range 11 {
		_, err := c.Publish(t.Context(), &paho.Publish{Topic: "up", QoS: 1, Payload: []byte(strconv.Itoa(i))})
		require.NoError(t, err)
	}

	require.NoError(t, extension.FlushMeter(t.Context(), p.ext))

	select {
	case code := <-c.gone:
		assert.Equal(t, byte(0x97), code)
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the client over its hard quota was not disconnected")
	}

	code := p.refused5(d.username, d.password, "dev-2")
	assert.Equal(t, byte(0x97), code, "a new CONNECT over the quota")

	denied, err := strconv.ParseFloat(metricValue(t, p.app, extension.MetricEntitlementChecks+`{result="denied"}`), 64)
	require.NoError(t, err)
	assert.Positive(t, denied)

	allowed, err := strconv.ParseFloat(metricValue(t, p.app, extension.MetricEntitlementChecks+`{result="allowed"}`), 64)
	require.NoError(t, err)
	assert.Positive(t, allowed)

	assert.Equal(t, "0", metricValue(t, p.app, extension.MetricMeteringErrors))
}

// refused5 sends an MQTT 5.0 CONNECT that the broker must refuse, and returns
// the CONNACK's reason code.
func (e *env) refused5(username, password, clientID string) byte {
	e.t.Helper()

	var d net.Dialer

	nc, err := d.DialContext(e.t.Context(), "tcp", e.ext.Broker().Addr().String())
	require.NoError(e.t, err)

	c := paho.NewClient(paho.ClientConfig{Conn: nc})

	ctx, cancel := context.WithTimeout(e.t.Context(), waitTimeout)
	defer cancel()

	ca, err := c.Connect(ctx, &paho.Connect{
		ClientID: clientID, KeepAlive: 30, CleanStart: true,
		Username: username, UsernameFlag: true,
		Password: []byte(password), PasswordFlag: true,
	})
	require.Error(e.t, err, "the CONNECT was accepted")
	require.NotNil(e.t, ca)

	return ca.ReasonCode
}

// A connect, a bad password and a REST provision reach chronicle. The first
// and the last carry the tenant's host pair; a bad password is refused before
// the credentials resolve to a tenant, so it carries the username alone.
func TestAuditTrailInChronicle(t *testing.T) {
	p := startProduct(t, extension.WithAudit(auditOn()))

	tid := p.tenant()
	p.role(tid, "sensor", []string{"up"}, nil)
	d := p.device(tid, "sensor")

	_, err := p.connect(d, "dev-1")
	require.NoError(t, err)

	_, err = p.connect(device{username: d.username, password: "tdk_live_wrong"}, "dev-2")
	require.Error(t, err)

	chron := p.chronicle.Chronicle()

	actions := func(q *chronicleaudit.Query) []string {
		q.Limit = 100
		res, qerr := chron.Query(t.Context(), q)
		require.NoError(t, qerr)

		out := make([]string, 0, len(res.Events))
		for _, ev := range res.Events {
			out = append(out, ev.Action)
		}

		return out
	}

	require.Eventually(t, func() bool {
		got := actions(&chronicleaudit.Query{AppID: testApp, TenantID: testOrg})

		return slices.Contains(got, audit.ActionConnect) && slices.Contains(got, audit.ActionDeviceProvision) &&
			slices.Contains(got, audit.ActionTenantCreate)
	}, waitTimeout, 20*time.Millisecond, "the tenant's connect, provision and create in chronicle")

	require.Eventually(t, func() bool {
		res, qerr := chron.Query(t.Context(), &chronicleaudit.Query{Actions: []string{audit.ActionAuthFailed}, Limit: 10})
		require.NoError(t, qerr)

		return slices.ContainsFunc(res.Events, func(ev *chronicleaudit.Event) bool {
			return ev.SubjectID == d.username && ev.Reason == "bad_credentials" && ev.Category == "tether"
		})
	}, waitTimeout, 20*time.Millisecond, "the bad password in chronicle")

	assert.Equal(t, "0", metricValue(t, p.app, extension.MetricAuditDropped))
}

func auditOn() audit.Config {
	cfg := audit.DefaultConfig()
	cfg.Enabled = true

	return cfg
}

// Ledger and chronicle in the app turn nothing on by themselves: each part
// runs only when its own enabled is true.
func TestLedgerAndChronicleAloneTurnNothingOn(t *testing.T) {
	p := startProduct(t)

	m, ent, rec := extension.ProductParts(p.ext)
	assert.False(t, m, "meter")
	assert.False(t, ent, "entitlements")
	assert.False(t, rec, "audit")

	tid := p.tenant()
	p.role(tid, "sensor", []string{"up"}, nil)
	d := p.device(tid, "sensor")

	_, err := p.connect(d, "dev-1")
	require.NoError(t, err)

	res, err := p.chronicle.Chronicle().Query(t.Context(), &chronicleaudit.Query{Categories: []string{"tether"}, Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, res.Events)
}

// The metering, entitlements and audit blocks of the app's config file turn
// the parts on, as the options do.
func TestProductConfigFromFile(t *testing.T) {
	dir := t.TempDir()
	yaml := "extensions:\n  tether:\n    metering:\n      enabled: true\n" +
		"    entitlements:\n      enabled: true\n      features: [tether.access]\n" +
		"    audit:\n      enabled: true\n      events: [admin]\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o600))

	p := startProductOn(t, newAppWith(t, forge.WithConfigSearchPaths(dir), forge.WithEnableAppScopedConfig(false)))

	m, ent, rec := extension.ProductParts(p.ext)
	assert.True(t, m, "meter")
	assert.True(t, ent, "entitlements")
	assert.True(t, rec, "audit")
}

// startBare boots keysmith, warden and Tether with opts and no ledger or
// chronicle, and returns Start's error.
func startBare(t *testing.T, opts ...extension.Option) error {
	t.Helper()

	ext := extension.New(append([]extension.Option{
		extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()),
	}, opts...)...)

	return boot(t, newApp(t), ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext)
}

func TestStartRefusesWithoutLedger(t *testing.T) {
	err := startBare(t, extension.WithMetering(meteringOn()))
	require.ErrorIs(t, err, extension.ErrLedgerRequired)

	ent := metering.DefaultEntitlementConfig()
	ent.Enabled = true

	err = startBare(t, extension.WithEntitlements(ent))
	require.ErrorIs(t, err, extension.ErrLedgerRequired)
}

func TestStartRefusesWithoutChronicle(t *testing.T) {
	err := startBare(t, extension.WithAudit(auditOn()))
	require.ErrorIs(t, err, extension.ErrChronicleRequired)
}

// R13: Start validates all three configs before it builds anything, and
// names the field.
func TestStartValidatesProductConfig(t *testing.T) {
	m := meteringOn()
	m.Features.MessagesIn = "has space"

	err := startBare(t, extension.WithMetering(m))
	require.ErrorContains(t, err, "features.messages_in")

	ent := metering.DefaultEntitlementConfig()
	ent.Enabled = true
	ent.CheckTimeout = time.Minute

	err = startBare(t, extension.WithEntitlements(ent))
	require.ErrorContains(t, err, "check_timeout")

	a := auditOn()
	a.Events = []string{"everything"}

	err = startBare(t, extension.WithAudit(a))
	require.ErrorContains(t, err, "everything")
}
