package main

import (
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	pahov3 "github.com/eclipse/paho.mqtt.golang"
	pahov3packets "github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/chronicle"
	chronicleaudit "github.com/xraph/chronicle/audit"
	"github.com/xraph/ledger"
	"github.com/xraph/ledger/plan"
	"github.com/xraph/ledger/subscription"
	"github.com/xraph/ledger/types"
	"github.com/xraph/vessel"

	"github.com/xraph/tether/audit"
)

// productYAML turns metering, entitlements and audit on, the way an operator
// writes it.
const productYAML = `tether:
  metering:
    enabled: true
  entitlements:
    enabled: true
  audit:
    enabled: true
`

// The binary starts with metering, entitlements and audit on, on SQLite:
// ledger and chronicle run on the binary's database, a tenant on a plan
// connects and publishes, one without a plan is refused, and the admin
// change is in chronicle.
func TestBinaryProductConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tether.yaml")
	require.NoError(t, os.WriteFile(path, []byte(productYAML), 0o600))

	file, err := loadConfig(path, true)
	require.NoError(t, err)

	cfg := testConfig(t)
	cfg.Tether.Metering = file.Tether.Metering
	cfg.Tether.Entitlements = file.Tether.Entitlements
	cfg.Tether.Audit = file.Tether.Audit

	require.True(t, cfg.Tether.Metering.Enabled && cfg.Tether.Entitlements.Enabled && cfg.Tether.Audit.Enabled)
	assert.Equal(t, time.Minute, cfg.Tether.Metering.Interval, "a key the file leaves out keeps its default")

	s := start(t, cfg, testToken, false)

	led, err := vessel.Inject[*ledger.Ledger](s.app.Container())
	require.NoError(t, err, "ledger runs in the binary")

	em, err := vessel.Inject[chronicle.Emitter](s.app.Container())
	require.NoError(t, err, "chronicle runs in the binary")

	ctx := t.Context()
	pl := &plan.Plan{
		Name: "tether", Slug: "tether", Currency: "usd", AppID: testApp,
		Features: []plan.Feature{{Key: "tether.access", Name: "Tether", Type: plan.FeatureBoolean, Limit: 1}},
		Pricing:  &plan.Pricing{BaseAmount: types.USD(100), BillingPeriod: plan.PeriodMonthly},
	}
	require.NoError(t, led.CreatePlan(ctx, pl))
	require.NoError(t, led.ActivatePlan(ctx, pl.ID))
	require.NoError(t, led.CreateSubscription(ctx, &subscription.Subscription{TenantID: testOrg, AppID: testApp, PlanID: pl.ID}))

	tid := s.tenant(t)
	s.role(t, tid, "sensor", []string{"up"}, nil)
	user, pass := s.device(t, tid, "pump", "sensor")

	addr := s.ext.Broker().Addr().String()
	c := s.dial(t, addr, "pump-1", user, pass)
	publish(t, c, "up", "7")

	// A tenant with no plan is not entitled: 0x97, which 3.1.1 reads as
	// server unavailable.
	other := s.do(t, http.MethodPost, "/tether/v1/tenants", testToken,
		map[string]any{"app_id": testApp, "external_id": "aorg_2", "name": "Zeta"}, http.StatusCreated)["id"].(string)
	s.role(t, other, "sensor", []string{"up"}, nil)
	ouser, opass := s.device(t, other, "pump", "sensor")

	tok := pahov3.NewClient(pahov3.NewClientOptions().AddBroker("tcp://" + addr).SetClientID("zeta-1").
		SetUsername(ouser).SetPassword(opass).SetProtocolVersion(4).SetAutoReconnect(false).
		SetConnectRetry(false).SetConnectTimeout(waitTimeout)).Connect()
	require.True(t, tok.WaitTimeout(waitTimeout))
	require.ErrorIs(t, tok.Error(), pahov3packets.ErrorRefusedServerUnavailable)

	chron, ok := em.(*chronicle.Chronicle)
	require.True(t, ok)

	require.Eventually(t, func() bool {
		res, qerr := chron.Query(ctx, &chronicleaudit.Query{AppID: testApp, TenantID: testOrg, Limit: 50})
		if qerr != nil {
			return false
		}

		return slices.ContainsFunc(res.Events, func(ev *chronicleaudit.Event) bool { return ev.Action == audit.ActionTenantCreate }) &&
			slices.ContainsFunc(res.Events, func(ev *chronicleaudit.Event) bool { return ev.Action == audit.ActionConnect })
	}, waitTimeout, 20*time.Millisecond, "the tenant's create and connect in chronicle")
}

// The binary registers ledger only for metering or entitlements, and
// chronicle only for audit. With all three off it registers neither.
func TestBinaryRegistersOnlyWhatIsOn(t *testing.T) {
	has := func(cfg config) (bool, bool) {
		s := start(t, cfg, testToken, false)

		_, lerr := vessel.Inject[*ledger.Ledger](s.app.Container())
		_, cerr := vessel.Inject[chronicle.Emitter](s.app.Container())

		return lerr == nil, cerr == nil
	}

	l, c := has(testConfig(t))
	assert.False(t, l, "ledger with everything off")
	assert.False(t, c, "chronicle with everything off")

	cfg := testConfig(t)
	cfg.Tether.Metering.Enabled = true
	l, c = has(cfg)
	assert.True(t, l, "ledger for metering")
	assert.False(t, c, "chronicle for metering")

	cfg = testConfig(t)
	cfg.Tether.Audit.Enabled = true
	l, c = has(cfg)
	assert.False(t, l, "ledger for audit")
	assert.True(t, c, "chronicle for audit")
}
