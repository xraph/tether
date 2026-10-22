package extension_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashboard "github.com/xraph/forge/extensions/dashboard"
	dashauth "github.com/xraph/forge/extensions/dashboard/auth"
	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/dispatcher"
	keysmithext "github.com/xraph/keysmith/extension"
	wardenext "github.com/xraph/warden/extension"

	"github.com/xraph/tether/extension"
)

// capabilitiesResponse is the shape the dashboard's discovery endpoint
// serves. Only the fields this test asserts on are modelled.
type capabilitiesResponse struct {
	Contributors []struct {
		Name       string `json:"name"`
		Version    string `json:"version"`
		Configured bool   `json:"configured"`
		Intents    []struct {
			Name     string `json:"name"`
			Versions []struct {
				N      int    `json:"n"`
				Status string `json:"status"`
			} `json:"versions"`
		} `json:"intents"`
	} `json:"contributors"`
}

// TestDashboardDiscoveryPublishesTetherContract boots a real Forge app with
// tether, keysmith, warden and the real dashboard, lets the dashboard's own
// discovery register tether's contract contributor, and asks the real
// capabilities endpoint what it ended up with.
//
// loader.Validate accepts manifests that contract.Registry.Register then
// rejects, and the dashboard logs that rejection and carries on. The visible
// symptom is not an error anywhere: the contributor is simply missing from
// /capabilities. Asserting on the served capabilities is the only check that
// catches it.
func TestDashboardDiscoveryPublishesTetherContract(t *testing.T) {
	app := newApp(t)
	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))

	require.NoError(t, boot(t, app,
		ext.Keysmith(keysmithext.WithDisableRoutes()),
		ext.Warden(wardenext.WithDisableRoutes()),
		ext,
		dashboard.NewExtension(),
	))

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/dashboard/api/dashboard/v1/capabilities", nil)
	w := httptest.NewRecorder()
	app.Router().ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body)

	var resp capabilitiesResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp), "body: %s", w.Body)

	var (
		names   []string
		intents []string
	)

	for _, c := range resp.Contributors {
		names = append(names, c.Name)

		if c.Name != "tether" {
			continue
		}

		// The dashboard asks the extension for its status (DashboardStatus),
		// so the plugin's version check and setup panel see a started Tether.
		assert.Equal(t, extension.ExtensionVersion, c.Version)
		assert.True(t, c.Configured)

		for _, in := range c.Intents {
			intents = append(intents, in.Name)

			require.Len(t, in.Versions, 1, in.Name)
			assert.Equal(t, 1, in.Versions[0].N, in.Name)
			assert.Equal(t, "active", in.Versions[0].Status, in.Name)
		}
	}

	require.NotEmpty(t, intents, "tether is missing from the capabilities; contributors = %v. "+
		"The dashboard swallows contributor registration errors, so this is what a rejected "+
		"manifest looks like from outside. Look for \"failed to register contract contributor\" "+
		"in its startup log.", names)

	// Every declared intent has to survive registration, not just the
	// contributor name: a partial manifest still produces an entry.
	assert.ElementsMatch(t, []string{
		"overview.stats", "tenants.list", "tenants.get", "devices.list",
		"devices.get", "roles.list", "roles.topics.get", "clients.list", "retained.list",
		"tenants.create", "tenants.update", "tenants.delete",
		"devices.provision", "devices.rotate", "devices.revoke",
		"devices.delete", "devices.roles.set", "roles.create", "roles.delete", "roles.topics.set",
		"clients.kick", "sessions.delete", "retained.delete",
	}, intents)
}

// The contributor reads Tether's services at request time: before Start it
// answers UNAVAILABLE, and once Tether has started it runs on the live ones.
func TestContractContributorFollowsTheExtensionLifecycle(t *testing.T) {
	dispatch := func(t *testing.T, disp *dispatcher.Dispatcher, kind dashcontract.Kind, intent, payload string) (json.RawMessage, error) {
		t.Helper()

		out, _, err := disp.Dispatch(t.Context(), dashcontract.Request{
			Envelope: "v1", Kind: kind, Contributor: "tether", Intent: intent, IntentVersion: 1,
			Payload: json.RawMessage(payload),
		}, dashcontract.Principal{}) // anonymous: WithInsecureDashboard lets it in.

		return out, err
	}

	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()), extension.WithInsecureDashboard())
	disp := dispatcher.New(nil)
	require.NoError(t, ext.RegisterContractContributor(disp, dashcontract.NewRegistry(), dashcontract.NewWardenRegistry()))

	_, err := dispatch(t, disp, dashcontract.KindQuery, "tenants.list", `{}`)

	var ce *dashcontract.Error

	require.ErrorAs(t, err, &ce)
	assert.Equal(t, dashcontract.CodeUnavailable, ce.Code)

	app := newApp(t)
	require.NoError(t, boot(t, app, ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext))

	out, err := dispatch(t, disp, dashcontract.KindCommand, "tenants.create", `{"appId":"aapp_1","externalId":"acme","name":"Acme"}`)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"externalId":"acme"`)

	out, err = dispatch(t, disp, dashcontract.KindQuery, "tenants.list", `{}`)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"externalId":"acme"`)

	// The broker is up, so the node total is available too.
	out, err = dispatch(t, disp, dashcontract.KindQuery, "overview.stats", `{}`)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"tenants":1`)
}

// The dashboard's auth is off unless the host turns it on, and then a request
// reaches the dispatcher with a principal that has no user. Tether refuses
// that by default and serves it with WithInsecureDashboard; a principal with
// a user passes either way.
func TestContractContributorRefusesAnonymousUnlessInsecure(t *testing.T) {
	signedIn := dashcontract.PrincipalFor(&dashauth.UserInfo{Subject: "admin-1"})

	call := func(t *testing.T, disp *dispatcher.Dispatcher, p dashcontract.Principal, kind dashcontract.Kind, intent, payload string) error {
		t.Helper()

		req := dashcontract.Request{Envelope: "v1", Kind: kind, Contributor: "tether", Intent: intent, IntentVersion: 1}
		if kind == dashcontract.KindQuery {
			req.Params = map[string]any{}
		} else {
			req.Payload = json.RawMessage(payload)
		}

		_, _, err := disp.Dispatch(t.Context(), req, p)

		return err
	}

	start := func(t *testing.T, opts ...extension.Option) *dispatcher.Dispatcher {
		t.Helper()

		ext := extension.New(append([]extension.Option{extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback())}, opts...)...)
		disp := dispatcher.New(nil)
		require.NoError(t, ext.RegisterContractContributor(disp, dashcontract.NewRegistry(), dashcontract.NewWardenRegistry()))
		require.NoError(t, boot(t, newApp(t), ext.Keysmith(keysmithext.WithDisableRoutes()), ext.Warden(wardenext.WithDisableRoutes()), ext))

		return disp
	}

	const create = `{"appId":"aapp_1","externalId":"acme"}`

	t.Run("default", func(t *testing.T) {
		disp := start(t)

		for _, err := range []error{
			call(t, disp, dashcontract.Principal{}, dashcontract.KindQuery, "tenants.list", ``),
			call(t, disp, dashcontract.Principal{}, dashcontract.KindCommand, "tenants.create", create),
		} {
			var ce *dashcontract.Error

			require.ErrorAs(t, err, &ce)
			assert.Equal(t, dashcontract.CodeUnauthenticated, ce.Code)
		}

		// The refused create made nothing.
		require.NoError(t, call(t, disp, signedIn, dashcontract.KindQuery, "tenants.list", ``))
		require.NoError(t, call(t, disp, signedIn, dashcontract.KindCommand, "tenants.create", create), "the external ID is still free")
	})

	t.Run("insecure dashboard", func(t *testing.T) {
		disp := start(t, extension.WithInsecureDashboard())

		require.NoError(t, call(t, disp, dashcontract.Principal{}, dashcontract.KindQuery, "tenants.list", ``))
		require.NoError(t, call(t, disp, dashcontract.Principal{}, dashcontract.KindCommand, "tenants.create", create))
	})
}

// The same refusal over the dashboard's real HTTP endpoint with its auth off
// (the default): the principal the transport builds has no user, which is
// what the admission check relies on.
func TestDashboardEndpointRefusesAnonymousUnlessInsecure(t *testing.T) {
	post := func(t *testing.T, opts ...extension.Option) (int, string) {
		t.Helper()

		app := newApp(t)
		ext := extension.New(append([]extension.Option{extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback())}, opts...)...)

		require.NoError(t, boot(t, app,
			ext.Keysmith(keysmithext.WithDisableRoutes()),
			ext.Warden(wardenext.WithDisableRoutes()),
			ext,
			dashboard.NewExtension(),
		))

		body := `{"envelope":"v1","kind":"query","contributor":"tether","intent":"tenants.list","intentVersion":1,"params":{}}`
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/dashboard/api/dashboard/v1", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		app.Router().ServeHTTP(w, req)

		return w.Code, w.Body.String()
	}

	t.Run("default", func(t *testing.T) {
		code, body := post(t)
		assert.Contains(t, body, "UNAUTHENTICATED")
		assert.GreaterOrEqual(t, code, 400, body)
	})

	t.Run("insecure dashboard", func(t *testing.T) {
		code, body := post(t, extension.WithInsecureDashboard())
		assert.Equal(t, http.StatusOK, code, body)
		assert.Contains(t, body, `"items"`)
	})
}
