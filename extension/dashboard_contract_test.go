package extension_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashboard "github.com/xraph/forge/extensions/dashboard"
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
		"devices.get", "roles.topics.get", "clients.list", "retained.list",
		"tenants.create", "tenants.update", "tenants.delete",
		"devices.provision", "devices.rotate", "devices.revoke",
		"devices.delete", "devices.roles.set", "roles.topics.set",
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
		}, dashcontract.Principal{})

		return out, err
	}

	ext := extension.New(extension.WithInsecureAdminRoutes(), extension.WithBrokerConfig(loopback()))
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
