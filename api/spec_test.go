package api_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/api"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/limits"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store/memory"
)

// Register mounts the documented routes and nothing else.
func TestRegisterMountsExactlyTheBriefsRoutes(t *testing.T) {
	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)

	km, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()))
	require.NoError(t, err)

	wd, err := warden.NewEngine(warden.WithStore(wmemory.New()))
	require.NoError(t, err)

	authn := auth.NewKeysmith(km, tenants, st)
	loader := perm.NewLoader(wd)

	cfg := broker.DefaultConfig()
	cfg.TCPAddress = "127.0.0.1:0"

	b, err := broker.New(cfg, broker.Deps{Auth: authn, Perms: loader})
	require.NoError(t, err)

	app := forge.New(forge.WithAppLogger(forge.NewNoopLogger()), forge.WithEnableConfigAutoDiscovery(false))
	router := app.Router()

	require.NoError(t, api.Register(router.Group("/tether"), router.Group("/tether"), api.Services{
		Tenants: tenants,
		Devices: service.NewDevices(st, tenants, km, service.NewWardenRoles(wd)),
		Roles:   perm.NewRoles(wd),
		Broker:  b,
		Auth:    authn,
		Perms:   loader,
		Limits:  limits.New(),
	}))

	want := map[string][]string{
		"/tether/v1/tenants":                                    {"POST", "GET"},
		"/tether/v1/tenants/:tenantId":                          {"GET", "PATCH", "DELETE"},
		"/tether/v1/tenants/:tenantId/devices":                  {"POST", "GET"},
		"/tether/v1/tenants/:tenantId/devices/:deviceId":        {"GET", "DELETE"},
		"/tether/v1/tenants/:tenantId/devices/:deviceId/rotate": {"POST"},
		"/tether/v1/tenants/:tenantId/devices/:deviceId/revoke": {"POST"},
		"/tether/v1/tenants/:tenantId/devices/:deviceId/roles":  {"PUT"},
		"/tether/v1/tenants/:tenantId/roles":                    {"POST", "GET"},
		"/tether/v1/tenants/:tenantId/roles/:roleSlug":          {"DELETE"},
		"/tether/v1/tenants/:tenantId/roles/:roleSlug/topics":   {"GET", "PUT"},
		"/tether/v1/tenants/:tenantId/clients":                  {"GET"},
		"/tether/v1/tenants/:tenantId/clients/:clientId/kick":   {"POST"},
		"/tether/v1/tenants/:tenantId/sessions/:clientId":       {"DELETE"},
		"/tether/v1/tenants/:tenantId/retained":                 {"GET", "DELETE"},
		"/tether/v1/tenants/:tenantId/stats":                    {"GET"},
		"/tether/v1/publish":                                    {"POST"},
	}

	got := map[string]bool{}
	for _, r := range router.Routes() {
		// Forge mounts routes of its own under /_/.
		if strings.HasPrefix(r.Path, "/tether/") {
			got[r.Method+" "+r.Path] = true
		}
	}

	for path, methods := range want {
		for _, m := range methods {
			assert.True(t, got[m+" "+path], "missing %s %s", m, path)
		}
	}

	n := 0
	for _, methods := range want {
		n += len(methods)
	}

	assert.Len(t, got, n, "no route beyond the documented ones")
}
