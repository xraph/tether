package scope_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	"github.com/xraph/keysmith/key"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"

	"github.com/xraph/tether/scope"
)

func TestWithSetsForgeScope(t *testing.T) {
	ctx := scope.With(context.Background(), "aapp_1", "aorg_1")

	s, ok := forge.ScopeFrom(ctx)
	require.True(t, ok)
	assert.Equal(t, "aapp_1", s.AppID())
	assert.Equal(t, "aorg_1", s.OrgID())
}

func TestWithSetsKeysmithScope(t *testing.T) {
	eng, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()))
	require.NoError(t, err)

	// The engine reads forge's scope first, so a key created under scope.With
	// carries the pair whichever mechanism it came from.
	res, err := eng.CreateKey(scope.With(context.Background(), "aapp_1", "aorg_1"), &keysmith.CreateKeyInput{
		Name: "pump", Prefix: "tdk", Environment: key.EnvLive,
	})
	require.NoError(t, err)
	assert.Equal(t, "aapp_1", res.Key.AppID)
	assert.Equal(t, "aorg_1", res.Key.TenantID)
}

func TestWithSetsWardenScope(t *testing.T) {
	// warden prefers its own explicit values over forge's scope. A later
	// forge.WithScope must therefore not change what warden reads, which
	// proves scope.With set warden's values and did not leave it to fall back.
	ctx := scope.With(context.Background(), "aapp_1", "aorg_1")
	ctx = forge.WithScope(ctx, forge.NewOrgScope("aapp_other", "aorg_other"))

	app, tenantID := warden.ScopeFromContext(ctx)
	assert.Equal(t, "aapp_1", app)
	assert.Equal(t, "aorg_1", tenantID)
}

func TestWithOverridesAnExistingScope(t *testing.T) {
	ctx := forge.WithScope(context.Background(), forge.NewOrgScope("aapp_old", "aorg_old"))
	ctx = scope.With(ctx, "aapp_1", "aorg_1")

	s, ok := forge.ScopeFrom(ctx)
	require.True(t, ok)
	assert.Equal(t, "aapp_1", s.AppID())
	assert.Equal(t, "aorg_1", s.OrgID())
}
