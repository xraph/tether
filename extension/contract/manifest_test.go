package contract

import (
	"bytes"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/forge/extensions/dashboard/contract/loader"
)

func TestEmbeddedManifestLoadsAndValidates(t *testing.T) {
	m, err := loader.Load(bytes.NewReader(manifestYAML), "tether/contract/manifest.yaml")
	require.NoError(t, err)

	assert.Equal(t, ContributorName, m.Contributor.Name)
	assert.Equal(t, []string{"tether.read", "tether.write"}, m.Contributor.Capabilities)
	assert.Len(t, m.Intents, 23)

	require.NotNil(t, m.Contributor.App)
	assert.Equal(t, "Tether", m.Contributor.App.DisplayName)
	assert.Equal(t, "tether", m.Contributor.App.Slug)
	assert.Equal(t, "radio-tower", m.Contributor.App.Icon)

	require.NoError(t, loader.Validate(m, dashcontract.NewWardenRegistry()))
}

// Every command says which queries it makes stale, and every one of those
// names an intent the manifest really has, as a query. A typo here is a list
// that never refreshes.
func TestManifestInvalidatesNameQueries(t *testing.T) {
	m, err := loader.Load(bytes.NewReader(manifestYAML), "tether/contract/manifest.yaml")
	require.NoError(t, err)

	queries := map[string]bool{}

	for _, in := range m.Intents {
		assert.Equal(t, 1, in.Version, in.Name)

		if in.Kind == dashcontract.IntentKindQuery {
			queries[in.Name] = true
		}
	}

	commands := 0

	for _, in := range m.Intents {
		if in.Kind != dashcontract.IntentKindCommand {
			continue
		}

		commands++

		assert.NotEmpty(t, in.Invalidates, "command %s must declare invalidates", in.Name)

		for _, q := range in.Invalidates {
			assert.True(t, queries[q], "command %s invalidates %q, which is not a query intent", in.Name, q)
		}
	}

	assert.Equal(t, 14, commands)
	assert.Len(t, queries, 9)
}

// The manifest and the registered handlers must list the same intents.
func TestEveryManifestIntentHasAHandler(t *testing.T) {
	m, err := loader.Load(bytes.NewReader(manifestYAML), "tether/contract/manifest.yaml")
	require.NoError(t, err)

	declared := make([]string, 0, len(m.Intents))

	for _, in := range m.Intents {
		declared = append(declared, in.Name)
	}

	slices.Sort(declared)

	h := newHarness(t, true)

	registered := h.registered()
	got := make([]string, 0, len(registered))

	for name := range registered {
		got = append(got, name)
	}

	slices.Sort(got)

	assert.Equal(t, declared, got)
}
