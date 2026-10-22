package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

func TestStaticAuthenticatesByUsername(t *testing.T) {
	want := &Identity{
		Tenant:        &tenant.Tenant{ID: id.NewTenantID(), Enabled: true},
		PrincipalKind: "api_key",
		PrincipalID:   "akey_1",
		Username:      "alice",
	}
	a := NewStatic(map[string]*Identity{"alice": want})

	got, err := a.Authenticate(t.Context(), Credentials{Username: "alice", Password: []byte("anything")})
	require.NoError(t, err)
	assert.Equal(t, want.PrincipalID, got.PrincipalID)
	assert.Equal(t, want.Tenant.ID, got.Tenant.ID)

	got.PrincipalID = "tampered"
	assert.Equal(t, "akey_1", want.PrincipalID, "callers get a copy")

	_, err = a.Authenticate(t.Context(), Credentials{Username: "bob"})
	require.ErrorIs(t, err, ErrBadCredentials)

	_, err = a.Authenticate(t.Context(), Credentials{})
	require.ErrorIs(t, err, ErrBadCredentials)
}
