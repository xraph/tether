package api

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/keysmith"
	"github.com/xraph/warden"

	"github.com/xraph/tether"
	"github.com/xraph/tether/auth"
)

func TestMapError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		status  int
		message string
	}{
		{"not found", tether.ErrNotFound, http.StatusNotFound, "not found"},
		{"wrapped not found hides its text", fmt.Errorf("tether/broker: client %q: %w", "secret-id", tether.ErrNotFound), http.StatusNotFound, "not found"},
		{"conflict", tether.ErrConflict, http.StatusConflict, "conflict"},
		{"conflict with a reason", fmt.Errorf("%w: tenant has devices", tether.ErrConflict), http.StatusConflict, "tenant has devices"},
		{"conflict from a store", fmt.Errorf("tether/sqlite: create tenant: %w", tether.ErrConflict), http.StatusConflict, "conflict"},
		{"invalid input", tether.ErrInvalidInput, http.StatusBadRequest, "invalid input"},
		{"invalid input with a reason", fmt.Errorf("tether/broker: topic %q: %w: has a wildcard", "a/+", tether.ErrInvalidInput), http.StatusBadRequest, "has a wildcard"},
		{"invalid input reason stops at a line break", fmt.Errorf("%w: first\nsecond is internal", tether.ErrInvalidInput), http.StatusBadRequest, "first"},
		{"bad credentials", auth.ErrBadCredentials, http.StatusUnauthorized, "invalid credentials"},
		{"bad credentials hide their detail", fmt.Errorf("%w: username is not the key ID", auth.ErrBadCredentials), http.StatusUnauthorized, "invalid credentials"},
		{"auth not authorized", fmt.Errorf("%w: tenant is disabled", auth.ErrNotAuthorized), http.StatusForbidden, "not authorized"},
		{"tenant disabled", tether.ErrTenantDisabled, http.StatusForbidden, "not authorized"},
		{"acl denial", fmt.Errorf("%w: publishing to this topic is not granted", tether.ErrNotAuthorized), http.StatusForbidden, "publishing to this topic is not granted"},
		{"auth unavailable", fmt.Errorf("%w: store down", auth.ErrUnavailable), http.StatusServiceUnavailable, "service unavailable"},
		{"auth rate limited", auth.ErrRateLimited, http.StatusTooManyRequests, "rate limit exceeded"},
		{"unknown role", fmt.Errorf("tether: assign role: %w", warden.ErrRoleNotFound), http.StatusBadRequest, "unknown role"},
		{"missing key", keysmith.ErrKeyNotFound, http.StatusNotFound, "not found"},
		{"rotating a revoked key", fmt.Errorf("tether: rotate device key: %w", keysmith.ErrInvalidStateTransition), http.StatusConflict, "the device's key is revoked"},
		{"unmapped", errors.New("pq: password authentication failed for user postgres"), http.StatusInternalServerError, "internal error"},
		{"joined", errors.Join(errors.New("rollback failed: key akey_x"), tether.ErrNotFound), http.StatusNotFound, "not found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mapError(tt.err)

			require.Equal(t, tt.status, statusOf(got))

			assert.Equal(t, tt.message, got.Error())
		})
	}
}

func TestMapErrorNeverEchoesInternalText(t *testing.T) {
	t.Parallel()

	got := mapError(fmt.Errorf("dial tcp 10.0.0.5:5432: %w", errors.New("connection refused")))

	assert.NotContains(t, got.Error(), "10.0.0.5")
	assert.NotContains(t, got.Error(), "refused")
}

func TestStatusOfUnmappedErrorIs500(t *testing.T) {
	t.Parallel()

	assert.Equal(t, http.StatusInternalServerError, statusOf(errors.New("plain")))
}

func TestPage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		in            pageRequest
		limit, offset int
		wantErr       bool
	}{
		{"defaults", pageRequest{}, DefaultPageSize, 0, false},
		{"explicit", pageRequest{Limit: 10, Offset: 20}, 10, 20, false},
		{"at the cap", pageRequest{Limit: MaxPageSize}, MaxPageSize, 0, false},
		{"over the cap", pageRequest{Limit: MaxPageSize + 1}, 0, 0, true},
		{"negative limit", pageRequest{Limit: -1}, 0, 0, true},
		{"negative offset", pageRequest{Offset: -1}, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			limit, offset, err := page(&tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, tether.ErrInvalidInput)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.limit, limit)
			assert.Equal(t, tt.offset, offset)
		})
	}
}

func TestRegisterRequiresServices(t *testing.T) {
	t.Parallel()

	err := Register(nil, nil, Services{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Tenants")
}
