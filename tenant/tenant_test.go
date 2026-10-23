package tenant_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

func TestTenantValidate(t *testing.T) {
	var nilTenant *tenant.Tenant

	require.Error(t, nilTenant.Validate())
	require.Error(t, (&tenant.Tenant{}).Validate(), "a zero ID is invalid")
	require.NoError(t, (&tenant.Tenant{ID: id.NewTenantID()}).Validate())
	require.Error(t, (&tenant.Tenant{ID: id.NewTenantID(), Limits: tenant.Limits{MaxSessionExpiry: time.Millisecond}}).Validate(),
		"the limits are checked too")
}

func TestLimitsValidate(t *testing.T) {
	tests := []struct {
		name    string
		l       tenant.Limits
		wantErr string
	}{
		{"zero value", tenant.Limits{}, ""},
		{"set", tenant.Limits{MaxConnections: 10, MessagesPerSecond: 2.5, Burst: 5, MaxSessionExpiry: time.Hour, MaxSessions: 20}, ""},
		{"one second expiry", tenant.Limits{MaxSessionExpiry: time.Second}, ""},
		{"fractional seconds expiry", tenant.Limits{MaxSessionExpiry: 1500 * time.Millisecond}, ""},
		{"sub-second expiry", tenant.Limits{MaxSessionExpiry: 500 * time.Millisecond}, "max_session_expiry"},
		{"one nanosecond expiry", tenant.Limits{MaxSessionExpiry: 1}, "max_session_expiry"},
		{"negative expiry", tenant.Limits{MaxSessionExpiry: -time.Second}, "negative"},
		{"negative connections", tenant.Limits{MaxConnections: -1}, "negative"},
		{"negative rate", tenant.Limits{MessagesPerSecond: -1}, "negative"},
		{"negative burst", tenant.Limits{Burst: -1}, "negative"},
		{"negative sessions", tenant.Limits{MaxSessions: -1}, "negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.l.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestListFilterValidate(t *testing.T) {
	tests := []struct {
		name    string
		f       tenant.ListFilter
		wantErr string
	}{
		{"zero value", tenant.ListFilter{}, ""},
		{"paged", tenant.ListFilter{Limit: 10, Offset: 20}, ""},
		{"negative limit", tenant.ListFilter{Limit: -1}, "limit"},
		{"negative offset", tenant.ListFilter{Offset: -1}, "offset"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.f.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
