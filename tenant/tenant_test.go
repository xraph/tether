package tenant_test

import (
	"testing"

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
