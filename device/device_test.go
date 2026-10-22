package device_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
)

func TestDeviceValidate(t *testing.T) {
	valid := func() *device.Device {
		return &device.Device{ID: id.NewDeviceID(), TenantID: id.NewTenantID(), KeyID: "akey_1"}
	}

	var nilDevice *device.Device

	tests := []struct {
		name    string
		d       *device.Device
		wantErr string
	}{
		{"valid", valid(), ""},
		{"nil", nilDevice, "device"},
		{"zero ID", func() *device.Device { d := valid(); d.ID = id.Nil; return d }(), "device ID"},
		{"zero tenant", func() *device.Device { d := valid(); d.TenantID = id.Nil; return d }(), "tenant ID"},
		{"empty key", func() *device.Device { d := valid(); d.KeyID = ""; return d }(), "key ID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.d.Validate()
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
	tn := id.NewTenantID()

	tests := []struct {
		name    string
		f       device.ListFilter
		wantErr string
	}{
		{"scoped", device.ListFilter{TenantID: tn}, ""},
		{"paged", device.ListFilter{TenantID: tn, Limit: 10, Offset: 20}, ""},
		{"zero tenant", device.ListFilter{}, "tenant ID"},
		{"negative limit", device.ListFilter{TenantID: tn, Limit: -1}, "limit"},
		{"negative offset", device.ListFilter{TenantID: tn, Offset: -1}, "offset"},
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
