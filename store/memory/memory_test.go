package memory_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/store/storetest"
	"github.com/xraph/tether/tenant"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) store.Store {
		t.Helper()

		s := memory.New()
		t.Cleanup(func() { _ = s.Close() })

		return s
	})
}

var _ store.Store = (*memory.Store)(nil)

func TestConcurrentUse(t *testing.T) {
	s := memory.New()
	ctx := context.Background()

	tn := &tenant.Tenant{ID: id.NewTenantID(), AppID: "app", ExternalID: "ext", Enabled: true}
	require.NoError(t, s.CreateTenant(ctx, tn))

	var wg sync.WaitGroup

	for i := range 16 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			d := &device.Device{
				ID:       id.NewDeviceID(),
				TenantID: tn.ID,
				KeyID:    fmt.Sprintf("akey_%d", i),
				Roles:    []string{"r"},
				Metadata: map[string]any{"i": float64(i)},
			}
			assert.NoError(t, s.CreateDevice(ctx, d))

			_, err := s.GetDeviceByKey(ctx, d.KeyID)
			assert.NoError(t, err)

			_, err = s.ListDevices(ctx, device.ListFilter{TenantID: tn.ID})
			assert.NoError(t, err)

			_, err = s.CountDevices(ctx, tn.ID)
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	n, err := s.CountDevices(ctx, tn.ID)
	require.NoError(t, err)
	assert.Equal(t, 16, n)
}

func TestPingAfterClose(t *testing.T) {
	s := memory.New()
	require.NoError(t, s.Ping(context.Background()))
	require.NoError(t, s.Close())
	require.Error(t, s.Ping(context.Background()))
}

func TestCanceledContext(t *testing.T) {
	s := memory.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := s.GetTenant(ctx, id.NewTenantID())
	require.ErrorIs(t, err, context.Canceled)
}
