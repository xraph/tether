package service_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether"
	"github.com/xraph/tether/service"
)

// recordingSessions records the sessions Devices asks to delete and answers
// with err.
type recordingSessions struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (r *recordingSessions) DeleteSession(tenantID, clientID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, tenantID+"/"+clientID)

	return r.err
}

func (r *recordingSessions) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.calls...)
}

// Revoking or deleting a device pinned to a client ID deletes that client's
// session, so nothing it subscribed to keeps queueing for a device that can
// never come back. An unpinned device's sessions are left to expire.
func TestRevokeAndDeleteEndThePinnedSession(t *testing.T) {
	e := newDevEnv(t)
	sessions := &recordingSessions{}
	devices := service.NewDevices(e.st, e.tenants, e.keys, e.roles, service.WithSessions(sessions))
	ctx := t.Context()
	tid := e.tenant.ID

	provision := func(name, clientID string) *service.Credentials {
		c, err := devices.Provision(ctx, tid, service.ProvisionInput{Name: name, ClientID: clientID})
		require.NoError(t, err)

		return c
	}

	pinned := provision("pump", "pump-1")
	loose := provision("loose", "")

	require.NoError(t, devices.Revoke(ctx, tid, pinned.Device.ID, "stolen"))
	require.NoError(t, devices.Revoke(ctx, tid, loose.Device.ID, "stolen"))
	assert.Equal(t, []string{tid.String() + "/pump-1"}, sessions.seen())

	require.NoError(t, devices.Delete(ctx, tid, pinned.Device.ID))
	require.NoError(t, devices.Delete(ctx, tid, loose.Device.ID))
	assert.Equal(t, []string{tid.String() + "/pump-1", tid.String() + "/pump-1"}, sessions.seen())

	// No session, or one the broker cannot delete right now, does not fail
	// the delete: the device is gone either way, and its session expires.
	for _, err := range []error{
		fmt.Errorf("no session: %w", tether.ErrNotFound),
		fmt.Errorf("busy: %w", tether.ErrConflict),
	} {
		sessions.err = err
		d := provision("gate", "gate-1")

		require.NoError(t, devices.Delete(ctx, tid, d.Device.ID))

		_, gerr := devices.Get(ctx, tid, d.Device.ID)
		require.ErrorIs(t, gerr, tether.ErrNotFound)
	}
}
