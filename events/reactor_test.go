package events_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kid "github.com/xraph/keysmith/id"
	"github.com/xraph/keysmith/key"
	kplugin "github.com/xraph/keysmith/plugin"
	"github.com/xraph/keysmith/rotation"
	"github.com/xraph/warden/assignment"
	wid "github.com/xraph/warden/id"
	wplugin "github.com/xraph/warden/plugin"
	"github.com/xraph/warden/role"

	"github.com/xraph/tether/events"
)

// call is one method call a fake saw, flattened.
type call struct {
	Method string
	Args   []any
}

// recorder records calls, as a Handler and as a BrokerControl.
type recorder struct {
	mu    sync.Mutex
	calls []call
	panic bool
}

func (r *recorder) add(method string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, call{Method: method, Args: args})

	if r.panic {
		panic("boom")
	}
}

func (r *recorder) all() []call {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]call(nil), r.calls...)
}

func (r *recorder) KeyInvalidated(_ context.Context, keyID string, disconnect bool) {
	r.add("KeyInvalidated", keyID, disconnect)
}

func (r *recorder) RolesChanged(_ context.Context, roleID string) {
	r.add("RolesChanged", roleID)
}

func (r *recorder) SubjectChanged(_ context.Context, kind, subjectID string) {
	r.add("SubjectChanged", kind, subjectID)
}

func (r *recorder) DisconnectPrincipal(principalID string, code packets.Code) int {
	r.add("DisconnectPrincipal", principalID, code.Code)

	return 1
}

func (r *recorder) ReloadRole(_ context.Context, roleID string) {
	r.add("ReloadRole", roleID)
}

func (r *recorder) ReloadPrincipal(_ context.Context, principalID string) {
	r.add("ReloadPrincipal", principalID)
}

// The plugins must satisfy the engines' hook interfaces exactly, or the
// engines silently never call them.
var (
	_ kplugin.Plugin       = (*events.KeysmithPlugin)(nil)
	_ kplugin.KeyRotated   = (*events.KeysmithPlugin)(nil)
	_ kplugin.KeyRevoked   = (*events.KeysmithPlugin)(nil)
	_ kplugin.KeySuspended = (*events.KeysmithPlugin)(nil)
	_ kplugin.KeyExpired   = (*events.KeysmithPlugin)(nil)

	_ wplugin.Plugin             = (*events.WardenPlugin)(nil)
	_ wplugin.RoleUpdated        = (*events.WardenPlugin)(nil)
	_ wplugin.RoleDeleted        = (*events.WardenPlugin)(nil)
	_ wplugin.PermissionAttached = (*events.WardenPlugin)(nil)
	_ wplugin.PermissionDetached = (*events.WardenPlugin)(nil)
	_ wplugin.RoleAssigned       = (*events.WardenPlugin)(nil)
	_ wplugin.RoleUnassigned     = (*events.WardenPlugin)(nil)

	_ events.BrokerControl = (*recorder)(nil)
)

func TestKeysmithPluginMapsHooks(t *testing.T) {
	p := events.NewKeysmithPlugin()
	assert.Equal(t, "tether", p.Name())

	k := &key.Key{ID: kid.NewKeyID()}
	ctx := t.Context()

	// Before Bind, events are dropped and the hooks still succeed.
	require.NoError(t, p.OnKeyRevoked(ctx, k, "early"))

	h := &recorder{}
	p.Bind(h)

	require.NoError(t, p.OnKeyRotated(ctx, k, &rotation.Record{GraceTTL: time.Hour}))
	require.NoError(t, p.OnKeyRotated(ctx, k, nil))
	require.NoError(t, p.OnKeyRotated(ctx, k, &rotation.Record{GraceTTL: 0})) // the old secret is dead at once
	require.NoError(t, p.OnKeyRevoked(ctx, k, "stolen"))
	require.NoError(t, p.OnKeySuspended(ctx, k))
	require.NoError(t, p.OnKeyExpired(ctx, k))

	id := k.ID.String()
	assert.Equal(t, []call{
		{"KeyInvalidated", []any{id, false}},
		{"KeyInvalidated", []any{id, false}},
		{"KeyInvalidated", []any{id, true}},
		{"KeyInvalidated", []any{id, true}},
		{"KeyInvalidated", []any{id, true}},
		{"KeyInvalidated", []any{id, true}},
	}, h.all())
}

func TestKeysmithPluginWired(t *testing.T) {
	p := events.NewKeysmithPlugin()
	assert.False(t, p.Wired())

	p.MarkWired()
	assert.True(t, p.Wired())
}

func TestWardenPluginWired(t *testing.T) {
	p := events.NewWardenPlugin(nil)
	assert.False(t, p.Wired())

	p.MarkWired()
	assert.True(t, p.Wired())
}

// Keysmith stops dispatching at the first plugin error and does not recover
// panics, so the plugin must swallow both.
func TestKeysmithPluginNeverFails(t *testing.T) {
	p := events.NewKeysmithPlugin()
	p.Bind(&recorder{panic: true})

	k := &key.Key{ID: kid.NewKeyID()}

	assert.NotPanics(t, func() {
		require.NoError(t, p.OnKeyRevoked(t.Context(), k, "x"))
		require.NoError(t, p.OnKeyRotated(t.Context(), k, nil))
	})

	require.NoError(t, p.OnKeyRevoked(t.Context(), nil, "x"), "a nil key is ignored")
}

func TestWardenPluginMapsHooks(t *testing.T) {
	h := &recorder{}
	p := events.NewWardenPlugin(h)
	assert.Equal(t, "tether", p.Name())

	ctx := t.Context()
	r := &role.Role{ID: wid.NewRoleID()}
	deleted := wid.NewRoleID()
	attached := wid.NewRoleID()
	detached := wid.NewRoleID()
	a := &assignment.Assignment{RoleID: r.ID, SubjectKind: "api_key", SubjectID: "akey_1"}
	u := &assignment.Assignment{RoleID: r.ID, SubjectKind: "user", SubjectID: "usr_1"}

	require.NoError(t, p.OnRoleUpdated(ctx, r))
	require.NoError(t, p.OnRoleDeleted(ctx, deleted))
	require.NoError(t, p.OnPermissionAttached(ctx, attached, wid.NewPermissionID()))
	require.NoError(t, p.OnPermissionDetached(ctx, detached, wid.NewPermissionID()))
	require.NoError(t, p.OnRoleAssigned(ctx, a))
	require.NoError(t, p.OnRoleUnassigned(ctx, u))

	assert.Equal(t, []call{
		{"RolesChanged", []any{r.ID.String()}},
		{"RolesChanged", []any{deleted.String()}},
		{"RolesChanged", []any{attached.String()}},
		{"RolesChanged", []any{detached.String()}},
		{"SubjectChanged", []any{"api_key", "akey_1"}},
		{"SubjectChanged", []any{"user", "usr_1"}},
	}, h.all())
}

func TestWardenPluginNeverFails(t *testing.T) {
	p := events.NewWardenPlugin(&recorder{panic: true})

	assert.NotPanics(t, func() {
		require.NoError(t, p.OnRoleUpdated(t.Context(), &role.Role{ID: wid.NewRoleID()}))
		require.NoError(t, p.OnRoleUpdated(t.Context(), nil))
		require.NoError(t, p.OnRoleAssigned(t.Context(), nil))
	})

	unbound := events.NewWardenPlugin(nil)
	require.NoError(t, unbound.OnRoleDeleted(t.Context(), wid.NewRoleID()), "events before Bind are dropped")

	h := &recorder{}
	unbound.Bind(h)

	roleID := wid.NewRoleID()
	require.NoError(t, unbound.OnRoleDeleted(t.Context(), roleID))
	assert.Equal(t, []call{{"RolesChanged", []any{roleID.String()}}}, h.all())

	unbound.Bind(nil)
	require.NoError(t, unbound.OnRoleDeleted(t.Context(), roleID))
	assert.Len(t, h.all(), 1, "unbound again")
}

func TestReactor(t *testing.T) {
	b := &recorder{}

	// The eviction is logged into the same recorder, so the order shows that
	// it comes before the broker hears of the change: a client that
	// reconnects after the disconnect cannot hit the old cache entry.
	r := events.NewReactor(func(keyID string) { b.add("Evict", keyID) }, b)

	ctx := t.Context()

	r.KeyInvalidated(ctx, "akey_revoked", true)
	r.KeyInvalidated(ctx, "akey_rotated", false)
	r.RolesChanged(ctx, "role_1")
	r.SubjectChanged(ctx, "api_key", "akey_2")
	r.SubjectChanged(ctx, "user", "usr_1")
	r.SubjectChanged(ctx, "api_key", "")
	r.RolesChanged(ctx, "")
	r.KeyInvalidated(ctx, "", true)

	assert.Equal(t, []call{
		{"Evict", []any{"akey_revoked"}},
		{"DisconnectPrincipal", []any{"akey_revoked", byte(0x98)}},
		{"Evict", []any{"akey_rotated"}},
		{"ReloadRole", []any{"role_1"}},
		{"ReloadPrincipal", []any{"akey_2"}},
	}, b.all(), "a rotation only evicts; user subjects and empty IDs are ignored")
}

func TestReactorNilEvict(t *testing.T) {
	b := &recorder{}
	r := events.NewReactor(nil, b)

	r.KeyInvalidated(t.Context(), "akey_1", true)

	assert.Equal(t, []call{{"DisconnectPrincipal", []any{"akey_1", byte(0x98)}}}, b.all())
}
