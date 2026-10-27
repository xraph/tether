package events_test

import (
	"context"
	"encoding/json"
	"errors"
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

	"github.com/xraph/tether"
	"github.com/xraph/tether/cluster"
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

func (r *recorder) KeyRotated(_ context.Context, keyID string, rotatedAt, graceEnds time.Time) {
	r.add("KeyRotated", keyID, rotatedAt, graceEnds)
}

func (r *recorder) ExpirePrincipal(principalID string, connectedBefore, at time.Time) int {
	r.add("ExpirePrincipal", principalID, connectedBefore, at)

	return 1
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

	createdAt := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	graceEnds := createdAt.Add(time.Hour)

	before := time.Now()

	require.NoError(t, p.OnKeyRotated(ctx, k, &rotation.Record{GraceTTL: time.Hour, CreatedAt: createdAt, GraceEnds: graceEnds}))
	require.NoError(t, p.OnKeyRotated(ctx, k, &rotation.Record{GraceTTL: time.Minute, CreatedAt: createdAt})) // no GraceEnds: TTL after

	after := time.Now()

	require.NoError(t, p.OnKeyRotated(ctx, k, nil))
	require.NoError(t, p.OnKeyRotated(ctx, k, &rotation.Record{GraceTTL: 0})) // the old secret is dead at once
	require.NoError(t, p.OnKeyRevoked(ctx, k, "stolen"))
	require.NoError(t, p.OnKeySuspended(ctx, k))
	require.NoError(t, p.OnKeyExpired(ctx, k))

	id := k.ID.String()
	calls := h.all()
	require.Len(t, calls, 7)

	// The rotation time is when the hook ran, not the record's CreatedAt:
	// keysmith makes the record before it stores the new secret, and an
	// old-secret connection made in between must not slip past the cut-off.
	// The grace end is still the record's.
	for i, ends := range []time.Time{graceEnds, createdAt.Add(time.Minute)} {
		c := calls[i]
		require.Equal(t, "KeyRotated", c.Method)
		require.Len(t, c.Args, 3)
		assert.Equal(t, id, c.Args[0])

		at, ok := c.Args[1].(time.Time)
		require.True(t, ok)
		assert.False(t, at.Before(before) || at.After(after), "rotatedAt %v is the hook's time", at)
		assert.Equal(t, ends, c.Args[2])
	}

	assert.Equal(t, []call{
		{"KeyInvalidated", []any{id, false}},
		{"KeyInvalidated", []any{id, true}},
		{"KeyInvalidated", []any{id, true}},
		{"KeyInvalidated", []any{id, true}},
		{"KeyInvalidated", []any{id, true}},
	}, calls[2:])
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

	at := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	r.KeyRotated(ctx, "akey_grace", at, at.Add(time.Minute))
	r.KeyRotated(ctx, "", at, at)
	r.RolesChanged(ctx, "role_1")
	r.SubjectChanged(ctx, "api_key", "akey_2")
	r.SubjectChanged(ctx, "user", "usr_1")
	r.SubjectChanged(ctx, "service_acct", "svc_1")
	r.SubjectChanged(ctx, "group", "grp_1")
	r.SubjectChanged(ctx, "service_account", "svc_2")
	r.SubjectChanged(ctx, "user", "")
	r.SubjectChanged(ctx, "api_key", "")
	r.RolesChanged(ctx, "")
	r.KeyInvalidated(ctx, "", true)

	assert.Equal(t, []call{
		{"Evict", []any{"akey_revoked"}},
		{"DisconnectPrincipal", []any{"akey_revoked", byte(0x98)}},
		{"Evict", []any{"akey_rotated"}},
		{"Evict", []any{"akey_grace"}},
		{"ExpirePrincipal", []any{"akey_grace", at, at.Add(time.Minute)}},
		{"ReloadRole", []any{"role_1"}},
		{"ReloadPrincipal", []any{"akey_2"}},
		{"ReloadPrincipal", []any{"usr_1"}},
		{"ReloadPrincipal", []any{"svc_1"}},
	}, b.all(), "an invalidation without disconnect only evicts, a graceful rotation ends old connections at the grace end; users, service accounts and keys reload; other subjects and empty IDs are ignored")
}

func TestReactorNilEvict(t *testing.T) {
	b := &recorder{}
	r := events.NewReactor(nil, b)

	r.KeyInvalidated(t.Context(), "akey_1", true)

	assert.Equal(t, []call{{"DisconnectPrincipal", []any{"akey_1", byte(0x98)}}}, b.all())
}

// fakeBus is an events.Bus that keeps what is published until deliver
// hands it to the handlers, as the control bus does on every node.
type fakeBus struct {
	mu        sync.Mutex
	handlers  map[string][]func([]byte)
	published []busEvent
	err       error
}

type busEvent struct {
	kind string
	data []byte
}

var (
	_ events.Bus = (*cluster.Control)(nil)
	_ events.Bus = (*fakeBus)(nil)
)

func (f *fakeBus) Publish(_ context.Context, kind string, v any) error {
	if f.err != nil {
		return f.err
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	f.mu.Lock()
	f.published = append(f.published, busEvent{kind, data})
	f.mu.Unlock()

	return nil
}

func (f *fakeBus) Subscribe(kind string, fn func([]byte)) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.handlers == nil {
		f.handlers = make(map[string][]func([]byte))
	}

	f.handlers[kind] = append(f.handlers[kind], fn)

	return nil
}

// kinds lists the kinds published so far.
func (f *fakeBus) kinds() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.published))
	for _, e := range f.published {
		out = append(out, e.kind)
	}

	return out
}

// deliver hands every published event to its handlers, in order, once.
func (f *fakeBus) deliver() {
	f.mu.Lock()
	evs := f.published
	f.published = nil
	f.mu.Unlock()

	for _, e := range evs {
		f.mu.Lock()
		fns := append([]func([]byte){}, f.handlers[e.kind]...)
		f.mu.Unlock()

		for _, fn := range fns {
			fn(e.data)
		}
	}
}

// calls of one method, in the order the recorder saw them.
func (r *recorder) of(method string) []call {
	var out []call

	for _, c := range r.all() {
		if c.Method == method {
			out = append(out, c)
		}
	}

	return out
}

// Ruling R41: a cluster reactor publishes every change and applies none
// until the event comes back from the bus; then it applies each once,
// eviction first and at once, the broker call in the background.
func TestClusterReactorAppliesOnReceipt(t *testing.T) {
	b := &recorder{}
	bus := &fakeBus{}

	h, err := events.NewClusterReactor(func(keyID string) { b.add("Evict", keyID) }, b, bus, nil)
	require.NoError(t, err)

	ctx := t.Context()
	at := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)

	h.KeyInvalidated(ctx, "akey_revoked", true)
	h.KeyInvalidated(ctx, "akey_rotated", false)
	h.KeyRotated(ctx, "akey_grace", at, at.Add(time.Minute))
	h.RolesChanged(ctx, "role_1")
	h.SubjectChanged(ctx, "api_key", "akey_2")
	h.SubjectChanged(ctx, "group", "grp_1")
	h.KeyInvalidated(ctx, "", true)

	assert.Empty(t, b.all(), "nothing is applied before the bus delivers")
	assert.Equal(t, []string{"key.invalidated", "key.invalidated", "key.invalidated", "role.changed", "subject.changed"}, bus.kinds(),
		"empty IDs and subjects that never connect are not sent")

	bus.deliver()

	assert.Equal(t, []call{
		{"Evict", []any{"akey_revoked"}},
		{"Evict", []any{"akey_rotated"}},
		{"Evict", []any{"akey_grace"}},
	}, b.of("Evict"), "evictions run at once, in order")

	require.Eventually(t, func() bool { return len(b.all()) == 7 }, waitTimeout, 10*time.Millisecond)

	assert.Equal(t, []call{{"DisconnectPrincipal", []any{"akey_revoked", byte(0x98)}}}, b.of("DisconnectPrincipal"))
	assert.Equal(t, []call{{"ExpirePrincipal", []any{"akey_grace", at, at.Add(time.Minute)}}}, b.of("ExpirePrincipal"))
	assert.Equal(t, []call{{"ReloadRole", []any{"role_1"}}}, b.of("ReloadRole"))
	assert.Equal(t, []call{{"ReloadPrincipal", []any{"akey_2"}}}, b.of("ReloadPrincipal"))
}

// Ruling R41: when the bus cannot take the change, the reactor applies it
// on this node at once, as a lone broker's reactor does.
func TestClusterReactorFallsBackWhenPublishFails(t *testing.T) {
	b := &recorder{}
	bus := &fakeBus{err: errors.New("nats down")}

	h, err := events.NewClusterReactor(func(keyID string) { b.add("Evict", keyID) }, b, bus, nil)
	require.NoError(t, err)

	h.KeyInvalidated(t.Context(), "akey_1", true)
	h.RolesChanged(t.Context(), "role_1")

	assert.Equal(t, []call{
		{"Evict", []any{"akey_1"}},
		{"DisconnectPrincipal", []any{"akey_1", byte(0x98)}},
		{"ReloadRole", []any{"role_1"}},
	}, b.all())
}

func TestClusterReactorNeedsBus(t *testing.T) {
	_, err := events.NewClusterReactor(nil, &recorder{}, nil, nil)
	require.ErrorIs(t, err, tether.ErrInvalidInput)
}

// The tenant cache of every node drops a tenant that changed.
func TestEvictTenantsOnUpdate(t *testing.T) {
	bus := &fakeBus{}

	var got []string

	require.NoError(t, events.EvictTenantsOnUpdate(bus, func(id string) { got = append(got, id) }))
	require.NoError(t, bus.Publish(t.Context(), cluster.KindTenantUpdated, cluster.TenantUpdated{TenantID: "ttnt_1"}))
	require.NoError(t, bus.Publish(t.Context(), cluster.KindTenantUpdated, cluster.TenantUpdated{}))

	bus.deliver()

	assert.Equal(t, []string{"ttnt_1"}, got)
	require.ErrorIs(t, events.EvictTenantsOnUpdate(nil, func(string) {}), tether.ErrInvalidInput)
}
