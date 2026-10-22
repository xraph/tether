package perm_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/warden"
	"github.com/xraph/warden/keysmithbridge"
	"github.com/xraph/warden/role"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/tenant"
)

const (
	testApp = "aapp_1"
	testOrg = "aorg_1"
	testKey = "akey_1"
)

// The broker consumes the loader through its own interface.
var _ broker.PermLoader = (*perm.Loader)(nil)

// updateRecorder is a warden plugin that records OnRoleUpdated calls.
type updateRecorder struct {
	mu      sync.Mutex
	updated []*role.Role
}

func (*updateRecorder) Name() string { return "update-recorder" }

func (u *updateRecorder) OnRoleUpdated(_ context.Context, r *role.Role) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.updated = append(u.updated, r)

	return nil
}

func (u *updateRecorder) calls() []*role.Role {
	u.mu.Lock()
	defer u.mu.Unlock()

	return append([]*role.Role(nil), u.updated...)
}

type env struct {
	eng *warden.Engine
	rec *updateRecorder
	ten *tenant.Tenant
	ctx context.Context
}

func newEnv(t *testing.T) *env {
	t.Helper()

	rec := &updateRecorder{}

	eng, err := warden.NewEngine(warden.WithStore(wmemory.New()), warden.WithPlugin(rec))
	require.NoError(t, err)

	ten := &tenant.Tenant{ID: id.NewTenantID(), AppID: testApp, ExternalID: testOrg, Enabled: true}

	return &env{eng: eng, rec: rec, ten: ten, ctx: scope.With(t.Context(), testApp, testOrg)}
}

func (e *env) createRole(t *testing.T, slug, parent string, md map[string]any) {
	t.Helper()

	require.NoError(t, e.eng.Store().CreateRole(e.ctx, &role.Role{
		TenantID:   testOrg,
		AppID:      testApp,
		Name:       slug,
		Slug:       slug,
		ParentSlug: parent,
		Metadata:   md,
	}))
}

func (e *env) assign(t *testing.T, slug string) {
	t.Helper()

	require.NoError(t, keysmithbridge.New(e.eng).AssignRoleToAPIKey(e.ctx, testOrg, testKey, slug))
}

func (e *env) identity() *auth.Identity {
	return &auth.Identity{Tenant: e.ten, PrincipalKind: auth.PrincipalKindAPIKey, PrincipalID: testKey}
}

func (e *env) roleID(t *testing.T, slug string) string {
	t.Helper()

	r, err := e.eng.Store().GetRoleBySlug(e.ctx, testOrg, "", slug)
	require.NoError(t, err)

	return r.ID.String()
}

func tetherMD(pub, sub any) map[string]any {
	return map[string]any{perm.MetadataKey: map[string]any{"publish": pub, "subscribe": sub}}
}

// stringsAlias is a named string slice type, as a driver might return.
type stringsAlias []string

// namedMap is a named map type, as bson.M is.
type namedMap map[string]any

func TestRulesFromMetadataShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		md      map[string]any
		want    acl.Rules
		wantErr string
	}{
		{
			name: "string slices",
			md:   tetherMD([]string{"a/b"}, []string{"c/#", "d"}),
			want: acl.Rules{Publish: []string{"a/b"}, Subscribe: []string{"c/#", "d"}},
		},
		{
			name: "any slices of strings",
			md:   tetherMD([]any{"a/b"}, []any{"c/#"}),
			want: acl.Rules{Publish: []string{"a/b"}, Subscribe: []string{"c/#"}},
		},
		{
			name: "named string slice by reflection",
			md:   tetherMD(stringsAlias{"a/b"}, stringsAlias{"c"}),
			want: acl.Rules{Publish: []string{"a/b"}, Subscribe: []string{"c"}},
		},
		{
			name: "named map by reflection",
			md:   map[string]any{perm.MetadataKey: namedMap{"publish": []any{"a"}}},
			want: acl.Rules{Publish: []string{"a"}},
		},
		{
			name: "one side only",
			md:   map[string]any{perm.MetadataKey: map[string]any{"subscribe": []string{"x"}}},
			want: acl.Rules{Subscribe: []string{"x"}},
		},
		{
			name: "null side is empty",
			md:   tetherMD(nil, []string{"x"}),
			want: acl.Rules{Subscribe: []string{"x"}},
		},
		{name: "missing key", md: map[string]any{"other": 1}, want: acl.Rules{}},
		{name: "nil metadata", md: nil, want: acl.Rules{}},
		{
			name:    "tether not a map",
			md:      map[string]any{perm.MetadataKey: "publish everything"},
			wantErr: `role "r1"`,
		},
		{
			name:    "publish not a slice",
			md:      tetherMD("a/b", []string{"c"}),
			wantErr: `role "r1"`,
		},
		{
			name:    "subscribe holds a non string",
			md:      tetherMD([]string{"a"}, []any{"c", 7}),
			wantErr: `role "r1"`,
		},
		{
			name:    "publish slice of ints",
			md:      tetherMD([]int{1}, nil),
			wantErr: `role "r1"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := perm.RulesFromMetadata("r1", tt.md)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorIs(t, err, tether.ErrInvalidInput)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want.Publish, got.Publish)
			assert.Equal(t, tt.want.Subscribe, got.Subscribe)
		})
	}
}

func TestRulesFromMetadataDoesNotAlias(t *testing.T) {
	t.Parallel()

	src := []string{"a/b"}
	got, err := perm.RulesFromMetadata("r1", tetherMD(src, nil))
	require.NoError(t, err)

	got.Publish[0] = "changed"
	assert.Equal(t, "a/b", src[0])
}

func TestLoadMergesInheritedRoles(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "base", "", tetherMD(nil, []string{"broadcast/#"}))
	e.createRole(t, "device", "base", tetherMD([]string{"devices/%c/up"}, nil))
	e.assign(t, "device")

	rules, roleIDs, err := perm.NewLoader(e.eng).Load(t.Context(), e.identity())
	require.NoError(t, err)

	assert.Equal(t, []string{"devices/%c/up"}, rules.Publish)
	assert.Equal(t, []string{"broadcast/#"}, rules.Subscribe)
	assert.ElementsMatch(t, []string{e.roleID(t, "device"), e.roleID(t, "base")}, roleIDs)
}

func TestLoadNoRoles(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	rules, roleIDs, err := perm.NewLoader(e.eng).Load(t.Context(), e.identity())
	require.NoError(t, err)

	assert.Empty(t, rules.Publish)
	assert.Empty(t, rules.Subscribe)
	assert.Empty(t, roleIDs)
}

func TestLoadIsTenantScoped(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", tetherMD([]string{"x"}, nil))
	e.assign(t, "device")

	other := &auth.Identity{
		Tenant:        &tenant.Tenant{ID: id.NewTenantID(), AppID: testApp, ExternalID: "aorg_other", Enabled: true},
		PrincipalKind: auth.PrincipalKindAPIKey,
		PrincipalID:   testKey,
	}

	rules, roleIDs, err := perm.NewLoader(e.eng).Load(t.Context(), other)
	require.NoError(t, err)

	assert.Empty(t, rules.Publish)
	assert.Empty(t, roleIDs)
}

func TestLoadSkipsMalformedRole(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "good", "", tetherMD([]string{"ok/up"}, nil))
	e.createRole(t, "bad", "", map[string]any{perm.MetadataKey: "nope"})
	e.assign(t, "good")
	e.assign(t, "bad")

	var logs bytes.Buffer

	loader := perm.NewLoader(e.eng, perm.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))

	rules, roleIDs, err := loader.Load(t.Context(), e.identity())
	require.NoError(t, err)

	assert.Equal(t, []string{"ok/up"}, rules.Publish)
	assert.ElementsMatch(t, []string{e.roleID(t, "good"), e.roleID(t, "bad")}, roleIDs)
	assert.Contains(t, logs.String(), "bad")
}

func TestLoadRejectsBadIdentity(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	l := perm.NewLoader(e.eng)

	for name, ident := range map[string]*auth.Identity{
		"nil":        nil,
		"no tenant":  {PrincipalKind: auth.PrincipalKindAPIKey, PrincipalID: testKey},
		"no key":     {Tenant: e.ten, PrincipalKind: auth.PrincipalKindAPIKey},
		"other kind": {Tenant: e.ten, PrincipalKind: "user", PrincipalID: testKey},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, err := l.Load(t.Context(), ident)
			require.Error(t, err)
			assert.ErrorIs(t, err, tether.ErrInvalidInput)
		})
	}
}

func TestSetRulesRoundTripAndEmits(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", map[string]any{"keep": "me"})

	roles := perm.NewRoles(e.eng)
	want := acl.Rules{Publish: []string{"devices/%c/up", "x/+/y"}, Subscribe: []string{"devices/%c/down", "broadcast/#"}}

	require.NoError(t, roles.SetRules(t.Context(), e.ten, "device", want))

	got, err := roles.GetRules(t.Context(), e.ten, "device")
	require.NoError(t, err)
	assert.Equal(t, want, got)

	calls := e.rec.calls()
	require.Len(t, calls, 1)
	assert.Equal(t, "device", calls[0].Slug)

	stored, err := e.eng.Store().GetRoleBySlug(e.ctx, testOrg, "", "device")
	require.NoError(t, err)
	assert.Equal(t, "me", stored.Metadata["keep"], "other metadata keys survive")

	// And the loader sees the new grants.
	e.assign(t, "device")

	loaded, _, err := perm.NewLoader(e.eng).Load(t.Context(), e.identity())
	require.NoError(t, err)
	assert.Equal(t, want.Publish, loaded.Publish)
}

func TestSetRulesDoesNotMutateStoredMetadata(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", map[string]any{"keep": "me"})

	// The memory store hands back the map it holds, so a mutation would show
	// through this earlier read.
	before, err := e.eng.Store().GetRoleBySlug(e.ctx, testOrg, "", "device")
	require.NoError(t, err)

	require.NoError(t, perm.NewRoles(e.eng).SetRules(t.Context(), e.ten, "device", acl.Rules{Publish: []string{"a"}}))

	assert.NotContains(t, before.Metadata, perm.MetadataKey)
}

func TestSetRulesEmptyRulesClearGrants(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", tetherMD([]string{"a"}, []string{"b"}))

	roles := perm.NewRoles(e.eng)
	require.NoError(t, roles.SetRules(t.Context(), e.ten, "device", acl.Rules{}))

	got, err := roles.GetRules(t.Context(), e.ten, "device")
	require.NoError(t, err)
	assert.Empty(t, got.Publish)
	assert.Empty(t, got.Subscribe)
}

func TestSetRulesRejectsInvalidFilter(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", nil)

	roles := perm.NewRoles(e.eng)

	for name, r := range map[string]acl.Rules{
		"publish":   {Publish: []string{"ok", "a/#/b"}},
		"subscribe": {Subscribe: []string{"a/#/b"}},
		"empty":     {Publish: []string{""}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := roles.SetRules(t.Context(), e.ten, "device", r)
			require.Error(t, err)
			assert.ErrorIs(t, err, tether.ErrInvalidInput)
		})
	}

	err := roles.SetRules(t.Context(), e.ten, "device", acl.Rules{Publish: []string{"a/#/b"}})
	assert.Contains(t, err.Error(), "a/#/b", "the error names the filter")
	assert.Empty(t, e.rec.calls(), "a rejected write emits nothing")

	got, err := roles.GetRules(t.Context(), e.ten, "device")
	require.NoError(t, err)
	assert.Empty(t, got.Publish, "a rejected write stores nothing")
}

func TestRolesMissingRole(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	err := roles.SetRules(t.Context(), e.ten, "ghost", acl.Rules{Publish: []string{"a"}})
	assert.ErrorIs(t, err, tether.ErrNotFound)

	_, err = roles.GetRules(t.Context(), e.ten, "ghost")
	assert.ErrorIs(t, err, tether.ErrNotFound)
	assert.False(t, errors.Is(err, tether.ErrInvalidInput))
}

func TestRolesRejectBadArguments(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.GetRules(t.Context(), nil, "device")
	assert.ErrorIs(t, err, tether.ErrInvalidInput)

	_, err = roles.GetRules(t.Context(), e.ten, "")
	assert.ErrorIs(t, err, tether.ErrInvalidInput)

	assert.ErrorIs(t, roles.SetRules(t.Context(), nil, "device", acl.Rules{}), tether.ErrInvalidInput)
	assert.ErrorIs(t, roles.SetRules(t.Context(), e.ten, "", acl.Rules{}), tether.ErrInvalidInput)
}

func TestRolesIsTenantScoped(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", tetherMD([]string{"a"}, nil))

	other := &tenant.Tenant{ID: id.NewTenantID(), AppID: testApp, ExternalID: "aorg_other", Enabled: true}

	_, err := perm.NewRoles(e.eng).GetRules(t.Context(), other, "device")
	assert.ErrorIs(t, err, tether.ErrNotFound)
}
