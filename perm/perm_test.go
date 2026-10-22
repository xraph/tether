package perm_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/warden"
	"github.com/xraph/warden/assignment"
	wid "github.com/xraph/warden/id"
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

// updateRecorder is a warden plugin that records role hook calls.
type updateRecorder struct {
	mu      sync.Mutex
	updated []*role.Role
	created []*role.Role
	deleted []wid.RoleID
}

func (u *updateRecorder) OnRoleCreated(_ context.Context, r *role.Role) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.created = append(u.created, r)

	return nil
}

func (u *updateRecorder) OnRoleDeleted(_ context.Context, rid wid.RoleID) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.deleted = append(u.deleted, rid)

	return nil
}

func (u *updateRecorder) hooks() (created []*role.Role, deleted []wid.RoleID) {
	u.mu.Lock()
	defer u.mu.Unlock()

	return append([]*role.Role(nil), u.created...), append([]wid.RoleID(nil), u.deleted...)
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
		"no user ID": {Tenant: e.ten, PrincipalKind: auth.PrincipalKindUser},
		"no kind":    {Tenant: e.ten, PrincipalID: testKey},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, _, err := l.Load(t.Context(), ident)
			require.Error(t, err)
			assert.ErrorIs(t, err, tether.ErrInvalidInput)
		})
	}
}

// assignKind gives the subject of kind (a warden subject kind) and id the role
// with slug, the way warden's own bridges do.
func (e *env) assignKind(t *testing.T, kind warden.SubjectKind, subjectID, slug string) {
	t.Helper()

	r, err := e.eng.Store().GetRoleBySlug(e.ctx, testOrg, "", slug)
	require.NoError(t, err)

	require.NoError(t, e.eng.Store().CreateAssignment(e.ctx, &assignment.Assignment{
		ID:          wid.NewAssignmentID(),
		TenantID:    testOrg,
		RoleID:      r.ID,
		SubjectKind: string(kind),
		SubjectID:   subjectID,
		CreatedAt:   time.Now().UTC(),
	}))
}

func TestLoadUserAndServiceAccountSubjects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		kind  string
		wkind warden.SubjectKind
	}{
		{"user", auth.PrincipalKindUser, warden.SubjectUser},
		{"service account", auth.PrincipalKindServiceAccount, warden.SubjectServiceAcct},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createRole(t, "base", "", tetherMD([]string{"shared/%c"}, nil))
			e.createRole(t, "viewer", "base", tetherMD(nil, []string{"users/%u/inbox"}))
			e.assignKind(t, tc.wkind, "ausr_alice", "viewer")

			ident := &auth.Identity{Tenant: e.ten, PrincipalKind: tc.kind, PrincipalID: "ausr_alice"}

			rules, roleIDs, err := perm.NewLoader(e.eng).Load(t.Context(), ident)
			require.NoError(t, err)

			assert.Equal(t, []string{"shared/%c"}, rules.Publish, "inherited grants come along")
			assert.Equal(t, []string{"users/%u/inbox"}, rules.Subscribe)
			assert.ElementsMatch(t, []string{e.roleID(t, "base"), e.roleID(t, "viewer")}, roleIDs)
		})
	}
}

func TestLoadDoesNotCrossSubjectKinds(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "viewer", "", tetherMD([]string{"x"}, nil))
	e.assignKind(t, warden.SubjectUser, "same-id", "viewer")

	l := perm.NewLoader(e.eng)

	for _, kind := range []string{auth.PrincipalKindAPIKey, auth.PrincipalKindServiceAccount} {
		rules, roleIDs, err := l.Load(t.Context(), &auth.Identity{Tenant: e.ten, PrincipalKind: kind, PrincipalID: "same-id"})
		require.NoError(t, err)

		assert.Empty(t, rules.Publish, kind)
		assert.Empty(t, roleIDs, kind)
	}
}

func TestLoadRefusesPrincipalsThatCannotConnect(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	l := perm.NewLoader(e.eng)

	for _, kind := range []string{"agent", "workload", "robot", "service_acct"} {
		_, _, err := l.Load(t.Context(), &auth.Identity{Tenant: e.ten, PrincipalKind: kind, PrincipalID: "x"})
		require.ErrorIs(t, err, auth.ErrNotAuthorized, kind)
		assert.False(t, errors.Is(err, tether.ErrInvalidInput), kind)
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

func TestCreateStoresRoleAndEmits(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	got, err := roles.Create(t.Context(), e.ten, perm.RoleInput{
		Slug:  "sensor",
		Name:  "Sensor",
		Rules: acl.Rules{Publish: []string{"sensors/%c/up"}, Subscribe: []string{"sensors/%c/#"}},
	})
	require.NoError(t, err)
	assert.Equal(t, &perm.RoleInfo{
		Slug: "sensor", Name: "Sensor",
		Rules: acl.Rules{Publish: []string{"sensors/%c/up"}, Subscribe: []string{"sensors/%c/#"}},
	}, got)

	// It is a warden role in the tenant root namespace, and the same code
	// paths that read grants see it.
	stored, err := e.eng.Store().GetRoleBySlug(e.ctx, testOrg, "", "sensor")
	require.NoError(t, err)
	assert.Equal(t, testApp, stored.AppID)
	assert.False(t, stored.ID.IsNil())

	rules, err := roles.GetRules(t.Context(), e.ten, "sensor")
	require.NoError(t, err)
	assert.Equal(t, got.Rules, rules)

	created, _ := e.rec.hooks()
	require.Len(t, created, 1, "OnRoleCreated fires once")
	assert.Equal(t, "sensor", created[0].Slug)

	// A device holding it gets the grants through the loader.
	e.assign(t, "sensor")

	loaded, ids, err := perm.NewLoader(e.eng).Load(t.Context(), e.identity())
	require.NoError(t, err)
	assert.Equal(t, rules.Publish, loaded.Publish)
	assert.Equal(t, []string{stored.ID.String()}, ids)
}

func TestCreateDefaultsAndEmptyRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	got, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "bare"})
	require.NoError(t, err)
	assert.Equal(t, "bare", got.Name, "the name defaults to the slug")
	assert.Equal(t, []string{}, got.Publish, "lists are never nil")
	assert.Equal(t, []string{}, got.Subscribe)
}

func TestCreateRejectsBadInput(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	for name, in := range map[string]perm.RoleInput{
		"empty slug":     {},
		"upper case":     {Slug: "Sensor"},
		"slash":          {Slug: "a/b"},
		"space":          {Slug: "a b"},
		"leading dash":   {Slug: "-a"},
		"too long":       {Slug: strings.Repeat("a", 64)},
		"bad publish":    {Slug: "ok", Rules: acl.Rules{Publish: []string{"a/#/b"}}},
		"bad subscribe":  {Slug: "ok", Rules: acl.Rules{Subscribe: []string{""}}},
		"unknown parent": {Slug: "ok", ParentSlug: "ghost"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := roles.Create(t.Context(), e.ten, in)
			require.Error(t, err)
			assert.ErrorIs(t, err, tether.ErrInvalidInput)
		})
	}

	_, err := roles.Create(t.Context(), nil, perm.RoleInput{Slug: "ok"})
	assert.ErrorIs(t, err, tether.ErrInvalidInput)

	got, err := roles.List(t.Context(), e.ten)
	require.NoError(t, err)
	assert.Empty(t, got, "a rejected create stores nothing")

	created, _ := e.rec.hooks()
	assert.Empty(t, created, "and emits nothing")
}

func TestCreateDuplicateSlugIsConflict(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "sensor"})
	require.NoError(t, err)

	_, err = roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "sensor"})
	require.Error(t, err)
	assert.ErrorIs(t, err, tether.ErrConflict)

	// Another tenant may use the same slug.
	other := &tenant.Tenant{ID: id.NewTenantID(), AppID: testApp, ExternalID: "aorg_other", Enabled: true}
	_, err = roles.Create(t.Context(), other, perm.RoleInput{Slug: "sensor"})
	require.NoError(t, err)
}

func TestCreateWithParentInherits(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "base", Rules: acl.Rules{Publish: []string{"base/up"}}})
	require.NoError(t, err)

	child, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "child", ParentSlug: "base", Rules: acl.Rules{Publish: []string{"child/up"}}})
	require.NoError(t, err)
	assert.Equal(t, "base", child.ParentSlug)

	e.assign(t, "child")

	loaded, _, err := perm.NewLoader(e.eng).Load(t.Context(), e.identity())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"base/up", "child/up"}, loaded.Publish)
}

func TestListSortedAndTenantScoped(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	none, err := roles.List(t.Context(), e.ten)
	require.NoError(t, err)
	assert.NotNil(t, none, "an empty list is not nil")
	assert.Empty(t, none)

	for _, slug := range []string{"zeta", "alpha", "mid"} {
		_, err = roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: slug, Rules: acl.Rules{Publish: []string{slug + "/up"}}})
		require.NoError(t, err)
	}

	// A role of another tenant, and one in a namespace below the root, are
	// not listed.
	other := &tenant.Tenant{ID: id.NewTenantID(), AppID: testApp, ExternalID: "aorg_other", Enabled: true}
	_, err = roles.Create(t.Context(), other, perm.RoleInput{Slug: "theirs"})
	require.NoError(t, err)

	require.NoError(t, e.eng.Store().CreateRole(e.ctx, &role.Role{
		TenantID: testOrg, AppID: testApp, NamespacePath: "team/a", Name: "nested", Slug: "nested",
	}))

	got, err := roles.List(t.Context(), e.ten)
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, []string{"alpha", "mid", "zeta"}, []string{got[0].Slug, got[1].Slug, got[2].Slug})
	assert.Equal(t, []string{"alpha/up"}, got[0].Publish)

	theirs, err := roles.List(t.Context(), other)
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.Equal(t, "theirs", theirs[0].Slug)
}

func TestListShowsMalformedRoleWithNoGrants(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "broken", "", map[string]any{perm.MetadataKey: "everything"})

	got, err := perm.NewRoles(e.eng).List(t.Context(), e.ten)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "broken", got[0].Slug)
	assert.Equal(t, acl.Rules{Publish: []string{}, Subscribe: []string{}}, got[0].Rules)
}

func TestDeleteRemovesRoleAndEmits(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "sensor"})
	require.NoError(t, err)

	rid := e.roleID(t, "sensor")

	require.NoError(t, roles.Delete(t.Context(), e.ten, "sensor"))

	_, err = roles.GetRules(t.Context(), e.ten, "sensor")
	assert.ErrorIs(t, err, tether.ErrNotFound)

	_, deleted := e.rec.hooks()
	require.Len(t, deleted, 1, "OnRoleDeleted fires once")
	assert.Equal(t, rid, deleted[0].String())

	// Deleting it again is a not found, and the slug can be reused.
	assert.ErrorIs(t, roles.Delete(t.Context(), e.ten, "sensor"), tether.ErrNotFound)

	_, err = roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "sensor"})
	require.NoError(t, err)
}

func TestDeleteRefusedWhileAssigned(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "sensor"})
	require.NoError(t, err)

	e.assign(t, "sensor")

	err = roles.Delete(t.Context(), e.ten, "sensor")
	require.Error(t, err)
	assert.ErrorIs(t, err, tether.ErrConflict)

	_, deleted := e.rec.hooks()
	assert.Empty(t, deleted, "a refused delete emits nothing")

	_, err = roles.GetRules(t.Context(), e.ten, "sensor")
	require.NoError(t, err, "and removes nothing")

	// Once the key lets go, the delete goes through.
	require.NoError(t, keysmithbridge.New(e.eng).UnassignRoleFromAPIKey(e.ctx, testOrg, testKey))
	require.NoError(t, roles.Delete(t.Context(), e.ten, "sensor"))
}

func TestDeleteRefusedWhileParent(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "base"})
	require.NoError(t, err)

	_, err = roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "child", ParentSlug: "base"})
	require.NoError(t, err)

	assert.ErrorIs(t, roles.Delete(t.Context(), e.ten, "base"), tether.ErrConflict)
	require.NoError(t, roles.Delete(t.Context(), e.ten, "child"))
	require.NoError(t, roles.Delete(t.Context(), e.ten, "base"))
}

func TestDeleteIsTenantScoped(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	roles := perm.NewRoles(e.eng)

	_, err := roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "sensor"})
	require.NoError(t, err)

	other := &tenant.Tenant{ID: id.NewTenantID(), AppID: testApp, ExternalID: "aorg_other", Enabled: true}

	assert.ErrorIs(t, roles.Delete(t.Context(), other, "sensor"), tether.ErrNotFound)

	_, err = roles.GetRules(t.Context(), e.ten, "sensor")
	require.NoError(t, err, "the other tenant's delete left it alone")

	assert.ErrorIs(t, roles.Delete(t.Context(), nil, "sensor"), tether.ErrInvalidInput)
	assert.ErrorIs(t, roles.Delete(t.Context(), e.ten, ""), tether.ErrInvalidInput)
}

// A shared filter is not a grant: acl drops it, so storing one would show a
// grant that does nothing. SetRules and Create refuse it in any casing.
func TestShareFiltersAreNotGrants(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createRole(t, "device", "", nil)

	roles := perm.NewRoles(e.eng)

	for _, f := range []string{"$share/g/a", "$SHARE/g/#", "$Share/g/x/y"} {
		err := roles.SetRules(t.Context(), e.ten, "device", acl.Rules{Subscribe: []string{f}})
		require.ErrorIs(t, err, tether.ErrInvalidInput, f)

		_, err = roles.Create(t.Context(), e.ten, perm.RoleInput{Slug: "r", Rules: acl.Rules{Subscribe: []string{f}}})
		require.ErrorIs(t, err, tether.ErrInvalidInput, f)
	}

	got, err := roles.GetRules(t.Context(), e.ten, "device")
	require.NoError(t, err)
	assert.Empty(t, got.Subscribe)
}

// A system role can be neither rewritten nor deleted through Tether: warden's
// store does not check, so perm does.
func TestSystemRolesAreReadOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.eng.Store().CreateRole(e.ctx, &role.Role{
		TenantID: testOrg, AppID: testApp, Name: "admin", Slug: "admin", IsSystem: true,
		Metadata: map[string]any{perm.MetadataKey: map[string]any{"publish": []any{"a/#"}, "subscribe": []any{}}},
	}))

	roles := perm.NewRoles(e.eng)

	err := roles.SetRules(t.Context(), e.ten, "admin", acl.Rules{Publish: []string{"#"}})
	require.ErrorIs(t, err, tether.ErrConflict)

	err = roles.Delete(t.Context(), e.ten, "admin")
	require.ErrorIs(t, err, tether.ErrConflict)

	got, err := roles.GetRules(t.Context(), e.ten, "admin")
	require.NoError(t, err, "the role is still there")
	assert.Equal(t, []string{"a/#"}, got.Publish, "and its grants are unchanged")
	assert.Empty(t, e.rec.calls())
}
