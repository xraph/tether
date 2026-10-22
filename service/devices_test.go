package service_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/keysmith"
	kid "github.com/xraph/keysmith/id"
	"github.com/xraph/keysmith/key"
	"github.com/xraph/keysmith/rotation"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"
	"github.com/xraph/warden/assignment"
	wid "github.com/xraph/warden/id"
	"github.com/xraph/warden/role"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/tenant"
)

const (
	devApp = "aapp_1"
	devOrg = "aorg_1"
)

// recordingKeys wraps a key manager and remembers every key it created and the
// context of every call.
type recordingKeys struct {
	service.KeyManager

	mu      sync.Mutex
	created []*key.Key
	ctxs    []context.Context
	// override, when set, rewrites what CreateKey returns.
	override func(*key.CreateResult) *key.CreateResult
}

func (r *recordingKeys) CreateKey(ctx context.Context, in *keysmith.CreateKeyInput) (*key.CreateResult, error) {
	res, err := r.KeyManager.CreateKey(ctx, in)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.created = append(r.created, res.Key)
	r.ctxs = append(r.ctxs, ctx)
	r.mu.Unlock()

	if r.override != nil {
		return r.override(res), nil
	}

	return res, nil
}

func (r *recordingKeys) RevokeKey(ctx context.Context, keyID kid.ID, reason string) error {
	r.mu.Lock()
	r.ctxs = append(r.ctxs, ctx)
	r.mu.Unlock()

	// A real keysmith store on a network checks the context. Memory does not.
	if err := ctx.Err(); err != nil {
		return err
	}

	return r.KeyManager.RevokeKey(ctx, keyID, reason)
}

// failingRoles fails AssignRoleToAPIKey for one slug, UnassignRoleFromAPIKey
// when failUnassign is set and UnassignOtherRolesFromAPIKey when
// failUnassignOthers is set.
type failingRoles struct {
	service.RoleAssigner

	failSlug           string
	failUnassign       bool
	failUnassignOthers bool
	unassigned         int
}

func (f *failingRoles) UnassignOtherRolesFromAPIKey(ctx context.Context, tenantID, keyID string, keep []string) error {
	if f.failUnassignOthers {
		return errors.New("unassign others boom")
	}

	return f.RoleAssigner.UnassignOtherRolesFromAPIKey(ctx, tenantID, keyID, keep)
}

func (f *failingRoles) AssignRoleToAPIKey(ctx context.Context, tenantID, keyID, slug string) error {
	if slug == f.failSlug {
		return errors.New("boom")
	}

	return f.RoleAssigner.AssignRoleToAPIKey(ctx, tenantID, keyID, slug)
}

func (f *failingRoles) UnassignRoleFromAPIKey(ctx context.Context, tenantID, keyID string) error {
	f.unassigned++

	if f.failUnassign {
		return errors.New("unassign boom")
	}

	return f.RoleAssigner.UnassignRoleFromAPIKey(ctx, tenantID, keyID)
}

// assignRecorder is a warden plugin that records role assignments and
// unassignments.
type assignRecorder struct {
	mu         sync.Mutex
	assigned   []*assignment.Assignment
	unassigned []*assignment.Assignment
}

func (*assignRecorder) Name() string { return "assign-recorder" }

func (a *assignRecorder) OnRoleAssigned(_ context.Context, x *assignment.Assignment) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.assigned = append(a.assigned, x)

	return nil
}

func (a *assignRecorder) OnRoleUnassigned(_ context.Context, x *assignment.Assignment) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.unassigned = append(a.unassigned, x)

	return nil
}

func (a *assignRecorder) reset() {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.assigned, a.unassigned = nil, nil
}

func (a *assignRecorder) counts() (assigned, unassigned []*assignment.Assignment) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]*assignment.Assignment(nil), a.assigned...), append([]*assignment.Assignment(nil), a.unassigned...)
}

type devEnv struct {
	st      store.Store
	tenants *service.Tenants
	tenant  *tenant.Tenant
	km      *keysmith.Engine
	keys    *recordingKeys
	wd      *warden.Engine
	roles   *failingRoles
	devices *service.Devices
	wctx    context.Context
	events  *assignRecorder
}

func newDevEnv(t *testing.T) *devEnv {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)

	tn, err := tenants.Create(t.Context(), service.CreateTenantInput{AppID: devApp, ExternalID: devOrg, Name: "Acme"})
	require.NoError(t, err)

	km, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()))
	require.NoError(t, err)

	rec := &assignRecorder{}

	wd, err := warden.NewEngine(warden.WithStore(wmemory.New()), warden.WithPlugin(rec))
	require.NoError(t, err)

	keys := &recordingKeys{KeyManager: km}
	roles := &failingRoles{RoleAssigner: service.NewWardenRoles(wd)}

	e := &devEnv{
		st: st, tenants: tenants, tenant: tn, km: km, keys: keys, wd: wd, roles: roles,
		devices: service.NewDevices(st, tenants, keys, roles),
		wctx:    scope.With(t.Context(), devApp, devOrg),
		events:  rec,
	}

	return e
}

func (e *devEnv) createRole(t *testing.T, slug string) {
	t.Helper()

	require.NoError(t, e.wd.Store().CreateRole(e.wctx, &role.Role{
		TenantID: devOrg, AppID: devApp, Name: slug, Slug: slug,
	}))
}

func (e *devEnv) roleSlugs(t *testing.T, keyID string) []string {
	t.Helper()

	direct, _, err := e.wd.SubjectRoles(e.wctx, warden.SubjectAPIKey, keyID)
	require.NoError(t, err)

	out := make([]string, 0, len(direct))
	for _, r := range direct {
		out = append(out, r.Slug)
	}

	return out
}

func (e *devEnv) roleID(t *testing.T, slug string) wid.RoleID {
	t.Helper()

	r, err := e.wd.Store().GetRoleBySlug(e.wctx, devOrg, "", slug)
	require.NoError(t, err)

	return r.ID
}

func (e *devEnv) validate(t *testing.T, raw string) (*keysmith.ValidationResult, error) {
	t.Helper()

	return e.km.ValidateKey(e.wctx, raw)
}

func (e *devEnv) provision(t *testing.T, in service.ProvisionInput) *service.Credentials {
	t.Helper()

	c, err := e.devices.Provision(t.Context(), e.tenant.ID, in)
	require.NoError(t, err)

	return c
}

func TestProvisionReturnsUsableCredentials(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump", ClientID: "pump-1"})

	require.NotNil(t, c.Device)
	assert.True(t, strings.HasPrefix(c.Username, "akey_"))
	assert.Equal(t, c.Username, c.Device.KeyID)
	assert.Equal(t, "pump", c.Device.Name)
	assert.Equal(t, "pump-1", c.Device.ClientID)
	assert.Equal(t, e.tenant.ID, c.Device.TenantID)
	assert.Equal(t, id.PrefixDevice, c.Device.ID.Prefix())
	assert.True(t, strings.HasPrefix(c.Password, "tdk_live_"), c.Password)

	res, err := e.validate(t, c.Password)
	require.NoError(t, err)
	assert.Equal(t, c.Username, res.Key.ID.String())
	assert.Equal(t, c.Device.ID.String(), res.Key.Metadata["tether_device_id"])
	assert.Equal(t, devApp, res.Key.AppID)
	assert.Equal(t, devOrg, res.Key.TenantID)

	stored, err := e.st.GetDeviceByKey(t.Context(), c.Username)
	require.NoError(t, err)
	assert.Equal(t, c.Device.ID, stored.ID)
}

func TestProvisionHonoursExpiry(t *testing.T) {
	e := newDevEnv(t)

	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	c := e.provision(t, service.ProvisionInput{Name: "pump", ExpiresAt: &exp})

	res, err := e.validate(t, c.Password)
	require.NoError(t, err)
	require.NotNil(t, res.Key.ExpiresAt)
	assert.WithinDuration(t, exp, *res.Key.ExpiresAt, time.Second)
}

func TestProvisionAssignsRoles(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "sensor")
	e.createRole(t, "viewer")

	c := e.provision(t, service.ProvisionInput{Name: "pump", Roles: []string{"sensor", "viewer", "sensor"}})

	assert.ElementsMatch(t, []string{"sensor", "viewer"}, e.roleSlugs(t, c.Username))
	assert.ElementsMatch(t, []string{"sensor", "viewer"}, c.Device.Roles, "the record shows the roles without a warden call")

	got, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sensor", "viewer"}, got.Roles)
}

func TestProvisionRollsBackOnRoleFailure(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "sensor")

	_, err := e.devices.Provision(t.Context(), e.tenant.ID, service.ProvisionInput{
		Name: "pump", Roles: []string{"sensor", "nope"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nope")

	require.Len(t, e.keys.created, 1)

	k := e.keys.created[0]

	got, err := e.km.GetKey(e.wctx, k.ID)
	require.NoError(t, err)
	assert.Equal(t, key.StateRevoked, got.State)

	assert.Empty(t, e.roleSlugs(t, k.ID.String()), "the role that was assigned before the failure is unassigned")

	n, err := e.st.CountDevices(t.Context(), e.tenant.ID)
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestProvisionRollbackSurvivesCancelledContext(t *testing.T) {
	e := newDevEnv(t)

	ctx, cancel := context.WithCancel(t.Context())

	// Cancel as the role assignment fails, so cleanup runs on a dead context.
	e.roles.RoleAssigner = cancellingRoles{cancel: cancel}

	_, err := e.devices.Provision(ctx, e.tenant.ID, service.ProvisionInput{Name: "pump", Roles: []string{"x"}})
	require.Error(t, err)
	require.Len(t, e.keys.created, 1)

	got, err := e.km.GetKey(e.wctx, e.keys.created[0].ID)
	require.NoError(t, err)
	assert.Equal(t, key.StateRevoked, got.State, "cleanup must not depend on the caller's context")
}

type cancellingRoles struct{ cancel context.CancelFunc }

func (c cancellingRoles) AssignRoleToAPIKey(context.Context, string, string, string) error {
	c.cancel()

	return errors.New("assign failed")
}

func (cancellingRoles) UnassignRoleFromAPIKey(context.Context, string, string) error { return nil }

func (cancellingRoles) UnassignOtherRolesFromAPIKey(context.Context, string, string, []string) error {
	return nil
}

func TestProvisionValidatesInput(t *testing.T) {
	e := newDevEnv(t)

	tests := []struct {
		name string
		in   service.ProvisionInput
	}{
		{"missing name", service.ProvisionInput{}},
		{"slash in client ID", service.ProvisionInput{Name: "p", ClientID: "a/b"}},
		{"plus in client ID", service.ProvisionInput{Name: "p", ClientID: "a+"}},
		{"hash in client ID", service.ProvisionInput{Name: "p", ClientID: "#"}},
		{"NUL in client ID", service.ProvisionInput{Name: "p", ClientID: "a\x00b"}},
		{"empty role slug", service.ProvisionInput{Name: "p", Roles: []string{""}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.devices.Provision(t.Context(), e.tenant.ID, tc.in)
			require.ErrorIs(t, err, tether.ErrInvalidInput)
		})
	}

	assert.Empty(t, e.keys.created, "invalid input never reaches keysmith")
}

func TestProvisionUnknownTenant(t *testing.T) {
	e := newDevEnv(t)

	_, err := e.devices.Provision(t.Context(), id.NewTenantID(), service.ProvisionInput{Name: "p"})
	require.ErrorIs(t, err, tether.ErrNotFound)
	assert.Empty(t, e.keys.created)
}

func TestProvisionUsesTenantScopeOnEveryCall(t *testing.T) {
	e := newDevEnv(t)

	// A caller context that carries some other tenant must not win.
	ctx := scope.With(t.Context(), "aapp_other", "aorg_other")

	c, err := e.devices.Provision(ctx, e.tenant.ID, service.ProvisionInput{Name: "p"})
	require.NoError(t, err)

	res, err := e.validate(t, c.Password)
	require.NoError(t, err)
	assert.Equal(t, devApp, res.Key.AppID)
	assert.Equal(t, devOrg, res.Key.TenantID)
}

func TestProvisionRefusesAKeyOutsideTheTenantScope(t *testing.T) {
	e := newDevEnv(t)

	// A key manager that ignores the scope would hand back a key for another
	// tenant. The service must not attach it to a device.
	e.keys.override = func(res *key.CreateResult) *key.CreateResult {
		res.Key.TenantID = "aorg_other"

		return res
	}

	_, err := e.devices.Provision(t.Context(), e.tenant.ID, service.ProvisionInput{Name: "p"})
	require.Error(t, err)

	n, err := e.st.CountDevices(t.Context(), e.tenant.ID)
	require.NoError(t, err)
	assert.Zero(t, n)

	require.Len(t, e.keys.created, 1)

	got, err := e.km.GetKey(e.wctx, e.keys.created[0].ID)
	require.NoError(t, err)
	assert.Equal(t, key.StateRevoked, got.State)
}

func TestRotateKeepsUsername(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	r, err := e.devices.Rotate(t.Context(), e.tenant.ID, c.Device.ID, 0)
	require.NoError(t, err)

	assert.Equal(t, c.Username, r.Username)
	assert.NotEqual(t, c.Password, r.Password)
	assert.Equal(t, c.Device.ID, r.Device.ID)

	res, err := e.validate(t, r.Password)
	require.NoError(t, err)
	assert.Equal(t, c.Username, res.Key.ID.String())

	// The previous password still validates inside the grace window.
	_, err = e.validate(t, c.Password)
	require.NoError(t, err)
}

func TestRotateWithShortGraceStopsOldPassword(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	_, err := e.devices.Rotate(t.Context(), e.tenant.ID, c.Device.ID, time.Nanosecond)
	require.NoError(t, err)

	time.Sleep(5 * time.Millisecond)

	_, err = e.validate(t, c.Password)
	require.Error(t, err)
}

func TestRotateRejectsNegativeGrace(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	_, err := e.devices.Rotate(t.Context(), e.tenant.ID, c.Device.ID, -time.Second)
	require.ErrorIs(t, err, tether.ErrInvalidInput)
}

func TestRotateRecordsManualReason(t *testing.T) {
	e := newDevEnv(t)

	spy := &rotateSpy{KeyManager: e.km}
	d := service.NewDevices(e.st, e.tenants, spy, e.roles)

	c, err := d.Provision(t.Context(), e.tenant.ID, service.ProvisionInput{Name: "pump"})
	require.NoError(t, err)

	_, err = d.Rotate(t.Context(), e.tenant.ID, c.Device.ID, 0)
	require.NoError(t, err)

	assert.Equal(t, rotation.ReasonManual, spy.reason)
	assert.Equal(t, 0, spy.opts, "zero grace passes no grace option, so keysmith's default applies")

	_, err = d.Rotate(t.Context(), e.tenant.ID, c.Device.ID, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, 1, spy.opts)
}

type rotateSpy struct {
	service.KeyManager

	reason rotation.Reason
	opts   int
}

func (r *rotateSpy) RotateKey(ctx context.Context, keyID kid.ID, reason rotation.Reason, opts ...keysmith.RotateOption) (*key.CreateResult, error) {
	r.reason = reason
	r.opts = len(opts)

	return r.KeyManager.RotateKey(ctx, keyID, reason, opts...)
}

func TestRotateRevokedDeviceFails(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})
	require.NoError(t, e.devices.Revoke(t.Context(), e.tenant.ID, c.Device.ID, "test"))

	_, err := e.devices.Rotate(t.Context(), e.tenant.ID, c.Device.ID, 0)
	require.Error(t, err)
}

func TestRevokeKeepsRecordButBlocksKey(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	require.NoError(t, e.devices.Revoke(t.Context(), e.tenant.ID, c.Device.ID, "stolen"))

	_, err := e.validate(t, c.Password)
	require.ErrorIs(t, err, keysmith.ErrKeyInactive)

	got, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.Equal(t, c.Username, got.KeyID)

	// A second revoke is not an error.
	require.NoError(t, e.devices.Revoke(t.Context(), e.tenant.ID, c.Device.ID, "again"))
}

func TestDeleteRevokesKey(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "sensor")

	c := e.provision(t, service.ProvisionInput{Name: "pump", Roles: []string{"sensor"}})

	require.NoError(t, e.devices.Delete(t.Context(), e.tenant.ID, c.Device.ID))

	_, err := e.validate(t, c.Password)
	require.ErrorIs(t, err, keysmith.ErrKeyInactive)

	assert.Empty(t, e.roleSlugs(t, c.Username))

	_, err = e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	_, err = e.st.GetDeviceByKey(t.Context(), c.Username)
	require.ErrorIs(t, err, tether.ErrNotFound)
}

func TestDeleteIsIdempotentOverARevokedKey(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})
	require.NoError(t, e.devices.Revoke(t.Context(), e.tenant.ID, c.Device.ID, "x"))

	require.NoError(t, e.devices.Delete(t.Context(), e.tenant.ID, c.Device.ID))

	_, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)
}

func TestDeleteKeepsRecordWhenUnassignFails(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	e.roles.failUnassign = true

	require.Error(t, e.devices.Delete(t.Context(), e.tenant.ID, c.Device.ID))

	// The key is already revoked and the record is still there, so a retry
	// finishes the job.
	_, err := e.validate(t, c.Password)
	require.ErrorIs(t, err, keysmith.ErrKeyInactive)

	e.roles.failUnassign = false

	require.NoError(t, e.devices.Delete(t.Context(), e.tenant.ID, c.Device.ID))
}

func TestSetRolesReplacesRoles(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "sensor")
	e.createRole(t, "viewer")
	e.createRole(t, "admin")

	c := e.provision(t, service.ProvisionInput{Name: "pump", Roles: []string{"sensor", "viewer"}})

	require.NoError(t, e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, []string{"admin"}))

	assert.Equal(t, []string{"admin"}, e.roleSlugs(t, c.Username))

	got, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"admin"}, got.Roles)

	require.NoError(t, e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, nil))
	assert.Empty(t, e.roleSlugs(t, c.Username))

	got, err = e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.Empty(t, got.Roles)
}

func TestSetRolesRestoresPreviousRolesOnFailure(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "sensor")

	c := e.provision(t, service.ProvisionInput{Name: "pump", Roles: []string{"sensor"}})

	e.events.reset()

	err := e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, []string{"nope"})
	require.Error(t, err)

	assert.Equal(t, []string{"sensor"}, e.roleSlugs(t, c.Username), "the old roles are back")

	_, unassigned := e.events.counts()
	assert.Empty(t, unassigned, "the role the device kept was never unassigned, not even by the restore")

	got, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"sensor"}, got.Roles)
}

// Controller ruling R12: SetRoles applies only the difference, so a role the
// device keeps is never unassigned (a live device would lose its
// subscriptions in the gap).
func TestSetRolesAppliesOnlyTheDiff(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "a")
	e.createRole(t, "b")

	c := e.provision(t, service.ProvisionInput{Name: "pump", Roles: []string{"a"}})

	e.events.reset()
	require.NoError(t, e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, []string{"a", "b"}))

	assigned, unassigned := e.events.counts()
	assert.ElementsMatch(t, []string{"a", "b"}, e.roleSlugs(t, c.Username))
	require.Len(t, assigned, 1, "only b is new")
	assert.Equal(t, e.roleID(t, "b"), assigned[0].RoleID)
	assert.Empty(t, unassigned, "a stays assigned throughout")

	e.events.reset()
	require.NoError(t, e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, []string{"b"}))

	assigned, unassigned = e.events.counts()
	assert.Equal(t, []string{"b"}, e.roleSlugs(t, c.Username))
	assert.Empty(t, assigned)
	require.Len(t, unassigned, 1, "exactly one RoleUnassigned, for a")
	assert.Equal(t, e.roleID(t, "a"), unassigned[0].RoleID)
	assert.Equal(t, "api_key", unassigned[0].SubjectKind)
	assert.Equal(t, c.Username, unassigned[0].SubjectID)

	got, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"b"}, got.Roles)
}

// A failed unassign of a dropped role restores the stored roles without
// touching the ones that stay.
func TestSetRolesRestoresWhenUnassignFails(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "a")
	e.createRole(t, "b")

	c := e.provision(t, service.ProvisionInput{Name: "pump", Roles: []string{"a", "b"}})

	e.roles.failUnassignOthers = true
	err := e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, []string{"b"})
	require.Error(t, err)

	e.roles.failUnassignOthers = false

	assert.ElementsMatch(t, []string{"a", "b"}, e.roleSlugs(t, c.Username))

	got, err := e.devices.Get(t.Context(), e.tenant.ID, c.Device.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, got.Roles)
}

// Unassignment through WardenRoles keeps the bridge's side effects: the
// key's cached warden decisions are dropped, so a check right after sees the
// role gone.
func TestWardenRolesUnassignOthers(t *testing.T) {
	e := newDevEnv(t)
	e.createRole(t, "a")
	e.createRole(t, "b")

	wr := service.NewWardenRoles(e.wd)
	require.NoError(t, wr.AssignRoleToAPIKey(e.wctx, devOrg, "akey_x", "a"))
	require.NoError(t, wr.AssignRoleToAPIKey(e.wctx, devOrg, "akey_x", "b"))
	require.NoError(t, wr.AssignRoleToAPIKey(e.wctx, devOrg, "akey_y", "a"))

	require.NoError(t, wr.UnassignOtherRolesFromAPIKey(e.wctx, devOrg, "akey_x", []string{"b", "missing"}))

	assert.Equal(t, []string{"b"}, e.roleSlugs(t, "akey_x"))
	assert.Equal(t, []string{"a"}, e.roleSlugs(t, "akey_y"), "another key is untouched")

	require.NoError(t, wr.UnassignOtherRolesFromAPIKey(e.wctx, devOrg, "akey_x", nil))
	assert.Empty(t, e.roleSlugs(t, "akey_x"))

	require.ErrorIs(t, wr.UnassignOtherRolesFromAPIKey(e.wctx, "", "akey_x", nil), warden.ErrTenantRequired)
	require.Error(t, wr.UnassignOtherRolesFromAPIKey(e.wctx, devOrg, "", nil))
}

func TestSetRolesValidatesSlugs(t *testing.T) {
	e := newDevEnv(t)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	err := e.devices.SetRoles(t.Context(), e.tenant.ID, c.Device.ID, []string{""})
	require.ErrorIs(t, err, tether.ErrInvalidInput)
}

func TestDevicesTenantScoped(t *testing.T) {
	e := newDevEnv(t)

	other, err := e.tenants.Create(t.Context(), service.CreateTenantInput{AppID: devApp, ExternalID: "aorg_2", Name: "Other"})
	require.NoError(t, err)

	c := e.provision(t, service.ProvisionInput{Name: "pump"})

	_, err = e.devices.Get(t.Context(), other.ID, c.Device.ID)
	require.ErrorIs(t, err, tether.ErrNotFound)

	_, err = e.devices.Rotate(t.Context(), other.ID, c.Device.ID, 0)
	require.ErrorIs(t, err, tether.ErrNotFound)

	require.ErrorIs(t, e.devices.Revoke(t.Context(), other.ID, c.Device.ID, "x"), tether.ErrNotFound)
	require.ErrorIs(t, e.devices.Delete(t.Context(), other.ID, c.Device.ID), tether.ErrNotFound)
	require.ErrorIs(t, e.devices.SetRoles(t.Context(), other.ID, c.Device.ID, nil), tether.ErrNotFound)

	// None of those touched the key.
	_, err = e.validate(t, c.Password)
	require.NoError(t, err)

	list, err := e.devices.List(t.Context(), device.ListFilter{TenantID: other.ID})
	require.NoError(t, err)
	assert.Empty(t, list)

	list, err = e.devices.List(t.Context(), device.ListFilter{TenantID: e.tenant.ID})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, c.Device.ID, list[0].ID)
}

func TestKeysAreScopedToTheTenantsOwnExternalID(t *testing.T) {
	e := newDevEnv(t)

	other, err := e.tenants.Create(t.Context(), service.CreateTenantInput{AppID: "aapp_2", ExternalID: "aorg_2"})
	require.NoError(t, err)

	c, err := e.devices.Provision(t.Context(), other.ID, service.ProvisionInput{Name: "p"})
	require.NoError(t, err)

	res, err := e.validate(t, c.Password)
	require.NoError(t, err)
	assert.Equal(t, "aapp_2", res.Key.AppID)
	assert.Equal(t, "aorg_2", res.Key.TenantID)

	_, err = kid.ParseKeyID(c.Username)
	require.NoError(t, err)
}
