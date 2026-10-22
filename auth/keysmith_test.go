package auth

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/keysmith"
	kid "github.com/xraph/keysmith/id"
	"github.com/xraph/keysmith/key"
	kstore "github.com/xraph/keysmith/store"
	kmemory "github.com/xraph/keysmith/store/memory"

	"github.com/xraph/tether/device"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/tenant"
)

const (
	testApp = "aapp_1"
	testOrg = "aorg_1"
	rawKey  = "tdk_live_secret"
)

// fakeValidator answers ValidateKey from fn and counts the calls.
type fakeValidator struct {
	calls atomic.Int32
	fn    func(raw string) (*keysmith.ValidationResult, error)
}

func (f *fakeValidator) ValidateKey(_ context.Context, raw string) (*keysmith.ValidationResult, error) {
	f.calls.Add(1)

	return f.fn(raw)
}

// countingResolver counts ResolveTenant calls.
type countingResolver struct {
	TenantResolver
	calls atomic.Int32
}

func (c *countingResolver) ResolveTenant(ctx context.Context, appID, externalID string) (*tenant.Tenant, error) {
	c.calls.Add(1)

	return c.TenantResolver.ResolveTenant(ctx, appID, externalID)
}

type env struct {
	st      store.Store
	tenants *service.Tenants
	tenant  *tenant.Tenant
	device  *device.Device
	keyID   kid.KeyID
	val     *fakeValidator
	auth    *Keysmith
	clock   time.Time
}

// newEnv builds a tenant for (testApp, testOrg), a device bound to a fresh key
// ID and a validator that accepts rawKey for that key.
func newEnv(t *testing.T, opts ...KeysmithOption) *env {
	t.Helper()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)

	tn, err := tenants.Create(t.Context(), service.CreateTenantInput{AppID: testApp, ExternalID: testOrg, Name: "Acme"})
	require.NoError(t, err)

	e := &env{st: st, tenants: tenants, tenant: tn, keyID: kid.NewKeyID(), clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}

	e.device = &device.Device{ID: id.NewDeviceID(), TenantID: tn.ID, Name: "pump", KeyID: e.keyID.String()}
	require.NoError(t, st.CreateDevice(t.Context(), e.device))

	e.val = &fakeValidator{fn: func(raw string) (*keysmith.ValidationResult, error) {
		if raw != rawKey {
			return nil, keysmith.ErrInvalidKey
		}

		return e.result(), nil
	}}

	e.auth = NewKeysmith(e.val, tenants, st, opts...)
	e.auth.now = func() time.Time { return e.clock }

	return e
}

func (e *env) result() *keysmith.ValidationResult {
	return &keysmith.ValidationResult{Key: &key.Key{ID: e.keyID, AppID: testApp, TenantID: testOrg, State: key.StateActive}}
}

func (e *env) creds() Credentials {
	return Credentials{ClientID: "pump-1", Username: e.keyID.String(), Password: []byte(rawKey)}
}

func TestUsernameMustEqualKeyID(t *testing.T) {
	e := newEnv(t)

	c := e.creds()
	c.Username = "akey_other"

	_, err := e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestEmptyCredentialsAreBadCredentials(t *testing.T) {
	e := newEnv(t)

	c := e.creds()
	c.Username = ""
	_, err := e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)

	c = e.creds()
	c.Password = nil
	_, err = e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)

	assert.Zero(t, e.val.calls.Load(), "no validator call without a username and password")
}

func TestWrongPasswordIsBadCredentials(t *testing.T) {
	e := newEnv(t)

	c := e.creds()
	c.Password = []byte("tdk_live_wrong")

	_, err := e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestTenantFromKey(t *testing.T) {
	e := newEnv(t)

	ident, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	assert.Equal(t, e.tenant.ID, ident.Tenant.ID)
	assert.Equal(t, "api_key", ident.PrincipalKind)
	assert.Equal(t, e.keyID.String(), ident.PrincipalID)
	assert.Equal(t, e.keyID.String(), ident.Username)
	assert.Equal(t, e.device.ID.String(), ident.DeviceID)
	assert.True(t, ident.ExpiresAt.IsZero())
}

func TestUnknownTenantRefused(t *testing.T) {
	e := newEnv(t)

	e.val.fn = func(string) (*keysmith.ValidationResult, error) {
		r := e.result()
		r.Key.TenantID = "aorg_unknown"

		return r, nil
	}

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestDisabledTenantRefused(t *testing.T) {
	e := newEnv(t)

	off := false
	_, err := e.tenants.Update(t.Context(), e.tenant.ID, service.UpdateTenantInput{Enabled: &off})
	require.NoError(t, err)

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestDisabledTenantRefusedOnCacheHit(t *testing.T) {
	e := newEnv(t)

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	off := false
	_, err = e.tenants.Update(t.Context(), e.tenant.ID, service.UpdateTenantInput{Enabled: &off})
	require.NoError(t, err)

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrNotAuthorized)
	assert.EqualValues(t, 1, e.val.calls.Load(), "the refusal came from the cache path")
}

func TestClientIDPin(t *testing.T) {
	e := newEnv(t)

	e.device.ClientID = "pump-1"
	require.NoError(t, e.st.UpdateDevice(t.Context(), e.device))

	c := e.creds()
	c.ClientID = "pump-2"
	_, err := e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrNotAuthorized)

	c.ClientID = "pump-1"
	_, err = e.auth.Authenticate(t.Context(), c)
	require.NoError(t, err)

	// The pin and the username come from the request, so a cached entry must
	// not skip them.
	c.ClientID = "pump-2"
	_, err = e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrNotAuthorized)

	c = e.creds()
	c.Username = "akey_other"
	_, err = e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)

	assert.EqualValues(t, 1, e.val.calls.Load(), "a refused pin still caches the key, later calls never reach keysmith")
}

// Controller ruling R16. A caller with no MQTT client ID (the HTTP publish
// endpoint) sets SkipClientIDPin and gets the device's pin back in the
// identity. The MQTT path never sets it.
func TestSkipClientIDPin(t *testing.T) {
	e := newEnv(t)

	e.device.ClientID = "pump-1"
	require.NoError(t, e.st.UpdateDevice(t.Context(), e.device))

	http := Credentials{Username: e.keyID.String(), Password: []byte(rawKey), SkipClientIDPin: true}

	// Cold cache, then warm: the skip applies on a cache hit too.
	for range 2 {
		ident, err := e.auth.Authenticate(t.Context(), http)
		require.NoError(t, err)
		assert.Equal(t, "pump-1", ident.ClientID, "the identity carries the pin")
	}

	// The MQTT path is unchanged: no flag, the pin is enforced, on the warm
	// cache entry the HTTP calls left behind as well.
	mqtt := e.creds()
	mqtt.ClientID = "pump-2"
	_, err := e.auth.Authenticate(t.Context(), mqtt)
	require.ErrorIs(t, err, ErrNotAuthorized)

	mqtt.ClientID = ""
	_, err = e.auth.Authenticate(t.Context(), mqtt)
	require.ErrorIs(t, err, ErrNotAuthorized)

	mqtt.ClientID = "pump-1"
	ident, err := e.auth.Authenticate(t.Context(), mqtt)
	require.NoError(t, err)
	assert.Equal(t, "pump-1", ident.ClientID)
}

func TestSkipClientIDPinKeepsEveryOtherCheck(t *testing.T) {
	e := newEnv(t)

	c := Credentials{Username: "akey_other", Password: []byte(rawKey), SkipClientIDPin: true}
	_, err := e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)

	c = Credentials{Username: e.keyID.String(), Password: []byte("tdk_live_nope"), SkipClientIDPin: true}
	_, err = e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrBadCredentials)

	disabled := false
	_, err = e.tenants.Update(t.Context(), e.tenant.ID, service.UpdateTenantInput{Enabled: &disabled})
	require.NoError(t, err)

	c = Credentials{Username: e.keyID.String(), Password: []byte(rawKey), SkipClientIDPin: true}
	_, err = e.auth.Authenticate(t.Context(), c)
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestIdentityClientIDIsEmptyForUnpinnedDevice(t *testing.T) {
	e := newEnv(t)

	ident, err := e.auth.Authenticate(t.Context(), Credentials{Username: e.keyID.String(), Password: []byte(rawKey), SkipClientIDPin: true})
	require.NoError(t, err)
	assert.Empty(t, ident.ClientID)

	// An MQTT client ID is not the device's pin.
	ident, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.Empty(t, ident.ClientID)
}

func TestUnpinnedDeviceAcceptsAnyClientID(t *testing.T) {
	e := newEnv(t)

	for _, cid := range []string{"a", "b", ""} {
		c := e.creds()
		c.ClientID = cid
		_, err := e.auth.Authenticate(t.Context(), c)
		require.NoError(t, err)
	}
}

func TestKeyWithoutDeviceRefused(t *testing.T) {
	e := newEnv(t)

	require.NoError(t, e.st.DeleteDevice(t.Context(), e.tenant.ID, e.device.ID))

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestDeviceOfAnotherTenantRefused(t *testing.T) {
	e := newEnv(t)

	other, err := e.tenants.Create(t.Context(), service.CreateTenantInput{AppID: testApp, ExternalID: "aorg_2"})
	require.NoError(t, err)

	require.NoError(t, e.st.DeleteDevice(t.Context(), e.tenant.ID, e.device.ID))

	e.device.TenantID = other.ID
	require.NoError(t, e.st.CreateDevice(t.Context(), e.device))

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrNotAuthorized)
}

func TestKeysmithErrorsMapToReasons(t *testing.T) {
	cases := []struct {
		name string
		in   error
		want error
	}{
		{"invalid", keysmith.ErrInvalidKey, ErrBadCredentials},
		{"inactive", keysmith.ErrKeyInactive, ErrBadCredentials},
		{"expired", keysmith.ErrKeyExpired, ErrBadCredentials},
		{"revoked", keysmith.ErrKeyRevoked, ErrBadCredentials},
		{"suspended", keysmith.ErrKeySuspended, ErrBadCredentials},
		{"wrapped inactive", errors.Join(errors.New("ctx"), keysmith.ErrKeyInactive), ErrBadCredentials},
		{"rate limited", keysmith.ErrRateLimited, ErrRateLimited},
		{"other", errors.New("db down"), ErrUnavailable},
		{"cancelled", context.Canceled, ErrUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.val.fn = func(string) (*keysmith.ValidationResult, error) { return nil, tc.in }

			_, err := e.auth.Authenticate(t.Context(), e.creds())
			require.ErrorIs(t, err, tc.want)

			// A failure is never cached.
			_, _ = e.auth.Authenticate(t.Context(), e.creds())
			assert.EqualValues(t, 2, e.val.calls.Load())
		})
	}
}

func TestInactiveKey(t *testing.T) {
	e := newEnv(t)

	e.val.fn = func(string) (*keysmith.ValidationResult, error) { return nil, keysmith.ErrKeyInactive }
	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrBadCredentials)

	e.val.fn = func(string) (*keysmith.ValidationResult, error) { return nil, errors.New("boom") }
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrUnavailable)
	assert.NotErrorIs(t, err, ErrBadCredentials)
}

func TestMalformedValidatorResultIsUnavailable(t *testing.T) {
	e := newEnv(t)

	e.val.fn = func(string) (*keysmith.ValidationResult, error) { return nil, nil } //nolint:nilnil // The malformed result under test.
	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrUnavailable)

	e.val.fn = func(string) (*keysmith.ValidationResult, error) { return &keysmith.ValidationResult{}, nil }
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrUnavailable)
}

type failingStore struct {
	store.Store
	err error
}

func (f failingStore) GetDeviceByKey(context.Context, string) (*device.Device, error) {
	return nil, f.err
}

func TestDeviceLookupFailureIsUnavailable(t *testing.T) {
	e := newEnv(t)
	e.auth = NewKeysmith(e.val, e.tenants, failingStore{Store: e.st, err: errors.New("db down")})

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrUnavailable)
}

type failingResolver struct{ err error }

func (f failingResolver) ResolveTenant(context.Context, string, string) (*tenant.Tenant, error) {
	return nil, f.err
}

func TestTenantResolveFailureIsUnavailable(t *testing.T) {
	e := newEnv(t)
	e.auth = NewKeysmith(e.val, failingResolver{err: errors.New("db down")}, e.st)

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrUnavailable)
}

func TestCacheAndEvict(t *testing.T) {
	e := newEnv(t)

	for range 2 {
		_, err := e.auth.Authenticate(t.Context(), e.creds())
		require.NoError(t, err)
	}

	assert.EqualValues(t, 1, e.val.calls.Load())

	e.auth.Evict(e.keyID.String())

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.val.calls.Load())
}

func TestEvictRemovesEveryEntryForTheKey(t *testing.T) {
	e := newEnv(t)

	// A rotated key authenticates under a second raw value for the same key ID.
	e.val.fn = func(raw string) (*keysmith.ValidationResult, error) {
		if raw != rawKey && raw != rawKey+"2" {
			return nil, keysmith.ErrInvalidKey
		}

		return e.result(), nil
	}

	c2 := e.creds()
	c2.Password = []byte(rawKey + "2")

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	_, err = e.auth.Authenticate(t.Context(), c2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.val.calls.Load())

	e.auth.Evict(e.keyID.String())

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	_, err = e.auth.Authenticate(t.Context(), c2)
	require.NoError(t, err)
	assert.EqualValues(t, 4, e.val.calls.Load())
}

func TestCacheEntryExpires(t *testing.T) {
	e := newEnv(t, WithCacheTTL(10*time.Second))

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	e.clock = e.clock.Add(9 * time.Second)
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.val.calls.Load())

	e.clock = e.clock.Add(2 * time.Second)
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.val.calls.Load())
}

func TestZeroCacheTTLDisablesCache(t *testing.T) {
	e := newEnv(t, WithCacheTTL(0))

	for range 2 {
		_, err := e.auth.Authenticate(t.Context(), e.creds())
		require.NoError(t, err)
	}

	assert.EqualValues(t, 2, e.val.calls.Load())
}

func TestDefaultCacheTTLIs60Seconds(t *testing.T) {
	e := newEnv(t)

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	e.clock = e.clock.Add(59 * time.Second)
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.val.calls.Load())

	e.clock = e.clock.Add(2 * time.Second)
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.val.calls.Load())
}

func TestExpiresAtFromKey(t *testing.T) {
	e := newEnv(t)

	exp := e.clock.Add(time.Hour)
	e.val.fn = func(string) (*keysmith.ValidationResult, error) {
		r := e.result()
		r.Key.ExpiresAt = &exp

		return r, nil
	}

	ident, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.True(t, exp.Equal(ident.ExpiresAt))
}

func TestCacheTTLStopsAtKeyExpiry(t *testing.T) {
	e := newEnv(t)

	exp := e.clock.Add(20 * time.Second)
	e.val.fn = func(string) (*keysmith.ValidationResult, error) {
		r := e.result()
		r.Key.ExpiresAt = &exp

		return r, nil
	}

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	e.clock = e.clock.Add(19 * time.Second)
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 1, e.val.calls.Load())

	// Past the key's expiry the entry is gone, well inside the 60 s default,
	// and keysmith gets to say the key expired.
	e.clock = e.clock.Add(2 * time.Second)
	e.val.fn = func(string) (*keysmith.ValidationResult, error) { return nil, keysmith.ErrKeyExpired }

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.ErrorIs(t, err, ErrBadCredentials)
	assert.EqualValues(t, 2, e.val.calls.Load())
}

func TestPreviousKeyIsBoundedByGraceWindow(t *testing.T) {
	e := newEnv(t)

	grace := e.clock.Add(30 * time.Second)
	e.val.fn = func(string) (*keysmith.ValidationResult, error) {
		r := e.result()
		r.ViaPreviousKey = true
		r.GraceEnds = &grace

		return r, nil
	}

	ident, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.True(t, grace.Equal(ident.ExpiresAt), "the session ends when the previous key stops validating")

	e.clock = e.clock.Add(31 * time.Second)
	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.EqualValues(t, 2, e.val.calls.Load())
}

func TestIdentityIsACopy(t *testing.T) {
	e := newEnv(t)

	first, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	first.PrincipalID = "tampered"
	first.Tenant.Name = "tampered"

	second, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.Equal(t, e.keyID.String(), second.PrincipalID)
	assert.Equal(t, "Acme", second.Tenant.Name)
}

func TestCacheHitResolvesTenantEachTime(t *testing.T) {
	e := newEnv(t)

	cr := &countingResolver{TenantResolver: e.tenants}
	e.auth = NewKeysmith(e.val, cr, e.st)

	for range 3 {
		_, err := e.auth.Authenticate(t.Context(), e.creds())
		require.NoError(t, err)
	}

	assert.EqualValues(t, 1, e.val.calls.Load())
	assert.EqualValues(t, 3, cr.calls.Load())
}

// TestRealKeysmithEngine runs one device through a real keysmith engine, the
// way the broker will: a key created under scope.With, a device bound to its
// key ID, and the MQTT credentials being the key ID and the raw key.
func TestRealKeysmithEngine(t *testing.T) {
	ctx := t.Context()

	eng, err := keysmith.NewEngine(keysmith.WithStore(kmemory.New()))
	require.NoError(t, err)

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	tenants := service.NewTenants(st)
	tn, err := tenants.Create(ctx, service.CreateTenantInput{AppID: testApp, ExternalID: testOrg})
	require.NoError(t, err)

	created, err := eng.CreateKey(scope.With(ctx, testApp, testOrg), &keysmith.CreateKeyInput{
		Name: "pump", Prefix: "tdk", Environment: key.EnvLive,
	})
	require.NoError(t, err)

	keyID := created.Key.ID.String()
	require.NoError(t, st.CreateDevice(ctx, &device.Device{
		ID: id.NewDeviceID(), TenantID: tn.ID, Name: "pump", KeyID: keyID,
	}))

	a := NewKeysmith(eng, tenants, st)

	ident, err := a.Authenticate(ctx, Credentials{ClientID: "c", Username: keyID, Password: []byte(created.RawKey)})
	require.NoError(t, err)
	assert.Equal(t, tn.ID, ident.Tenant.ID)

	_, err = a.Authenticate(ctx, Credentials{ClientID: "c", Username: keyID, Password: []byte("tdk_live_nope")})
	require.ErrorIs(t, err, ErrBadCredentials)

	// Revoking the key takes effect once the cache entry is evicted.
	require.NoError(t, eng.RevokeKey(scope.With(ctx, testApp, testOrg), created.Key.ID, "test"))
	a.Evict(keyID)

	_, err = a.Authenticate(ctx, Credentials{ClientID: "c", Username: keyID, Password: []byte(created.RawKey)})
	require.ErrorIs(t, err, ErrBadCredentials)
}

// failingKeyLookupStore is a keysmith store whose key lookup by hash fails the
// way a database outage would. Everything else delegates to the embedded store.
type failingKeyLookupStore struct {
	kstore.Store
}

func (s failingKeyLookupStore) Keys() key.Store {
	return failingKeyLookup{Store: s.Store.Keys()}
}

type failingKeyLookup struct {
	key.Store
}

func (failingKeyLookup) GetByHash(context.Context, string) (*key.Key, error) {
	return nil, errors.New("connection refused")
}

// TestKeysmithLookupOutageSurfacesAsBadCredentials pins a known keysmith
// v1.8.0 limitation, not behaviour we want. keysmith reports any key lookup
// failure, an outage included, as keysmith.ErrInvalidKey, so Tether sees a
// wrong key and answers ErrBadCredentials (CONNACK 0x86) instead of
// ErrUnavailable (0x88). When keysmith distinguishes outages, this test fails:
// flip the expectation to ErrUnavailable and update auth/llm.txt and the
// ErrUnavailable doc comment.
func TestKeysmithLookupOutageSurfacesAsBadCredentials(t *testing.T) {
	ctx := t.Context()

	eng, err := keysmith.NewEngine(keysmith.WithStore(failingKeyLookupStore{Store: kmemory.New()}))
	require.NoError(t, err)

	e := newEnv(t)
	a := NewKeysmith(eng, e.tenants, e.st)

	_, err = a.Authenticate(ctx, Credentials{ClientID: "c", Username: e.keyID.String(), Password: []byte(rawKey)})
	require.ErrorIs(t, err, ErrBadCredentials)
	require.NotErrorIs(t, err, ErrUnavailable)
	require.ErrorIs(t, err, keysmith.ErrInvalidKey)
}

// A key revoked while its validation is in flight: the revoke's Evict runs
// before the validation stores its result. The late store must not bring the
// key back for the cache TTL, or a revoked device keeps connecting.
func TestEvictDuringValidationIsNotUndone(t *testing.T) {
	e := newEnv(t)

	first := true
	e.val.fn = func(string) (*keysmith.ValidationResult, error) {
		if first {
			first = false
			// The revoke lands here, after keysmith answered but before
			// the answer is cached.
			e.auth.Evict(e.keyID.String())
		}

		return e.result(), nil
	}

	_, err := e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.Equal(t, int32(2), e.val.calls.Load(), "a result validated across an Evict is not cached")

	_, err = e.auth.Authenticate(t.Context(), e.creds())
	require.NoError(t, err)
	assert.Equal(t, int32(2), e.val.calls.Load(), "a validation with no Evict in between is cached as before")
}
