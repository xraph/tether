package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xraph/keysmith"

	"github.com/xraph/tether"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/tenant"
)

// defaultCacheTTL is how long a successful keysmith validation is remembered.
const defaultCacheTTL = 60 * time.Second

// KeyValidator validates a raw API key. *keysmith.Engine satisfies it.
type KeyValidator interface {
	ValidateKey(ctx context.Context, rawKey string) (*keysmith.ValidationResult, error)
}

// TenantResolver maps the host application's (app, external) pair to a
// tenant. *service.Tenants satisfies it.
type TenantResolver interface {
	ResolveTenant(ctx context.Context, appID, externalID string) (*tenant.Tenant, error)
}

// DeviceLookup finds the device that owns a keysmith key. Every store.Store
// satisfies it.
type DeviceLookup interface {
	GetDeviceByKey(ctx context.Context, keyID string) (*device.Device, error)
}

// KeysmithOption configures a Keysmith authenticator.
type KeysmithOption func(*Keysmith)

// WithCacheTTL sets how long a successful key validation is cached. The
// default is 60 s. Zero or less turns the cache off. An entry never outlives
// the key's own expiry.
func WithCacheTTL(d time.Duration) KeysmithOption {
	return func(k *Keysmith) { k.ttl = d }
}

// Keysmith authenticates devices with keysmith API keys. The MQTT username is
// the key ID and the password is the raw key.
//
// Authenticate validates the key, requires the username to equal the key's ID
// (the raw key does not carry its ID), reads the tenant from the key's
// (AppID, TenantID) pair, and requires an enabled tenant, a Tether device for
// the key in that tenant, and a matching client ID when the device is pinned
// to one.
//
// A successful validation is cached for the TTL, keyed by the SHA-256 of the
// raw password. A cache hit still checks the username and the client ID pin,
// which come from the request, and resolves the tenant again, so disabling a
// tenant takes effect at once. It does not call keysmith again, which means a
// revoked key keeps working until its entry expires or Evict(keyID) is called,
// and keysmith's per-key rate limit and usage counting see one call per TTL
// instead of one per CONNECT.
type Keysmith struct {
	v   KeyValidator
	t   TenantResolver
	d   DeviceLookup
	ttl time.Duration
	now func() time.Time

	mu        sync.Mutex
	entries   map[[sha256.Size]byte]*entry
	byKey     map[string]map[[sha256.Size]byte]struct{}
	lastSweep time.Time

	// evictions counts Evict calls. A validation that saw it change while
	// keysmith was answering does not cache its result, so an Evict for a
	// revoke can never be undone by a validation that started before it.
	evictions uint64
}

// entry is what a successful validation leaves behind. Request-derived checks
// and the tenant lookup run again on every hit.
type entry struct {
	keyID          string
	appID          string
	externalID     string
	deviceID       string
	deviceTenantID string
	pinnedClientID string
	expiresAt      time.Time // Of the credentials; zero means never.
	cacheUntil     time.Time
}

// NewKeysmith returns a keysmith-backed Authenticator.
func NewKeysmith(v KeyValidator, t TenantResolver, d DeviceLookup, opts ...KeysmithOption) *Keysmith {
	k := &Keysmith{
		v:       v,
		t:       t,
		d:       d,
		ttl:     defaultCacheTTL,
		now:     time.Now,
		entries: make(map[[sha256.Size]byte]*entry),
		byKey:   make(map[string]map[[sha256.Size]byte]struct{}),
	}

	for _, o := range opts {
		o(k)
	}

	return k
}

// Authenticate implements Authenticator.
func (k *Keysmith) Authenticate(ctx context.Context, c Credentials) (*Identity, error) {
	if c.Username == "" || len(c.Password) == 0 {
		return nil, fmt.Errorf("%w: username and password are required", ErrBadCredentials)
	}

	sum := sha256.Sum256(c.Password)

	e, ok := k.lookup(sum)
	if !ok {
		var err error

		e, err = k.validate(ctx, c, sum)
		if err != nil {
			return nil, err
		}
	}

	if e.keyID != c.Username {
		return nil, fmt.Errorf("%w: username is not the key ID", ErrBadCredentials)
	}

	return k.identity(ctx, e, c)
}

// Evict forgets every cached validation for the key ID. Call it when a key is
// revoked, rotated or suspended, or its device changes. A validation already
// running when Evict is called does not cache its result.
func (k *Keysmith) Evict(keyID string) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.evictions++

	for sum := range k.byKey[keyID] {
		delete(k.entries, sum)
	}

	delete(k.byKey, keyID)
}

// lookup returns the live cache entry for the hashed password.
func (k *Keysmith) lookup(sum [sha256.Size]byte) (*entry, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()

	e, ok := k.entries[sum]
	if !ok {
		return nil, false
	}

	if !k.now().Before(e.cacheUntil) {
		k.removeLocked(sum, e)

		return nil, false
	}

	return e, true
}

// validate asks keysmith about the raw key, finds the device and caches the
// result.
func (k *Keysmith) validate(ctx context.Context, c Credentials, sum [sha256.Size]byte) (*entry, error) {
	k.mu.Lock()
	epoch := k.evictions
	k.mu.Unlock()

	res, err := k.v.ValidateKey(ctx, string(c.Password))
	if err != nil {
		return nil, mapKeysmithError(err)
	}

	if res == nil || res.Key == nil || res.Key.ID.IsNil() {
		return nil, fmt.Errorf("%w: key service returned no key", ErrUnavailable)
	}

	keyID := res.Key.ID.String()
	if keyID != c.Username {
		return nil, fmt.Errorf("%w: username is not the key ID", ErrBadCredentials)
	}

	dev, err := k.d.GetDeviceByKey(ctx, keyID)
	if err != nil {
		if errors.Is(err, tether.ErrNotFound) {
			return nil, fmt.Errorf("%w: key has no device", ErrNotAuthorized)
		}

		return nil, fmt.Errorf("%w: look up device: %w", ErrUnavailable, err)
	}

	now := k.now()
	e := &entry{
		keyID:          keyID,
		appID:          res.Key.AppID,
		externalID:     res.Key.TenantID,
		deviceID:       dev.ID.String(),
		deviceTenantID: dev.TenantID.String(),
		pinnedClientID: dev.ClientID,
	}

	if res.Key.ExpiresAt != nil {
		e.expiresAt = *res.Key.ExpiresAt
	}

	// A key presented after rotation stops validating when its grace window
	// ends, so the session ends there too.
	if res.ViaPreviousKey && res.GraceEnds != nil && (e.expiresAt.IsZero() || res.GraceEnds.Before(e.expiresAt)) {
		e.expiresAt = *res.GraceEnds
	}

	ttl := k.ttl
	if !e.expiresAt.IsZero() {
		ttl = min(ttl, e.expiresAt.Sub(now))
	}

	if ttl > 0 {
		e.cacheUntil = now.Add(ttl)
		k.store(sum, e, epoch)
	}

	return e, nil
}

// identity runs the checks that depend on live state or on the request and
// builds the result.
func (k *Keysmith) identity(ctx context.Context, e *entry, c Credentials) (*Identity, error) {
	t, err := k.t.ResolveTenant(ctx, e.appID, e.externalID)
	if err != nil {
		if errors.Is(err, tether.ErrNotFound) {
			return nil, fmt.Errorf("%w: no tenant for this key", ErrNotAuthorized)
		}

		return nil, fmt.Errorf("%w: resolve tenant: %w", ErrUnavailable, err)
	}

	if !t.Enabled {
		return nil, fmt.Errorf("%w: tenant is disabled", ErrNotAuthorized)
	}

	if t.ID.String() != e.deviceTenantID {
		return nil, fmt.Errorf("%w: device belongs to another tenant", ErrNotAuthorized)
	}

	if !c.SkipClientIDPin && e.pinnedClientID != "" && e.pinnedClientID != c.ClientID {
		return nil, fmt.Errorf("%w: device is pinned to another client ID", ErrNotAuthorized)
	}

	return &Identity{
		Tenant:        t,
		PrincipalKind: PrincipalKindAPIKey,
		PrincipalID:   e.keyID,
		Username:      c.Username,
		DeviceID:      e.deviceID,
		ClientID:      e.pinnedClientID,
		ExpiresAt:     e.expiresAt,
	}, nil
}

// store adds an entry and, at most once per TTL, sweeps out dead ones. It
// stores nothing when Evict ran since epoch was read: the validation may
// predate a revoke.
func (k *Keysmith) store(sum [sha256.Size]byte, e *entry, epoch uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.evictions != epoch {
		return
	}

	now := k.now()

	if now.Sub(k.lastSweep) >= k.ttl {
		for s, old := range k.entries {
			if !now.Before(old.cacheUntil) {
				k.removeLocked(s, old)
			}
		}

		k.lastSweep = now
	}

	if old, ok := k.entries[sum]; ok {
		k.removeLocked(sum, old)
	}

	k.entries[sum] = e

	set, ok := k.byKey[e.keyID]
	if !ok {
		set = make(map[[sha256.Size]byte]struct{})
		k.byKey[e.keyID] = set
	}

	set[sum] = struct{}{}
}

// removeLocked drops one entry and its index slot. The caller holds k.mu.
func (k *Keysmith) removeLocked(sum [sha256.Size]byte, e *entry) {
	delete(k.entries, sum)

	if set := k.byKey[e.keyID]; set != nil {
		delete(set, sum)

		if len(set) == 0 {
			delete(k.byKey, e.keyID)
		}
	}
}

// mapKeysmithError turns a ValidateKey failure into an auth error. Only the
// failures that say something about the credentials are bad credentials. A
// key over its rate or its usage quota is rate limited (CONNACK 0x9F), so a
// device backs off rather than retrying into an outage code. Anything else
// is an outage.
func mapKeysmithError(err error) error {
	switch {
	case errors.Is(err, keysmith.ErrRateLimited), errors.Is(err, keysmith.ErrQuotaExceeded):
		return fmt.Errorf("%w: %w", ErrRateLimited, err)
	case errors.Is(err, keysmith.ErrInvalidKey),
		errors.Is(err, keysmith.ErrKeyInactive),
		errors.Is(err, keysmith.ErrKeyExpired),
		errors.Is(err, keysmith.ErrKeyRevoked),
		errors.Is(err, keysmith.ErrKeySuspended):
		return fmt.Errorf("%w: %w", ErrBadCredentials, err)
	default:
		return fmt.Errorf("%w: validate key: %w", ErrUnavailable, err)
	}
}
