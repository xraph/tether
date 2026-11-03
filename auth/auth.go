// Package auth turns MQTT CONNECT credentials into a tenant-scoped identity.
package auth

import (
	"context"
	"errors"
	"time"

	"github.com/xraph/tether/tenant"
)

// Errors an Authenticator returns. The broker matches them with errors.Is and
// maps each to a CONNACK reason code. Every failure an Authenticator reports
// wraps exactly one of them.
var (
	// ErrBadCredentials means the credentials are not recognised: an unknown,
	// inactive or expired key, or a username that is not the key's ID.
	// CONNACK 0x86.
	ErrBadCredentials = errors.New("tether/auth: bad credentials")

	// ErrNotAuthorized means the credentials are valid but may not connect:
	// the tenant is unknown or disabled, the key has no device, or the device
	// is pinned to another client ID. CONNACK 0x87.
	ErrNotAuthorized = errors.New("tether/auth: not authorized")

	// ErrUnavailable means authentication could not be decided, and the
	// failure is not one this package can tell is about the credentials. It
	// is returned when the device or tenant lookup fails with anything but
	// "not found", when the key validator returns an error that is not a
	// recognised keysmith credential error, and when it returns no key at all.
	// A keysmith key store that cannot answer is one of those: since keysmith
	// v1.9.0 its lookup error is not keysmith.ErrInvalidKey. The broker
	// answers it with CONNACK 0x88 on MQTT 5 and 0x03 on 3.1.1 and counts it
	// as reason "unavailable", and POST /v1/publish answers 503 with no
	// WWW-Authenticate header.
	ErrUnavailable = errors.New("tether/auth: unavailable")

	// ErrRateLimited means the key has been rate limited by its policy.
	// CONNACK 0x9F.
	ErrRateLimited = errors.New("tether/auth: rate limited")
)

// PrincipalKindAPIKey is the Identity.PrincipalKind of a keysmith key.
const PrincipalKindAPIKey = "api_key"

// Credentials are the values from an MQTT CONNECT packet.
type Credentials struct {
	// ClientID is the MQTT client identifier.
	ClientID string

	// Username is the MQTT username. For keysmith it is the key ID.
	Username string

	// Password is the MQTT password. For keysmith it is the raw key.
	Password []byte

	// Remote is the peer's network address, for logs.
	Remote string

	// SkipClientIDPin makes the authenticator ignore the device's client ID
	// pin. Set it only in a non-MQTT caller that has no client ID to offer
	// (the HTTP publish endpoint), and read the pin from Identity.ClientID.
	// The broker never sets it, so a CONNECT is always checked against the
	// pin.
	SkipClientIDPin bool
}

// Identity is an authenticated principal and the tenant it belongs to.
type Identity struct {
	// Tenant is the tenant the principal connects as.
	Tenant *tenant.Tenant

	// PrincipalKind says what PrincipalID names, "api_key" for keysmith.
	PrincipalKind string

	// PrincipalID identifies the principal. For keysmith it is the key ID.
	PrincipalID string

	// Username is the MQTT username the principal authenticated with.
	Username string

	// DeviceID is the Tether device the principal maps to, when there is one.
	DeviceID string

	// ClientID is the client ID the device is pinned to, empty when it is
	// not pinned. It is the device's setting, not the ID a client sent.
	ClientID string

	// ExpiresAt is when the credentials stop being valid. The zero time means
	// they do not expire. The broker disconnects the client at that time.
	ExpiresAt time.Time
}

// copyIdentity returns a copy that shares no mutable state with in.
func copyIdentity(in *Identity) *Identity {
	out := *in

	if in.Tenant != nil {
		t := *in.Tenant
		out.Tenant = &t
	}

	return &out
}

// Authenticator decides whether CONNECT credentials may connect.
type Authenticator interface {
	// Authenticate returns the identity for c, or an error wrapping
	// ErrBadCredentials, ErrNotAuthorized, ErrUnavailable or ErrRateLimited.
	Authenticate(ctx context.Context, c Credentials) (*Identity, error)
}
