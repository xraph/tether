package auth

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/xraph/tether"
)

const (
	// PrincipalKindUser is the Identity.PrincipalKind of a person's JWT.
	PrincipalKindUser = "user"

	// PrincipalKindServiceAccount is the Identity.PrincipalKind of a service
	// account's JWT.
	PrincipalKindServiceAccount = "service_account"

	defaultRefreshInterval = 10 * time.Minute
	defaultLeeway          = 30 * time.Second

	// maxTokenBytes bounds the password that is parsed as a JWT.
	maxTokenBytes = 8 << 10
)

// defaultAlgorithms are the signature algorithms accepted when the config
// names none.
var defaultAlgorithms = []string{"RS256", "EdDSA", "ES256"}

// allowedAlgorithms are the algorithms a config may list. HMAC algorithms and
// "none" are not here and never will be: the verifier holds public keys, and
// a shared-secret algorithm would let anyone who can read the JWKS forge a
// token.
var allowedAlgorithms = []string{
	"RS256", "RS384", "RS512", "PS256", "PS384", "PS512",
	"ES256", "ES384", "ES512", "EdDSA",
}

// JWTConfig configures the authsome JWT authenticator. The zero value means
// JWT authentication is off.
type JWTConfig struct {
	// JWKSURL is where the identity provider publishes its signing keys,
	// authsome's /.well-known/jwks.json. Plain http is allowed, so local
	// development works. Required.
	JWKSURL string `json:"jwks_url" yaml:"jwks_url"`

	// Issuer is the exact "iss" a token must carry. Required.
	Issuer string `json:"issuer" yaml:"issuer"`

	// Audience lists the "aud" values Tether answers to. A token must carry at
	// least one of them. Required.
	Audience []string `json:"audience" yaml:"audience"`

	// Algorithms lists the accepted signature algorithms. Default RS256,
	// EdDSA, ES256. HMAC algorithms and "none" are refused.
	Algorithms []string `json:"algorithms" yaml:"algorithms"`

	// RefreshInterval is how long fetched keys are used before the JWKS is
	// fetched again. Default 10 minutes. Independently of this, a token that
	// names an unknown key ID makes the JWKS fetch again, at most once a
	// minute.
	RefreshInterval time.Duration `json:"refresh_interval" yaml:"refresh_interval"`

	// Leeway is the clock skew tolerated on "exp" and "nbf". Default 30
	// seconds; a negative value means none.
	Leeway time.Duration `json:"leeway" yaml:"leeway"`
}

// IsZero reports whether no field is set, which means JWT authentication is
// off.
func (c JWTConfig) IsZero() bool {
	return c.JWKSURL == "" && c.Issuer == "" && len(c.Audience) == 0 && len(c.Algorithms) == 0 &&
		c.RefreshInterval == 0 && c.Leeway == 0
}

// JWT authenticates MQTT clients with a JWT that authsome issued. The JWT is
// the MQTT password. The username must be the token's subject.
//
// Authenticate checks the signature against the issuer's JWKS, then "exp"
// (required), "nbf", "iss" (equal to the configured issuer) and "aud" (any of
// the configured audiences). authsome's own validator checks neither "iss"
// nor "aud", so a token meant for another service verifies there; here it
// does not.
//
// The tenant comes from the token's "app_id" and "org_id" claims, resolved
// the way a keysmith key's (AppID, TenantID) is. A token without either is
// refused as not authorized. The identity is a copy the caller owns.
//
// Tokens that need more than a bearer check are refused: ones bound to a
// DPoP key ("cnf", which MQTT cannot prove) and ones issued to a third-party
// OAuth client ("client_id", whose scope Tether cannot enforce). So are
// principals other than users and service accounts.
type JWT struct {
	tenants   TenantResolver
	keys      *jwksCache
	algs      []string
	issuer    string
	audiences []string
	leeway    time.Duration
	parser    *jwt.Parser
	now       func() time.Time
}

// wireClaims mirrors the claims authsome puts in an access token
// (tokenformat/jwt.go). Only the ones Tether reads are declared.
type wireClaims struct {
	jwt.RegisteredClaims

	AppID         string `json:"app_id,omitempty"`
	OrgID         string `json:"org_id,omitempty"`
	PrincipalKind string `json:"pk,omitempty"`
	PrincipalID   string `json:"pid,omitempty"`
	ClientID      string `json:"client_id,omitempty"`
	Confirmation  *struct {
		JKT string `json:"jkt,omitempty"`
	} `json:"cnf,omitempty"`
}

// NewJWT returns the JWT authenticator for cfg. It fetches nothing until the
// first token arrives. The error names the field when the config is not
// usable.
func NewJWT(cfg JWTConfig, t TenantResolver) (*JWT, error) {
	if t == nil {
		return nil, fmt.Errorf("%w: auth: jwt needs a tenant resolver", tether.ErrInvalidInput)
	}

	if err := checkJWKSURL(cfg.JWKSURL); err != nil {
		return nil, err
	}

	if cfg.Issuer == "" {
		return nil, fmt.Errorf("%w: auth: jwt issuer is required", tether.ErrInvalidInput)
	}

	if len(cfg.Audience) == 0 || slices.Contains(cfg.Audience, "") {
		return nil, fmt.Errorf("%w: auth: jwt audience needs at least one non-empty value", tether.ErrInvalidInput)
	}

	algs := slices.Clone(cfg.Algorithms)
	if len(algs) == 0 {
		algs = slices.Clone(defaultAlgorithms)
	}

	for _, a := range algs {
		if !slices.Contains(allowedAlgorithms, a) {
			return nil, fmt.Errorf("%w: auth: jwt algorithm %q is not accepted (allowed: %s)",
				tether.ErrInvalidInput, a, strings.Join(allowedAlgorithms, ", "))
		}
	}

	algs = slices.Compact(slices.Clone(algs))

	if cfg.RefreshInterval < 0 {
		return nil, fmt.Errorf("%w: auth: jwt refresh_interval must not be negative", tether.ErrInvalidInput)
	}

	refresh := cfg.RefreshInterval
	if refresh == 0 {
		refresh = defaultRefreshInterval
	}

	leeway := cfg.Leeway

	switch {
	case leeway == 0:
		leeway = defaultLeeway
	case leeway < 0:
		leeway = 0
	}

	j := &JWT{
		tenants:   t,
		algs:      algs,
		issuer:    cfg.Issuer,
		audiences: slices.Clone(cfg.Audience),
		leeway:    leeway,
		now:       time.Now,
	}

	clock := func() time.Time { return j.now() }

	j.keys = newJWKSCache(cfg.JWKSURL, refresh, clock)
	j.parser = jwt.NewParser(
		jwt.WithValidMethods(algs),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(leeway),
		jwt.WithTimeFunc(clock),
	)

	return j, nil
}

func checkJWKSURL(raw string) error {
	if raw == "" {
		return fmt.Errorf("%w: auth: jwt jwks_url is required", tether.ErrInvalidInput)
	}

	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%w: auth: jwt jwks_url %q must be an absolute http or https URL", tether.ErrInvalidInput, raw)
	}

	return nil
}

// Authenticate implements Authenticator. SkipClientIDPin has no meaning for a
// JWT and is ignored.
func (j *JWT) Authenticate(ctx context.Context, c Credentials) (*Identity, error) {
	if len(c.Password) == 0 || len(c.Password) > maxTokenBytes {
		return nil, fmt.Errorf("%w: a token is required as the password", ErrBadCredentials)
	}

	var (
		claims wireClaims
		keyErr error
	)

	_, err := j.parser.ParseWithClaims(string(c.Password), &claims, func(tok *jwt.Token) (any, error) {
		kid, err := tokenKID(tok)
		if err != nil {
			keyErr = err

			return nil, err
		}

		key, err := j.keys.key(ctx, kid, tok.Method.Alg())
		if err != nil {
			keyErr = err

			return nil, err
		}

		return key, nil
	})
	if err != nil {
		if keyErr != nil {
			return nil, keyErr
		}

		return nil, fmt.Errorf("%w: %w", ErrBadCredentials, err)
	}

	return j.identity(ctx, &claims, c)
}

// tokenKID reads the "kid" header: absent is empty, anything but a string is
// an error.
func tokenKID(tok *jwt.Token) (string, error) {
	raw, ok := tok.Header["kid"]
	if !ok {
		return "", nil
	}

	kid, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("%w: token kid must be a string", ErrBadCredentials)
	}

	return kid, nil
}

// identity checks the claims the signature does not cover and builds the
// result. The token's signature, issuer, expiry and not-before are already
// checked.
func (j *JWT) identity(ctx context.Context, claims *wireClaims, c Credentials) (*Identity, error) {
	if !j.audienceOK(claims) {
		return nil, fmt.Errorf("%w: token audience is not one of this broker's", ErrBadCredentials)
	}

	subject := claims.Subject
	if subject == "" {
		subject = claims.PrincipalID
	}

	if subject == "" {
		return nil, fmt.Errorf("%w: token has no sub", ErrBadCredentials)
	}

	// The broker expands %u from the CONNECT username, so it has to be the
	// subject, as a keysmith username has to be the key ID.
	if c.Username != subject {
		return nil, fmt.Errorf("%w: username is not the token subject", ErrBadCredentials)
	}

	kind := claims.PrincipalKind
	if kind == "" {
		kind = PrincipalKindUser
	}

	if kind != PrincipalKindUser && kind != PrincipalKindServiceAccount {
		return nil, fmt.Errorf("%w: principal kind %q cannot connect", ErrNotAuthorized, kind)
	}

	if claims.Confirmation != nil && claims.Confirmation.JKT != "" {
		return nil, fmt.Errorf("%w: token is bound to a DPoP key (cnf), which MQTT cannot prove", ErrNotAuthorized)
	}

	if claims.ClientID != "" {
		return nil, fmt.Errorf("%w: tokens issued to a third-party client (client_id) are not accepted", ErrNotAuthorized)
	}

	if claims.AppID == "" {
		return nil, fmt.Errorf("%w: token has no app_id claim", ErrNotAuthorized)
	}

	if claims.OrgID == "" {
		return nil, fmt.Errorf("%w: token has no org_id claim (authsome must issue tokens with the organization)", ErrNotAuthorized)
	}

	t, err := j.tenants.ResolveTenant(ctx, claims.AppID, claims.OrgID)
	if err != nil {
		if errors.Is(err, tether.ErrNotFound) {
			return nil, fmt.Errorf("%w: no tenant for this token's app and organization", ErrNotAuthorized)
		}

		return nil, fmt.Errorf("%w: resolve tenant: %w", ErrUnavailable, err)
	}

	if !t.Enabled {
		return nil, fmt.Errorf("%w: tenant is disabled", ErrNotAuthorized)
	}

	ident := &Identity{
		Tenant:        t,
		PrincipalKind: kind,
		PrincipalID:   subject,
		Username:      subject,
	}

	if claims.ExpiresAt != nil {
		ident.ExpiresAt = claims.ExpiresAt.Time
	}

	return ident, nil
}

func (j *JWT) audienceOK(claims *wireClaims) bool {
	aud, err := claims.GetAudience()
	if err != nil {
		return false
	}

	for _, a := range aud {
		if slices.Contains(j.audiences, a) {
			return true
		}
	}

	return false
}
