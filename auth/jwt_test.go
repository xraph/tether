package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/tether/service"
)

const (
	testIssuer   = "https://auth.example.com"
	testAudience = "tether"
)

var (
	keyOnce sync.Once
	rsaKeyA *rsa.PrivateKey
	rsaKeyB *rsa.PrivateKey
	ecKey   *ecdsa.PrivateKey
	edPub   ed25519.PublicKey
	edPriv  ed25519.PrivateKey
)

func genKeys(t *testing.T) {
	t.Helper()

	keyOnce.Do(func() {
		var err error

		rsaKeyA, err = rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		rsaKeyB, err = rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		ecKey, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)

		edPub, edPriv, err = ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
	})
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func rsaJWK(kid string, pub *rsa.PublicKey) map[string]any {
	jwk := map[string]any{
		"kty": "RSA", "use": "sig", "alg": "RS256",
		"n": b64(pub.N.Bytes()),
		"e": b64(big.NewInt(int64(pub.E)).Bytes()),
	}
	if kid != "" {
		jwk["kid"] = kid
	}

	return jwk
}

func ecJWK(pub *ecdsa.PublicKey) map[string]any {
	raw, err := pub.Bytes() // 0x04 || X || Y
	if err != nil {
		panic(err)
	}

	return map[string]any{
		"kty": "EC", "use": "sig", "alg": "ES256", "crv": "P-256", "kid": "ec-1",
		"x": b64(raw[1:33]),
		"y": b64(raw[33:]),
	}
}

func edJWK(kid string, pub ed25519.PublicKey) map[string]any {
	jwk := map[string]any{"kty": "OKP", "use": "sig", "alg": "EdDSA", "crv": "Ed25519", "x": b64(pub)}
	if kid != "" {
		jwk["kid"] = kid
	}

	return jwk
}

// jwtKit is a JWKS server, an authenticator pointed at it and a token minter.
type jwtKit struct {
	t     *testing.T
	e     *env
	srv   *httptest.Server
	hits  atomic.Int32
	delay atomic.Int64 // Nanoseconds the JWKS handler sleeps.

	mu       sync.Mutex
	keys     []map[string]any
	override func(w http.ResponseWriter, r *http.Request)

	clock time.Time
	j     *JWT
}

func newJWTKit(t *testing.T, mutate ...func(*JWTConfig)) *jwtKit {
	t.Helper()
	genKeys(t)

	k := &jwtKit{t: t, e: newEnv(t), clock: time.Now().Truncate(time.Second)}
	k.keys = []map[string]any{rsaJWK("rsa-a", &rsaKeyA.PublicKey), ecJWK(&ecKey.PublicKey)}

	k.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.hits.Add(1)

		if d := k.delay.Load(); d > 0 {
			time.Sleep(time.Duration(d))
		}

		k.mu.Lock()
		over, keys := k.override, k.keys
		body, _ := json.Marshal(map[string]any{"keys": keys})
		k.mu.Unlock()

		if over != nil {
			over(w, r)

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(k.srv.Close)

	cfg := JWTConfig{JWKSURL: k.srv.URL + "/.well-known/jwks.json", Issuer: testIssuer, Audience: []string{testAudience}}
	for _, m := range mutate {
		m(&cfg)
	}

	j, err := NewJWT(cfg, k.e.tenants)
	require.NoError(t, err)

	j.now = func() time.Time { return k.clock }
	k.j = j

	return k
}

func (k *jwtKit) setKeys(keys ...map[string]any) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.keys = keys
}

func (k *jwtKit) setOverride(f func(w http.ResponseWriter, r *http.Request)) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.override = f
}

// claims returns a valid claim set for the test tenant.
func (k *jwtKit) claims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss":    testIssuer,
		"aud":    []string{testAudience},
		"sub":    "ausr_alice",
		"iat":    k.clock.Unix(),
		"nbf":    k.clock.Unix(),
		"exp":    k.clock.Add(time.Hour).Unix(),
		"app_id": testApp,
		"org_id": testOrg,
		"env_id": "aenv_1",
		"sid":    "asess_1",
	}
}

func (k *jwtKit) sign(method jwt.SigningMethod, key any, kid string, claims jwt.MapClaims) string {
	k.t.Helper()

	tok := jwt.NewWithClaims(method, claims)
	if kid != "" {
		tok.Header["kid"] = kid
	}

	s, err := tok.SignedString(key)
	require.NoError(k.t, err)

	return s
}

func (k *jwtKit) rs256(claims jwt.MapClaims) string {
	return k.sign(jwt.SigningMethodRS256, rsaKeyA, "rsa-a", claims)
}

func (k *jwtKit) auth(token string) (*Identity, error) {
	return k.j.Authenticate(k.t.Context(), Credentials{ClientID: "c1", Username: "ausr_alice", Password: []byte(token)})
}

func (k *jwtKit) mustAuth(token string) *Identity {
	k.t.Helper()

	ident, err := k.auth(token)
	require.NoError(k.t, err)

	return ident
}

func TestJWTValidTokenResolvesTenant(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	ident := k.mustAuth(k.rs256(claims))

	assert.Equal(t, k.e.tenant.ID, ident.Tenant.ID)
	assert.Equal(t, PrincipalKindUser, ident.PrincipalKind)
	assert.Equal(t, "ausr_alice", ident.PrincipalID)
	assert.Equal(t, "ausr_alice", ident.Username)
	assert.Empty(t, ident.DeviceID)
	assert.Empty(t, ident.ClientID)
	assert.True(t, ident.ExpiresAt.Equal(k.clock.Add(time.Hour)), "ExpiresAt follows exp, got %s", ident.ExpiresAt)
}

func TestJWTES256AndEdDSATokens(t *testing.T) {
	k := newJWTKit(t)
	k.setKeys(ecJWK(&ecKey.PublicKey), edJWK("ed-1", edPub))

	k.mustAuth(k.sign(jwt.SigningMethodES256, ecKey, "ec-1", k.claims()))
	k.mustAuth(k.sign(jwt.SigningMethodEdDSA, edPriv, "ed-1", k.claims()))
}

func TestJWTRejectsWrongIssuer(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	claims["iss"] = "https://evil.example.com"

	_, err := k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials)

	delete(claims, "iss")

	_, err = k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWTAudienceMustIntersect(t *testing.T) {
	k := newJWTKit(t, func(c *JWTConfig) { c.Audience = []string{"tether", "tether-eu"} })

	claims := k.claims()
	claims["aud"] = []string{"somebody-else"}

	_, err := k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials)

	delete(claims, "aud")

	_, err = k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials)

	// Any one of the configured audiences is enough, in a list or as a string.
	claims["aud"] = []string{"x", "tether-eu"}
	k.mustAuth(k.rs256(claims))

	claims["aud"] = "tether"
	k.mustAuth(k.rs256(claims))
}

func TestJWTExpiry(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	claims["exp"] = k.clock.Add(-time.Minute).Unix()

	_, err := k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials)

	// Inside the leeway an expired token still passes, outside it does not.
	claims["exp"] = k.clock.Add(-10 * time.Second).Unix()
	k.mustAuth(k.rs256(claims))

	delete(claims, "exp")

	_, err = k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials, "a token without exp never expires, so it is refused")
}

func TestJWTNotBefore(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	claims["nbf"] = k.clock.Add(time.Hour).Unix()

	_, err := k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWTLeewayIsConfigurable(t *testing.T) {
	k := newJWTKit(t, func(c *JWTConfig) { c.Leeway = -1 })

	claims := k.claims()
	claims["exp"] = k.clock.Add(-time.Second).Unix()

	_, err := k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrBadCredentials, "a negative leeway turns it off")
}

func TestJWTMissingTenantClaimsAreNotAuthorized(t *testing.T) {
	for _, name := range []string{"org_id", "app_id"} {
		t.Run(name, func(t *testing.T) {
			k := newJWTKit(t)

			claims := k.claims()
			delete(claims, name)

			_, err := k.auth(k.rs256(claims))
			require.ErrorIs(t, err, ErrNotAuthorized)
			assert.Contains(t, err.Error(), name)

			claims[name] = ""

			_, err = k.auth(k.rs256(claims))
			require.ErrorIs(t, err, ErrNotAuthorized)
			assert.Contains(t, err.Error(), name)
		})
	}
}

func TestJWTTenantErrors(t *testing.T) {
	t.Run("unknown tenant", func(t *testing.T) {
		k := newJWTKit(t)

		claims := k.claims()
		claims["org_id"] = "aorg_other"

		_, err := k.auth(k.rs256(claims))
		require.ErrorIs(t, err, ErrNotAuthorized)
	})

	t.Run("disabled tenant", func(t *testing.T) {
		k := newJWTKit(t)

		disabled := false
		_, err := k.e.tenants.Update(t.Context(), k.e.tenant.ID, service.UpdateTenantInput{Enabled: &disabled})
		require.NoError(t, err)

		_, err = k.auth(k.rs256(k.claims()))
		require.ErrorIs(t, err, ErrNotAuthorized)
	})

	t.Run("resolver outage", func(t *testing.T) {
		k := newJWTKit(t)
		k.j.tenants = failingResolver{err: errors.New("db down")}

		_, err := k.auth(k.rs256(k.claims()))
		require.ErrorIs(t, err, ErrUnavailable)
	})

	t.Run("token claims pick the tenant", func(t *testing.T) {
		k := newJWTKit(t)

		// Same org ID under another app is another tenant (or none).
		claims := k.claims()
		claims["app_id"] = "aapp_other"

		_, err := k.auth(k.rs256(claims))
		require.ErrorIs(t, err, ErrNotAuthorized)
	})
}

func TestJWTUsernameMustEqualSubject(t *testing.T) {
	k := newJWTKit(t)
	token := k.rs256(k.claims())

	for _, user := range []string{"", "ausr_mallory"} {
		_, err := k.j.Authenticate(t.Context(), Credentials{ClientID: "c1", Username: user, Password: []byte(token)})
		require.ErrorIs(t, err, ErrBadCredentials, "username %q", user)
	}
}

func TestJWTSubjectFallsBackToPID(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	delete(claims, "sub")
	claims["pid"] = "asvc_1"
	claims["pk"] = "service_account"

	ident, err := k.j.Authenticate(t.Context(), Credentials{Username: "asvc_1", Password: []byte(k.rs256(claims))})
	require.NoError(t, err)
	assert.Equal(t, "asvc_1", ident.PrincipalID)
	assert.Equal(t, PrincipalKindServiceAccount, ident.PrincipalKind)

	delete(claims, "pid")

	_, err = k.j.Authenticate(t.Context(), Credentials{Username: "asvc_1", Password: []byte(k.rs256(claims))})
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWTPrincipalKinds(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	claims["pk"] = "user"
	assert.Equal(t, PrincipalKindUser, k.mustAuth(k.rs256(claims)).PrincipalKind)

	claims["pk"] = "service_account"
	assert.Equal(t, PrincipalKindServiceAccount, k.mustAuth(k.rs256(claims)).PrincipalKind)

	for _, pk := range []string{"agent", "workload", "robot"} {
		claims["pk"] = pk

		_, err := k.auth(k.rs256(claims))
		require.ErrorIs(t, err, ErrNotAuthorized, pk)
		assert.Contains(t, err.Error(), pk)
	}
}

func TestJWTRefusesTokensThatNeedMoreThanBearer(t *testing.T) {
	k := newJWTKit(t)

	claims := k.claims()
	claims["cnf"] = map[string]any{"jkt": "thumbprint"}

	_, err := k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrNotAuthorized, "a DPoP-bound token cannot be proven over MQTT")

	claims = k.claims()
	claims["client_id"] = "third-party-app"

	_, err = k.auth(k.rs256(claims))
	require.ErrorIs(t, err, ErrNotAuthorized, "a third-party OAuth token carries a scope Tether cannot enforce")
}

func TestJWTAlgNoneRefused(t *testing.T) {
	k := newJWTKit(t)

	token := k.sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, "rsa-a", k.claims())

	_, err := k.auth(token)
	require.ErrorIs(t, err, ErrBadCredentials)

	// Hand built, with the signature segment empty and with it forged.
	head := b64([]byte(`{"alg":"none","typ":"JWT","kid":"rsa-a"}`))
	body, err := json.Marshal(k.claims())
	require.NoError(t, err)

	for _, sig := range []string{"", b64([]byte("forged"))} {
		_, err = k.auth(head + "." + b64(body) + "." + sig)
		require.ErrorIs(t, err, ErrBadCredentials)
	}
}

func TestJWTHMACRefused(t *testing.T) {
	k := newJWTKit(t)

	// The classic confusion: sign with the public key's bytes as the HMAC secret.
	secret := []byte(`{"kty":"RSA"}`)
	token := k.sign(jwt.SigningMethodHS256, secret, "rsa-a", k.claims())

	_, err := k.auth(token)
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWTConfigRefusesUnsafeAlgorithms(t *testing.T) {
	genKeys(t)

	e := newEnv(t)
	base := JWTConfig{JWKSURL: "https://auth.example.com/jwks", Issuer: testIssuer, Audience: []string{testAudience}}

	for _, alg := range []string{"HS256", "HS384", "HS512", "none", "NONE", "", "RS1", "ES256K"} {
		cfg := base
		cfg.Algorithms = []string{"RS256", alg}

		_, nerr := NewJWT(cfg, e.tenants)
		require.Error(t, nerr, "algorithm %q", alg)
	}

	for _, alg := range []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"} {
		cfg := base
		cfg.Algorithms = []string{alg}

		_, nerr := NewJWT(cfg, e.tenants)
		require.NoError(t, nerr, "algorithm %q", alg)
	}
}

func TestJWTConfigValidation(t *testing.T) {
	e := newEnv(t)
	good := JWTConfig{JWKSURL: "https://auth.example.com/jwks", Issuer: testIssuer, Audience: []string{testAudience}}

	_, err := NewJWT(good, e.tenants)
	require.NoError(t, err)

	cases := map[string]func(*JWTConfig){
		"no jwks url":      func(c *JWTConfig) { c.JWKSURL = "" },
		"relative url":     func(c *JWTConfig) { c.JWKSURL = "/jwks" },
		"ftp url":          func(c *JWTConfig) { c.JWKSURL = "ftp://auth.example.com/jwks" },
		"no host":          func(c *JWTConfig) { c.JWKSURL = "https:///jwks" },
		"no issuer":        func(c *JWTConfig) { c.Issuer = "" },
		"no audience":      func(c *JWTConfig) { c.Audience = nil },
		"empty audience":   func(c *JWTConfig) { c.Audience = []string{""} },
		"negative refresh": func(c *JWTConfig) { c.RefreshInterval = -time.Second },
	}

	for name, mutate := range cases {
		cfg := good
		mutate(&cfg)

		_, nerr := NewJWT(cfg, e.tenants)
		require.Error(t, nerr, name)
	}

	_, err = NewJWT(good, nil)
	require.Error(t, err, "a tenant resolver is required")
}

func TestJWTDefaults(t *testing.T) {
	e := newEnv(t)

	j, err := NewJWT(JWTConfig{JWKSURL: "http://127.0.0.1:1/jwks", Issuer: testIssuer, Audience: []string{testAudience}}, e.tenants)
	require.NoError(t, err)

	assert.Equal(t, []string{"RS256", "EdDSA", "ES256"}, j.algs)
	assert.Equal(t, 10*time.Minute, j.keys.refresh)
	assert.Equal(t, 30*time.Second, j.leeway)
}

func TestJWTAlgorithmListRestrictsTokens(t *testing.T) {
	k := newJWTKit(t, func(c *JWTConfig) { c.Algorithms = []string{"ES256"} })

	_, err := k.auth(k.rs256(k.claims()))
	require.ErrorIs(t, err, ErrBadCredentials)

	k.mustAuth(k.sign(jwt.SigningMethodES256, ecKey, "ec-1", k.claims()))
}

func TestJWTKeyAlgHintIsHonoured(t *testing.T) {
	k := newJWTKit(t, func(c *JWTConfig) { c.Algorithms = []string{"RS256", "PS256"} })

	// The JWKS entry says alg RS256, so a PS256 signature from the same key is refused.
	token := k.sign(jwt.SigningMethodPS256, rsaKeyA, "rsa-a", k.claims())

	_, err := k.auth(token)
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWTTamperedAndForeignSignatures(t *testing.T) {
	k := newJWTKit(t)

	token := k.rs256(k.claims())
	parts := strings.Split(token, ".")

	// A different payload under the old signature.
	claims := k.claims()
	claims["org_id"] = "aorg_other"
	forged, err := json.Marshal(claims)
	require.NoError(t, err)

	_, err = k.auth(parts[0] + "." + b64(forged) + "." + parts[2])
	require.ErrorIs(t, err, ErrBadCredentials)

	// Signed by another key but naming the trusted kid.
	_, err = k.auth(k.sign(jwt.SigningMethodRS256, rsaKeyB, "rsa-a", k.claims()))
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWTGarbagePasswords(t *testing.T) {
	k := newJWTKit(t)

	for _, pw := range []string{"", "x", "a.b.c", "...", "a.b.c.d", strings.Repeat("A", 10000)} {
		_, err := k.j.Authenticate(t.Context(), Credentials{Username: "u", Password: []byte(pw)})
		require.ErrorIs(t, err, ErrBadCredentials, "password %.20q", pw)
	}

	assert.Zero(t, k.hits.Load(), "garbage must not reach the JWKS endpoint")
}

func TestJWTUnknownKidRefetchesOnceThenFails(t *testing.T) {
	k := newJWTKit(t)

	k.mustAuth(k.rs256(k.claims()))
	require.Equal(t, int32(1), k.hits.Load())

	rotated := k.sign(jwt.SigningMethodRS256, rsaKeyB, "rsa-b", k.claims())

	_, err := k.auth(rotated)
	require.ErrorIs(t, err, ErrBadCredentials)
	assert.Equal(t, int32(2), k.hits.Load(), "one refetch for the unknown kid")

	// Further unknown kids inside a minute do not refetch.
	for range 5 {
		_, err = k.auth(rotated)
		require.ErrorIs(t, err, ErrBadCredentials)
	}

	assert.Equal(t, int32(2), k.hits.Load())

	// The IdP publishes the new key. Still throttled until a minute has passed.
	k.setKeys(rsaJWK("rsa-a", &rsaKeyA.PublicKey), rsaJWK("rsa-b", &rsaKeyB.PublicKey))

	_, err = k.auth(rotated)
	require.ErrorIs(t, err, ErrBadCredentials)
	assert.Equal(t, int32(2), k.hits.Load())

	k.clock = k.clock.Add(61 * time.Second)

	// The token minted at the old clock is still valid (inside its hour).
	k.mustAuth(rotated)
	assert.Equal(t, int32(3), k.hits.Load())

	// A known kid never refetches.
	k.mustAuth(k.rs256(k.claims()))
	assert.Equal(t, int32(3), k.hits.Load())
}

func TestJWTUnknownKidFloodIsSingleFlight(t *testing.T) {
	k := newJWTKit(t)

	k.mustAuth(k.rs256(k.claims()))
	require.Equal(t, int32(1), k.hits.Load())

	k.delay.Store(int64(100 * time.Millisecond))

	unknown := k.sign(jwt.SigningMethodRS256, rsaKeyB, "nobody", k.claims())

	const n = 100

	var wg sync.WaitGroup

	errs := make(chan error, n)

	for range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			_, err := k.auth(unknown)
			errs <- err
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, ErrBadCredentials)
	}

	assert.Equal(t, int32(2), k.hits.Load(), "a flood of unknown kids costs one refetch")
}

func TestJWTRotatedKeyArrivesWithinOneRequest(t *testing.T) {
	k := newJWTKit(t)

	k.mustAuth(k.rs256(k.claims()))

	k.setKeys(rsaJWK("rsa-a", &rsaKeyA.PublicKey), rsaJWK("rsa-b", &rsaKeyB.PublicKey))

	k.mustAuth(k.sign(jwt.SigningMethodRS256, rsaKeyB, "rsa-b", k.claims()))
}

func TestJWTNoKidTokenNeedsExactlyOneKeyOfItsFamily(t *testing.T) {
	t.Run("one rsa key among others", func(t *testing.T) {
		k := newJWTKit(t)
		k.setKeys(rsaJWK("", &rsaKeyA.PublicKey), ecJWK(&ecKey.PublicKey))

		k.mustAuth(k.sign(jwt.SigningMethodRS256, rsaKeyA, "", k.claims()))
		k.mustAuth(k.sign(jwt.SigningMethodES256, ecKey, "", k.claims()))
	})

	t.Run("two rsa keys", func(t *testing.T) {
		k := newJWTKit(t)
		k.setKeys(rsaJWK("rsa-a", &rsaKeyA.PublicKey), rsaJWK("rsa-b", &rsaKeyB.PublicKey))

		_, err := k.auth(k.sign(jwt.SigningMethodRS256, rsaKeyA, "", k.claims()))
		require.ErrorIs(t, err, ErrBadCredentials)
	})

	t.Run("no key of the family", func(t *testing.T) {
		k := newJWTKit(t)
		k.setKeys(ecJWK(&ecKey.PublicKey))

		_, err := k.auth(k.sign(jwt.SigningMethodRS256, rsaKeyA, "", k.claims()))
		require.ErrorIs(t, err, ErrBadCredentials)
	})
}

func TestJWKSTooLargeIsRefused(t *testing.T) {
	k := newJWTKit(t)

	k.setOverride(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"keys":[],"pad":"` + strings.Repeat("x", 2<<20) + `"}`))
	})

	_, err := k.auth(k.rs256(k.claims()))
	require.ErrorIs(t, err, ErrUnavailable)
	assert.Contains(t, err.Error(), "too large")
}

func TestJWKSBodyJustUnderTheCapIsAccepted(t *testing.T) {
	k := newJWTKit(t)

	k.setOverride(func(w http.ResponseWriter, _ *http.Request) {
		body, _ := json.Marshal(map[string]any{"keys": []any{rsaJWK("rsa-a", &rsaKeyA.PublicKey)}})
		pad := strings.Repeat(" ", maxJWKSBytes-len(body))
		_, _ = w.Write(append(body, pad...))
	})

	k.mustAuth(k.rs256(k.claims()))
}

func TestJWKSFetchFailures(t *testing.T) {
	cases := map[string]func(w http.ResponseWriter, r *http.Request){
		"server error": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusInternalServerError) },
		"not json":     func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("<html>")) },
		"no keys":      func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"keys":[]}`)) },
		"redirect loop": func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, r.URL.Path, http.StatusFound)
		},
	}

	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			k := newJWTKit(t)
			k.setOverride(h)

			_, err := k.auth(k.rs256(k.claims()))
			require.ErrorIs(t, err, ErrUnavailable)
		})
	}

	t.Run("unreachable", func(t *testing.T) {
		k := newJWTKit(t)
		k.srv.Close()

		_, err := k.auth(k.rs256(k.claims()))
		require.ErrorIs(t, err, ErrUnavailable)
	})
}

func TestJWKSFailedLoadIsNotRetriedInABurst(t *testing.T) {
	k := newJWTKit(t)
	k.setOverride(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", http.StatusBadGateway) })

	token := k.rs256(k.claims())

	for range 20 {
		_, err := k.auth(token)
		require.ErrorIs(t, err, ErrUnavailable)
	}

	assert.Equal(t, int32(1), k.hits.Load())

	// After the backoff it tries again, and a healthy endpoint recovers.
	k.setOverride(nil)
	k.clock = k.clock.Add(time.Minute)

	k.mustAuth(token)
	assert.Equal(t, int32(2), k.hits.Load())
}

func TestJWKSRefreshInterval(t *testing.T) {
	k := newJWTKit(t, func(c *JWTConfig) { c.RefreshInterval = time.Minute })

	token := k.rs256(k.claims())
	k.mustAuth(token)
	k.mustAuth(token)
	require.Equal(t, int32(1), k.hits.Load())

	// The IdP retires the key. Within the interval the cached key still verifies.
	k.setKeys(ecJWK(&ecKey.PublicKey))
	k.mustAuth(token)

	k.clock = k.clock.Add(61 * time.Second)

	_, err := k.auth(token)
	require.ErrorIs(t, err, ErrBadCredentials, "a retired key stops verifying after the next refresh")
	assert.Equal(t, int32(3), k.hits.Load(), "the refresh, then the one throttled refetch for the now unknown kid")
}

func TestJWKSStaleKeysSurviveAnOutage(t *testing.T) {
	k := newJWTKit(t, func(c *JWTConfig) { c.RefreshInterval = time.Minute })

	token := k.rs256(k.claims())
	k.mustAuth(token)

	k.setOverride(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) })
	k.clock = k.clock.Add(2 * time.Minute)

	k.mustAuth(token)
	assert.Equal(t, int32(2), k.hits.Load())

	// Not hammered while it is down.
	for range 10 {
		k.mustAuth(token)
	}

	assert.Equal(t, int32(2), k.hits.Load())
}

func TestJWKSSkipsUnusableKeys(t *testing.T) {
	k := newJWTKit(t)

	small, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	k.setKeys(
		rsaJWK("small", &small.PublicKey),
		map[string]any{"kty": "oct", "kid": "sym", "k": b64([]byte("secret"))},
		map[string]any{"kty": "RSA", "kid": "broken", "n": "!!", "e": "AQAB"},
		map[string]any{"kty": "RSA", "kid": "enc", "use": "enc", "n": b64(rsaKeyA.N.Bytes()), "e": "AQAB"},
		rsaJWK("rsa-a", &rsaKeyA.PublicKey),
	)

	k.mustAuth(k.rs256(k.claims()))

	for _, kid := range []string{"small", "sym", "broken", "enc"} {
		_, err := k.auth(k.sign(jwt.SigningMethodRS256, rsaKeyA, kid, k.claims()))
		require.ErrorIs(t, err, ErrBadCredentials, kid)
	}
}

func TestJWKSKeyIDMustBeAString(t *testing.T) {
	k := newJWTKit(t)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, k.claims())
	tok.Header["kid"] = 42

	s, err := tok.SignedString(rsaKeyA)
	require.NoError(t, err)

	_, err = k.auth(s)
	require.ErrorIs(t, err, ErrBadCredentials)
}

func TestJWKSContextCancelledWhileFetching(t *testing.T) {
	k := newJWTKit(t)
	k.delay.Store(int64(300 * time.Millisecond))

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	_, err := k.j.Authenticate(ctx, Credentials{Username: "ausr_alice", Password: []byte(k.rs256(k.claims()))})
	require.ErrorIs(t, err, ErrUnavailable)

	// The fetch finished for everyone else.
	k.delay.Store(0)

	time.Sleep(400 * time.Millisecond)
	k.mustAuth(k.rs256(k.claims()))
	assert.Equal(t, int32(1), k.hits.Load(), "the abandoned fetch was not wasted")
}

func TestJWTIdentityIsACopy(t *testing.T) {
	k := newJWTKit(t)

	a := k.mustAuth(k.rs256(k.claims()))
	a.PrincipalID = "changed"
	a.Tenant.Name = "changed"

	b := k.mustAuth(k.rs256(k.claims()))
	assert.Equal(t, "ausr_alice", b.PrincipalID)
	assert.NotEqual(t, "changed", b.Tenant.Name)
}

type recordingAuth struct {
	calls atomic.Int32
	last  Credentials
	err   error
}

func (r *recordingAuth) Authenticate(_ context.Context, c Credentials) (*Identity, error) {
	r.calls.Add(1)
	r.last = c

	return &Identity{PrincipalID: "x"}, r.err
}

func TestChainRoutesByPasswordShape(t *testing.T) {
	genKeys(t)

	jw, ks := &recordingAuth{}, &recordingAuth{}
	chain := Chain(jw, ks)

	jwtPassword := "eyJhbGciOiJSUzI1NiIsImtpZCI6ImEifQ.eyJzdWIiOiJ1In0.c2ln-_Z9"
	for _, pw := range []string{jwtPassword} {
		_, err := chain.Authenticate(t.Context(), Credentials{Username: "u", Password: []byte(pw), SkipClientIDPin: true})
		require.NoError(t, err)
	}

	assert.Equal(t, int32(1), jw.calls.Load())
	assert.Zero(t, ks.calls.Load())

	for _, pw := range []string{"tdk_live_abc123", "tdk_live_a.b.c", "a.b", "a.b.c.d", "a..c", ".b.c", "a.b.", "a.b.c=", "a.b.c d", "", "é.b.c", "ab.b.c", "eyJ.b.c d"} {
		_, err := chain.Authenticate(t.Context(), Credentials{Username: "akey_1", Password: []byte(pw), SkipClientIDPin: true})
		require.NoError(t, err)
	}

	assert.Equal(t, int32(1), jw.calls.Load(), "nothing else looks like a JWT")
	assert.Equal(t, int32(13), ks.calls.Load())
	assert.True(t, ks.last.SkipClientIDPin, "the pin switch passes through to keysmith")
}

func TestChainPassesCredentialsAndErrorsThrough(t *testing.T) {
	jw, ks := &recordingAuth{err: ErrNotAuthorized}, &recordingAuth{err: ErrRateLimited}
	chain := Chain(jw, ks)

	_, err := chain.Authenticate(t.Context(), Credentials{ClientID: "cid", Username: "u", Password: []byte("eyJ.b.c"), SkipClientIDPin: true})
	require.ErrorIs(t, err, ErrNotAuthorized)
	assert.Equal(t, "cid", jw.last.ClientID)

	_, err = chain.Authenticate(t.Context(), Credentials{Username: "u", Password: []byte("tdk_live_1")})
	require.ErrorIs(t, err, ErrRateLimited)
}

func TestChainWithoutJWTIsKeysmith(t *testing.T) {
	ks := &recordingAuth{}

	got := Chain(nil, ks)
	assert.Same(t, Authenticator(ks), got)

	_, err := got.Authenticate(t.Context(), Credentials{Username: "u", Password: []byte("a.b.c")})
	require.NoError(t, err)
	assert.Equal(t, int32(1), ks.calls.Load())
}

func TestChainEndToEndWithRealAuthenticators(t *testing.T) {
	k := newJWTKit(t)

	chain := Chain(k.j, k.e.auth)

	ident, err := chain.Authenticate(t.Context(), Credentials{Username: "ausr_alice", Password: []byte(k.rs256(k.claims()))})
	require.NoError(t, err)
	assert.Equal(t, PrincipalKindUser, ident.PrincipalKind)

	ident, err = chain.Authenticate(t.Context(), k.e.creds())
	require.NoError(t, err)
	assert.Equal(t, PrincipalKindAPIKey, ident.PrincipalKind)

	// A JWT-shaped password never falls back to keysmith.
	before := k.e.val.calls.Load()
	_, err = chain.Authenticate(t.Context(), Credentials{Username: "ausr_alice", Password: []byte("eyaa.bb.cc")})
	require.ErrorIs(t, err, ErrBadCredentials)
	assert.Equal(t, before, k.e.val.calls.Load())
}

func TestJWTConfigIsZero(t *testing.T) {
	assert.True(t, JWTConfig{}.IsZero())
	assert.False(t, JWTConfig{Issuer: "x"}.IsZero())
	assert.False(t, JWTConfig{Leeway: time.Second}.IsZero())
	assert.False(t, JWTConfig{Algorithms: []string{"RS256"}}.IsZero())
}

// Compile-time check.
var _ Authenticator = (*JWT)(nil)
