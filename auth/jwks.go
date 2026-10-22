package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"sync"
	"time"
)

const (
	// maxJWKSBytes caps the JWKS response body.
	maxJWKSBytes = 1 << 20

	// jwksTimeout bounds one JWKS request.
	jwksTimeout = 5 * time.Second

	// unknownKidInterval is the least time between two refetches caused by a
	// token whose key ID (or key family) the cached set does not know. It is
	// what stops a stream of forged tokens from turning into a stream of
	// requests to the identity provider.
	unknownKidInterval = time.Minute

	// jwksFailBackoff is how long a failed fetch keeps the next scheduled
	// refresh away, so an outage costs one request per interval, not one per
	// CONNECT.
	jwksFailBackoff = 10 * time.Second

	// minRSABits is the smallest RSA modulus a JWKS key may have.
	minRSABits = 2048

	maxRedirects = 3
)

// errThrottled means an unknown-kid refetch was refused because one ran
// within unknownKidInterval.
var errThrottled = errors.New("jwks refetch throttled")

// Key families an algorithm can use.
const (
	familyRSA     = "RSA"
	familyP256    = "EC:P-256"
	familyP384    = "EC:P-384"
	familyP521    = "EC:P-521"
	familyEd25519 = "OKP:Ed25519"
)

// algFamily returns the key family a JWS algorithm verifies with, empty for
// an algorithm this package does not accept.
func algFamily(alg string) string {
	switch alg {
	case "RS256", "RS384", "RS512", "PS256", "PS384", "PS512":
		return familyRSA
	case "ES256":
		return familyP256
	case "ES384":
		return familyP384
	case "ES512":
		return familyP521
	case "EdDSA":
		return familyEd25519
	default:
		return ""
	}
}

// keyEntry is one usable key from a JWKS.
type keyEntry struct {
	alg    string // The JWK's own "alg", empty when it names none.
	family string
	key    crypto.PublicKey
}

// keySet is the parsed JWKS: the keys that carry a kid by kid, and all keys.
type keySet struct {
	byKID map[string]*keyEntry
	all   []*keyEntry
}

// find returns the key a token with this kid and algorithm verifies with.
//
// A token with a kid needs a key with that kid, of the algorithm's family,
// whose own "alg" (when it has one) is the token's. A token without a kid
// matches only when exactly one key of the family (and algorithm) exists.
func (s *keySet) find(kid, alg string) (crypto.PublicKey, bool) {
	family := algFamily(alg)
	if family == "" {
		return nil, false
	}

	if kid != "" {
		e := s.byKID[kid]
		if e == nil || e.family != family || (e.alg != "" && e.alg != alg) {
			return nil, false
		}

		return e.key, true
	}

	var found *keyEntry

	for _, e := range s.all {
		if e.family != family || (e.alg != "" && e.alg != alg) {
			continue
		}

		if found != nil {
			return nil, false
		}

		found = e
	}

	if found == nil {
		return nil, false
	}

	return found.key, true
}

// flight is one JWKS request that every waiting caller shares.
type flight struct {
	done chan struct{}
	err  error
}

func (f *flight) wait(ctx context.Context) error {
	select {
	case <-f.done:
		return f.err
	case <-ctx.Done():
		return fmt.Errorf("%w: waiting for the jwks: %w", ErrUnavailable, ctx.Err())
	}
}

// jwksCache holds the identity provider's keys.
//
// The keys refresh when they are older than refresh, and are looked up again
// from the endpoint when a token names a key ID they lack, at most once per
// unknownKidInterval. Every fetch is single-flight: callers that arrive while
// one runs wait for it. A fetch is not tied to the caller that started it, so
// a CONNECT that gives up does not fail the others. When a refresh fails the
// previous keys stay in use.
type jwksCache struct {
	url     string
	client  *http.Client
	refresh time.Duration
	now     func() time.Time

	mu          sync.Mutex
	set         *keySet
	next        time.Time // When the keys are next due, success or backoff.
	lastErr     error     // The last failed fetch, for callers while there are no keys.
	lastUnknown time.Time // When the last unknown-kid refetch started.
	gen         uint64    // Bumped whenever a fetch ends.
	flight      *flight
}

func newJWKSCache(url string, refresh time.Duration, now func() time.Time) *jwksCache {
	return &jwksCache{
		url:     url,
		refresh: refresh,
		now:     now,
		client: &http.Client{
			Timeout: jwksTimeout,
			// A redirect may move the JWKS within its origin, never to
			// another scheme or host: the keys that verify every token
			// have to come from the server the config names.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) > maxRedirects {
					return errors.New("too many redirects")
				}

				if first := via[0].URL; req.URL.Scheme != first.Scheme || req.URL.Host != first.Host {
					return fmt.Errorf("redirect from %s://%s to %s://%s refused: the JWKS must stay on its origin",
						first.Scheme, first.Host, req.URL.Scheme, req.URL.Host)
				}

				return nil
			},
		},
	}
}

// snapshot is a consistent view of the cache.
type snapshot struct {
	set *keySet
	gen uint64
}

// key returns the public key for a token's kid and algorithm. Unknown keys are
// an error wrapping ErrBadCredentials, an unreachable endpoint with no keys to
// fall back on one wrapping ErrUnavailable.
func (c *jwksCache) key(ctx context.Context, kid, alg string) (crypto.PublicKey, error) {
	snap, err := c.current(ctx)
	if err != nil {
		return nil, err
	}

	if k, ok := snap.set.find(kid, alg); ok {
		return k, nil
	}

	// The set may be out of date: the provider rotated and this is the first
	// token signed by the new key. Look again, but only so often.
	err = c.refetch(ctx, snap.gen, true)

	switch {
	case err == nil:
	case errors.Is(err, errThrottled):
		return nil, unknownKeyError(kid, alg)
	default:
		return nil, err
	}

	c.mu.Lock()
	set := c.set
	c.mu.Unlock()

	if set != nil {
		if k, ok := set.find(kid, alg); ok {
			return k, nil
		}
	}

	return nil, unknownKeyError(kid, alg)
}

func unknownKeyError(kid, alg string) error {
	if kid == "" {
		return fmt.Errorf("%w: no single %s key in the jwks for a token without a kid", ErrBadCredentials, alg)
	}

	return fmt.Errorf("%w: no %s key with kid %q in the jwks", ErrBadCredentials, alg, kid)
}

// current returns the keys, fetching them first when there are none or they
// are due. A failed refresh keeps the old keys.
func (c *jwksCache) current(ctx context.Context) (snapshot, error) {
	c.mu.Lock()
	now := c.now()

	if c.set != nil && now.Before(c.next) {
		snap := snapshot{set: c.set, gen: c.gen}
		c.mu.Unlock()

		return snap, nil
	}

	if c.set == nil && now.Before(c.next) {
		err := c.lastErr
		c.mu.Unlock()

		return snapshot{}, err
	}

	gen := c.gen
	c.mu.Unlock()

	err := c.refetch(ctx, gen, false)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.set != nil {
		return snapshot{set: c.set, gen: c.gen}, nil
	}

	if err == nil {
		err = c.lastErr
	}

	if err == nil {
		err = fmt.Errorf("%w: no jwks keys", ErrUnavailable)
	}

	return snapshot{}, err
}

// refetch fetches the JWKS, or joins the fetch already running. It does
// nothing when a fetch has ended since the caller read gen: the caller's view
// is stale and its next look will see the new keys. gated applies the
// unknown-kid throttle.
func (c *jwksCache) refetch(ctx context.Context, gen uint64, gated bool) error {
	c.mu.Lock()

	if c.gen != gen {
		c.mu.Unlock()

		return nil
	}

	if f := c.flight; f != nil {
		c.mu.Unlock()

		return f.wait(ctx)
	}

	if gated {
		now := c.now()
		if !c.lastUnknown.IsZero() && now.Sub(c.lastUnknown) < unknownKidInterval {
			c.mu.Unlock()

			return errThrottled
		}

		c.lastUnknown = now
	}

	f := &flight{done: make(chan struct{})}
	c.flight = f
	c.mu.Unlock()

	// The request outlives this caller's context: others wait on it too.
	go c.run(context.WithoutCancel(ctx), f)

	return f.wait(ctx)
}

func (c *jwksCache) run(ctx context.Context, f *flight) {
	set, err := c.fetch(ctx)

	c.mu.Lock()

	now := c.now()
	if err == nil {
		c.set = set
		c.next = now.Add(c.refresh)
		c.lastErr = nil
	} else {
		c.next = now.Add(jwksFailBackoff)
		c.lastErr = err
	}

	c.gen++
	c.flight = nil
	f.err = err
	c.mu.Unlock()

	close(f.done)
}

// fetch requests the JWKS and parses it.
func (c *jwksCache) fetch(ctx context.Context) (*keySet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("%w: build jwks request: %w", ErrUnavailable, err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: fetch jwks: %w", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: fetch jwks: status %d", ErrUnavailable, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read jwks: %w", ErrUnavailable, err)
	}

	if len(body) > maxJWKSBytes {
		return nil, fmt.Errorf("%w: jwks response too large (over %d bytes)", ErrUnavailable, maxJWKSBytes)
	}

	set, err := parseJWKS(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	return set, nil
}

// jwk is the part of a JSON Web Key this package reads.
type jwk struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	N   string `json:"n"`
	E   string `json:"e"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

// parseJWKS reads a JWKS. A key that is malformed, for encryption, of a type
// or size this package does not accept is skipped; a set with no usable key is
// an error, so a broken response never replaces good keys.
func parseJWKS(body []byte) (*keySet, error) {
	var doc struct {
		Keys []jwk `json:"keys"`
	}

	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("parse jwks: %w", err)
	}

	set := &keySet{byKID: make(map[string]*keyEntry)}

	for i := range doc.Keys {
		k := &doc.Keys[i]
		if k.Use != "" && k.Use != "sig" {
			continue
		}

		e, ok := k.entry()
		if !ok {
			continue
		}

		set.all = append(set.all, e)

		if k.Kid != "" {
			if _, dup := set.byKID[k.Kid]; !dup {
				set.byKID[k.Kid] = e
			}
		}
	}

	if len(set.all) == 0 {
		return nil, errors.New("jwks has no usable signing keys")
	}

	return set, nil
}

func (k *jwk) entry() (*keyEntry, bool) {
	switch k.Kty {
	case "RSA":
		pub, ok := k.rsaKey()
		if !ok {
			return nil, false
		}

		return &keyEntry{alg: k.Alg, family: familyRSA, key: pub}, true
	case "EC":
		pub, family, ok := k.ecKey()
		if !ok {
			return nil, false
		}

		return &keyEntry{alg: k.Alg, family: family, key: pub}, true
	case "OKP":
		if k.Crv != "Ed25519" {
			return nil, false
		}

		x, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, false
		}

		return &keyEntry{alg: k.Alg, family: familyEd25519, key: ed25519.PublicKey(x)}, true
	default:
		return nil, false
	}
}

func (k *jwk) rsaKey() (*rsa.PublicKey, bool) {
	nb, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, false
	}

	eb, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil || len(eb) == 0 || len(eb) > 4 {
		return nil, false
	}

	n := new(big.Int).SetBytes(nb)
	e := int(new(big.Int).SetBytes(eb).Int64())

	if n.BitLen() < minRSABits || e < 3 || e%2 == 0 || e > math.MaxInt32 {
		return nil, false
	}

	return &rsa.PublicKey{N: n, E: e}, true
}

func (k *jwk) ecKey() (*ecdsa.PublicKey, string, bool) {
	var (
		curve  elliptic.Curve
		family string
	)

	switch k.Crv {
	case "P-256":
		curve, family = elliptic.P256(), familyP256
	case "P-384":
		curve, family = elliptic.P384(), familyP384
	case "P-521":
		curve, family = elliptic.P521(), familyP521
	default:
		return nil, "", false
	}

	size := (curve.Params().BitSize + 7) / 8

	x, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil || len(x) == 0 || len(x) > size {
		return nil, "", false
	}

	y, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil || len(y) == 0 || len(y) > size {
		return nil, "", false
	}

	point := make([]byte, 1+2*size)
	point[0] = 4

	copy(point[1+size-len(x):], x)
	copy(point[1+2*size-len(y):], y)

	// This also checks the point is on the curve.
	pub, err := ecdsa.ParseUncompressedPublicKey(curve, point)
	if err != nil {
		return nil, "", false
	}

	return pub, family, true
}
