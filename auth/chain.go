package auth

import (
	"context"
	"fmt"
	"strings"
)

// chain sends a JWT-shaped password to one authenticator and anything else to
// the other.
type chain struct {
	jwt      Authenticator
	keysmith Authenticator
}

// Chain returns an Authenticator that sends a password that looks like a JWT
// to jwt and every other password to keysmith. A nil jwt returns keysmith
// itself.
//
// A JWT is three dot-separated base64url segments, the first one starting
// "ey" (the encoding of the header object's opening brace and quote). A
// keysmith key has no such shape. The choice is made from the password alone,
// and a JWT-shaped password that jwt refuses is never retried against
// keysmith. Credentials, SkipClientIDPin included, go to the chosen
// authenticator unchanged.
func Chain(jwt, keysmith Authenticator) Authenticator {
	if jwt == nil {
		return keysmith
	}

	return &chain{jwt: jwt, keysmith: keysmith}
}

// Authenticate implements Authenticator.
func (c *chain) Authenticate(ctx context.Context, cr Credentials) (*Identity, error) {
	if looksLikeJWT(cr.Password) {
		return c.jwt.Authenticate(ctx, cr)
	}

	if c.keysmith == nil {
		return nil, fmt.Errorf("%w: only tokens are accepted here", ErrBadCredentials)
	}

	return c.keysmith.Authenticate(ctx, cr)
}

// looksLikeJWT reports whether p is three non-empty base64url segments, the
// first starting "ey".
func looksLikeJWT(p []byte) bool {
	s := string(p)
	if !strings.HasPrefix(s, "ey") || strings.Count(s, ".") != 2 {
		return false
	}

	for _, seg := range strings.Split(s, ".") {
		if seg == "" {
			return false
		}

		for i := range len(seg) {
			if !isBase64URL(seg[i]) {
				return false
			}
		}
	}

	return true
}

// isBase64URL reports whether ch is in the unpadded base64url alphabet.
func isBase64URL(ch byte) bool {
	switch {
	case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9':
		return true
	default:
		return ch == '-' || ch == '_'
	}
}
