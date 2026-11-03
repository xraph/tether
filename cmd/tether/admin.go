package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/xraph/forge"
	forgeauth "github.com/xraph/forge/extensions/auth"
)

// adminTokenEnv names the environment variable that holds the admin bearer
// token.
const adminTokenEnv = "TETHER_ADMIN_TOKEN"

// errNoAdminToken is what startup returns when no token is set and the
// operator did not ask for an open admin API.
var errNoAdminToken = errors.New(
	"tether: " + adminTokenEnv + " is empty: set it to a long random value, " +
		"or pass --insecure-admin to leave the admin API and dashboard open")

// adminSubject is the Subject of the auth context adminAuth sets: the actor
// the audit trail records for an admin change made with the token.
const adminSubject = "admin-token"

// adminAuth returns a middleware that lets a request through only when it
// carries "Authorization: Bearer <token>". Anything else gets 401 with a
// "WWW-Authenticate: Bearer" challenge.
//
// A request it lets through gets a forge auth context with Subject
// "admin-token", on the forge context and on the request's context, the way
// forge's auth extension sets one, so the REST API records an actor on the
// admin changes it audits (ruling R24).
//
// The comparison runs on SHA-256 digests with crypto/subtle, so it takes the
// same time whatever the length of what the caller sent.
func adminAuth(token string) forge.Middleware {
	want := sha256.Sum256([]byte(token))

	return func(next forge.Handler) forge.Handler {
		return func(c forge.Context) error {
			if got, ok := bearerToken(c.Request().Header.Get("Authorization")); ok {
				sum := sha256.Sum256([]byte(got))
				if subtle.ConstantTimeCompare(sum[:], want[:]) == 1 {
					ac := &forgeauth.AuthContext{Subject: adminSubject}
					c.Set("auth_context", ac)
					c.WithContext(forgeauth.WithContext(c.Context(), ac))

					return next(c)
				}
			}

			c.SetHeader("WWW-Authenticate", "Bearer")

			return forge.NewHTTPError(http.StatusUnauthorized, "missing or invalid admin token")
		}
	}
}

// bearerToken returns the token of a "Bearer <token>" header. The scheme is
// case-insensitive, and an empty token is not a token.
func bearerToken(header string) (string, bool) {
	scheme, rest, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}

	rest = strings.TrimSpace(rest)

	return rest, rest != ""
}
