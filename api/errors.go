package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/xraph/forge"
	"github.com/xraph/keysmith"
	"github.com/xraph/warden"

	"github.com/xraph/tether"
	"github.com/xraph/tether/auth"
)

// errUnavailable is what a handler returns while the broker is not running.
func errUnavailable() error {
	return forge.NewHTTPError(http.StatusServiceUnavailable, "the broker is not running")
}

// statusOf returns the HTTP status mapError chose for err.
func statusOf(err error) int {
	var he interface{ StatusCode() int }
	if errors.As(err, &he) {
		return he.StatusCode()
	}

	return http.StatusInternalServerError
}

// mapError turns an error from a service, the broker or the auth layer into
// the HTTP error a client sees. Every handler returns errors through it.
//
//	tether.ErrInvalidInput                     400, with the reason
//	auth.ErrBadCredentials                     401
//	auth.ErrNotAuthorized, tether.ErrTenantDisabled   403, fixed text
//	tether.ErrNotAuthorized                    403, with the reason (ACL denial)
//	tether.ErrNotFound                         404, fixed text
//	tether.ErrConflict                         409, with the reason
//	warden.ErrRoleNotFound                     400, "unknown role"
//	keysmith.ErrKeyNotFound                    404
//	keysmith.ErrInvalidStateTransition, ErrKeyRevoked   409
//	auth.ErrRateLimited                        429
//	auth.ErrUnavailable                        503
//	anything else                              500, fixed text
//
// Only the tether sentinels' own reasons reach a client. The text of an
// authentication failure is fixed, so an unauthenticated caller cannot tell a
// wrong key from a revoked one, and an unmapped error never leaves the
// process: the 500 body is generic and Forge redacts 5xx bodies anyway.
func mapError(err error) error {
	switch {
	case errors.Is(err, tether.ErrInvalidInput):
		return forge.NewHTTPError(http.StatusBadRequest, reason(err, tether.ErrInvalidInput, "invalid input"))
	case errors.Is(err, auth.ErrBadCredentials):
		return forge.NewHTTPError(http.StatusUnauthorized, "invalid credentials")
	case errors.Is(err, auth.ErrNotAuthorized), errors.Is(err, tether.ErrTenantDisabled):
		return forge.NewHTTPError(http.StatusForbidden, "not authorized")
	case errors.Is(err, tether.ErrNotAuthorized):
		return forge.NewHTTPError(http.StatusForbidden, reason(err, tether.ErrNotAuthorized, "not authorized"))
	case errors.Is(err, tether.ErrNotFound):
		return forge.NewHTTPError(http.StatusNotFound, "not found")
	case errors.Is(err, tether.ErrConflict):
		return forge.NewHTTPError(http.StatusConflict, reason(err, tether.ErrConflict, "conflict"))
	case errors.Is(err, warden.ErrRoleNotFound):
		// A device call that names a role warden does not have.
		return forge.NewHTTPError(http.StatusBadRequest, "unknown role")
	case errors.Is(err, keysmith.ErrKeyNotFound):
		return forge.NewHTTPError(http.StatusNotFound, "not found")
	case errors.Is(err, keysmith.ErrInvalidStateTransition), errors.Is(err, keysmith.ErrKeyRevoked):
		// Rotating a key that is already revoked.
		return forge.NewHTTPError(http.StatusConflict, "the device's key is revoked")
	case errors.Is(err, auth.ErrRateLimited):
		return forge.NewHTTPError(http.StatusTooManyRequests, "rate limit exceeded")
	case errors.Is(err, auth.ErrUnavailable):
		return forge.NewHTTPError(http.StatusServiceUnavailable, "service unavailable")
	default:
		return forge.NewHTTPError(http.StatusInternalServerError, "internal error")
	}
}

// reason returns what follows sentinel in err's message (the reason a tether
// validation gave), cut at the first line, or fallback when there is none.
// Store and service errors put the sentinel last, so they give fallback.
func reason(err, sentinel error, fallback string) string {
	_, after, ok := strings.Cut(err.Error(), sentinel.Error()+": ")
	if !ok {
		return fallback
	}

	after, _, _ = strings.Cut(after, "\n")

	if after = strings.TrimSpace(after); after != "" {
		return after
	}

	return fallback
}

// fail logs err when it is one the client will not see the cause of, then
// returns mapError(err). op names the operation for the log.
func (a *API) fail(op string, err error) error {
	mapped := mapError(err)

	if statusOf(mapped) >= http.StatusInternalServerError {
		a.Logger.Error("tether/api: request failed", "op", op, "error", err)
	}

	return mapped
}

// brokerFail is fail for an error from a broker call. A broker that is not
// running answers with tether.ErrNotFound or tether.ErrConflict, which would
// read as "no such client" or "already exists", so it is 503 instead.
func (a *API) brokerFail(op string, err error) error {
	if !a.Broker.Running() {
		return errUnavailable()
	}

	return a.fail(op, err)
}
