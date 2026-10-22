package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/xraph/forge"
	"github.com/xraph/forge/middleware"

	"github.com/xraph/tether"
	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/topic"
)

// publishBodyLimit is the largest request body POST /v1/publish reads: a
// payload of maxPayload bytes in base64, plus room for the topic and the
// JSON around it.
func publishBodyLimit(maxPayload int) int64 {
	return int64((maxPayload+2)/3*4 + 4096)
}

const (
	// ctxIdentity is the context key the publish authentication leaves the
	// device's identity under.
	ctxIdentity = "tether.identity"

	// ctxSubject is the context key forge's idempotency middleware reads the
	// principal from.
	ctxSubject = "auth.subject"
)

// publishRequest is the body of POST /v1/publish. Payload is standard base64.
type publishRequest struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
	QoS     int    `json:"qos"`
	Retain  bool   `json:"retain"`
}

// publishResponse is the body of a successful publish.
type publishResponse struct {
	Published bool `json:"published"`
}

func (a *API) registerPublish(r forge.Router) error {
	// publishAuth comes before the idempotency middleware on purpose: that
	// middleware keys on the principal and replays a stored response without
	// running the handler, so a caller has to be authenticated first. A
	// revoked key therefore gets 401, never a replay.
	return r.POST("/v1/publish", a.publish,
		forge.WithName("publish"),
		forge.WithTags("publish"),
		forge.WithSummary("Publish a message as a device over HTTP"),
		forge.WithMiddleware(a.publishAuth),
		forge.WithIdempotency(middleware.IdempotencyMaxBody(publishBodyLimit(a.MaxPublishPayload))),
	)
}

// publishAuth authenticates the request with HTTP Basic (the device's key ID
// and raw key) through the same authenticator MQTT CONNECTs use. There is no
// client ID to check a pin against, so the check is skipped and the device's
// pin (if any) is read from the identity.
func (a *API) publishAuth(next forge.Handler) forge.Handler {
	return func(c forge.Context) error {
		if !a.Broker.Running() {
			return errUnavailable()
		}

		r := c.Request()

		user, pass, ok := r.BasicAuth()
		if !ok || user == "" || pass == "" {
			return a.unauthorized(c, auth.ErrBadCredentials)
		}

		ident, err := a.Auth.Authenticate(c.Context(), auth.Credentials{
			Username: user,
			Password: []byte(pass),
			Remote:   r.RemoteAddr,

			// There is no client ID over HTTP. The pin comes back in
			// ident.ClientID and stands in for it in the grants.
			SkipClientIDPin: true,
		})
		if err != nil {
			return a.unauthorized(c, err)
		}

		switch {
		case ident == nil || ident.Tenant == nil:
			a.Logger.Error("tether/api: authenticator returned no identity or tenant")

			return mapError(auth.ErrUnavailable)
		case !ident.Tenant.Enabled:
			return mapError(auth.ErrNotAuthorized)
		case !ident.ExpiresAt.IsZero() && !time.Now().Before(ident.ExpiresAt):
			return a.unauthorized(c, auth.ErrBadCredentials)
		}

		c.Set(ctxIdentity, ident)
		c.Set(ctxSubject, ident.Tenant.ID.String()+"/"+ident.PrincipalID)

		return next(c)
	}
}

// unauthorized answers an authentication failure, with the headers that go
// with 401 and 429.
func (a *API) unauthorized(c forge.Context, err error) error {
	switch {
	case errors.Is(err, auth.ErrBadCredentials):
		c.SetHeader("WWW-Authenticate", `Basic realm="tether", charset="UTF-8"`)
	case errors.Is(err, auth.ErrRateLimited):
		c.SetHeader("Retry-After", "1")
	}

	return a.fail("publish authenticate", err)
}

// publish runs the checks a device's MQTT PUBLISH goes through: the topic,
// the tenant's message rate, then the grants, then the broker. The rate comes
// before the grants on purpose: loading them asks warden, and a device over
// its budget must not be able to make it do that at will. So the tenant's
// budget bounds the load, and a publish refused by its grants has still
// spent a token, as an MQTT PUBLISH refused by the ACL has used the network.
//
// "$" topics follow MQTT: "$SYS" and "$share" (any casing) are refused, any
// other "$" name needs a grant that names it, since a filter that starts
// with a wildcard never matches one.
//
// The grants expand "%u" to the username and "%c" to the device's pinned
// client ID. An unpinned device has no client ID over HTTP, so a grant that
// uses "%c" is dropped for it. An accepted publish counts as a message in
// for the tenant (broker.DevicePublish).
func (a *API) publish(c forge.Context) error {
	ident, ok := c.Get(ctxIdentity).(*auth.Identity)
	if !ok {
		a.Logger.Error("tether/api: publish ran without an authenticated identity")

		return mapError(auth.ErrUnavailable)
	}

	req, payload, err := readPublish(c, a.MaxPublishPayload)
	if err != nil {
		return err
	}

	if err = topic.Valid(req.Topic, false, 0); err != nil {
		return mapError(err)
	}

	if isSys(req.Topic) {
		return mapError(fmt.Errorf("%w: $SYS topics are the broker's", tether.ErrNotAuthorized))
	}

	tn := ident.Tenant
	if !a.Limits.AllowMessage(tn.ID.String(), tn.Limits) {
		c.SetHeader("Retry-After", "1")

		return mapError(fmt.Errorf("%w: tenant message rate exceeded", auth.ErrRateLimited))
	}

	rules, _, err := a.Perms.Load(c.Context(), ident)
	if err != nil {
		return a.loadFailed(err)
	}

	set := acl.Compile(rules, acl.Vars{ClientID: ident.ClientID, Username: ident.Username})
	if !set.CanPublish(req.Topic) {
		return mapError(fmt.Errorf("%w: publishing to this topic is not granted", tether.ErrNotAuthorized))
	}

	var qos byte
	if req.QoS == 1 {
		qos = 1
	}

	if err := a.Broker.DevicePublish(c.Context(), tn.ID.String(), req.Topic, payload, qos, req.Retain); err != nil {
		return a.brokerFail("publish", err)
	}

	return c.JSON(http.StatusOK, publishResponse{Published: true})
}

// isSys reports whether name is under "$SYS" in any casing, which a client
// may never publish to (mochi drops those, and the broker refuses them).
func isSys(name string) bool {
	return len(name) >= len(sysPrefix) && strings.EqualFold(name[:len(sysPrefix)], sysPrefix)
}

const sysPrefix = "$SYS"

// loadFailed maps a failed grant lookup the way the broker treats one at
// CONNECT: a refused principal is 403, anything else is an outage (503) and
// is logged.
func (a *API) loadFailed(err error) error {
	switch {
	case errors.Is(err, auth.ErrBadCredentials),
		errors.Is(err, auth.ErrNotAuthorized),
		errors.Is(err, tether.ErrNotFound),
		errors.Is(err, tether.ErrInvalidInput):
		return mapError(auth.ErrNotAuthorized)
	}

	a.Logger.Error("tether/api: load grants failed", "error", err)

	return mapError(auth.ErrUnavailable)
}

// readPublish reads and checks the request body. A body over the limit, or a
// payload over maxPayload bytes once decoded, is 413; anything else wrong
// with it is 400.
func readPublish(c forge.Context, maxPayload int) (*publishRequest, []byte, error) {
	r := c.Request()
	r.Body = http.MaxBytesReader(c.Response(), r.Body, publishBodyLimit(maxPayload))

	var req publishRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			return nil, nil, errPayloadTooLarge(maxPayload)
		}

		return nil, nil, mapError(fmt.Errorf("%w: the body must be a JSON object with topic, payload, qos and retain", tether.ErrInvalidInput))
	}

	if req.QoS != 0 && req.QoS != 1 {
		return nil, nil, mapError(fmt.Errorf("%w: qos must be 0 or 1", tether.ErrInvalidInput))
	}

	if base64.StdEncoding.DecodedLen(len(req.Payload)) > maxPayload+2 {
		// DecodedLen overshoots by at most two bytes of padding; the exact
		// check follows the decode.
		return nil, nil, errPayloadTooLarge(maxPayload)
	}

	payload, err := base64.StdEncoding.DecodeString(req.Payload)
	if err != nil {
		return nil, nil, mapError(fmt.Errorf("%w: payload is not valid base64", tether.ErrInvalidInput))
	}

	if len(payload) > maxPayload {
		return nil, nil, errPayloadTooLarge(maxPayload)
	}

	return &req, payload, nil
}

func errPayloadTooLarge(maxPayload int) error {
	return forge.NewHTTPError(http.StatusRequestEntityTooLarge,
		fmt.Sprintf("the payload may be at most %d bytes", maxPayload))
}
