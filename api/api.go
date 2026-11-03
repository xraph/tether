// Package api is Tether's REST layer: an admin API for tenants, devices,
// roles, live clients, retained messages and stats, and a public HTTP publish
// endpoint for devices.
//
// Register mounts both on Forge routers. The caller owns authentication of
// the admin group: Register adds no admin auth of its own.
package api

import (
	"errors"
	"log/slog"

	"github.com/xraph/forge"
	forgeauth "github.com/xraph/forge/extensions/auth"

	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/limits"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/service"
)

const (
	// DefaultPageSize is how many tenants or devices a list returns when the
	// request gives no limit.
	DefaultPageSize = 100

	// MaxPageSize caps the limit a list request may ask for.
	MaxPageSize = 500

	// MaxPublishPayload is the largest decoded payload POST /v1/publish takes
	// when Services.MaxPublishPayload is zero: the broker's default
	// MaxPacketSize. A larger one is 413.
	MaxPublishPayload = 1 << 20
)

// Services are what the handlers call. Everything but Logger is required.
type Services struct {
	Tenants *service.Tenants
	Devices *service.Devices
	Roles   *perm.Roles
	Broker  *broker.Broker

	// Auth, Perms and Limits serve POST /v1/publish: the same authenticator,
	// grant loader and rate limiter the broker runs MQTT connections through.
	Auth   auth.Authenticator
	Perms  broker.PermLoader
	Limits *limits.Limiter

	// Logger receives the internal errors the API hides from clients. Nil
	// discards them.
	Logger *slog.Logger

	// MaxPublishPayload is the largest decoded payload POST /v1/publish
	// takes, in bytes. Zero (or less) means the MaxPublishPayload constant.
	// Set it to the broker's MaxPacketSize so HTTP and MQTT agree.
	MaxPublishPayload int

	// Audit hears about every admin change once it ran, with its outcome
	// and the caller. The dashboard contract emits through the same field.
	// Nil records nothing.
	Audit audit.Emitter
}

// API holds the services the handlers share.
type API struct {
	Services
}

// Register mounts the admin routes on admin and POST /v1/publish on public,
// all relative to those routers' base path. It does not authenticate the
// admin group: add your own auth middleware to admin with admin.Use first.
// The routes go on groups nested in admin, which inherit both admin.Use and
// forge.WithGroupMiddleware since forge v1.13.0 (older forge dropped the
// second, so prefer Use if you build against one). The publish route
// authenticates itself with HTTP Basic (key ID and raw key).
//
// It returns an error when a required service is missing or a route cannot
// be registered; routes registered before the failure stay mounted.
func Register(admin, public forge.Router, s Services) error {
	if err := s.check(); err != nil {
		return err
	}

	if s.Logger == nil {
		s.Logger = slog.New(slog.DiscardHandler)
	}

	if s.MaxPublishPayload <= 0 {
		s.MaxPublishPayload = MaxPublishPayload
	}

	a := &API{Services: s}

	return errors.Join(
		a.registerTenants(admin),
		a.registerDevices(admin),
		a.registerRoles(admin),
		a.registerClients(admin),
		a.registerPublish(public),
	)
}

// Emit stamps an admin event with the broker's node and hands it to Audit.
// It does nothing without one. Callers fill Action, TenantID, Actor and
// what the change acted on; err is what the change returned, and makes the
// outcome a failure with err as the reason.
func (s *Services) Emit(ev audit.Event, err error) {
	if s == nil || s.Audit == nil {
		return
	}

	ev.Outcome = audit.OutcomeSuccess
	if err != nil {
		ev.Outcome = audit.OutcomeFailure
		ev.Reason = err.Error()
	}

	if s.Broker != nil {
		ev.Node = s.Broker.NodeID()
	}

	s.Audit.Emit(ev)
}

// actor is who made a REST call: the Subject of the forge auth context the
// admin middleware set, on the forge context or on the request's.
func actor(c forge.Context) string {
	if ac, ok := forgeauth.GetAuthContext(c); ok && ac != nil {
		return ac.Subject
	}

	if ac, ok := forgeauth.GetAuthContextFromStdContext(c.Context()); ok && ac != nil {
		return ac.Subject
	}

	return ""
}

// record emits the admin change ev of the REST call c, which returned err.
func (a *API) record(c forge.Context, ev audit.Event, err error) {
	if a.Audit == nil {
		return
	}

	ev.Actor = actor(c)
	a.Emit(ev, err)
}

// audited wraps h, a handler that changes state, so the change is recorded
// once h returned: describe builds the event from the request, record adds
// the caller and the outcome. A request forge refuses before h runs (a body
// that does not bind) is not a change and records nothing.
func audited[R, O any](a *API, describe func(req *R) audit.Event,
	h func(forge.Context, *R) (*O, error),
) func(forge.Context, *R) (*O, error) {
	return func(c forge.Context, req *R) (*O, error) {
		out, err := h(c, req)
		a.record(c, describe(req), err)

		return out, err
	}
}

// check reports the first missing required service.
func (s Services) check() error {
	switch {
	case s.Tenants == nil:
		return errors.New("tether/api: Services.Tenants is required")
	case s.Devices == nil:
		return errors.New("tether/api: Services.Devices is required")
	case s.Roles == nil:
		return errors.New("tether/api: Services.Roles is required")
	case s.Broker == nil:
		return errors.New("tether/api: Services.Broker is required")
	case s.Auth == nil:
		return errors.New("tether/api: Services.Auth is required")
	case s.Perms == nil:
		return errors.New("tether/api: Services.Perms is required")
	case s.Limits == nil:
		return errors.New("tether/api: Services.Limits is required")
	}

	return nil
}

// empty is the response type of handlers that write their own response.
type empty struct{}

// list is the body of every list response.
type list[T any] struct {
	Items []T `json:"items"`
}

// items wraps s, turning nil into an empty list so the JSON is [] and not null.
func items[T any](s []T) list[T] {
	if s == nil {
		s = []T{}
	}

	return list[T]{Items: s}
}

// written ends a handler that wrote its own response with err (nil on
// success). Forge writes nothing more for a nil response.
func written(err error) (*empty, error) {
	if err != nil {
		return nil, err
	}

	return nil, nil //nolint:nilnil // the response is already written.
}
