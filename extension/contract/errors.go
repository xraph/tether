package contract

import (
	"context"
	"errors"
	"fmt"
	"strings"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"
	"github.com/xraph/keysmith"
	"github.com/xraph/warden"

	"github.com/xraph/tether"
	"github.com/xraph/tether/api"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/tenant"
)

// requireUser is the admission check of every intent: the principal must
// carry an authenticated user. The dashboard gives a request no user when its
// auth is off or the request did not authenticate, and nothing else here
// checks who is calling, so without this an unauthenticated caller reaches
// every admin operation.
func requireUser(_ context.Context, _ dashcontract.Request, p dashcontract.Principal) error {
	if p.User.Authenticated() {
		return nil
	}

	return &dashcontract.Error{Code: dashcontract.CodeUnauthenticated, Message: "authentication required"}
}

// services returns the live services, or UNAVAILABLE before Tether has
// started: the dashboard can register the contributor before Start runs.
func services(sp ServicesProvider) (*api.Services, error) {
	if s := sp(); s != nil {
		return s, nil
	}

	return nil, unavailable("tether has not started")
}

func unavailable(msg string) *dashcontract.Error {
	return &dashcontract.Error{Code: dashcontract.CodeUnavailable, Message: msg, Retryable: true}
}

func badRequest(format string, args ...any) *dashcontract.Error {
	return &dashcontract.Error{Code: dashcontract.CodeBadRequest, Message: fmt.Sprintf(format, args...)}
}

// mapErr turns an error from a service, the broker or a store into the
// contract error the dashboard sees. A contract error passes through. Only
// the tether sentinels' own reasons reach the caller; anything unmapped is a
// generic INTERNAL.
//
//	tether.ErrInvalidInput                              BAD_REQUEST, with the reason
//	warden.ErrRoleNotFound                              BAD_REQUEST "unknown role"
//	tether.ErrNotFound, keysmith.ErrKeyNotFound         NOT_FOUND
//	tether.ErrConflict                                  CONFLICT, with the reason
//	keysmith.ErrInvalidStateTransition, ErrKeyRevoked   CONFLICT (rotating a revoked key)
//	tether.ErrTenantDisabled, tether.ErrNotAuthorized   PERMISSION_DENIED
//	anything else                                       INTERNAL
func mapErr(err error) *dashcontract.Error {
	var ce *dashcontract.Error
	if errors.As(err, &ce) {
		return ce
	}

	switch {
	case errors.Is(err, tether.ErrInvalidInput):
		return &dashcontract.Error{Code: dashcontract.CodeBadRequest, Message: reason(err, tether.ErrInvalidInput, "invalid input")}
	case errors.Is(err, warden.ErrRoleNotFound):
		return &dashcontract.Error{Code: dashcontract.CodeBadRequest, Message: "unknown role"}
	case errors.Is(err, tether.ErrNotFound), errors.Is(err, keysmith.ErrKeyNotFound):
		return &dashcontract.Error{Code: dashcontract.CodeNotFound, Message: "not found"}
	case errors.Is(err, tether.ErrConflict):
		return &dashcontract.Error{Code: dashcontract.CodeConflict, Message: reason(err, tether.ErrConflict, "conflict")}
	case errors.Is(err, keysmith.ErrInvalidStateTransition), errors.Is(err, keysmith.ErrKeyRevoked):
		return &dashcontract.Error{Code: dashcontract.CodeConflict, Message: "the device's key is revoked"}
	case errors.Is(err, tether.ErrTenantDisabled):
		return &dashcontract.Error{Code: dashcontract.CodePermissionDenied, Message: "tenant is disabled"}
	case errors.Is(err, tether.ErrNotAuthorized):
		return &dashcontract.Error{Code: dashcontract.CodePermissionDenied, Message: reason(err, tether.ErrNotAuthorized, "not authorized")}
	default:
		return &dashcontract.Error{Code: dashcontract.CodeInternal, Message: "internal error"}
	}
}

// reason returns what follows sentinel in err's message (the reason a tether
// validation gave), cut at the first line, or fallback when there is none.
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

// fail maps err and logs it when the caller will not see the cause. op names
// the operation for the log.
func fail(s *api.Services, op string, err error) error {
	ce := mapErr(err)

	if ce.Code == dashcontract.CodeInternal && s.Logger != nil {
		s.Logger.Error("tether/contract: request failed", "op", op, "error", err)
	}

	return ce
}

// brokerFail is fail for an error from a broker call. A broker that is not
// running answers with tether.ErrNotFound or tether.ErrConflict, which would
// read as "no such client" or "already exists", so it is UNAVAILABLE instead.
func brokerFail(s *api.Services, op string, err error) error {
	if !s.Broker.Running() {
		return errBrokerDown()
	}

	return fail(s, op, err)
}

func errBrokerDown() *dashcontract.Error {
	return unavailable("the broker is not running")
}

// tenantID parses the tenantId an intent names, after scoped has replaced it
// with the tenant the caller may act on. An empty one is PERMISSION_DENIED,
// never "every tenant". A malformed one is BAD_REQUEST.
func tenantID(raw string) (id.TenantID, error) {
	if raw == "" {
		return id.TenantID{}, &dashcontract.Error{
			Code:    dashcontract.CodePermissionDenied,
			Message: "tenantId is required",
		}
	}

	tid, err := id.ParseTenantID(raw)
	if err != nil {
		return id.TenantID{}, badRequest("malformed tenantId")
	}

	return tid, nil
}

// deviceID parses the deviceId an intent names.
func deviceID(raw string) (id.DeviceID, error) {
	if raw == "" {
		return id.DeviceID{}, badRequest("deviceId is required")
	}

	did, err := id.ParseDeviceID(raw)
	if err != nil {
		return id.DeviceID{}, badRequest("malformed deviceId")
	}

	return did, nil
}

// required rejects an empty value of a field an intent cannot do without.
func required(field, value string) error {
	if value == "" {
		return badRequest("%s is required", field)
	}

	return nil
}

// loadTenant parses the tenantId and loads the tenant: unknown is NOT_FOUND.
func loadTenant(ctx context.Context, s *api.Services, raw string) (*tenant.Tenant, error) {
	tid, err := tenantID(raw)
	if err != nil {
		return nil, err
	}

	t, err := s.Tenants.Get(ctx, tid)
	if err != nil {
		return nil, fail(s, "get tenant", err)
	}

	return t, nil
}

// liveTenant is the start of every intent that calls the broker for one
// tenant: the tenantId, then UNAVAILABLE while the broker is not running,
// then the tenant (NOT_FOUND when unknown). It returns the tenant's
// canonical ID string, the only form the broker takes.
func liveTenant(ctx context.Context, s *api.Services, raw string) (string, error) {
	if _, err := tenantID(raw); err != nil {
		return "", err
	}

	if !s.Broker.Running() {
		return "", errBrokerDown()
	}

	t, err := loadTenant(ctx, s, raw)
	if err != nil {
		return "", err
	}

	return t.ID.String(), nil
}

// page returns the limit and offset of a list request: api.DefaultPageSize
// when no limit is given, BAD_REQUEST when either is negative or the limit
// passes api.MaxPageSize. It is the same rule the REST API applies.
func page(limit, offset int) (limitOut, offsetOut int, err error) {
	switch {
	case limit < 0, offset < 0:
		return 0, 0, badRequest("limit and offset must not be negative")
	case limit > api.MaxPageSize:
		return 0, 0, badRequest("limit must be at most %d", api.MaxPageSize)
	case limit == 0:
		return api.DefaultPageSize, offset, nil
	}

	return limit, offset, nil
}
