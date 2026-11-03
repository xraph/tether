package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/tether"
	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/service"
)

// optionalBodyLimit caps the optional JSON bodies of rotate and revoke.
const optionalBodyLimit = 4 << 10

// defaultRevokeReason is what keysmith records when the request gives none.
const defaultRevokeReason = "revoked through the admin api"

// credentialsBody is the body of a provision or rotate response: the device,
// the MQTT username (its keysmith key ID) and the raw key, the MQTT password.
// This is the only time the password is shown. It is never stored, so a
// device that loses it needs a rotation.
type credentialsBody struct {
	Device   *device.Device `json:"device"`
	Username string         `json:"username"`
	Password string         `json:"password"`
}

// provisionRequest is POST /v1/tenants/:tenantId/devices.
type provisionRequest struct {
	TenantID  string     `path:"tenantId"`
	Name      string     `json:"name"`
	ClientID  string     `json:"client_id,omitempty"`
	Roles     []string   `json:"roles,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// listDevicesRequest is GET /v1/tenants/:tenantId/devices.
type listDevicesRequest struct {
	TenantID string `path:"tenantId"`
	Limit    int    `query:"limit" optional:"true"`
	Offset   int    `query:"offset" optional:"true"`
}

// devicePath binds the tenant and device IDs of a path.
type devicePath struct {
	TenantID string `path:"tenantId"`
	DeviceID string `path:"deviceId"`
}

// rotateBody is the optional body of POST .../devices/:deviceId/rotate. The
// old password keeps working for GraceSeconds; zero takes keysmith's default.
type rotateBody struct {
	GraceSeconds int64 `json:"grace_seconds"`
}

// revokeBody is the optional body of POST .../devices/:deviceId/revoke.
type revokeBody struct {
	Reason string `json:"reason"`
}

// setRolesRequest is PUT .../devices/:deviceId/roles. Roles is the device's
// whole set of warden role slugs; an empty list removes them all.
type setRolesRequest struct {
	TenantID string   `path:"tenantId"`
	DeviceID string   `path:"deviceId"`
	Roles    []string `json:"roles"`
}

func (a *API) registerDevices(r forge.Router) error {
	g := r.Group("/v1/tenants/:tenantId/devices", forge.WithGroupTags("devices"))

	return errors.Join(
		g.POST("", a.provisionDevice, forge.WithName("provisionDevice"),
			forge.WithSummary("Provision a device and return its credentials once")),
		g.GET("", a.listDevices, forge.WithName("listDevices"), forge.WithSummary("List a tenant's devices")),
		g.GET("/:deviceId", a.getDevice, forge.WithName("getDevice"), forge.WithSummary("Get a device")),
		g.DELETE("/:deviceId", audited(a, deviceEvent(audit.ActionDeviceDelete), a.deleteDevice), forge.WithName("deleteDevice"),
			forge.WithSummary("Revoke a device's key and delete the device")),
		g.POST("/:deviceId/rotate", audited(a, deviceEvent(audit.ActionDeviceRotate), a.rotateDevice), forge.WithName("rotateDevice"),
			forge.WithSummary("Rotate a device's key and return the new password once")),
		g.POST("/:deviceId/revoke", audited(a, deviceEvent(audit.ActionDeviceRevoke), a.revokeDevice), forge.WithName("revokeDevice"),
			forge.WithSummary("Revoke a device's key and keep the device")),
		g.PUT("/:deviceId/roles", audited(a, rolesSetEvent, a.setDeviceRoles), forge.WithName("setDeviceRoles"),
			forge.WithSummary("Replace a device's roles")),
	)
}

// provisionDevice records its own audit event: the device it made is the
// event's subject, and the response is written here, not returned.
func (a *API) provisionDevice(c forge.Context, req *provisionRequest) (*empty, error) {
	ev := audit.Event{Action: audit.ActionDeviceProvision, TenantID: req.TenantID, ClientID: req.ClientID}

	err := a.provision(c, req, &ev)
	a.record(c, ev, err)

	return written(err)
}

// provision provisions the device and writes the credentials, and names the
// device in ev.
func (a *API) provision(c forge.Context, req *provisionRequest, ev *audit.Event) error {
	tid, err := parseTenantID(req.TenantID)
	if err != nil {
		return mapError(err)
	}

	creds, err := a.Devices.Provision(c.Context(), tid, service.ProvisionInput{
		Name:      req.Name,
		ClientID:  req.ClientID,
		Roles:     req.Roles,
		ExpiresAt: req.ExpiresAt,
	})
	if err != nil {
		return a.fail("provision device", err)
	}

	ev.Subject = creds.Device.ID.String()
	ev.Meta = audit.Target(ev.Subject)

	return a.writeCredentials(c, http.StatusCreated, creds)
}

func (a *API) listDevices(c forge.Context, req *listDevicesRequest) (*list[*device.Device], error) {
	// The tenant is checked first: the store lists a tenant that does not
	// exist as empty.
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	limit, offset, err := page(&pageRequest{Limit: req.Limit, Offset: req.Offset})
	if err != nil {
		return nil, mapError(err)
	}

	ds, err := a.Devices.List(c.Context(), device.ListFilter{TenantID: tn.ID, Limit: limit, Offset: offset})
	if err != nil {
		return nil, a.fail("list devices", err)
	}

	out := items(ds)

	return &out, nil
}

func (a *API) getDevice(c forge.Context, req *devicePath) (*device.Device, error) {
	return a.device(c, req.TenantID, req.DeviceID)
}

func (a *API) deleteDevice(c forge.Context, req *devicePath) (*empty, error) {
	tid, did, err := parseDevicePath(req.TenantID, req.DeviceID)
	if err != nil {
		return nil, mapError(err)
	}

	// The keysmith plugin events disconnect and evict a deleted device's
	// connections; nothing to tell the broker here.
	if err := a.Devices.Delete(c.Context(), tid, did); err != nil {
		return nil, a.fail("delete device", err)
	}

	return written(c.NoContent(http.StatusNoContent))
}

func (a *API) rotateDevice(c forge.Context, req *devicePath) (*empty, error) {
	tid, did, err := parseDevicePath(req.TenantID, req.DeviceID)
	if err != nil {
		return nil, mapError(err)
	}

	var body rotateBody
	err = optionalBody(c, &body)
	if err != nil {
		return nil, err
	}

	if body.GraceSeconds < 0 {
		return nil, mapError(fmt.Errorf("%w: grace_seconds must not be negative", tether.ErrInvalidInput))
	}

	creds, err := a.Devices.Rotate(c.Context(), tid, did, time.Duration(body.GraceSeconds)*time.Second)
	if err != nil {
		return nil, a.fail("rotate device", err)
	}

	return written(a.writeCredentials(c, http.StatusOK, creds))
}

func (a *API) revokeDevice(c forge.Context, req *devicePath) (*empty, error) {
	tid, did, err := parseDevicePath(req.TenantID, req.DeviceID)
	if err != nil {
		return nil, mapError(err)
	}

	var body revokeBody
	err = optionalBody(c, &body)
	if err != nil {
		return nil, err
	}

	reason := body.Reason
	if reason == "" {
		reason = defaultRevokeReason
	}

	if err := a.Devices.Revoke(c.Context(), tid, did, reason); err != nil {
		return nil, a.fail("revoke device", err)
	}

	return written(c.NoContent(http.StatusNoContent))
}

func (a *API) setDeviceRoles(c forge.Context, req *setRolesRequest) (*device.Device, error) {
	tid, did, err := parseDevicePath(req.TenantID, req.DeviceID)
	if err != nil {
		return nil, mapError(err)
	}

	if req.Roles == nil {
		return nil, mapError(fmt.Errorf("%w: roles is required (use [] for none)", tether.ErrInvalidInput))
	}

	if err = a.Devices.SetRoles(c.Context(), tid, did, req.Roles); err != nil {
		return nil, a.fail("set device roles", err)
	}

	d, err := a.Devices.Get(c.Context(), tid, did)
	if err != nil {
		return nil, a.fail("get device", err)
	}

	return d, nil
}

// deviceEvent describes a change to the device a path names: the device is
// both its subject and its target.
func deviceEvent(action string) func(*devicePath) audit.Event {
	return func(req *devicePath) audit.Event {
		return audit.Event{Action: action, TenantID: req.TenantID, Subject: req.DeviceID, Meta: audit.Target(req.DeviceID)}
	}
}

// rolesSetEvent describes PUT .../devices/:deviceId/roles.
func rolesSetEvent(req *setRolesRequest) audit.Event {
	ev := deviceEvent(audit.ActionDeviceRoles)(&devicePath{TenantID: req.TenantID, DeviceID: req.DeviceID})
	ev.Meta["roles"] = req.Roles

	return ev
}

// device loads the device a path names. The service looks it up inside the
// tenant, so another tenant's device ID is 404, the same as an unknown one.
func (a *API) device(c forge.Context, rawTenant, rawDevice string) (*device.Device, error) {
	tid, did, err := parseDevicePath(rawTenant, rawDevice)
	if err != nil {
		return nil, mapError(err)
	}

	d, err := a.Devices.Get(c.Context(), tid, did)
	if err != nil {
		return nil, a.fail("get device", err)
	}

	return d, nil
}

// writeCredentials writes a response that carries a raw key. It must not be
// cached or stored anywhere on the way back.
func (a *API) writeCredentials(c forge.Context, status int, creds *service.Credentials) error {
	c.SetHeader("Cache-Control", "no-store")

	return c.JSON(status, credentialsBody{Device: creds.Device, Username: creds.Username, Password: creds.Password})
}

// optionalBody decodes a small JSON body into v. An empty body leaves v as
// it is; anything else that does not decode is 400.
func optionalBody(c forge.Context, v any) error {
	r := c.Request()

	err := json.NewDecoder(http.MaxBytesReader(c.Response(), r.Body, optionalBodyLimit)).Decode(v)
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}

	return mapError(fmt.Errorf("%w: the body must be a JSON object or empty", tether.ErrInvalidInput))
}
