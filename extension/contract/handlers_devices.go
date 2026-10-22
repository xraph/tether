package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/api"
	"github.com/xraph/tether/device"
	"github.com/xraph/tether/service"
)

// defaultRevokeReason is what keysmith records when the intent gives none.
const defaultRevokeReason = "revoked through the dashboard"

func devicesListHandler(sp ServicesProvider) handlerFunc[DeviceListInput, DeviceList] {
	return func(ctx context.Context, in DeviceListInput, _ dashcontract.Principal) (DeviceList, error) {
		s, err := services(sp)
		if err != nil {
			return DeviceList{}, err
		}

		// The tenant is checked first: the store lists a tenant that does
		// not exist as empty.
		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return DeviceList{}, err
		}

		limit, offset, err := page(in.Limit, in.Offset)
		if err != nil {
			return DeviceList{}, err
		}

		ds, err := s.Devices.List(ctx, device.ListFilter{TenantID: t.ID, Limit: limit, Offset: offset})
		if err != nil {
			return DeviceList{}, fail(s, "list devices", err)
		}

		items := make([]Device, 0, len(ds))
		for _, d := range ds {
			items = append(items, fromDevice(d))
		}

		return DeviceList{Items: items, Limit: limit, Offset: offset}, nil
	}
}

func devicesGetHandler(sp ServicesProvider) handlerFunc[DeviceRef, Device] {
	return func(ctx context.Context, in DeviceRef, _ dashcontract.Principal) (Device, error) {
		s, err := services(sp)
		if err != nil {
			return Device{}, err
		}

		return getDevice(ctx, s, in.TenantID, in.DeviceID)
	}
}

func devicesProvisionHandler(sp ServicesProvider) handlerFunc[ProvisionInput, Credentials] {
	return func(ctx context.Context, in ProvisionInput, _ dashcontract.Principal) (Credentials, error) {
		s, err := services(sp)
		if err != nil {
			return Credentials{}, err
		}

		tid, err := tenantID(in.TenantID)
		if err != nil {
			return Credentials{}, err
		}

		creds, err := s.Devices.Provision(ctx, tid, service.ProvisionInput{
			Name:      in.Name,
			ClientID:  in.ClientID,
			Roles:     in.Roles,
			ExpiresAt: in.ExpiresAt,
		})
		if err != nil {
			return Credentials{}, fail(s, "provision device", err)
		}

		return fromCredentials(creds), nil
	}
}

func devicesRotateHandler(sp ServicesProvider) handlerFunc[RotateInput, Credentials] {
	return func(ctx context.Context, in RotateInput, _ dashcontract.Principal) (Credentials, error) {
		s, err := services(sp)
		if err != nil {
			return Credentials{}, err
		}

		tid, err := tenantID(in.TenantID)
		if err != nil {
			return Credentials{}, err
		}

		did, err := deviceID(in.DeviceID)
		if err != nil {
			return Credentials{}, err
		}

		grace, err := seconds("graceSeconds", in.GraceSeconds)
		if err != nil {
			return Credentials{}, mapErr(err)
		}

		creds, err := s.Devices.Rotate(ctx, tid, did, grace)
		if err != nil {
			return Credentials{}, fail(s, "rotate device", err)
		}

		return fromCredentials(creds), nil
	}
}

func devicesRevokeHandler(sp ServicesProvider) handlerFunc[RevokeInput, Ack] {
	return func(ctx context.Context, in RevokeInput, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		tid, err := tenantID(in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		did, err := deviceID(in.DeviceID)
		if err != nil {
			return Ack{}, err
		}

		reason := in.Reason
		if reason == "" {
			reason = defaultRevokeReason
		}

		// The keysmith plugin events disconnect a revoked device's
		// connections; nothing to tell the broker here.
		if err := s.Devices.Revoke(ctx, tid, did, reason); err != nil {
			return Ack{}, fail(s, "revoke device", err)
		}

		return Ack{OK: true}, nil
	}
}

func devicesDeleteHandler(sp ServicesProvider) handlerFunc[DeviceRef, Ack] {
	return func(ctx context.Context, in DeviceRef, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		tid, err := tenantID(in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		did, err := deviceID(in.DeviceID)
		if err != nil {
			return Ack{}, err
		}

		if err := s.Devices.Delete(ctx, tid, did); err != nil {
			return Ack{}, fail(s, "delete device", err)
		}

		return Ack{OK: true}, nil
	}
}

func devicesRolesSetHandler(sp ServicesProvider) handlerFunc[SetDeviceRolesInput, Device] {
	return func(ctx context.Context, in SetDeviceRolesInput, _ dashcontract.Principal) (Device, error) {
		s, err := services(sp)
		if err != nil {
			return Device{}, err
		}

		tid, err := tenantID(in.TenantID)
		if err != nil {
			return Device{}, err
		}

		did, err := deviceID(in.DeviceID)
		if err != nil {
			return Device{}, err
		}

		if in.Roles == nil {
			return Device{}, badRequest("roles is required (use [] for none)")
		}

		if err := s.Devices.SetRoles(ctx, tid, did, in.Roles); err != nil {
			return Device{}, fail(s, "set device roles", err)
		}

		return getDevice(ctx, s, in.TenantID, in.DeviceID)
	}
}

// getDevice loads the device an intent names. The service looks it up inside
// the tenant, so another tenant's device ID is NOT_FOUND, the same as an
// unknown one.
func getDevice(ctx context.Context, s *api.Services, rawTenant, rawDevice string) (Device, error) {
	tid, err := tenantID(rawTenant)
	if err != nil {
		return Device{}, err
	}

	did, err := deviceID(rawDevice)
	if err != nil {
		return Device{}, err
	}

	d, err := s.Devices.Get(ctx, tid, did)
	if err != nil {
		return Device{}, fail(s, "get device", err)
	}

	return fromDevice(d), nil
}

func fromCredentials(c *service.Credentials) Credentials {
	return Credentials{Device: fromDevice(c.Device), Username: c.Username, Password: c.Password}
}
