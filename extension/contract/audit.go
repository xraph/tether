package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/audit"
)

// audited runs h and records the admin change it made, or tried to, with
// the dashboard user as the actor and h's error as the failure. describe
// builds the event from the input and the answer. Commands register it
// inside scoped, so the input's tenantId is the one the change acted on.
func audited[I, O any](sp ServicesProvider, describe func(in I, out O) audit.Event, h handlerFunc[I, O]) handlerFunc[I, O] {
	return func(ctx context.Context, in I, p dashcontract.Principal) (O, error) {
		out, err := h(ctx, in, p)

		if s := sp(); s != nil && s.Audit != nil {
			ev := describe(in, out)
			if p.User != nil {
				ev.Actor = p.User.Subject
			}

			s.Emit(ev, err)
		}

		return out, err
	}
}

// The describe functions of the commands, one per input.

func tenantCreated(in CreateTenantInput, out Tenant) audit.Event {
	ev := audit.Event{Action: audit.ActionTenantCreate, TenantID: out.ID,
		Meta: map[string]any{"app_id": in.AppID, "external_id": in.ExternalID}}
	if out.ID != "" {
		ev.Meta[audit.MetaTarget] = out.ID
	}

	return ev
}

func tenantUpdated(in UpdateTenantInput, _ Tenant) audit.Event {
	return audit.Event{Action: audit.ActionTenantUpdate, TenantID: in.TenantID, Meta: audit.Target(in.TenantID)}
}

func tenantDeleted(in TenantRef, _ Ack) audit.Event {
	return audit.Event{Action: audit.ActionTenantDelete, TenantID: in.TenantID, Meta: audit.Target(in.TenantID)}
}

func deviceProvisioned(in ProvisionInput, out Credentials) audit.Event {
	ev := audit.Event{Action: audit.ActionDeviceProvision, TenantID: in.TenantID, ClientID: in.ClientID, Subject: out.Device.ID}
	if out.Device.ID != "" {
		ev.Meta = audit.Target(out.Device.ID)
	}

	return ev
}

func deviceEvent(action, tenantID, deviceID string) audit.Event {
	return audit.Event{Action: action, TenantID: tenantID, Subject: deviceID, Meta: audit.Target(deviceID)}
}

func deviceRotated(in RotateInput, _ Credentials) audit.Event {
	return deviceEvent(audit.ActionDeviceRotate, in.TenantID, in.DeviceID)
}

func deviceRevoked(in RevokeInput, _ Ack) audit.Event {
	return deviceEvent(audit.ActionDeviceRevoke, in.TenantID, in.DeviceID)
}

func deviceDeleted(in DeviceRef, _ Ack) audit.Event {
	return deviceEvent(audit.ActionDeviceDelete, in.TenantID, in.DeviceID)
}

func deviceRolesSet(in SetDeviceRolesInput, _ Device) audit.Event {
	ev := deviceEvent(audit.ActionDeviceRoles, in.TenantID, in.DeviceID)
	ev.Meta["roles"] = in.Roles

	return ev
}

func roleCreated(in CreateRoleInput, _ Role) audit.Event {
	return audit.Event{Action: audit.ActionRoleCreate, TenantID: in.TenantID, Meta: audit.Target(in.Slug)}
}

func roleDeleted(in RoleRef, _ Ack) audit.Event {
	return audit.Event{Action: audit.ActionRoleDelete, TenantID: in.TenantID, Meta: audit.Target(in.RoleSlug)}
}

func roleTopicsSet(in SetTopicsInput, _ TopicRules) audit.Event {
	ev := audit.Event{Action: audit.ActionRoleTopics, TenantID: in.TenantID, Meta: audit.Target(in.RoleSlug)}
	ev.Meta["publish"], ev.Meta["subscribe"] = in.Publish, in.Subscribe

	return ev
}

func clientKicked(in ClientRef, _ Ack) audit.Event {
	return audit.Event{Action: audit.ActionClientKick, TenantID: in.TenantID, ClientID: in.ClientID}
}

func sessionDeleted(in ClientRef, _ Ack) audit.Event {
	return audit.Event{Action: audit.ActionSessionDelete, TenantID: in.TenantID, ClientID: in.ClientID}
}

func retainedDeleted(in RetainedRef, _ Ack) audit.Event {
	return audit.Event{Action: audit.ActionRetainedDelete, TenantID: in.TenantID, Meta: audit.Target(in.Topic)}
}
