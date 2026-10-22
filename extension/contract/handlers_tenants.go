package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/service"
	"github.com/xraph/tether/tenant"
)

// tenantsListHandler is tenants.list: every tenant for an operator, and the
// caller's own tenant alone for a tenant user (paged the same way, so an
// offset past it is an empty page).
func tenantsListHandler(g *gate) handlerFunc[PageInput, TenantList] {
	return func(ctx context.Context, in PageInput, p dashcontract.Principal) (TenantList, error) {
		s, err := services(g.sp)
		if err != nil {
			return TenantList{}, err
		}

		limit, offset, err := page(in.Limit, in.Offset)
		if err != nil {
			return TenantList{}, err
		}

		if !g.operator {
			own, ownErr := g.own(ctx, s, p)
			if ownErr != nil {
				return TenantList{}, ownErr
			}

			items := []Tenant{}
			if offset == 0 {
				items = append(items, fromTenant(own))
			}

			return TenantList{Items: items, Limit: limit, Offset: offset}, nil
		}

		ts, err := s.Tenants.List(ctx, tenant.ListFilter{Limit: limit, Offset: offset})
		if err != nil {
			return TenantList{}, fail(s, "list tenants", err)
		}

		items := make([]Tenant, 0, len(ts))
		for _, t := range ts {
			items = append(items, fromTenant(t))
		}

		return TenantList{Items: items, Limit: limit, Offset: offset}, nil
	}
}

func tenantsGetHandler(sp ServicesProvider) handlerFunc[TenantRef, Tenant] {
	return func(ctx context.Context, in TenantRef, _ dashcontract.Principal) (Tenant, error) {
		s, err := services(sp)
		if err != nil {
			return Tenant{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return Tenant{}, err
		}

		return fromTenant(t), nil
	}
}

func tenantsCreateHandler(sp ServicesProvider) handlerFunc[CreateTenantInput, Tenant] {
	return func(ctx context.Context, in CreateTenantInput, _ dashcontract.Principal) (Tenant, error) {
		s, err := services(sp)
		if err != nil {
			return Tenant{}, err
		}

		limits, err := toLimits(in.Limits)
		if err != nil {
			return Tenant{}, mapErr(err)
		}

		t, err := s.Tenants.Create(ctx, service.CreateTenantInput{
			AppID:      in.AppID,
			ExternalID: in.ExternalID,
			Name:       in.Name,
			Limits:     limits,
		})
		if err != nil {
			return Tenant{}, fail(s, "create tenant", err)
		}

		return fromTenant(t), nil
	}
}

func tenantsUpdateHandler(sp ServicesProvider) handlerFunc[UpdateTenantInput, Tenant] {
	return func(ctx context.Context, in UpdateTenantInput, _ dashcontract.Principal) (Tenant, error) {
		s, err := services(sp)
		if err != nil {
			return Tenant{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return Tenant{}, err
		}

		patch := service.UpdateTenantInput{Name: in.Name, Enabled: in.Enabled}

		if in.Limits != nil {
			var limits tenant.Limits

			if limits, err = toLimits(*in.Limits); err != nil {
				return Tenant{}, mapErr(err)
			}

			patch.Limits = &limits
		}

		updated, err := s.Tenants.Update(ctx, t.ID, patch)
		if err != nil {
			return Tenant{}, fail(s, "update tenant", err)
		}

		// Live connections keep the tenant they connected with until told:
		// this gives them the new limits, or disconnects them when it is
		// disabled. It is harmless while the broker is stopped.
		s.Broker.UpdateTenant(updated)

		return fromTenant(updated), nil
	}
}

func tenantsDeleteHandler(sp ServicesProvider) handlerFunc[TenantRef, Ack] {
	return func(ctx context.Context, in TenantRef, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		// Delete refuses while the tenant has devices, and then nothing
		// changed: connections are only touched once the tenant is gone.
		if err := s.Tenants.Delete(ctx, t.ID); err != nil {
			return Ack{}, fail(s, "delete tenant", err)
		}

		gone := *t
		gone.Enabled = false

		s.Broker.UpdateTenant(&gone)

		return Ack{OK: true}, nil
	}
}
