package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/perm"
)

func rolesListHandler(sp ServicesProvider) handlerFunc[TenantRef, RoleList] {
	return func(ctx context.Context, in TenantRef, _ dashcontract.Principal) (RoleList, error) {
		s, err := services(sp)
		if err != nil {
			return RoleList{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return RoleList{}, err
		}

		roles, err := s.Roles.List(ctx, t)
		if err != nil {
			return RoleList{}, fail(s, "list roles", err)
		}

		items := make([]Role, 0, len(roles))
		for i := range roles {
			items = append(items, fromRole(&roles[i]))
		}

		return RoleList{Items: items}, nil
	}
}

func rolesCreateHandler(sp ServicesProvider) handlerFunc[CreateRoleInput, Role] {
	return func(ctx context.Context, in CreateRoleInput, _ dashcontract.Principal) (Role, error) {
		s, err := services(sp)
		if err != nil {
			return Role{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return Role{}, err
		}

		got, err := s.Roles.Create(ctx, t, perm.RoleInput{
			Slug:       in.Slug,
			Name:       in.Name,
			ParentSlug: in.ParentSlug,
			Rules:      acl.Rules{Publish: in.Publish, Subscribe: in.Subscribe},
		})
		if err != nil {
			return Role{}, fail(s, "create role", err)
		}

		return fromRole(got), nil
	}
}

func rolesDeleteHandler(sp ServicesProvider) handlerFunc[RoleRef, Ack] {
	return func(ctx context.Context, in RoleRef, _ dashcontract.Principal) (Ack, error) {
		s, err := services(sp)
		if err != nil {
			return Ack{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return Ack{}, err
		}

		err = required("roleSlug", in.RoleSlug)
		if err != nil {
			return Ack{}, err
		}

		if err = s.Roles.Delete(ctx, t, in.RoleSlug); err != nil {
			return Ack{}, fail(s, "delete role", err)
		}

		return Ack{OK: true}, nil
	}
}

func rolesTopicsGetHandler(sp ServicesProvider) handlerFunc[RoleRef, TopicRules] {
	return func(ctx context.Context, in RoleRef, _ dashcontract.Principal) (TopicRules, error) {
		s, err := services(sp)
		if err != nil {
			return TopicRules{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return TopicRules{}, err
		}

		err = required("roleSlug", in.RoleSlug)
		if err != nil {
			return TopicRules{}, err
		}

		rules, err := s.Roles.GetRules(ctx, t, in.RoleSlug)
		if err != nil {
			return TopicRules{}, fail(s, "get role topics", err)
		}

		return fromRules(in.RoleSlug, rules), nil
	}
}

func rolesTopicsSetHandler(sp ServicesProvider) handlerFunc[SetTopicsInput, TopicRules] {
	return func(ctx context.Context, in SetTopicsInput, _ dashcontract.Principal) (TopicRules, error) {
		s, err := services(sp)
		if err != nil {
			return TopicRules{}, err
		}

		t, err := loadTenant(ctx, s, in.TenantID)
		if err != nil {
			return TopicRules{}, err
		}

		err = required("roleSlug", in.RoleSlug)
		if err != nil {
			return TopicRules{}, err
		}

		// The call replaces both lists, so a missing one would clear
		// grants the caller never meant to touch: both must be named.
		if in.Publish == nil || in.Subscribe == nil {
			return TopicRules{}, badRequest("publish and subscribe are both required (use [] for none)")
		}

		rules := acl.Rules{Publish: in.Publish, Subscribe: in.Subscribe}
		if err = s.Roles.SetRules(ctx, t, in.RoleSlug, rules); err != nil {
			return TopicRules{}, fail(s, "set role topics", err)
		}

		// Answer with what is stored, not what was sent.
		stored, err := s.Roles.GetRules(ctx, t, in.RoleSlug)
		if err != nil {
			return TopicRules{}, fail(s, "get role topics", err)
		}

		return fromRules(in.RoleSlug, stored), nil
	}
}

func fromRules(slug string, r acl.Rules) TopicRules {
	return TopicRules{RoleSlug: slug, Publish: strs(r.Publish), Subscribe: strs(r.Subscribe)}
}
