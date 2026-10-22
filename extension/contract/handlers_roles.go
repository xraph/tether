package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/acl"
)

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
