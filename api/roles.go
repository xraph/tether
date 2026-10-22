package api

import (
	"errors"

	"github.com/xraph/forge"

	"github.com/xraph/tether/acl"
)

// topicsPath binds the tenant ID and role slug of a path.
type topicsPath struct {
	TenantID string `path:"tenantId"`
	RoleSlug string `path:"roleSlug"`
}

// setTopicsRequest is PUT /v1/tenants/:tenantId/roles/:roleSlug/topics. The
// two lists replace the role's grants; they use the "%c" and "%u"
// placeholders acl.Compile expands.
type setTopicsRequest struct {
	TenantID  string   `path:"tenantId"`
	RoleSlug  string   `path:"roleSlug"`
	Publish   []string `json:"publish,omitempty"`
	Subscribe []string `json:"subscribe,omitempty"`
}

func (a *API) registerRoles(r forge.Router) error {
	g := r.Group("/v1/tenants/:tenantId/roles/:roleSlug/topics", forge.WithGroupTags("roles"))

	return errors.Join(
		g.GET("", a.getTopics, forge.WithName("getRoleTopics"), forge.WithSummary("Get a role's topic grants")),
		g.PUT("", a.setTopics, forge.WithName("setRoleTopics"), forge.WithSummary("Replace a role's topic grants")),
	)
}

func (a *API) getTopics(c forge.Context, req *topicsPath) (*acl.Rules, error) {
	return a.rules(c, req.TenantID, req.RoleSlug)
}

func (a *API) setTopics(c forge.Context, req *setTopicsRequest) (*acl.Rules, error) {
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	rules := acl.Rules{Publish: req.Publish, Subscribe: req.Subscribe}
	if err := a.Roles.SetRules(c.Context(), tn, req.RoleSlug, rules); err != nil {
		return nil, a.fail("set role topics", err)
	}

	return a.rules(c, req.TenantID, req.RoleSlug)
}

// rules reads a role's grants, as non-nil lists so the JSON has [] for none.
func (a *API) rules(c forge.Context, rawTenant, slug string) (*acl.Rules, error) {
	tn, err := a.tenant(c, rawTenant)
	if err != nil {
		return nil, err
	}

	rules, err := a.Roles.GetRules(c.Context(), tn, slug)
	if err != nil {
		return nil, a.fail("get role topics", err)
	}

	if rules.Publish == nil {
		rules.Publish = []string{}
	}

	if rules.Subscribe == nil {
		rules.Subscribe = []string{}
	}

	return &rules, nil
}
