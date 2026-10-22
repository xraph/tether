package api

import (
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/perm"
)

// createRoleRequest is POST /v1/tenants/:tenantId/roles. The two lists are
// the role's topic grants, with the "%c" and "%u" placeholders acl.Compile
// expands, and may be left out for a role that grants nothing yet.
type createRoleRequest struct {
	TenantID   string   `path:"tenantId"`
	Slug       string   `json:"slug"`
	Name       string   `json:"name,omitempty"`
	ParentSlug string   `json:"parent_slug,omitempty"`
	Publish    []string `json:"publish,omitempty"`
	Subscribe  []string `json:"subscribe,omitempty"`
}

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
	roles := r.Group("/v1/tenants/:tenantId/roles", forge.WithGroupTags("roles"))
	topics := r.Group("/v1/tenants/:tenantId/roles/:roleSlug/topics", forge.WithGroupTags("roles"))

	return errors.Join(
		roles.POST("", a.createRole, forge.WithName("createRole"), forge.WithSummary("Create a role")),
		roles.GET("", a.listRoles, forge.WithName("listRoles"), forge.WithSummary("List a tenant's roles")),
		roles.DELETE("/:roleSlug", a.deleteRole, forge.WithName("deleteRole"),
			forge.WithSummary("Delete a role that no device holds")),
		topics.GET("", a.getTopics, forge.WithName("getRoleTopics"), forge.WithSummary("Get a role's topic grants")),
		topics.PUT("", a.setTopics, forge.WithName("setRoleTopics"), forge.WithSummary("Replace a role's topic grants")),
	)
}

func (a *API) createRole(c forge.Context, req *createRoleRequest) (*empty, error) {
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	got, err := a.Roles.Create(c.Context(), tn, perm.RoleInput{
		Slug:       req.Slug,
		Name:       req.Name,
		ParentSlug: req.ParentSlug,
		Rules:      acl.Rules{Publish: req.Publish, Subscribe: req.Subscribe},
	})
	if err != nil {
		return nil, a.fail("create role", err)
	}

	return written(c.JSON(http.StatusCreated, got))
}

func (a *API) listRoles(c forge.Context, req *tenantPath) (*list[perm.RoleInfo], error) {
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	got, err := a.Roles.List(c.Context(), tn)
	if err != nil {
		return nil, a.fail("list roles", err)
	}

	out := items(got)

	return &out, nil
}

func (a *API) deleteRole(c forge.Context, req *topicsPath) (*empty, error) {
	tn, err := a.tenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	if err := a.Roles.Delete(c.Context(), tn, req.RoleSlug); err != nil {
		return nil, a.fail("delete role", err)
	}

	return written(c.NoContent(http.StatusNoContent))
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
