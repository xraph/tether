package api

import (
	"errors"
	"net/http"

	"github.com/xraph/forge"

	"github.com/xraph/tether/audit"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/topic"
)

// retainedFilterDefault lists every retained message of the tenant except
// its $SYS topics, which a leading wildcard never matches.
const retainedFilterDefault = "#"

// clientPath binds the tenant and client IDs of a path.
type clientPath struct {
	TenantID string `path:"tenantId"`
	ClientID string `path:"clientId"`
}

// listRetainedRequest is GET /v1/tenants/:tenantId/retained.
type listRetainedRequest struct {
	TenantID string `path:"tenantId"`
	Filter   string `query:"filter" optional:"true"`
}

// deleteRetainedRequest is DELETE /v1/tenants/:tenantId/retained.
type deleteRetainedRequest struct {
	TenantID string `path:"tenantId"`
	Topic    string `query:"topic"`
}

// statsBody is a tenant's activity on this node.
type statsBody struct {
	Connected     int    `json:"connected"`
	Sessions      int    `json:"sessions"`
	Subscriptions int    `json:"subscriptions"`
	Retained      int    `json:"retained"`
	MessagesIn    uint64 `json:"messages_in"`
	MessagesOut   uint64 `json:"messages_out"`
}

func (a *API) registerClients(r forge.Router) error {
	g := r.Group("/v1/tenants/:tenantId", forge.WithGroupTags("clients"))

	return errors.Join(
		g.GET("/clients", a.listClients, forge.WithName("listClients"),
			forge.WithSummary("List a tenant's connections and kept sessions on this node")),
		g.POST("/clients/:clientId/kick", audited(a, clientEvent(audit.ActionClientKick), a.kickClient), forge.WithName("kickClient"),
			forge.WithSummary("Disconnect a client and keep its session")),
		g.DELETE("/sessions/:clientId", audited(a, clientEvent(audit.ActionSessionDelete), a.deleteSession), forge.WithName("deleteSession"),
			forge.WithSummary("Disconnect a client and delete its session")),
		g.GET("/retained", a.listRetained, forge.WithName("listRetained"),
			forge.WithSummary("List retained messages matching a filter")),
		g.DELETE("/retained", audited(a, retainedDeleted, a.deleteRetained), forge.WithName("deleteRetained"),
			forge.WithSummary("Delete the retained message on a topic")),
		g.GET("/stats", a.stats, forge.WithName("tenantStats"), forge.WithSummary("A tenant's counters on this node")),
	)
}

// clientEvent describes a change to the client or session a path names.
func clientEvent(action string) func(*clientPath) audit.Event {
	return func(req *clientPath) audit.Event {
		return audit.Event{Action: action, TenantID: req.TenantID, ClientID: req.ClientID}
	}
}

// retainedDeleted describes DELETE .../retained?topic=.
func retainedDeleted(req *deleteRetainedRequest) audit.Event {
	return audit.Event{Action: audit.ActionRetainedDelete, TenantID: req.TenantID, Meta: audit.Target(req.Topic)}
}

func (a *API) listClients(c forge.Context, req *tenantPath) (*list[broker.ClientInfo], error) {
	tid, err := a.liveTenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	out := items(a.Broker.Clients(tid))

	return &out, nil
}

func (a *API) kickClient(c forge.Context, req *clientPath) (*empty, error) {
	tid, err := a.liveTenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	if err := a.Broker.Kick(tid, req.ClientID); err != nil {
		return nil, a.brokerFail("kick client", err)
	}

	return written(c.NoContent(http.StatusNoContent))
}

func (a *API) deleteSession(c forge.Context, req *clientPath) (*empty, error) {
	tid, err := a.liveTenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	if err := a.Broker.DeleteSession(tid, req.ClientID); err != nil {
		return nil, a.brokerFail("delete session", err)
	}

	return written(c.NoContent(http.StatusNoContent))
}

func (a *API) listRetained(c forge.Context, req *listRetainedRequest) (*list[broker.RetainedInfo], error) {
	tid, err := a.liveTenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	filter := req.Filter
	if filter == "" {
		filter = retainedFilterDefault
	}

	// The broker lists nothing for a filter it rejects, which would read as
	// "no retained messages". Say why instead.
	if err := topic.Valid(filter, true, 0); err != nil {
		return nil, mapError(err)
	}

	out := items(a.Broker.Retained(tid, filter))

	return &out, nil
}

func (a *API) deleteRetained(c forge.Context, req *deleteRetainedRequest) (*empty, error) {
	tid, err := a.liveTenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	if err := a.Broker.DeleteRetained(tid, req.Topic); err != nil {
		return nil, a.brokerFail("delete retained", err)
	}

	return written(c.NoContent(http.StatusNoContent))
}

func (a *API) stats(c forge.Context, req *tenantPath) (*statsBody, error) {
	tid, err := a.liveTenant(c, req.TenantID)
	if err != nil {
		return nil, err
	}

	s := a.Broker.Stats(tid)

	return &statsBody{
		Connected:     s.Connected,
		Sessions:      s.Sessions,
		Subscriptions: s.Subscriptions,
		Retained:      s.Retained,
		MessagesIn:    s.MessagesIn,
		MessagesOut:   s.MessagesOut,
	}, nil
}

// liveTenant is the start of every route that calls the broker: 503 while the
// broker is not running, then the tenant a path names (404 when unknown). It
// returns the tenant's canonical ID string, the only form the broker takes.
func (a *API) liveTenant(c forge.Context, raw string) (string, error) {
	if !a.Broker.Running() {
		return "", errUnavailable()
	}

	tn, err := a.tenant(c, raw)
	if err != nil {
		return "", err
	}

	return tn.ID.String(), nil
}
