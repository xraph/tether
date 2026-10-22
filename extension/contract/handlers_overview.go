package contract

import (
	"context"

	dashcontract "github.com/xraph/forge/extensions/dashboard/contract"

	"github.com/xraph/tether/api"
	"github.com/xraph/tether/tenant"
)

// Scopes of a Stats answer.
const (
	scopeNode   = "node"
	scopeTenant = "tenant"
)

// overviewStatsHandler is overview.stats: one tenant's counters on this node
// with a tenantId, or the node total without one. It is the one tenant
// optional intent, and the only place an absent tenantId means every tenant:
// it returns sums, never rows.
func overviewStatsHandler(sp ServicesProvider) handlerFunc[StatsInput, Stats] {
	return func(ctx context.Context, in StatsInput, _ dashcontract.Principal) (Stats, error) {
		s, err := services(sp)
		if err != nil {
			return Stats{}, err
		}

		if !s.Broker.Running() {
			return Stats{}, errBrokerDown()
		}

		if in.TenantID != "" {
			return tenantStats(ctx, s, in.TenantID)
		}

		return nodeStats(ctx, s)
	}
}

func tenantStats(ctx context.Context, s *api.Services, raw string) (Stats, error) {
	t, err := loadTenant(ctx, s, raw)
	if err != nil {
		return Stats{}, err
	}

	b := s.Broker.Stats(t.ID.String())

	return Stats{
		Scope:         scopeTenant,
		TenantID:      t.ID.String(),
		Connected:     b.Connected,
		Sessions:      b.Sessions,
		Subscriptions: b.Subscriptions,
		Retained:      b.Retained,
		MessagesIn:    b.MessagesIn,
		MessagesOut:   b.MessagesOut,
	}, nil
}

// nodeStats adds up every tenant's counters. The broker keeps them per
// tenant, so this asks it once for each.
func nodeStats(ctx context.Context, s *api.Services) (Stats, error) {
	out := Stats{Scope: scopeNode}
	count := 0

	for offset := 0; ; offset += api.MaxPageSize {
		ts, err := s.Tenants.List(ctx, tenant.ListFilter{Limit: api.MaxPageSize, Offset: offset})
		if err != nil {
			return Stats{}, fail(s, "list tenants", err)
		}

		for _, t := range ts {
			b := s.Broker.Stats(t.ID.String())

			out.Connected += b.Connected
			out.Sessions += b.Sessions
			out.Subscriptions += b.Subscriptions
			out.Retained += b.Retained
			out.MessagesIn += b.MessagesIn
			out.MessagesOut += b.MessagesOut
		}

		count += len(ts)

		if len(ts) < api.MaxPageSize {
			break
		}
	}

	out.Tenants = &count

	return out, nil
}
