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

// overviewStatsHandler is overview.stats: one tenant's counters on this node,
// or for an operator who names no tenant the node total. That is the only
// place an absent tenantId means every tenant, and it returns sums, never
// rows. A tenant user who names no tenant gets their own.
func overviewStatsHandler(g *gate) handlerFunc[StatsInput, Stats] {
	return func(ctx context.Context, in StatsInput, p dashcontract.Principal) (Stats, error) {
		s, err := services(g.sp)
		if err != nil {
			return Stats{}, err
		}

		if !s.Broker.Running() {
			return Stats{}, errBrokerDown()
		}

		if g.operator && in.TenantID == "" {
			return nodeStats(ctx, s)
		}

		raw, err := g.tenantFor(ctx, s, p, in.TenantID)
		if err != nil {
			return Stats{}, err
		}

		return tenantStats(ctx, s, raw)
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
