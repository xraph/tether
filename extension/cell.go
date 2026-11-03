package extension

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xraph/forge"

	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/events"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store"
	"github.com/xraph/tether/tenant"
)

// provisionWait bounds Provision when Start's context has no deadline. A
// fresh embedded cell places its replicated streams only once its peers have
// joined, so it gets as long as Connect.
const provisionWait = 30 * time.Second

// cell is what a node runs beside the broker when it is part of a cell: the
// NATS connection and the services the extension makes and stops itself. The
// broker makes its own Router and Shared on the connection and closes them
// in its Stop.
type cell struct {
	conn    *cluster.Conn
	members *cluster.Members
	control *cluster.Control
	deps    *broker.Cluster
}

// openCell connects to the cell the config names (starting the embedded
// server in embedded mode), provisions its streams and buckets with
// cluster.log_max_age as the log's MaxAge, and makes the services the broker
// runs on. The tenant cache's eviction goes on the control bus first, before
// the broker's handlers, so it runs first. It returns nil without a cell.
// Nothing it makes is started yet: Start starts the bus before the broker and
// Members after it.
func (e *Extension) openCell(ctx context.Context, st store.Store, tenants *service.Tenants, log *slog.Logger) (*cell, error) {
	cfg := e.config.Cluster
	if !cfg.Enabled {
		return nil, nil //nolint:nilnil // no cell is not an error
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("tether: cluster config: %w", err)
	}

	conn, err := cluster.Connect(ctx, cfg, log)
	if err != nil {
		return nil, fmt.Errorf("tether: join the cell: %w", err)
	}

	pctx, cancel := ctx, context.CancelFunc(func() {})
	if _, ok := ctx.Deadline(); !ok {
		pctx, cancel = context.WithTimeout(ctx, provisionWait)
	}

	err = cluster.Provision(pctx, conn, cfg, cfg.LogAge())

	cancel()

	if err != nil {
		conn.Close()

		return nil, fmt.Errorf("tether: provision the cell: %w", err)
	}

	e.warnLogAge(ctx, st)

	c := &cell{
		conn:    conn,
		members: cluster.NewMembers(conn, cfg),
		control: cluster.NewControl(conn, cfg),
	}

	c.deps = &broker.Cluster{
		Conn:     conn,
		Config:   cfg,
		Sessions: cluster.NewSessions(conn, cfg),
		Log:      cluster.NewLog(conn, cfg),
		Retained: cluster.NewRetained(conn, cfg),
		Load:     cluster.NewLoad(conn, cfg),
		Members:  c.members,
		Control:  c.control,
	}

	err = events.EvictTenantsOnUpdate(c.control, func(tenantID string) {
		if tid, perr := id.ParseTenantID(tenantID); perr == nil {
			tenants.Evict(tid)
		}
	})
	if err != nil {
		c.close()

		return nil, fmt.Errorf("tether: control bus: %w", err)
	}

	return c, nil
}

// start starts the cell's side once the broker runs: Members, so the
// broker's fence and dead-peer callbacks find a running broker.
func (c *cell) start(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if err := c.members.Start(ctx); err != nil {
		return fmt.Errorf("tether: join the cell's members: %w", err)
	}

	return nil
}

// close stops what the cell runs, in the order a stop needs after the
// broker's Stop: Members first (while it heartbeats, the cell takes this node
// for alive; once it stops, the peers declare the node dead after the lease
// and publish the delayed wills it held), then the control bus, then the
// connection, which also shuts the embedded server down. Safe on nil and on
// a cell that never started.
func (c *cell) close() {
	if c == nil {
		return
	}

	c.members.Stop()
	c.control.Stop()
	c.conn.Close()
}

// controlBus is the cell's control bus, nil without a cell.
func (c *cell) controlBus() *cluster.Control {
	if c == nil {
		return nil
	}

	return c.control
}

// brokerDeps is the broker's Deps.Cluster, nil without a cell.
func (c *cell) brokerDeps() *broker.Cluster {
	if c == nil {
		return nil
	}

	return c.deps
}

// warnLogAge logs a Warn when a session may outlive the log (ruling R44): an
// offline session keeps its subscriptions for its expiry, but its messages
// only for cluster.log_max_age. The broker's cap bounds every tenant's, so
// nothing is said when it is within the log's age; otherwise the cap is named,
// and so is every tenant whose own cap goes past the log's age too. Tenants
// changed later are not checked here; the broker's cap still bounds them.
func (e *Extension) warnLogAge(ctx context.Context, st store.Store) {
	age := e.config.Cluster.LogAge()

	brokerCap := e.config.Broker.MaxSessionExpiry
	if brokerCap == 0 {
		brokerCap = broker.DefaultMaxSessionExpiry
	}

	if brokerCap <= age {
		return
	}

	e.Logger().Warn("tether: broker.max_session_expiry is longer than cluster.log_max_age; "+
		"an offline session away longer than the log's age loses the messages past it",
		forge.F("max_session_expiry", brokerCap.String()), forge.F("log_max_age", age.String()))

	ts, err := st.ListTenants(ctx, tenant.ListFilter{})
	if err != nil {
		e.Logger().Warn("tether: could not list tenants to check their session expiry", forge.F("error", err.Error()))

		return
	}

	for _, t := range ts {
		if own := t.Limits.MaxSessionExpiry; own > age {
			e.Logger().Warn("tether: a tenant's max_session_expiry is longer than cluster.log_max_age",
				forge.F("tenant", t.ID.String()), forge.F("max_session_expiry", min(own, brokerCap).String()),
				forge.F("log_max_age", age.String()))
		}
	}
}
