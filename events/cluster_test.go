package events_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xraph/keysmith"
	kmemory "github.com/xraph/keysmith/store/memory"
	"github.com/xraph/warden"
	wmemory "github.com/xraph/warden/store/memory"

	"github.com/xraph/tether/acl"
	"github.com/xraph/tether/auth"
	"github.com/xraph/tether/broker"
	"github.com/xraph/tether/cluster"
	"github.com/xraph/tether/cluster/clustertest"
	"github.com/xraph/tether/events"
	"github.com/xraph/tether/id"
	"github.com/xraph/tether/perm"
	"github.com/xraph/tether/scope"
	"github.com/xraph/tether/service"
	"github.com/xraph/tether/store/memory"
	"github.com/xraph/tether/tenant"
)

// node is one broker of a cell with its own keysmith and warden engines,
// authenticator, tenant service and control bus, the way a process of a
// cell has them. The stores are shared, as a cell shares its database.
type node struct {
	b       *broker.Broker
	addr    string
	tenants *service.Tenants
	devices *service.Devices
	roles   *perm.Roles
}

// cellEnv is a cell of two nodes over a three-server NATS cluster.
type cellEnv struct {
	nodes  []*node
	tenant *tenant.Tenant
	wd     *warden.Engine // node 0's, for creating roles
}

func newCellEnv(t *testing.T) *cellEnv {
	t.Helper()

	nc := clustertest.StartCluster(t, 3)
	urls := nc.URLs()

	st := memory.New()
	t.Cleanup(func() { _ = st.Close() })

	kst, wst := kmemory.New(), wmemory.New()

	tn, err := service.NewTenants(st).Create(t.Context(), service.CreateTenantInput{AppID: testApp, ExternalID: testOrg, Name: "Acme"})
	require.NoError(t, err)

	env := &cellEnv{tenant: tn}

	const nodes = 2

	brokers := make([]*broker.Broker, 0, nodes)
	ctls := make([]*cluster.Control, 0, nodes)
	conns := make([]*cluster.Conn, 0, nodes)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()

		for _, b := range brokers {
			_ = b.Stop(ctx)
		}

		for _, c := range ctls {
			c.Stop()
		}

		for _, c := range conns {
			c.Close()
		}
	})

	for i := range nodes {
		ccfg := cluster.DefaultConfig()
		ccfg.Enabled = true
		ccfg.NodeID = fmt.Sprintf("n%d", i+1)
		ccfg.Cell = "ev"
		ccfg.Mode = cluster.ModeExternal
		ccfg.URLs = []string{urls[i]}
		ccfg.Replicas = 3
		ccfg.NoDiscovery = true

		bcfg := broker.DefaultConfig()
		bcfg.TCPAddress = "127.0.0.1:0"

		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		conn, err := cluster.Connect(ctx, ccfg, slog.New(slog.DiscardHandler))

		if err == nil && i == 0 {
			err = cluster.Provision(ctx, conn, ccfg, bcfg.MaxSessionExpiry)
		}

		cancel()
		require.NoError(t, err)

		if i == 0 {
			// Every server can route a write to the cell's streams (the log,
			// the control bus and the three buckets) before a node uses
			// them: right after Provision a publish through a server their
			// interest has not reached yet gets no responders.
			nc.WaitInterest(t, "d.x", "ctl.x",
				"$KV."+ccfg.SessionsBucket()+".x", "$KV."+ccfg.RetainedBucket()+".x", "$KV."+ccfg.MembersBucket()+".x")
		}

		conns = append(conns, conn)

		ks := events.NewKeysmithPlugin()
		km, err := keysmith.NewEngine(keysmith.WithStore(kst), keysmith.WithExtension(ks))
		require.NoError(t, err)

		ks.MarkWired()

		wp := events.NewWardenPlugin(nil)
		wd, err := warden.NewEngine(warden.WithStore(wst), warden.WithPlugin(wp))
		require.NoError(t, err)

		tenants := service.NewTenants(st)
		authn := auth.NewKeysmith(km, tenants, st)

		// The order the extension wires it in: the tenant eviction first,
		// then the broker's handlers (New), then the reactor's, then Start.
		ctl := cluster.NewControl(conn, ccfg)
		ctls = append(ctls, ctl)

		require.NoError(t, events.EvictTenantsOnUpdate(ctl, func(tenantID string) {
			if tid, perr := id.ParseTenantID(tenantID); perr == nil {
				tenants.Evict(tid)
			}
		}))

		b, err := broker.New(bcfg, broker.Deps{
			Auth:    authn,
			Perms:   perm.NewLoader(wd),
			Cluster: &broker.Cluster{Conn: conn, Config: ccfg, Control: ctl},
		})
		require.NoError(t, err)

		reactor, err := events.NewClusterReactor(authn.Evict, b, ctl, nil)
		require.NoError(t, err)

		ks.Bind(reactor)
		wp.Bind(reactor)

		require.NoError(t, ctl.Start(t.Context()))
		require.NoError(t, b.Start(t.Context()))

		brokers = append(brokers, b)

		env.nodes = append(env.nodes, &node{
			b:       b,
			addr:    b.Addr().String(),
			tenants: tenants,
			devices: service.NewDevices(st, tenants, km, service.NewWardenRoles(wd)),
			roles:   perm.NewRoles(wd),
		})

		if i == 0 {
			env.wd = wd
		}
	}

	return env
}

// role creates a role through node 0's warden engine.
func (c *cellEnv) role(t *testing.T, slug string, rules acl.Rules) {
	t.Helper()

	(&env{wd: c.wd, wctx: scope.With(t.Context(), testApp, testOrg)}).role(t, slug, rules)
}

// provision makes a device through node 0.
func (c *cellEnv) provision(t *testing.T, roles ...string) *service.Credentials {
	t.Helper()

	creds, err := c.nodes[0].devices.Provision(t.Context(), c.tenant.ID, service.ProvisionInput{Name: "device", Roles: roles})
	require.NoError(t, err)

	return creds
}

// A device connected to node B is disconnected within 2 s when its key is
// revoked in node A's process, and B's validation cache no longer serves
// the key.
func TestRevokeDisconnectsOnEveryNode(t *testing.T) {
	c := newCellEnv(t)
	c.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#"}})
	creds := c.provision(t, "dev")

	onB := mustConnect(t, c.nodes[1].addr, "dev-1", creds)
	require.Equal(t, []byte{1}, subscribe(t, onB, "cmd/#"))

	require.NoError(t, c.nodes[0].devices.Revoke(t.Context(), c.tenant.ID, creds.Device.ID, "stolen"))

	assert.Equal(t, byte(0x98), disconnectedWithin(t, onB, 2*time.Second), "administrative action")

	_, ca, err := connect(t, c.nodes[1].addr, "dev-1", creds.Username, creds.Password)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.Equal(t, byte(0x86), ca.ReasonCode, "B's validation cache dropped the key")
}

// A role narrowed through node A's warden engine prunes the subscriptions
// of a device connected to node B.
func TestRoleChangePrunesOnEveryNode(t *testing.T) {
	c := newCellEnv(t)
	c.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#", "ok/#"}})
	creds := c.provision(t, "dev")

	b := c.nodes[1].b
	onB := mustConnect(t, c.nodes[1].addr, "dev-1", creds)
	require.Equal(t, []byte{1, 1}, subscribe(t, onB, "cmd/#", "ok/#"))
	require.Equal(t, 2, b.Stats(c.tenant.ID.String()).Subscriptions)

	require.NoError(t, c.nodes[0].roles.SetRules(t.Context(), c.tenant, "dev", acl.Rules{Subscribe: []string{"ok/#"}}))

	require.Eventually(t, func() bool { return b.Stats(c.tenant.ID.String()).Subscriptions == 1 }, waitTimeout, 10*time.Millisecond,
		"cmd/# is pruned on B")
	assert.Equal(t, []byte{0x87}, subscribe(t, onB, "cmd/#"), "and B refuses it now")
	assertConnected(t, onB)
}

// Disabling a tenant through node A (as the admin API does: the tenant
// service, then the broker) disconnects its devices on node B, and B's
// tenant cache no longer lets them back in.
func TestTenantDisableAcrossNodes(t *testing.T) {
	c := newCellEnv(t)
	c.role(t, "dev", acl.Rules{Subscribe: []string{"cmd/#"}})
	creds := c.provision(t, "dev")

	onB := mustConnect(t, c.nodes[1].addr, "dev-1", creds)

	off := false
	updated, err := c.nodes[0].tenants.Update(t.Context(), c.tenant.ID, service.UpdateTenantInput{Enabled: &off})
	require.NoError(t, err)

	c.nodes[0].b.UpdateTenant(updated)

	assert.Equal(t, byte(0x98), disconnectedWithin(t, onB, 2*time.Second), "administrative action")

	_, ca, err := connect(t, c.nodes[1].addr, "dev-1", creds.Username, creds.Password)
	require.Error(t, err)
	require.NotNil(t, ca)
	assert.GreaterOrEqual(t, ca.ReasonCode, byte(0x80), "B's tenant cache dropped the enabled tenant")
}
